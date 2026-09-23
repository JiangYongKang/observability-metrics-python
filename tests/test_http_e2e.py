"""HTTP 端到端：并发守恒、归一化、异常路径、链路 ID、告警全生命周期。"""

from __future__ import annotations

import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from fastapi.testclient import TestClient

from observability import AlertRule, AlertState
from tests.helpers import FakeClock, build_app


@pytest.fixture
def client_factory():
    apps = []

    def _factory(**kw):
        app, registry, slow_ev, err_ev = build_app(**kw)
        client = TestClient(app, raise_server_exceptions=False)
        apps.append(client)
        return client, registry, slow_ev, err_ev

    yield _factory
    for c in apps:
        c.close()


# ---------------------------------------------------------------- 并发守恒
def test_concurrent_requests_counter_conservation(client_factory):
    client, registry, _, _ = client_factory()
    total = 200

    def hit(i):
        if i % 5 == 0:
            return client.get("/fail").status_code
        if i % 7 == 0:
            return client.get(f"/users/{i}").status_code
        return client.get("/").status_code

    with ThreadPoolExecutor(16) as pool:
        statuses = list(pool.map(hit, range(total)))

    assert len(statuses) == total
    # 所有请求（含 500）都被记录，总量守恒。
    assert registry.total_requests() == total
    snap = registry.snapshot()
    assert sum(s["count"] for s in snap["series"]) == total
    assert sum(s["count"] for s in snap["series"] if s["status"] >= 500) == total // 5


def test_unmatched_routes_collapse_to_single_label(client_factory):
    client, registry, _, _ = client_factory()
    for i in range(50):
        assert client.get(f"/random/path/{i}").status_code == 404
    unmatched = [
        s for s in registry.snapshot()["series"] if s["route"] == "unmatched"
    ]
    assert len(unmatched) == 1 and unmatched[0]["count"] == 50


def test_path_params_are_normalized(client_factory):
    client, registry, _, _ = client_factory()
    for uid in (1, 2, 300):
        client.get(f"/users/{uid}")
    user_series = [
        s for s in registry.snapshot()["series"] if s["route"] == "/users/{user_id}"
    ]
    assert len(user_series) == 1 and user_series[0]["count"] == 3


# ---------------------------------------------------------------- 链路标识
def test_request_id_propagates_to_body_header_and_logs(client_factory):
    buf = io.StringIO()
    client, _, _, _ = client_factory(log_stream=buf)
    resp = client.get("/", headers={"X-Request-ID": "trace-e2e-1"})
    assert resp.status_code == 200
    assert resp.headers["x-request-id"] == "trace-e2e-1"
    assert resp.json()["request_id"] == "trace-e2e-1"

    logs = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert {log["event"] for log in logs} >= {"request.start", "request.complete"}
    assert all(log["request_id"] == "trace-e2e-1" for log in logs)


def test_invalid_external_id_is_replaced_and_warned(client_factory):
    buf = io.StringIO()
    client, _, _, _ = client_factory(log_stream=buf)
    resp = client.get("/", headers={"X-Request-ID": "illegal id!"})
    rid = resp.headers["x-request-id"]
    assert rid != "illegal id!" and len(rid) == 32
    logs = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert any(log["event"] == "request.id_invalid" for log in logs)
    assert all(log["request_id"] == rid for log in logs)


def test_exception_path_keeps_request_id_and_metrics(client_factory):
    buf = io.StringIO()
    client, registry, _, _ = client_factory(log_stream=buf)
    resp = client.get("/fail", headers={"X-Request-ID": "trace-fail-1"})
    assert resp.status_code == 500
    assert resp.headers["x-request-id"] == "trace-fail-1"

    snap = registry.snapshot()
    assert snap["total_requests"] == 1  # 异常不丢指标
    assert snap["series"][0]["status"] == 500
    logs = [json.loads(line) for line in buf.getvalue().splitlines()]
    events = {log["event"] for log in logs}
    assert {"request.start", "request.exception", "request.complete"} <= events
    assert all(log["request_id"] == "trace-fail-1" for log in logs)


# ---------------------------------------------------------------- 4xx 语义
def test_client_errors_are_recorded_but_not_server_errors(client_factory):
    client, registry, _, _ = client_factory()
    client.get("/missing/widget")
    s = registry.snapshot()["series"][0]
    assert s["status"] == 404 and s["route"] == "/missing/{thing}"


# ---------------------------------------------------------------- 告警全链路
def _error_rule():
    return AlertRule(
        name="error_rate",
        window_ms=10_000,
        threshold=0.5,
        min_samples=2,
        cooldown_ms=3_000,
        recover_threshold=0.2,
    )


def test_error_rate_alert_fires_and_resolves_over_http(client_factory):
    clock = FakeClock()
    client, registry, _, err_ev = client_factory(error_rule=_error_rule(), clock=clock)
    assert err_ev.state is AlertState.OK  # 重启初始状态确定

    # 连续 5xx：达到 min_samples 后触发。
    client.get("/fail"); clock.advance(10)
    fired = client.get("/fail")
    assert fired.status_code == 500
    assert err_ev.state is AlertState.FIRING

    # 冷却期内全部恢复正常，也不允许解除。
    for _ in range(3):
        clock.advance(100)
        client.get("/")
    assert err_ev.state is AlertState.FIRING

    # 时间快进到错误样本滑出窗口、冷却结束：下一次成功请求触发解除。
    clock.advance(10_000)
    client.get("/")
    assert err_ev.state is AlertState.OK

    # 全程指标不丢。
    snap = registry.snapshot()
    assert snap["total_requests"] == sum(s["count"] for s in snap["series"])


def test_slow_request_alert_uses_latency_over_http(client_factory):
    clock = FakeClock()
    rule = AlertRule(
        name="slow",
        window_ms=10_000,
        threshold=0.5,
        min_samples=2,
        cooldown_ms=1_000,
        recover_threshold=0.5,
        match_threshold=200.0,
    )
    client, registry, slow_ev, _ = client_factory(slow_rule=rule, clock=clock)
    client.get("/timed")   # 处理期间消耗 250ms 虚拟时间，>= 200ms 视为慢
    client.get("/timed")   # 2/2 慢 -> FIRING
    assert slow_ev.state is AlertState.FIRING
    assert registry.total_requests() == 2
