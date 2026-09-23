"""指标采集测试：并发守恒、路由归一化、时延桶、异常不丢指标。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from observability.metrics import (
    DEFAULT_BUCKETS_MS,
    MetricsRegistry,
    RequestRecord,
    UNMATCHED_ROUTE,
)


def test_normalize_route_uses_template_for_path_params():
    reg = MetricsRegistry()
    assert reg.normalize_route("/users/1", "/users/{user_id}") == "/users/{user_id}"
    assert reg.normalize_route("/users/999/items/a-b", "/users/{user_id}/items/{item}") == (
        "/users/{user_id}/items/{item}"
    )


@pytest.mark.parametrize("path", ["/nope", "/a/b/c", "/users/1/../../etc", "/%%random%%"])
def test_normalize_route_unmatched_is_single_label(path):
    # 任意未匹配路径都归并到同一个标签，防止标签爆炸。
    assert MetricsRegistry.normalize_route(path, None) == UNMATCHED_ROUTE


def test_concurrent_counter_conservation():
    reg = MetricsRegistry()
    n_threads = 32
    per_thread = 200

    def worker(i):
        for j in range(per_thread):
            route = "/users/{user_id}" if j % 2 else UNMATCHED_ROUTE
            status = 200 if j % 3 else 500
            reg.record(
                RequestRecord(
                    route=route, method="get", status=status, duration_ms=float(j)
                )
            )

    with ThreadPoolExecutor(n_threads) as pool:
        list(pool.map(worker, range(n_threads)))

    total_expected = n_threads * per_thread
    # 核心不变量：各维度计数总和必须与请求总数守恒。
    assert reg.total_requests() == total_expected
    assert reg.series_count_sum() == total_expected
    snap = reg.snapshot()
    assert sum(s["count"] for s in snap["series"]) == total_expected
    # 标签空间有界：只有两种路由、两种状态。
    assert len(snap["series"]) == 4


def test_unmatched_paths_do_not_explode_labels():
    reg = MetricsRegistry()
    for i in range(500):
        reg.record(
            RequestRecord(route=UNMATCHED_ROUTE, method="GET", status=404, duration_ms=1.0)
        )
    unmatched = [s for s in reg.snapshot()["series"] if s["route"] == UNMATCHED_ROUTE]
    assert len(unmatched) == 1
    assert unmatched[0]["count"] == 500


def test_latency_buckets_are_cumulative():
    reg = MetricsRegistry()
    for ms in (1.0, 7.0, 7.0, 300.0):
        reg.record(RequestRecord(route="/", method="GET", status=200, duration_ms=ms))
    series = reg.snapshot()["series"][0]
    buckets = series["buckets"]
    assert buckets["+Inf"] == 4
    # 累积单调不减，最后一个有限桶 +Inf 语义自洽。
    values = [buckets["%.3f" % b] for b in DEFAULT_BUCKETS_MS]
    assert values == sorted(values)
    assert values[-1] == 4
    # 1ms 落入第一桶(<=5)，两个 7ms 落入 <=10，300ms 落入 <=500。
    assert buckets["5.000"] == 1
    assert buckets["10.000"] == 3
    assert buckets["500.000"] == 4
    assert series["avg_ms"] == 78.75


def test_exception_request_is_still_recorded():
    reg = MetricsRegistry()
    # 模拟异常路径：中间件兜底状态码 500，调用方必须照常 record。
    reg.record(RequestRecord(route="/fail", method="GET", status=500, duration_ms=2.0))
    assert reg.total_requests() == 1
    assert reg.snapshot()["series"][0]["status"] == 500
