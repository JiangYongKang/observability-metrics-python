"""End-to-end tests: concurrency conservation, exception path, correlation."""

from __future__ import annotations

import asyncio
import json
import logging

import httpx
import pytest

from app.correlation import CORRELATION_HEADER
from app.metrics import UNMATCHED_ROUTE
from main import create_app


def make_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


def test_concurrent_requests_conserve_total_counts():
    app = create_app()
    per_route = 50

    async def run() -> None:
        async with make_client(app) as client:
            tasks = []
            for i in range(per_route):
                tasks.append(client.get("/"))
                tasks.append(client.get(f"/items/{i}"))
                tasks.append(client.get("/boom"))
                tasks.append(client.get(f"/no/such/{i}"))
            responses = await asyncio.gather(*tasks)
            assert all(r.status_code in (200, 404, 500) for r in responses)

    asyncio.run(run())

    expected = per_route * 4
    registry = app.state.registry
    snap = registry.snapshot()
    assert registry.total_requests() == expected
    assert sum(m.count for m in snap.values()) == expected
    # Path params and unmatched paths collapsed onto bounded label sets.
    assert snap[("/items/{item_id}", "GET", 200)].count == per_route
    assert snap[("/boom", "GET", 500)].count == per_route
    assert snap[(UNMATCHED_ROUTE, "GET", 404)].count == per_route
    assert snap[("/", "GET", 200)].count == per_route


def test_exception_path_keeps_metrics_logs_and_correlation(caplog):
    app = create_app()

    async def run() -> httpx.Response:
        async with make_client(app) as client:
            return await client.get("/boom")

    with caplog.at_level(logging.INFO, logger="observability"):
        response = asyncio.run(run())

    assert response.status_code == 500
    header_id = response.headers[CORRELATION_HEADER]
    body_id = response.json()["correlation_id"]
    assert header_id == body_id

    # Metrics were recorded despite the exception.
    snap = app.state.registry.snapshot()
    assert snap[("/boom", "GET", 500)].count == 1
    assert app.state.registry.total_requests() == 1

    # Both the exception log and the completion log share the same ID.
    records = [
        json.loads(r.getMessage()) if False else r for r in caplog.records
    ]
    exception_logs = [r for r in records if r.message == "unhandled exception during request"]
    completion_logs = [r for r in records if r.message == "request completed"]
    assert len(exception_logs) == 1
    assert len(completion_logs) == 1
    assert exception_logs[0].extra_data["error"] == "RuntimeError"
    assert completion_logs[0].extra_data["status"] == 500
    assert completion_logs[0].extra_data["route"] == "/boom"


def test_correlation_id_shared_across_response_and_logs(caplog):
    app = create_app()

    async def run() -> httpx.Response:
        async with make_client(app) as client:
            return await client.get("/items/7", headers={CORRELATION_HEADER: "ext-1"})

    with caplog.at_level(logging.INFO, logger="observability"):
        response = asyncio.run(run())

    assert response.status_code == 200
    assert response.headers[CORRELATION_HEADER] == "ext-1"
    completion = [
        r for r in caplog.records if r.message == "request completed"
    ]
    assert len(completion) == 1
    assert completion[0].extra_data["correlation_id_reused"] is True


def test_invalid_external_id_is_replaced_deterministically():
    app = create_app()

    async def run() -> list[httpx.Response]:
        async with make_client(app) as client:
            return [
                await client.get("/", headers={CORRELATION_HEADER: "bad id"}),
                await client.get("/", headers={CORRELATION_HEADER: "bad id"}),
            ]

    responses = asyncio.run(run())
    ids = [r.headers[CORRELATION_HEADER] for r in responses]
    # Both invalid inputs are replaced by fresh, valid, distinct IDs.
    assert all(" " not in i and i for i in ids)
    assert ids[0] != "bad id" and ids[1] != "bad id"


def test_alert_states_endpoint_is_deterministic_initially():
    app = create_app()

    async def run() -> dict:
        async with make_client(app) as client:
            return (await client.get("/alerts")).json()

    states = asyncio.run(run())
    assert states == {"slow_request_rate": "ok", "error_rate": "ok"}


# ---------------------------------------------------------------------------
# Alert evaluation history endpoint
# ---------------------------------------------------------------------------


def test_alert_history_endpoint_reports_inputs_and_verdicts(caplog):
    app = create_app(min_samples=2, fire_threshold=0.8, recover_threshold=0.2)

    async def run() -> None:
        async with make_client(app) as client:
            await client.get("/boom")  # 1/1 violation but not enough samples
            await client.get("/boom")  # 2/2 -> fires

    with caplog.at_level(logging.INFO, logger="observability"):
        asyncio.run(run())

    async def query() -> dict:
        async with make_client(app) as client:
            return (await client.get("/alerts/error_rate/history?limit=10")).json()

    payload = asyncio.run(query())
    assert payload["alert"] == "error_rate"
    assert payload["state"] == "firing"
    records = payload["records"]
    assert len(records) == 2
    newest = records[0]
    assert newest["current"] == payload["state"]
    # Inputs of the decision.
    assert newest["violated"] is True
    assert newest["window_size"] == 2
    assert newest["min_samples"] == 2
    assert newest["fire_threshold"] == 0.8
    assert newest["recover_threshold"] == 0.2
    # Verdict of the decision.
    assert newest["transitioned"] is True
    assert newest["insufficient_samples"] is False
    assert records[1]["insufficient_samples"] is True
    assert records[1]["transitioned"] is False


def test_alert_history_unknown_alert_returns_404():
    app = create_app()

    async def run() -> int:
        async with make_client(app) as client:
            return (await client.get("/alerts/nope/history")).status_code

    assert asyncio.run(run()) == 404


def test_alert_history_is_read_only_and_consistent_with_state():
    app = create_app(min_samples=1, fire_threshold=0.1, recover_threshold=0.01)

    async def run() -> None:
        async with make_client(app) as client:
            for _ in range(3):
                await client.get("/boom")

    asyncio.run(run())
    alerts = app.state.alerts

    state, records = alerts.state_and_history("error_rate")
    assert records[0].current is state
    seqs = [r.sequence for r in records]
    assert seqs == sorted(seqs, reverse=True)
    assert len(seqs) == len(set(seqs))
    # A second read returns the same content (read-only snapshot).
    state2, records2 = alerts.state_and_history("error_rate")
    assert state is state2
    assert [r.sequence for r in records2] == seqs


# ---------------------------------------------------------------------------
# Percentiles in the metrics snapshot
# ---------------------------------------------------------------------------


def test_metrics_snapshot_exposes_approximate_percentiles():
    app = create_app()

    async def run() -> dict:
        async with make_client(app) as client:
            for _ in range(10):
                await client.get("/")
            return (await client.get("/metrics")).json()

    payload = asyncio.run(run())
    root = [s for s in payload["series"] if s["route"] == "/"][0]
    assert root["count"] == 10
    assert sum(root["latency_buckets"]) == 10
    q = root["latency_quantiles"]
    assert set(q) == {"p50", "p95", "p99"}
    assert q["p50"] is not None and q["p95"] is not None and q["p99"] is not None
    assert q["p50"] <= q["p95"] <= q["p99"]


def test_plus_inf_quantile_serializes_as_null():
    app = create_app()
    # /slow sleeps beyond the 5.0 largest finite bucket via monkeypatched
    # threshold is hard; instead verify via registry directly with huge data.
    registry = app.state.registry
    for _ in range(3):
        registry.record("/x", "GET", 200, 10_000.0)

    async def run() -> dict:
        async with make_client(app) as client:
            return (await client.get("/metrics")).json()

    series = [s for s in asyncio.run(run())["series"] if s["route"] == "/x"][0]
    assert series["latency_buckets"][-1] == 3
    # JSON cannot carry Infinity; the snapshot renders it as null.
    assert series["latency_quantiles"]["p50"] is None


# ---------------------------------------------------------------------------
# Every alert evaluation is traceable in the service's structured logs
# ---------------------------------------------------------------------------


def test_alert_evaluation_log_line_is_structured_json(caplog):
    app = create_app(min_samples=1, fire_threshold=0.5, recover_threshold=0.2)

    async def run() -> None:
        async with make_client(app) as client:
            await client.get("/boom")

    with caplog.at_level(logging.INFO, logger="observability"):
        asyncio.run(run())

    from app.logging import JsonLineFormatter

    formatter = JsonLineFormatter()
    lines = [
        json.loads(formatter.format(r))
        for r in caplog.records
        if r.message.startswith("alert evaluation")
    ]
    # Two rules evaluated per request, each with full inputs + verdict.
    by_alert = {line["alert"]: line for line in lines}
    for name in ("slow_request_rate", "error_rate"):
        line = by_alert[name]
        assert line["event"] == "alert_evaluation"
        for field in (
            "timestamp",
            "violated",
            "value",
            "window_size",
            "fire_threshold",
            "recover_threshold",
            "min_samples",
            "previous",
            "current",
            "transitioned",
            "suppressed_by_cooldown",
            "insufficient_samples",
        ):
            assert field in line, field
    assert by_alert["error_rate"]["current"] == "firing"
    assert by_alert["error_rate"]["transitioned"] is True
