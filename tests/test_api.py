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
