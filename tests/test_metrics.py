"""Tests for route normalization and metrics conservation under concurrency."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from app.metrics import (
    LATENCY_BUCKETS,
    MetricsRegistry,
    UNMATCHED_ROUTE,
    normalize_route,
)

ROUTES = {
    "/",
    "/items/{item_id}",
    "/items/{item_id}/sub/{sub_id}",
    "/users/{user_id}",
    "/static/x.json",
}


def test_path_params_normalize_to_template():
    assert normalize_route("/items/1", ROUTES) == "/items/{item_id}"
    assert normalize_route("/items/abc-9", ROUTES) == "/items/{item_id}"
    assert (
        normalize_route("/items/7/sub/x", ROUTES)
        == "/items/{item_id}/sub/{sub_id}"
    )
    assert normalize_route("/users/42", ROUTES) == "/users/{user_id}"


def test_exact_literal_route_wins():
    assert normalize_route("/static/x.json", ROUTES) == "/static/x.json"
    assert normalize_route("/", ROUTES) == "/"


def test_unmatched_paths_share_one_label():
    for path in ("/nope", "/a/b/c/d", "/items/1/extra", "/../etc"):
        assert normalize_route(path, ROUTES) == UNMATCHED_ROUTE


def test_histogram_buckets_cover_each_observation():
    registry = MetricsRegistry()
    registry.record("/", "GET", 200, 0.0)
    registry.record("/", "GET", 200, 0.03)
    registry.record("/", "GET", 200, 99.0)
    snap = registry.snapshot()
    series = snap[("/", "GET", 200)]
    assert series.count == 3
    assert sum(series.latency_buckets) == 3
    assert len(series.latency_buckets) == len(LATENCY_BUCKETS) + 1


def test_concurrent_counts_are_conserved():
    registry = MetricsRegistry()
    per_thread = 500
    threads = 8

    def worker(worker_id: int) -> None:
        for i in range(per_thread):
            route = normalize_route(f"/items/{i}", ROUTES)
            status = 200 if worker_id % 2 else 500
            registry.record(route, "GET", status, 0.007)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(worker, range(threads)))

    expected = per_thread * threads
    snap = registry.snapshot()
    assert registry.total_requests() == expected
    assert sum(series.count for series in snap.values()) == expected
    assert sum(series.latency_buckets[1] for series in snap.values()) == expected
    # All path values collapsed onto one template label set per status.
    assert all(route == "/items/{item_id}" for route, _, _ in snap)
