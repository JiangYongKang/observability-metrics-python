"""Tests for route normalization and metrics conservation under concurrency."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

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


# ---------------------------------------------------------------------------
# Bucket boundary ownership: value == upper bound -> bucket named by bound
# ---------------------------------------------------------------------------


def test_boundary_value_falls_into_bucket_named_after_bound():
    from app.metrics import latency_bucket_index

    for index, bound in enumerate(LATENCY_BUCKETS):
        assert latency_bucket_index(bound) == index
    # Straddling points on either side of each finite bound.
    assert latency_bucket_index(0.004999) == 0
    assert latency_bucket_index(0.005001) == 1
    assert latency_bucket_index(0.009999) == 1
    assert latency_bucket_index(0.01) == 1
    # Zero and negative values belong to the first (0..0.005] bucket.
    assert latency_bucket_index(0.0) == 0
    assert latency_bucket_index(-1.0) == 0
    # Anything strictly above the largest finite bound goes to +Inf.
    assert latency_bucket_index(LATENCY_BUCKETS[-1] + 1e-9) == len(LATENCY_BUCKETS)
    assert latency_bucket_index(float("inf")) == len(LATENCY_BUCKETS)


def test_boundary_records_conserved_per_bucket():
    registry = MetricsRegistry()
    # One observation exactly on every bound, one below, one huge.
    for value in (*LATENCY_BUCKETS, 0.0, 9_999.0):
        registry.record("/", "GET", 200, value)
    series = registry.snapshot()[("/", "GET", 200)]
    assert series.count == len(LATENCY_BUCKETS) + 2
    # Each finite bound owns one observation, zero adds another to bucket 0,
    # and the huge value lands in +Inf: sums stay conserved.
    assert series.latency_buckets[0] == 2  # 0.0 plus the 0.005 boundary
    for index in range(1, len(LATENCY_BUCKETS)):
        assert series.latency_buckets[index] == 1
    assert series.latency_buckets[-1] == 1  # +Inf
    assert sum(series.latency_buckets) == series.count


def test_bulk_observations_keep_buckets_conserved():
    import random

    from app.metrics import latency_bucket_index

    registry = MetricsRegistry()
    random.seed(7)
    values = (
        [0.0] * 200
        + [b for b in LATENCY_BUCKETS for _ in range(50)]
        + [10_000.0] * 100
        + [random.random() * 10.0 for _ in range(1000)]
    )
    for value in values:
        registry.record("/", "GET", 200, value)
    series = registry.snapshot()[("/", "GET", 200)]
    assert series.count == len(values)
    assert sum(series.latency_buckets) == series.count
    assert registry.total_requests() == series.count
    # Independently recount from the raw values using the boundary rule.
    expected = [0] * (len(LATENCY_BUCKETS) + 1)
    for value in values:
        expected[latency_bucket_index(value)] += 1
    assert series.latency_buckets == expected


# ---------------------------------------------------------------------------
# Histogram quantile approximation
# ---------------------------------------------------------------------------


def test_quantile_is_none_without_observations():
    from app.metrics import histogram_quantile

    assert histogram_quantile([0] * (len(LATENCY_BUCKETS) + 1), 0.5) is None
    registry = MetricsRegistry()
    assert registry.series_quantile("/", "GET", 200, 0.95) is None


def test_quantile_interpolates_within_located_bucket():
    import math

    from app.metrics import histogram_quantile

    n = len(LATENCY_BUCKETS) + 1
    buckets = [0] * n
    # 100 observations, all exactly at the 0.01 upper bound (bucket 1).
    buckets[1] = 100
    # rank for p50 is 50; bucket 1 spans (0.005, 0.01]; 49 before, frac 0.5.
    assert histogram_quantile(buckets, 0.5) == pytest.approx(0.0075)
    assert histogram_quantile(buckets, 0.0) == pytest.approx(0.005)
    assert histogram_quantile(buckets, 1.0) == pytest.approx(0.01)

    # Distribution across two buckets: 4 below/on 0.005, 6 in (0.005, 0.01].
    buckets = [4, 6] + [0] * (n - 2)
    # p90 rank 9 lands in bucket 1 with fraction (9 - 4)/6.
    assert histogram_quantile(buckets, 0.9) == pytest.approx(
        0.005 + 0.005 * (5 / 6)
    )

    # Quantile beyond the largest finite bucket: +Inf, documented as inf.
    buckets = [0] * (n - 1) + [5]
    assert histogram_quantile(buckets, 0.99) is math.inf


def test_quantile_invalid_argument():
    from app.metrics import histogram_quantile

    with pytest.raises(ValueError):
        histogram_quantile([1], -0.1)
    with pytest.raises(ValueError):
        histogram_quantile([1], 1.1)


def test_registry_quantile_snapshot_is_consistent():
    registry = MetricsRegistry()
    for _ in range(10):
        registry.record("/", "GET", 200, 0.003)
    for _ in range(5):
        registry.record("/", "GET", 500, 3.0)
    snapshot = registry.quantile_snapshot((0.5, 0.95))
    ok = snapshot[("/", "GET", 200)]
    assert ok[0.5] == pytest.approx(0.005 * 0.5)
    assert ok[0.95] == pytest.approx(0.005 * 0.95)
    errors = snapshot[("/", "GET", 500)]
    # 3.0 sits in (1.0, 5.0] bucket: bounds check only.
    assert 1.0 <= errors[0.5] <= 5.0
