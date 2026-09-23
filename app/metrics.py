"""Request-level metrics collection with label normalization."""

from __future__ import annotations

import bisect
import re
import threading
from dataclasses import dataclass, field

#: Label used for routes that did not match any registered route.
UNMATCHED_ROUTE = "__unmatched__"

#: Latency histogram bucket upper bounds in seconds (last bucket is +Inf).
LATENCY_BUCKETS: tuple[float, ...] = (0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)

_TEMPLATE_SEGMENT = re.compile(r"\{[^/]+\}")


@dataclass
class RouteMetrics:
    """Metrics for a single (route, method, status_code) label set."""

    count: int = 0
    latency_buckets: list[int] = field(
        default_factory=lambda: [0] * (len(LATENCY_BUCKETS) + 1)
    )
    latency_sum: float = 0.0


def _compile_template(template: str) -> re.Pattern[str]:
    # Split literals from {placeholders}, escape only the literals so
    # special chars in fixed parts cannot leak into the pattern.
    literals = _TEMPLATE_SEGMENT.split(template)
    pattern = "[^/]+".join(re.escape(part) for part in literals)
    return re.compile(r"\A" + pattern + r"\Z")


def _literal_segment_count(template: str) -> int:
    return sum(1 for seg in template.split("/") if seg and "{" not in seg)


def normalize_route(path: str, route_paths: set[str]) -> str:
    """Normalize a request path to a route template label.

    Matching rules (deterministic):
    * an exact path match wins;
    * otherwise templates are tried ordered by literal-segment count
      (most specific) then lexicographically, first full match wins;
    * if nothing matches, :data:`UNMATCHED_ROUTE` is returned, so
      arbitrary paths never inflate the label cardinality.
    """
    if path in route_paths:
        return path
    candidates = sorted(
        (t for t in route_paths if "{" in t),
        key=lambda t: (-_literal_segment_count(t), t),
    )
    for template in candidates:
        if _compile_template(template).match(path):
            return template
    return UNMATCHED_ROUTE


def latency_bucket_index(latency_seconds: float) -> int:
    """Return the histogram bucket index for a latency observation."""
    if latency_seconds < 0:
        latency_seconds = 0.0
    return bisect.bisect_right(LATENCY_BUCKETS, latency_seconds)


class MetricsRegistry:
    """Thread-safe registry of request counts and latency histograms.

    Every request increments exactly one label set, hence the sum of
    all series counts always equals the number of recorded requests.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[tuple[str, str, int], RouteMetrics] = {}

    def record(
        self, route: str, method: str, status_code: int, latency_seconds: float
    ) -> None:
        """Record one request observation under exactly one label set."""
        with self._lock:
            series = self._data.setdefault(
                (route, method, status_code), RouteMetrics()
            )
            series.count += 1
            series.latency_buckets[latency_bucket_index(latency_seconds)] += 1
            series.latency_sum += latency_seconds

    def snapshot(self) -> dict[tuple[str, str, int], RouteMetrics]:
        """Return a consistent deep copy of all metrics."""
        with self._lock:
            return {
                key: RouteMetrics(
                    count=value.count,
                    latency_buckets=list(value.latency_buckets),
                    latency_sum=value.latency_sum,
                )
                for key, value in self._data.items()
            }

    def total_requests(self) -> int:
        """Total number of recorded requests across all label sets."""
        with self._lock:
            return sum(value.count for value in self._data.values())
