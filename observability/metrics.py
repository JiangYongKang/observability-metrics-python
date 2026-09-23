"""请求级指标采集：按 route/method/status 维度计数与时延分布。

语义与不变量：
- 每个请求 ``record`` 恰好落入一个 (route, method, status) 序列，
  且在同一把锁内同时递增该序列计数与全局总数，因此
  ``sum(各序列计数) == total_requests`` 在并发下恒成立（计数守恒）。
- 路由标签归一化：匹配到路由时使用路由模板（如 ``/users/{user_id}``），
  未匹配路由统一归并为 ``"unmatched"``，杜绝任意路径造成的标签爆炸。
- 时延使用显式上界桶（累积直方图），同时保留总和与样本数。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

# 时延分布桶上界（毫秒），最后一个桶等价于 +Inf。
DEFAULT_BUCKETS_MS: tuple[float, ...] = (
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
)

# 未匹配任何路由的请求统一使用的归一化标签。
UNMATCHED_ROUTE = "unmatched"


@dataclass(frozen=True)
class Labels:
    route: str
    method: str
    status: int


@dataclass
class Series:
    """单个标签组合的计数与时延直方图。"""

    labels: Labels
    count: int = 0
    sum_ms: float = 0.0
    # 累积桶计数：bucket_counts[i] 为时延 <= bounds[i] 的请求数。
    bucket_counts: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.bucket_counts:
            self.bucket_counts = [0 for _ in DEFAULT_BUCKETS_MS]


@dataclass
class RequestRecord:
    route: str = ""
    method: str = ""
    status: int = 0
    duration_ms: float = 0.0
    request_id: str = ""


class MetricsRegistry:
    """线程安全的请求指标注册表。"""

    def __init__(self, buckets_ms: tuple[float, ...] = DEFAULT_BUCKETS_MS) -> None:
        self._lock = threading.Lock()
        self._total = 0
        self._series: dict[Labels, Series] = {}
        self._buckets = tuple(buckets_ms)

    @staticmethod
    def normalize_route(path: str, route_template: str | None) -> str:
        """归一化路由标签。

        - 匹配成功（``route_template`` 非空）：直接使用模板，路径参数天然收敛；
        - 未匹配：无论原始路径是什么，统一归并为 :data:`UNMATCHED_ROUTE`。
        """
        if route_template:
            return route_template
        return UNMATCHED_ROUTE

    def record(self, record: RequestRecord) -> None:
        """登记一次请求。成功、失败、异常路径都必须调用，不允许静默丢失。"""
        labels = Labels(
            route=record.route,
            method=record.method.upper(),
            status=int(record.status),
        )
        with self._lock:
            series = self._series.get(labels)
            if series is None:
                series = Series(labels=labels)
                # 允许自定义桶的测试场景：重建与桶数一致的计数数组。
                if len(series.bucket_counts) != len(self._buckets):
                    series.bucket_counts = [0 for _ in self._buckets]
                self._series[labels] = series
            series.count += 1
            series.sum_ms += float(record.duration_ms)
            for i, bound in enumerate(self._buckets):
                if record.duration_ms <= bound:
                    series.bucket_counts[i] += 1
            # 总数与序列计数在同一临界区内一起递增，保证守恒。
            self._total += 1

    def total_requests(self) -> int:
        with self._lock:
            return self._total

    def series_count_sum(self) -> int:
        """所有维度序列的计数总和，用于断言守恒。"""
        with self._lock:
            return sum(s.count for s in self._series.values())

    def snapshot(self) -> dict:
        """导出只读快照，供断言、展示与告警侧读取。"""
        with self._lock:
            series_out = []
            for labels, s in self._series.items():
                buckets = {
                    ("%.3f" % bound): count
                    for bound, count in zip(self._buckets, s.bucket_counts)
                }
                buckets["+Inf"] = s.count
                series_out.append(
                    {
                        "route": labels.route,
                        "method": labels.method,
                        "status": labels.status,
                        "count": s.count,
                        "sum_ms": round(s.sum_ms, 3),
                        "avg_ms": round(s.sum_ms / s.count, 3) if s.count else 0.0,
                        "buckets": buckets,
                    }
                )
            series_out.sort(key=lambda x: (x["route"], x["method"], x["status"]))
            return {"total_requests": self._total, "series": series_out}
