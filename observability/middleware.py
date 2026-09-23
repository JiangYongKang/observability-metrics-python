"""观测中间件：把请求生命周期与指标、告警、关联标识串联。

覆盖性保证（指标不丢）：
- 正常响应：从 ``http.response.start`` 捕获真实状态码；
- 404/未匹配路由：由 Starlette 的 ExceptionMiddleware 产生响应，
  依然经过 send 包装，状态码可观测，路由标签为 ``unmatched``；
- 业务未捕获异常：包装层兜底按 500 记录后再抛出，
  外层 ServerErrorMiddleware 负责生成 500 响应；
- ``finally`` 中以幂等守卫保证每个请求恰好记录一次，
  因此并发下指标总数 == 实际请求总数。
"""

from __future__ import annotations

import time
from typing import Callable

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .context import StructuredLogger, get_request_id, logger as default_logger
from .metrics import MetricsRegistry, RequestRecord
from .alerts import AlertEvaluator, AlertEvent


def default_clock_ms() -> float:
    return time.perf_counter() * 1000.0


class ObservabilityMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        registry: MetricsRegistry,
        slow_evaluator: AlertEvaluator | None = None,
        error_evaluator: AlertEvaluator | None = None,
        clock_ms: Callable[[], float] = default_clock_ms,
        log: StructuredLogger | None = None,
    ) -> None:
        self.app = app
        self.registry = registry
        self.slow = slow_evaluator
        self.errors = error_evaluator
        self.clock_ms = clock_ms
        self.log = log or default_logger

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_ms = self.clock_ms()
        status_code = 500  # 异常路径的兜底状态码
        recorded = False
        error_exc = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            error_exc = True
            raise
        finally:
            if not recorded:
                recorded = True
                self._record(scope, start_ms, status_code, error_exc)

    def _record(
        self, scope: Scope, start_ms: float, status_code: int, error_exc: bool
    ) -> None:
        now_ms = self.clock_ms()
        duration_ms = max(0.0, now_ms - start_ms)
        route = scope.get("route")
        template = getattr(route, "path", None)
        normalized = MetricsRegistry.normalize_route(
            scope.get("path", ""), template
        )
        record = RequestRecord(
            route=normalized,
            method=scope.get("method", "GET"),
            status=status_code,
            duration_ms=duration_ms,
            request_id=get_request_id(),
        )
        self.registry.record(record)

        events: list[AlertEvent] = []
        is_error = error_exc or status_code >= 500
        if self.slow is not None:
            event = self.slow.observe(now_ms, value=duration_ms, is_error=False)
            if event:
                events.append(event)
        if self.errors is not None:
            event = self.errors.observe(now_ms, value=float(status_code), is_error=is_error)
            if event:
                events.append(event)

        self.log.info(
            "request.complete",
            method=record.method,
            route=normalized,
            raw_path=scope.get("path", ""),
            status=status_code,
            duration_ms=round(duration_ms, 3),
            request_id=record.request_id,
            alerts=[
                {"rule": e.rule, "state": e.state.value, "reason": e.reason}
                for e in events
            ],
        )
