"""测试用应用工厂：独立注册表/告警器/可控时钟，避免用例间状态污染。"""

from __future__ import annotations

import threading
import time

from fastapi import FastAPI, HTTPException

from observability import (
    AlertRule,
    ErrorRateEvaluator,
    MetricsRegistry,
    ObservabilityMiddleware,
    RequestContextMiddleware,
    SlowRequestEvaluator,
)
from observability.context import StructuredLogger


class FakeClock:
    """线程安全的可控毫秒时钟：可快进，也可按固定步长自增。"""

    def __init__(self, start_ms: float = 1_000.0) -> None:
        self._lock = threading.Lock()
        self._now = float(start_ms)

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, ms: float) -> float:
        with self._lock:
            self._now += ms
            return self._now


def build_app(log_stream=None, slow_rule=None, error_rule=None, clock=None):
    app = FastAPI()
    registry = MetricsRegistry()
    log = StructuredLogger(stream=log_stream) if log_stream is not None else None

    slow_ev = SlowRequestEvaluator(slow_rule, log=None) if slow_rule else None
    err_ev = ErrorRateEvaluator(error_rule, log=None) if error_rule else None

    app.add_middleware(
        ObservabilityMiddleware,
        registry=registry,
        slow_evaluator=slow_ev,
        error_evaluator=err_ev,
        clock_ms=clock or (lambda: time.perf_counter() * 1000.0),
        log=log,
    )
    app.add_middleware(RequestContextMiddleware, log=log)

    @app.get("/")
    def root():
        from observability import get_request_id

        return {"request_id": get_request_id()}

    @app.get("/timed")
    def timed():
        # 请求处理期间消耗虚拟时间，供慢请求告警测试使用。
        clock.advance(250)
        from observability import get_request_id

        return {"request_id": get_request_id()}

    @app.get("/users/{user_id}")
    def read_user(user_id: int):
        from observability import get_request_id

        return {"user_id": user_id, "request_id": get_request_id()}

    @app.get("/fail")
    def fail():
        raise RuntimeError("boom")

    @app.get("/missing/{thing}")
    def missing(thing: str):
        raise HTTPException(status_code=404, detail="not found")

    return app, registry, slow_ev, err_ev
