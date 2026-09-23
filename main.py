"""示例 FastAPI 服务：挂载观测中间件并暴露指标快照。

本地运行：``uv run fastapi dev main.py`` 或 ``uv run uvicorn main:app``。
"""

from __future__ import annotations

import time

from fastapi import FastAPI, HTTPException

from observability import (
    AlertRule,
    ErrorRateEvaluator,
    MetricsRegistry,
    ObservabilityMiddleware,
    RequestContextMiddleware,
    SlowRequestEvaluator,
    get_request_id,
)

# ---- 告警规则（窗口/阈值/冷却，语义见 README 与 alerts 模块文档） --------
# 时钟使用 perf_counter（毫秒），与中间件记录时延的时钟一致。
slow_rule = AlertRule(
    name="slow_request_rate",
    window_ms=10_000,
    threshold=0.5,          # 窗口内慢请求占比 >= 50% 触发
    min_samples=5,
    cooldown_ms=5_000,      # 触发后 5 秒内不再发生状态变化
    recover_threshold=0.25, # 占比 < 25% 才解除（迟滞）
    match_threshold=200.0,  # 单请求时延 >= 200ms 视为慢请求
)
error_rule = AlertRule(
    name="error_rate",
    window_ms=10_000,
    threshold=0.5,
    min_samples=5,
    cooldown_ms=5_000,
    recover_threshold=0.25,
)

registry = MetricsRegistry()
slow_evaluator = SlowRequestEvaluator(slow_rule)
error_evaluator = ErrorRateEvaluator(error_rule)

app = FastAPI(title="observability-metrics-python")
# 中间件执行顺序（洋葱模型）：先加入的在外层。
# ObservabilityMiddleware 在最外层，保证异常也能记录；
# RequestContextMiddleware 在内层，负责生成/沿用 request_id。
app.add_middleware(
    ObservabilityMiddleware,
    registry=registry,
    slow_evaluator=slow_evaluator,
    error_evaluator=error_evaluator,
)
app.add_middleware(RequestContextMiddleware)


@app.get("/")
def read_root():
    return {"Hello": "World", "request_id": get_request_id()}


@app.get("/users/{user_id}")
def read_user(user_id: int):
    # 路径参数被归一化为 /users/{user_id}，不会按具体 ID 打标签。
    return {"user_id": user_id, "request_id": get_request_id()}


@app.get("/slow")
def slow_endpoint(ms: int = 250):
    time.sleep(ms / 1000.0)
    return {"slept_ms": ms, "request_id": get_request_id()}


@app.get("/fail")
def fail_endpoint():
    # 业务异常（500）：指标、告警、结构化日志都不缺失。
    raise RuntimeError("boom: an expected failure for demo")


@app.get("/missing/{thing}")
def missing(thing: str):
    # 显式 404，用于演示错误率不把 4xx 计入分子。
    raise HTTPException(status_code=404, detail=f"{thing} not found")


@app.get("/metrics")
def metrics_snapshot():
    return {
        "metrics": registry.snapshot(),
        "alerts": {
            slow_rule.name: slow_evaluator.state.value,
            error_rule.name: error_evaluator.state.value,
        },
    }
