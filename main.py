"""Demo service wiring metrics, alerting and correlation together."""

from __future__ import annotations

import asyncio

from fastapi import FastAPI

from app.alerting import AlertConfig, AlertManager
from app.logging import configure_logging
from app.metrics import MetricsRegistry
from app.middleware import ObservabilityMiddleware

configure_logging()


def create_app(
    slow_request_seconds: float = 0.5,
    window_seconds: float = 60.0,
    threshold: float = 0.2,
    cooldown_seconds: float = 30.0,
) -> FastAPI:
    """Build the service with its observability stack attached.

    Returns the FastAPI app; the metrics registry and alert manager are
    reachable as ``app.state.registry`` / ``app.state.alerts``.
    """
    registry = MetricsRegistry()
    alerts = AlertManager(
        [
            AlertConfig(
                name="slow_request_rate",
                window_seconds=window_seconds,
                threshold=threshold,
                cooldown_seconds=cooldown_seconds,
            ),
            AlertConfig(
                name="error_rate",
                window_seconds=window_seconds,
                threshold=threshold,
                cooldown_seconds=cooldown_seconds,
            ),
        ]
    )

    app = FastAPI()
    app.state.registry = registry
    app.state.alerts = alerts

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.get("/items/{item_id}")
    def read_item(item_id: int):
        return {"item_id": item_id}

    @app.get("/slow")
    async def slow():
        await asyncio.sleep(slow_request_seconds + 0.1)
        return {"slow": True}

    @app.get("/boom")
    def boom():
        raise RuntimeError("deliberate failure")

    @app.get("/metrics")
    def metrics_snapshot():
        snap = registry.snapshot()
        return {
            "total_requests": registry.total_requests(),
            "series": [
                {
                    "route": route,
                    "method": method,
                    "status_code": status,
                    "count": m.count,
                    "latency_sum": m.latency_sum,
                    "latency_buckets": m.latency_buckets,
                }
                for (route, method, status), m in sorted(snap.items())
            ],
        }

    @app.get("/alerts")
    def alert_states():
        return {name: state.value for name, state in alerts.states().items()}

    # Middleware is added last so it sees the final route table.
    app.add_middleware(
        ObservabilityMiddleware,
        registry=registry,
        alerts=alerts,
        route_paths={r.path for r in app.routes},
        slow_request_seconds=slow_request_seconds,
    )
    return app


app = create_app()
