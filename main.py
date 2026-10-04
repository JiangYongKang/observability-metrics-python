"""Demo service wiring metrics, alerting and correlation together."""

from __future__ import annotations

import asyncio
import math

from fastapi import FastAPI, HTTPException, Query

from app.alerting import AlertConfig, AlertManager
from app.logging import configure_logging
from app.metrics import MetricsRegistry
from app.middleware import ObservabilityMiddleware

configure_logging()

#: Quantiles offered by the metrics snapshot (on-call triage set).
DEFAULT_QUANTILES = (0.5, 0.95, 0.99)


def create_app(
    slow_request_seconds: float = 0.5,
    window_seconds: float = 60.0,
    threshold: float = 0.2,
    cooldown_seconds: float = 30.0,
    min_samples: int = 1,
    fire_threshold: float | None = None,
    recover_threshold: float | None = None,
) -> FastAPI:
    """Build the service with its observability stack attached.

    Returns the FastAPI app; the metrics registry and alert manager are
    reachable as ``app.state.registry`` / ``app.state.alerts``.

    ``min_samples``, ``fire_threshold`` and ``recover_threshold`` are
    applied uniformly to every rule; when the dual thresholds are left
    unset both default to ``threshold`` (single-line, backward
    compatible behavior).
    """
    registry = MetricsRegistry()

    def make_config(name: str) -> AlertConfig:
        return AlertConfig(
            name=name,
            window_seconds=window_seconds,
            threshold=threshold,
            cooldown_seconds=cooldown_seconds,
            fire_threshold=fire_threshold,
            recover_threshold=recover_threshold,
            min_samples=min_samples,
        )

    alerts = AlertManager(
        [make_config("slow_request_rate"), make_config("error_rate")]
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
        quantiles = registry.quantile_snapshot(DEFAULT_QUANTILES)
        return {
            "total_requests": registry.total_requests(),
            "quantiles": list(DEFAULT_QUANTILES),
            "series": [
                {
                    "route": route,
                    "method": method,
                    "status_code": status,
                    "count": m.count,
                    "latency_sum": m.latency_sum,
                    "latency_buckets": m.latency_buckets,
                    "latency_quantiles": {
                        f"p{int(q * 100)}": _json_number(value)
                        for q, value in quantiles[(route, method, status)].items()
                    },
                }
                for (route, method, status), m in sorted(snap.items())
            ],
        }

    @app.get("/alerts")
    def alert_states():
        return {name: state.value for name, state in alerts.states().items()}

    @app.get("/alerts/{name}/history")
    def alert_history(
        name: str, limit: int = Query(default=50, ge=1, le=1024)
    ):
        """Read-only recent evaluation records for one alert.

        Records are returned newest-first, each carrying the evaluation
        input (timestamp, violated flag, window size and rule knobs) and
        the verdict (previous/current state, whether a transition
        happened, whether cooldown blocked it, whether too few samples
        made the verdict undecided). The snapshot is taken atomically
        with the state, so ``state`` always matches the newest record.
        """
        if name not in alerts.names():
            raise HTTPException(status_code=404, detail="unknown alert")
        state, records = alerts.state_and_history(name, limit=limit)
        return {
            "alert": name,
            "state": state.value,
            "count": len(records),
            "records": [_event_payload(event) for event in records],
        }

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


def _json_number(value: float | None):
    """Serialize a quantile for JSON: ``+Inf`` becomes ``null``."""
    if value is None:
        return None
    if math.isinf(value):
        return None
    return value


def _event_payload(event) -> dict:
    """JSON-safe view of one alert evaluation record."""
    return {
        "sequence": event.sequence,
        "timestamp": event.timestamp,
        "violated": event.violated,
        "value": event.value,
        "fire_threshold": event.fire_threshold,
        "recover_threshold": event.recover_threshold,
        "min_samples": event.min_samples,
        "window_size": event.window_size,
        "previous": event.previous.value,
        "current": event.current.value,
        "transitioned": event.transitioned,
        "suppressed_by_cooldown": event.suppressed_by_cooldown,
        "insufficient_samples": event.insufficient_samples,
    }
