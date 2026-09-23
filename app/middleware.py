"""ASGI middleware wiring correlation IDs, metrics and alerting together."""

from __future__ import annotations

import json
import time
from typing import Callable

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .correlation import CORRELATION_HEADER, reset, resolve_id, set_current
from .logging import log_event
from .alerting import AlertManager
from .metrics import MetricsRegistry, normalize_route


class ObservabilityMiddleware:
    """Pure-ASGI middleware.

    Per request it:
    * resolves the correlation ID (valid external header reused, any
      missing/invalid value replaced) and echoes it on the response;
    * times the call and records exactly one metrics series even when
      the application raises;
    * feeds the slow-request and error-rate alert rules;
    * emits one structured JSON log line carrying the correlation ID.
    """

    def __init__(
        self,
        app: ASGIApp,
        registry: MetricsRegistry,
        alerts: AlertManager,
        route_paths: set[str],
        slow_request_seconds: float = 0.5,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        self.app = app
        self.registry = registry
        self.alerts = alerts
        self.route_paths = route_paths
        self.slow_request_seconds = slow_request_seconds
        self.time_func = time_func

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = scope["path"]
        route = normalize_route(path, self.route_paths)
        correlation_id, reused = resolve_id(
            _first_header(scope.get("headers", []), CORRELATION_HEADER)
        )
        token = set_current(correlation_id)

        status_code = 500
        started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, started
            if message["type"] == "http.response.start":
                started = True
                status_code = message["status"]
                headers = list(message.get("headers", []))
                headers.append(
                    (CORRELATION_HEADER.lower().encode(), correlation_id.encode())
                )
                message = dict(message, headers=headers)
            await send(message)

        error_name: str | None = None
        started_at = self.time_func()
        try:
            try:
                await self.app(scope, receive, send_wrapper)
            except Exception as exc:  # safety net; normally handled upstream
                error_name = type(exc).__name__
                log_event(
                    "request_exception",
                    "unhandled exception during request",
                    correlation_id_reused=reused,
                    method=method,
                    path=path,
                    route=route,
                    error=error_name,
                    exc_info=exc,
                )
                if not started:
                    await self._send_error(send, correlation_id, exc)
                    status_code = 500
                # never re-raise silently: metrics below are always recorded
        finally:
            latency = self.time_func() - started_at
            self.registry.record(route, method, status_code, latency)
            now = time.time()
            self.alerts.observe(
                "slow_request_rate", now, latency >= self.slow_request_seconds
            )
            self.alerts.observe("error_rate", now, status_code >= 500)
            log_event(
                "request",
                "request completed",
                correlation_id_reused=reused,
                method=method,
                path=path,
                route=route,
                status=status_code,
                latency_seconds=round(latency, 6),
                error=error_name,
            )
            reset(token)

    async def _send_error(
        self, send: Send, correlation_id: str, exc: Exception
    ) -> None:
        body = json.dumps(
            {"detail": "internal error", "correlation_id": correlation_id}
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [
                    (b"content-type", b"application/json"),
                    (CORRELATION_HEADER.lower().encode(), correlation_id.encode()),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": body,
                "more_body": False,
            }
        )


def _first_header(raw_headers: list[tuple[bytes, bytes]], name: str) -> str | None:
    wanted = name.lower().encode()
    for key, value in raw_headers:
        if key == wanted:
            return value.decode("latin-1", errors="replace")
    return None
