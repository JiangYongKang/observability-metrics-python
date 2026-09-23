"""Structured logging that always carries the correlation ID."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from .correlation import get_current


class JsonLineFormatter(logging.Formatter):
    """Emit one JSON object per log record, including the correlation ID."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_current(),
        }
        extra = getattr(record, "extra_data", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Install the JSON formatter on the root logger exactly once."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLineFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(name: str, message: str, **fields: Any) -> None:
    """Emit a structured event under the observability logger."""
    logging.getLogger("observability").info(
        message, extra={"extra_data": {"event": name, **fields}}
    )
