"""Correlation ID generation, validation and request-scoped storage."""

from __future__ import annotations

import contextvars
import uuid

#: HTTP header carrying the correlation ID.
CORRELATION_HEADER = "X-Correlation-ID"

#: Maximum accepted length for an externally supplied correlation ID.
MAX_ID_LENGTH = 128

#: Accepted characters for an external ID: printable, no whitespace.
import re

_VALID_ID = re.compile(r"[!-~]+\Z")

_request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def is_valid_id(value: str) -> bool:
    """Return True when an external ID is safe to reuse.

    Valid IDs are non-empty, bounded in length and contain only
    printable ASCII without whitespace or control characters.
    """
    return 0 < len(value) <= MAX_ID_LENGTH and bool(_VALID_ID.fullmatch(value))


def generate_id() -> str:
    """Generate a fresh correlation ID (deterministic shape: uuid4 hex)."""
    return uuid.uuid4().hex


def resolve_id(external: str | None) -> tuple[str, bool]:
    """Resolve the correlation ID for a request.

    Returns ``(id, reused)``. A present, valid external ID is reused
    verbatim; a missing or invalid one is replaced by a freshly
    generated ID (``reused=False``), giving a single deterministic rule
    for missing, conflicting and malformed inputs alike.
    """
    if external is not None:
        candidate = external.strip()
        if is_valid_id(candidate):
            return candidate, True
    return generate_id(), False


def set_current(value: str) -> contextvars.Token:
    """Bind the correlation ID to the current context."""
    return _request_id.set(value)


def get_current() -> str | None:
    """Return the correlation ID bound to the current context, if any."""
    return _request_id.get()


def reset(token: contextvars.Token) -> None:
    """Restore the previous context binding."""
    _request_id.reset(token)
