"""Tests for correlation ID resolution rules."""

from __future__ import annotations

from app.correlation import (
    MAX_ID_LENGTH,
    generate_id,
    is_valid_id,
    resolve_id,
)


def test_valid_external_id_is_reused_verbatim():
    for external in ("req-123", "abc", "A" * MAX_ID_LENGTH, "a.b/c:d"):
        resolved, reused = resolve_id(external)
        assert reused is True
        assert resolved == external


def test_missing_or_invalid_id_is_replaced():
    invalid = [None, "", "   ", "has space", "with\ttab", "x" * (MAX_ID_LENGTH + 1)]
    for external in invalid:
        resolved, reused = resolve_id(external)
        assert reused is False, external
        assert resolved != external
        assert is_valid_id(resolved)


def test_generated_ids_are_unique_and_well_formed():
    ids = {generate_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(is_valid_id(i) for i in ids)


def test_resolution_is_deterministic_per_input():
    # Same external input always yields the same decision.
    assert resolve_id("fixed-id") == ("fixed-id", True)
    assert resolve_id("fixed-id") == ("fixed-id", True)
    first, reused = resolve_id("bad id")
    assert reused is False
