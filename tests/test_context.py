"""请求关联标识与结构化日志测试。"""

from __future__ import annotations

import io
import json

import pytest

from observability.context import (
    StructuredLogger,
    get_request_id,
    is_valid_request_id,
    new_request_id,
    resolve_request_id,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("abc-123_XYZ", True),
        ("", False),
        ("has space", False),
        ("tab\tid", False),
        ("x" * 129, False),
        ("x" * 128, True),
        ("中文id", False),
        ("with\nnewline", False),
    ],
)
def test_request_id_validation(raw, expected):
    assert is_valid_request_id(raw) is expected


def test_missing_id_is_generated():
    rid, reused = resolve_request_id(None)
    assert reused is False and len(rid) == 32
    rid2, _ = resolve_request_id("")
    assert rid2 != rid


def test_valid_id_is_reused():
    rid, reused = resolve_request_id("trace-001")
    assert (rid, reused) == ("trace-001", True)


def test_conflict_multi_value_takes_first_valid_fragment():
    rid, reused = resolve_request_id("trace-001, trace-002")
    assert (rid, reused) == ("trace-001", True)


def test_invalid_first_fragment_falls_back_to_generated():
    rid, reused = resolve_request_id("bad id, trace-002")
    assert reused is False and rid != "trace-002"  # 不回退到第二片段，确定地重新生成


def test_structured_log_is_json_with_request_id():
    from observability.context import _request_id_var

    buf = io.StringIO()
    token = _request_id_var.set("req-xyz")
    try:
        log = StructuredLogger(stream=buf)
        log.info("demo.event", status=200, ok=True)
        log.error("demo.error", error_type="RuntimeError")
    finally:
        _request_id_var.reset(token)

    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert [line["event"] for line in lines] == ["demo.event", "demo.error"]
    assert all(line["request_id"] == "req-xyz" for line in lines)
    assert lines[0]["status"] == 200 and lines[1]["level"] == "error"


def test_bind_does_not_mutate_parent():
    buf = io.StringIO()
    parent = StructuredLogger(stream=buf)
    child = parent.bind(route="/users/{id}")
    child.info("child.event")
    parent.info("parent.event")
    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert lines[0]["route"] == "/users/{id}"
    assert "route" not in lines[1]
    assert get_request_id() == ""  # 无请求上下文时确定为空串


def test_new_request_id_is_unique():
    ids = {new_request_id() for _ in range(1000)}
    assert len(ids) == 1000
