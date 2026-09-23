"""请求关联标识与结构化日志。

规则：
- 每个请求在进入时获得唯一关联 ID（``X-Request-ID``）：
  * 请求头携带合法 ID 时沿用；
  * 缺失、为空、非法（非 ASCII / 含空白或控制字符 / 超长）时忽略外部值，
    重新生成，不抛错——冲突与非法值按"丢弃并生成"的确定规则处理；
  * 多值或逗号拼接的外部 ID 只取第一个非空片段，仍非法则重新生成。
- ID 存入 :class:`contextvars.ContextVar`，结构化日志、异常处理、
  业务代码通过 :func:`get_request_id` 共享同一标识。
- 响应头始终回写最终生效的 ID，成功与失败响应一致。
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import time
import uuid
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

# 外部请求 ID 的确定性约束。
MAX_REQUEST_ID_LEN = 128
_REQUEST_ID_HEADER = b"x-request-id"

_request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default=""
)


def get_request_id() -> str:
    """获取当前上下文的请求 ID；无请求上下文时返回空串。"""
    return _request_id_var.get()


def new_request_id() -> str:
    return uuid.uuid4().hex


def is_valid_request_id(value: str) -> bool:
    """合法：非空、长度受限、全部为可打印 ASCII 且无空白。"""
    if not value or len(value) > MAX_REQUEST_ID_LEN:
        return False
    if any(ch.isspace() for ch in value):
        return False
    return all(33 <= ord(ch) <= 126 for ch in value)


def resolve_request_id(raw: str | None) -> tuple[str, bool]:
    """解析外部传入的 ID。

    返回 ``(request_id, reused)``：``reused=False`` 表示外部值非法被丢弃、
    已重新生成（冲突的确定处理）。
    """
    if raw:
        # 逗号拼接/多值：取第一个非空片段，去除首尾空白。
        candidate = raw.split(",", 1)[0].strip()
        if is_valid_request_id(candidate):
            return candidate, True
    return new_request_id(), False


class StructuredLogger:
    """行分隔 JSON 的结构化日志器，每条日志绑定 request_id。"""

    def __init__(self, name: str = "observability.request", stream: Any = None) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self._bound: dict[str, Any] = {}

    def bind(self, **fields: Any) -> "StructuredLogger":
        child = StructuredLogger(stream=self._stream)
        child._bound = {**self._bound, **fields}
        return child

    def _emit(self, level: str, event: str, fields: dict[str, Any]) -> None:
        payload: dict[str, Any] = {"level": level, "event": event}
        payload.update(self._bound)
        payload.update(fields)
        # 保证 request_id 字段存在（即使未绑定也不缺字段）。
        payload.setdefault("request_id", get_request_id())
        self._stream.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        self._stream.flush()

    def info(self, event: str, **fields: Any) -> None:
        self._emit("info", event, fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._emit("warning", event, fields)

    def error(self, event: str, **fields: Any) -> None:
        self._emit("error", event, fields)


# 默认日志实例，供业务直接使用；测试可替换或捕获 stderr。
logger = StructuredLogger()


def _header_get(scope: Scope, name: bytes) -> str | None:
    for k, v in scope.get("headers", []):
        if k == name:
            try:
                return v.decode("ascii")
            except UnicodeDecodeError:
                return None
    return None


class RequestContextMiddleware:
    """纯 ASGI 中间件：解析/生成请求 ID，写入上下文并回写响应头。

    只负责关联标识；指标采集由 metrics 中间件负责，二者读取同一上下文。
    """

    def __init__(self, app: ASGIApp, log: StructuredLogger | None = None) -> None:
        self.app = app
        self.log = log or logger

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        raw = _header_get(scope, _REQUEST_ID_HEADER)
        request_id, reused = resolve_request_id(raw)
        token = _request_id_var.set(request_id)
        started = time.perf_counter()
        response_started = False
        self.log.info(
            "request.start",
            method=scope.get("method", ""),
            path=scope.get("path", ""),
            request_id_reused=reused,
        )
        if not reused and raw:
            self.log.warning(
                "request.id_invalid",
                detail="external request id rejected; generated a new one",
            )

        def _with_request_id(headers: list[tuple[bytes, bytes]]) -> list[tuple[bytes, bytes]]:
            # 幂等覆盖，确保回写的是最终生效 ID。
            cleaned = [(k, v) for k, v in headers if k != _REQUEST_ID_HEADER]
            cleaned.append((_REQUEST_ID_HEADER, request_id.encode("ascii")))
            return cleaned

        async def send_wrapper(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                message["headers"] = _with_request_id(message.get("headers", []))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self.log.error(
                "request.exception",
                elapsed_ms=round(elapsed_ms, 3),
                error_type="unhandled_exception",
            )
            if not response_started:
                # 响应尚未开始：由本层发送带关联 ID 的 500，保证失败响应
                # 也携带同一标识；随后再抛出，交由外层做异常传播。
                await send(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        "headers": _with_request_id(
                            [(b"content-type", b"text/plain; charset=utf-8")]
                        ),
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": b"Internal Server Error",
                    }
                )
            raise
        finally:
            _request_id_var.reset(token)
