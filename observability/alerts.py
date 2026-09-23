"""基于滑动窗口与阈值、冷却期的告警判定。

规则与不变量：
- 窗口：观测样本按时间戳保留最近 ``window_ms`` 毫秒的数据；
  判定边界为 ``now - window_ms``，恰好在边界上的样本保留（左闭右开）。
- 触发：窗口内样本数 >= ``min_samples`` 且指标比例 >= ``threshold``
  时进入 ``FIRING``；恢复：比例回落到 ``recover_threshold`` 以下才回到
  ``OK``（迟滞区间），避免临界值抖动反复触发。
- 冷却：状态发生一次变化后开始计时，``cooldown_ms`` 内不再产生任何状态
  变化事件；冷却结束后只允许下一次"真实变化"，因此同一状态在冷却期内
  至多变化一次。
- 重启：进程重启后没有任何历史样本，初始状态确定为 ``OK``，
  且不会凭空调用触发。
- 每次判定都会通过标准 ``logging`` 打印输入（窗口样本数、比例、阈值、
  当前状态、冷却状态）与结论（是否触发/解除/抑制）。
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from typing import Deque
from collections import deque

logger = logging.getLogger("observability.alerts")


class AlertState(enum.Enum):
    OK = "ok"
    FIRING = "firing"


@dataclass(frozen=True)
class AlertRule:
    name: str
    window_ms: float
    threshold: float
    min_samples: int = 1
    cooldown_ms: float = 0.0
    # 恢复阈值：低于该值才解除；缺省与触发阈值相同（无迟滞时仍有冷却保护）。
    recover_threshold: float | None = None
    # 样本分类阈值：如慢请求的时延毫秒数；错误率告警不使用。
    match_threshold: float | None = None

    def recovery_bound(self) -> float:
        return self.threshold if self.recover_threshold is None else self.recover_threshold


@dataclass(frozen=True)
class AlertEvent:
    rule: str
    state: AlertState
    timestamp_ms: float
    value: float
    samples: int
    reason: str = ""


@dataclass
class _Sample:
    timestamp_ms: float
    matched: bool  # 是否为异常/慢请求样本（计入分子）


class AlertEvaluator:
    """通用比例窗口告警判定器。

    子类只需决定每个样本是否计入分子（慢请求 / 错误）。
    """

    kind = "ratio"

    def __init__(
        self,
        rule: AlertRule,
        now_ms: float = 0.0,
        log: logging.Logger | None = None,
    ) -> None:
        self.rule = rule
        self._state = AlertState.OK  # 重启后确定的初始状态
        self._last_change_ms: float | None = None
        self._samples: Deque[_Sample] = deque()
        self._log = log or logger

    # ---- 子类钩子 -------------------------------------------------------
    def _classify(self, value: float, is_error: bool) -> bool:
        raise NotImplementedError

    @property
    def state(self) -> AlertState:
        return self._state

    def in_cooldown(self, now_ms: float) -> bool:
        if self._last_change_ms is None:
            return False
        return (now_ms - self._last_change_ms) < self.rule.cooldown_ms

    def _evict(self, now_ms: float) -> None:
        """淘汰落在窗口左边界之前的样本（边界样本保留）。"""
        floor = now_ms - self.rule.window_ms
        while self._samples and self._samples[0].timestamp_ms < floor:
            self._samples.popleft()

    def window_ratio(self, now_ms: float) -> tuple[int, float]:
        """返回 (窗口样本数, 命中比例)。"""
        self._evict(now_ms)
        n = len(self._samples)
        if n == 0:
            return 0, 0.0
        hits = sum(1 for s in self._samples if s.matched)
        return n, hits / n

    def observe(
        self,
        timestamp_ms: float,
        value: float = 0.0,
        is_error: bool = False,
    ) -> AlertEvent | None:
        """登记一个样本并进行一次判定；若产生状态变化返回事件，否则 None。"""
        self._samples.append(_Sample(timestamp_ms, self._classify(value, is_error)))

        # 以最新样本时间作为"当前时间"，窗口随数据推进。
        n, ratio = self.window_ratio(timestamp_ms)
        desired = self._state
        reason = ""
        enough = n >= self.rule.min_samples

        if self._state is AlertState.OK and enough and ratio >= self.rule.threshold:
            desired = AlertState.FIRING
            reason = f"ratio={ratio:.3f} >= threshold={self.rule.threshold:.3f}"
        elif (
            self._state is AlertState.FIRING
            and enough
            and ratio < self.rule.recovery_bound()
        ):
            desired = AlertState.OK
            reason = (
                f"ratio={ratio:.3f} < recover={self.rule.recovery_bound():.3f}"
            )
        elif self._state is AlertState.FIRING and not enough:
            # 样本不足以维持判定时同样解除（迟滞语义的确定性补集）。
            desired = AlertState.OK
            reason = f"samples={n} < min_samples={self.rule.min_samples}"

        conclusion = "hold"
        event = None
        if desired != self._state:
            if self.in_cooldown(timestamp_ms):
                conclusion = "suppressed_by_cooldown"
            else:
                self._state = desired
                self._last_change_ms = timestamp_ms
                conclusion = "fired" if desired is AlertState.FIRING else "resolved"
                event = AlertEvent(
                    rule=self.rule.name,
                    state=desired,
                    timestamp_ms=timestamp_ms,
                    value=ratio,
                    samples=n,
                    reason=reason,
                )

        self._log.info(
            "[alert-eval] rule=%s kind=%s input(now=%.1f samples=%d ratio=%.3f "
            "threshold=%.3f recover=%.3f min_samples=%d state=%s cooldown=%s) "
            "-> conclusion=%s %s",
            self.rule.name,
            self.kind,
            timestamp_ms,
            n,
            ratio,
            self.rule.threshold,
            self.rule.recovery_bound(),
            self.rule.min_samples,
            self._state.value,
            self.in_cooldown(timestamp_ms),
            conclusion,
            reason,
        )
        return event


class SlowRequestEvaluator(AlertEvaluator):
    """慢请求比例告警：``value`` 为请求时延（毫秒），超过阈值即命中。"""

    kind = "slow_request_ratio"

    def _classify(self, value: float, is_error: bool) -> bool:
        bound = (
            self.rule.match_threshold
            if self.rule.match_threshold is not None
            else self.rule.threshold
        )
        return value >= bound


class ErrorRateEvaluator(AlertEvaluator):
    """错误率告警：``is_error=True``（或 5xx）的样本计入分子。"""

    kind = "error_rate"

    def _classify(self, value: float, is_error: bool) -> bool:
        return bool(is_error) or value >= 500
