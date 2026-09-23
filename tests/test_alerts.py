"""告警判定测试：窗口边界、阈值迟滞、冷却抑制、抖动、重启初始状态。

要求：每次判定都会打印输入与结论（caplog 中断言日志存在）。
"""

from __future__ import annotations

import logging

import pytest

from observability.alerts import (
    AlertEvent,
    AlertRule,
    AlertState,
    ErrorRateEvaluator,
    SlowRequestEvaluator,
)


@pytest.fixture
def caplog_alert(caplog):
    caplog.set_level(logging.INFO, logger="observability.alerts")
    return caplog


def rule(**overrides) -> AlertRule:
    defaults = dict(
        name="error_rate",
        window_ms=10_000,
        threshold=0.5,
        min_samples=2,
        cooldown_ms=3_000,
        recover_threshold=0.2,
    )
    defaults.update(overrides)
    return AlertRule(**defaults)


def test_initial_state_is_ok_after_restart():
    ev = ErrorRateEvaluator(rule())
    assert ev.state is AlertState.OK
    # 没有任何样本时不允许凭空调用。
    assert ev.observe(0, is_error=True) is None
    assert ev.state is AlertState.OK  # min_samples 未满足


def test_trigger_then_cooldown_suppresses_repeated_changes(caplog_alert):
    # 短窗口 + 长冷却：错误样本先出窗（理应恢复），但仍处在冷却期内。
    ev = ErrorRateEvaluator(
        rule(window_ms=500, min_samples=1, cooldown_ms=5_000)
    )

    e1 = ev.observe(0, is_error=True)
    assert isinstance(e1, AlertEvent) and e1.state is AlertState.FIRING
    fired_at = e1.timestamp_ms

    # t=600：错误样本已滑出 500ms 窗口，比例 0/1 < 恢复阈值；
    # 但距上次变化仅 600ms < 5000ms 冷却，解除必须被抑制。
    assert ev.observe(600, is_error=False) is None
    assert ev.state is AlertState.FIRING
    assert ev.in_cooldown(fired_at + ev.rule.cooldown_ms - 1)
    assert not ev.in_cooldown(fired_at + ev.rule.cooldown_ms)

    # 每次判定的输入与结论都出现在日志中。
    logs = caplog_alert.text
    assert "[alert-eval]" in logs
    assert "fired" in logs
    assert "suppressed_by_cooldown" in logs
    assert "input(now=" in logs and "ratio=" in logs


def test_recover_only_after_cooldown_and_below_threshold():
    ev = ErrorRateEvaluator(rule())
    ev.observe(0, is_error=True)
    ev.observe(10, is_error=True)
    assert ev.state is AlertState.FIRING

    # 冷却已过，但错误率仍高于恢复阈值 -> 保持 FIRING（迟滞）。
    assert ev.observe(4_000, is_error=False) is None
    assert ev.state is AlertState.FIRING

    # 让错误样本滑出窗口：t=11_000 时窗口 [1000,11000]，两个错误样本均出窗。
    e = ev.observe(11_000, is_error=False)
    assert e is not None and e.state is AlertState.OK
    assert e.reason.startswith("ratio=0.000")


def test_window_boundary_sample_is_kept():
    # 窗口 [now-window, now]：恰好在左边界上的样本保留。
    ev = ErrorRateEvaluator(rule(window_ms=10_000, min_samples=1, cooldown_ms=0))
    ev.observe(1_000, is_error=True)  # 触发
    assert ev.state is AlertState.FIRING
    # now=11_000，边界=1_000，t=1_000 的样本恰好保留，错误率仍为 1。
    assert ev.observe(11_000, is_error=False) is None
    assert ev.state is AlertState.FIRING
    # now=11_001，边界样本被淘汰，仅剩一个成功样本 -> 解除。
    e = ev.observe(11_001, is_error=False)
    assert e is not None and e.state is AlertState.OK


def test_short_jitter_does_not_flap(caplog_alert):
    # 无冷却的对照组：比例在阈值附近抖动会反复变化。
    no_cd = ErrorRateEvaluator(rule(cooldown_ms=0, recover_threshold=0.5))
    no_cd.observe(0, is_error=True)
    no_cd.observe(1, is_error=True)  # FIRING
    e = no_cd.observe(2, is_error=False)  # 比例 2/3≈0.67 仍 firing
    assert e is None
    e = no_cd.observe(3, is_error=False)  # 0.5，恢复需严格 < 0.5
    assert e is None
    e = no_cd.observe(4, is_error=False)  # 0.4 < 0.5 -> resolve
    assert e and e.state is AlertState.OK

    # 有冷却的判定器：恢复事件被抑制，不抖动。
    cooled = ErrorRateEvaluator(rule(cooldown_ms=10_000, recover_threshold=0.5))
    cooled.observe(0, is_error=True)
    cooled.observe(1, is_error=True)
    for t in (2, 3, 4, 5):
        assert cooled.observe(t, is_error=False) is None
    assert cooled.state is AlertState.FIRING
    assert "suppressed_by_cooldown" in caplog_alert.text


def test_slow_request_evaluator_uses_latency_threshold():
    r = AlertRule(
        name="slow",
        window_ms=1_000,
        threshold=0.5,
        min_samples=2,
        cooldown_ms=0,
        recover_threshold=0.5,
        match_threshold=100.0,
    )
    ev = SlowRequestEvaluator(r)
    ev.observe(0, value=150)
    ev.observe(10, value=200)
    assert ev.state is AlertState.FIRING
    # 99ms 不算慢。
    ev.observe(20, value=99)
    assert ev.state is AlertState.FIRING  # 2/3 仍 >= 0.5
    ev.observe(30, value=10)
    # 2/4=0.5，恢复需严格 < 0.5
    assert ev.state is AlertState.FIRING
    ev.observe(40, value=10)
    assert ev.state is AlertState.OK  # 2/5=0.4


def test_insufficient_samples_resolves_deterministically():
    ev = ErrorRateEvaluator(rule(min_samples=5, cooldown_ms=0))
    for t in range(5):
        ev.observe(t, is_error=True)
    assert ev.state is AlertState.FIRING
    # 所有错误样本滑出窗口 -> 样本不足 -> 确定解除。
    e = ev.observe(20_000, is_error=False)
    assert e is not None and e.state is AlertState.OK
    assert "min_samples" in e.reason
