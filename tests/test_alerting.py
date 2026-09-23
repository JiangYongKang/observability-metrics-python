"""Tests for windowed threshold alerts: trigger, recovery, cooldown, eviction."""

from __future__ import annotations

import logging

import pytest

from app.alerting import AlertConfig, AlertManager, AlertState


def make_manager(cooldown: float = 5.0, threshold: float = 0.5) -> AlertManager:
    return AlertManager(
        [
            AlertConfig(
                "err",
                window_seconds=10.0,
                threshold=threshold,
                cooldown_seconds=cooldown,
            )
        ]
    )


def test_fires_when_threshold_crossed(caplog):
    manager = make_manager()
    with caplog.at_level(logging.INFO, logger="observability.alerting"):
        manager.observe("err", 1.0, False)
        manager.observe("err", 2.0, False)
        manager.observe("err", 3.0, True)
        fire_event = manager.observe("err", 4.0, True)

    assert fire_event.value == pytest.approx(0.5)
    assert fire_event.transitioned is True
    assert fire_event.previous is AlertState.OK
    assert fire_event.current is AlertState.FIRING
    assert manager.states()["err"] is AlertState.FIRING


def test_recovers_when_back_below_threshold_after_cooldown(caplog):
    manager = make_manager()
    with caplog.at_level(logging.INFO, logger="observability.alerting"):
        for ts in range(1, 7):  # six violations -> ratio 1.0, fires at ts=6
            manager.observe("err", float(ts), True)
        recover = manager.observe("err", 17.0, False)

    # At t=17 the boundary is t=7: all six violations (ts<=6) are evicted,
    # so value == 0; and 17-6 = 11 >= cooldown(5): recovery is applied.
    assert recover.value == 0.0
    assert recover.window_size == 1
    assert recover.transitioned is True
    assert recover.current is AlertState.OK
    assert manager.states()["err"] is AlertState.OK


def test_recovery_inside_cooldown_is_suppressed_then_applied_once(caplog):
    manager = make_manager()
    with caplog.at_level(logging.INFO, logger="observability.alerting"):
        manager.observe("err", 1.0, False)  # 0/1
        fire_at_2 = manager.observe("err", 2.0, True)  # 1/2 = 0.5 -> fires
        manager.observe("err", 3.0, True)  # 2/3
        manager.observe("err", 4.0, False)  # 2/4 = 0.5
        at5 = manager.observe("err", 5.0, False)  # 2/5 < 0.5: demand, in cooldown
        at6 = manager.observe("err", 6.0, False)  # 2/6: demand, in cooldown
        at7 = manager.observe("err", 7.0, False)  # 7-2=5: cooldown elapsed

    assert fire_at_2.transitioned is True
    for event in (at5, at6):
        assert event.suppressed_by_cooldown is True
        assert event.transitioned is False
        assert event.current is AlertState.FIRING
    assert at7.suppressed_by_cooldown is False
    assert at7.transitioned is True
    assert at7.current is AlertState.OK
    transitions = [e for e in manager.history("err") if e.transitioned]
    assert len(transitions) == 2  # one fire + one recovery


def test_single_transition_per_cooldown_period():
    manager = make_manager(cooldown=10.0, threshold=0.5)
    manager.observe("err", 1.0, True)
    manager.observe("err", 2.0, True)
    manager.observe("err", 3.0, True)  # fires
    manager.observe("err", 4.0, False)
    manager.observe("err", 5.0, False)
    manager.observe("err", 6.0, False)
    demand_at_7 = manager.observe("err", 7.0, False)  # 3/7 < 0.5, suppressed
    bounce_at_8 = manager.observe("err", 8.0, True)  # 4/8 = 0.5, no demand anyway
    demand_at_9 = manager.observe("err", 9.0, False)  # 4/9 < 0.5, suppressed

    assert demand_at_7.suppressed_by_cooldown is True
    assert bounce_at_8.current is AlertState.FIRING
    assert demand_at_9.suppressed_by_cooldown is True
    assert manager.states()["err"] is AlertState.FIRING
    transitions = [e for e in manager.history("err") if e.transitioned]
    assert len(transitions) == 1


def test_window_boundary_evicts_old_observations():
    manager = make_manager()
    for ts in range(1, 7):
        manager.observe("err", float(ts), True)
    assert manager.states()["err"] is AlertState.FIRING
    # At t=16 the boundary is t=6: observations at ts<=6 are all evicted.
    event = manager.observe("err", 16.0, False)
    assert event.window_size == 1
    assert event.value == 0.0
    # Recovery demand is past cooldown (16-6 >= 5): it transitions.
    assert event.current is AlertState.OK


def test_every_evaluation_logs_inputs_and_verdict(caplog):
    manager = make_manager()
    with caplog.at_level(logging.INFO, logger="observability.alerting"):
        manager.observe("err", 42.0, True)

    records = [r for r in caplog.records if r.message == "alert evaluation"]
    assert len(records) == 1
    record = records[0]
    # Inputs of the decision:
    assert record.alert == "err"
    assert record.timestamp == 42.0
    assert record.violated is True
    assert record.threshold == 0.5
    assert record.window_size == 1
    # Verdict of the decision:
    assert record.value == 1.0
    assert record.previous == "ok"
    assert record.current == "firing"
    assert record.transitioned is True
    assert record.suppressed_by_cooldown is False


def test_initial_state_is_deterministic_after_restart():
    first = make_manager()
    second = make_manager()
    assert first.states() == {"err": AlertState.OK}
    assert second.states() == {"err": AlertState.OK}
    assert second.history("err") == []
