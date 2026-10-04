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

    records = [
        r for r in caplog.records if r.message.startswith("alert evaluation")
    ]
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


# ---------------------------------------------------------------------------
# Minimum-sample gate
# ---------------------------------------------------------------------------


def make_hysteresis_manager(
    fire: float = 0.8,
    recover: float = 0.2,
    min_samples: int = 1,
    cooldown: float = 0.0,
    name: str = "err",
) -> AlertManager:
    return AlertManager(
        [
            AlertConfig(
                name,
                window_seconds=100.0,
                fire_threshold=fire,
                recover_threshold=recover,
                min_samples=min_samples,
                cooldown_seconds=cooldown,
            )
        ]
    )


def test_verdict_is_undecided_until_min_samples_accumulated():
    manager = make_hysteresis_manager(fire=0.8, min_samples=5)
    events = []
    for ts in range(1, 5):  # four all-violating samples: ratio 1.0, but too few
        events.append(manager.observe("err", float(ts), True))

    assert all(e.insufficient_samples for e in events)
    assert all(e.current is AlertState.OK for e in events)
    assert manager.states()["err"] is AlertState.OK

    fifth = manager.observe("err", 5.0, True)  # 5/5 >= 0.8 -> now decided
    assert fifth.insufficient_samples is False
    assert fifth.transitioned is True
    assert fifth.current is AlertState.FIRING


def test_insufficient_samples_neither_transitions_nor_counts_as_suppression():
    # Cooldown must not be "spent" while the verdict is undecided: after
    # enough samples arrive the (long passed) cooldown never blocks.
    manager = make_hysteresis_manager(
        fire=0.5, recover=0.5, min_samples=3, cooldown=10.0
    )
    first = manager.observe("err", 1.0, True)
    second = manager.observe("err", 2.0, True)
    assert first.suppressed_by_cooldown is False
    assert second.suppressed_by_cooldown is False
    third = manager.observe("err", 3.0, True)  # decided for the first time
    assert third.transitioned is True
    assert third.current is AlertState.FIRING


# ---------------------------------------------------------------------------
# Dual thresholds with a hysteresis band
# ---------------------------------------------------------------------------


def test_hysteresis_middle_band_holds_whichever_state_is_current():
    manager = make_hysteresis_manager(fire=0.8, recover=0.2)
    # Build the ratio upwards while staying in OK:
    # 0/1, 1/2=0.5 (band), 2/3, 2/4=0.5 (band), 3/5=0.6 (band)...
    for ts, violated, expected in (
        (1.0, False, 0.0),
        (2.0, True, 0.5),
        (3.0, True, 2 / 3),
        (4.0, False, 0.5),
        (5.0, True, 0.6),
    ):
        event = manager.observe("err", ts, violated)
        assert event.value == pytest.approx(expected)
        assert event.current is AlertState.OK
        assert event.transitioned is False

    fired = manager.observe("err", 6.0, True)  # 4/6 still below 0.8
    assert fired.current is AlertState.OK
    manager.observe("err", 7.0, True)  # 5/7 still below 0.8
    fired = manager.observe("err", 8.0, True)  # 6/8 = 0.75 still below
    assert fired.current is AlertState.OK
    fired = manager.observe("err", 9.0, True)  # 7/9 still below 0.8
    assert fired.current is AlertState.OK
    fired = manager.observe("err", 10.0, True)  # 8/10 = 0.8 -> fires
    assert fired.transitioned is True
    assert fired.current is AlertState.FIRING

    # Walk the ratio back down: ratios on/above the recovery line hold
    # the state; 0.2 exactly is NOT recovered (strictly-below rule).
    for ts, violated, expected in (
        (11.0, False, 8 / 11),   # ~0.727, still in the band
        (12.0, False, 8 / 12),   # 0.667, still in the band
    ):
        event = manager.observe("err", ts, violated)
        assert event.value == pytest.approx(expected)
        assert event.current is AlertState.FIRING

    # Push down to the recovery line: 8 violations over a growing window.
    # At t=40: 8/40 = 0.2 exactly -> still firing.
    for ts in range(13, 41):
        event = manager.observe("err", float(ts), False)
    assert event.value == pytest.approx(0.2)
    assert event.current is AlertState.FIRING

    recovered = manager.observe("err", 41.0, False)  # 8/41 < 0.2
    assert recovered.value < 0.2
    assert recovered.transitioned is True
    assert recovered.current is AlertState.OK


def test_fire_needs_high_line_recover_needs_low_line():
    manager = make_hysteresis_manager(fire=0.75, recover=0.25, min_samples=1)
    manager.observe("err", 1.0, False)
    manager.observe("err", 2.0, False)
    manager.observe("err", 3.0, True)  # 1/3 below 0.75: still ok
    assert manager.states()["err"] is AlertState.OK
    fired = manager.observe("err", 4.0, True)  # 2/4 = 0.5: still ok
    assert fired.current is AlertState.OK
    fired = manager.observe("err", 5.0, True)  # 3/5 = 0.6: still ok
    assert fired.current is AlertState.OK
    fired = manager.observe("err", 6.0, True)  # 4/6: still ok
    assert fired.current is AlertState.OK
    manager.observe("err", 7.0, True)  # 5/7: still ok
    fired = manager.observe("err", 8.0, True)  # 6/8 = 0.75: fires
    assert fired.transitioned is True
    assert fired.current is AlertState.FIRING


def test_thresholds_are_configurable_per_rule():
    manager = AlertManager(
        [
            AlertConfig(
                "strict",
                window_seconds=100.0,
                fire_threshold=0.2,
                recover_threshold=0.1,
                min_samples=1,
            ),
            AlertConfig(
                "loose",
                window_seconds=100.0,
                fire_threshold=0.9,
                recover_threshold=0.1,
                min_samples=1,
            ),
        ]
    )
    manager.observe("strict", 1.0, False)
    manager.observe("loose", 1.0, False)
    manager.observe("strict", 2.0, True)  # 1/2 = 0.5: strict fires, loose untouched
    manager.observe("loose", 2.0, True)  # 1/2 = 0.5: below 0.9
    assert manager.states()["strict"] is AlertState.FIRING
    assert manager.states()["loose"] is AlertState.OK


def test_invalid_threshold_relations_are_rejected():
    from app.alerting import WindowedAlert

    def construct(**kwargs):
        WindowedAlert(AlertConfig("bad", **{**{"window_seconds": 10.0}, **kwargs}))

    with pytest.raises(ValueError):
        construct(fire_threshold=0.2, recover_threshold=0.5)  # recover above fire
    with pytest.raises(ValueError):
        construct(fire_threshold=0.0)
    with pytest.raises(ValueError):
        construct(fire_threshold=2.0)
    with pytest.raises(ValueError):
        construct(fire_threshold=0.5, min_samples=0)
    with pytest.raises(ValueError):
        construct(fire_threshold=0.5, window_seconds=0)


def test_single_threshold_still_backward_compatible():
    manager = make_manager(cooldown=0.0, threshold=0.5)
    manager.observe("err", 1.0, False)
    fired = manager.observe("err", 2.0, True)  # 0.5 on the single line
    assert fired.transitioned is True
    recovered = manager.observe("err", 3.0, False)  # 1/3 < 0.5
    assert recovered.transitioned is True
    assert recovered.current is AlertState.OK


# ---------------------------------------------------------------------------
# Traceability: structured logs + read-only, concurrency-safe history
# ---------------------------------------------------------------------------


def test_structured_log_carries_all_inputs_and_verdict_fields(caplog):
    manager = make_hysteresis_manager(fire=0.8, recover=0.2, min_samples=2)
    with caplog.at_level(logging.INFO, logger="observability.alerting"):
        manager.observe("err", 9.0, True)

    record = [
        r for r in caplog.records if r.message.startswith("alert evaluation")
    ][0]
    structured = record.extra_data
    # Inputs.
    assert structured["event"] == "alert_evaluation"
    assert structured["alert"] == "err"
    assert structured["timestamp"] == 9.0
    assert structured["violated"] is True
    assert structured["window_size"] == 1
    assert structured["min_samples"] == 2
    assert structured["fire_threshold"] == 0.8
    assert structured["recover_threshold"] == 0.2
    # Verdict.
    assert structured["value"] == 1.0
    assert structured["previous"] == "ok"
    assert structured["current"] == "ok"
    assert structured["transitioned"] is False
    assert structured["suppressed_by_cooldown"] is False
    assert structured["insufficient_samples"] is True

    # The service's own JSON line output contains the fields directly.
    from app.logging import JsonLineFormatter

    line = JsonLineFormatter().format(record)
    assert '"event": "alert_evaluation"' in line
    assert '"insufficient_samples": true' in line
    assert '"fire_threshold": 0.8' in line


def test_history_snapshot_state_matches_newest_record_newest_first():
    manager = make_hysteresis_manager(fire=0.8, recover=0.2, min_samples=1)
    for ts in range(1, 5):
        manager.observe("err", float(ts), True)
    state, records = manager.state_and_history("err")
    assert state is AlertState.FIRING
    assert records[0].current is state  # newest record agrees with state
    assert [r.sequence for r in records] == [3, 2, 1, 0]
    # Read-only: mutating the returned list must not affect later reads.
    records.clear()
    assert len(manager.state_and_history("err")[1]) == 4


def test_history_limit_returns_only_most_recent():
    manager = make_manager()
    for ts in range(10):
        manager.observe("err", float(ts), False)
    _, records = manager.state_and_history("err", limit=3)
    assert [r.sequence for r in records] == [9, 8, 7]


def test_history_is_bounded():
    manager = AlertManager(
        [AlertConfig("err", window_seconds=100.0, threshold=0.5)],
        history_size=4,
    )
    for ts in range(10):
        manager.observe("err", float(ts), False)
    _, records = manager.state_and_history("err")
    assert [r.sequence for r in records] == [9, 8, 7, 6]


def test_history_snapshot_is_consistent_under_concurrent_reads():
    from concurrent.futures import ThreadPoolExecutor
    from itertools import count

    total = 2000
    manager = AlertManager(
        [AlertConfig("err", window_seconds=10000.0, threshold=0.5)],
        history_size=1024,
    )
    ticks = count(1)

    def writer(_: int) -> None:
        ts = float(next(ticks))
        manager.observe("err", ts, ts % 2 == 0)

    errors: list[str] = []

    def reader(_: int) -> None:
        state, records = manager.state_and_history("err")
        if records and records[0].current is not state:
            errors.append("state disagrees with newest record")
        seqs = [r.sequence for r in records]
        if len(seqs) != len(set(seqs)):
            errors.append("duplicate sequence numbers")
        if seqs != sorted(seqs, reverse=True):
            errors.append("history not newest-first")
        if len(seqs) > 1:
            if any(a - b != 1 for a, b in zip(seqs, seqs[1:])):
                errors.append("gap in history")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(writer, range(total)))
        list(pool.map(reader, range(500)))

    assert errors == []
    state, records = manager.state_and_history("err")
    assert len(records) == 1024  # bounded, oldest evicted, none lost within cap
    assert records[0].sequence == total - 1
    assert state is records[0].current
