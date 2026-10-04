"""Window- and threshold-based alerting with cooldown and hysteresis.

Semantics
---------
* Each alert rule keeps a sliding window of boolean observations
  (``(timestamp, violated)``). The window covers ``(now - window, now]``;
  observations at or before ``now - window`` are evicted.
* The evaluated value is ``violated / total`` inside the window.
* While the window holds fewer than ``min_samples`` observations the
  verdict is *undecided*: the current state is maintained and no
  transition (nor cooldown suppression) is recorded.
* Two different thresholds with a hysteresis band in between:
  - from ``ok`` the alert fires only when ``value >= fire_threshold``;
  - from ``firing`` it recovers only when ``value < recover_threshold``;
  - with ``recover_threshold <= value < fire_threshold`` the state is
    kept as-is, so values wobbling in the middle cannot flap it.
* Cooldown: after a state transition at time ``t0``, no further
  transition is allowed before ``t0 + cooldown_seconds``. A demanded
  transition inside the cooldown is *suppressed* (the state stays as-is
  and the demand is dropped), so short bursts cannot flap the state and
  each state change happens at most once per cooldown period.
* Initial state is always ``OK``; a fresh instance after restart is
  therefore deterministic.

Every rule -- existing or added later -- goes through the exact same
:class:`WindowedAlert` pipeline, so minimum samples, the dual
thresholds, the hysteresis band and cooldown apply uniformly.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Deque

import logging

#: How many evaluation records are retained per rule by default.
DEFAULT_HISTORY_SIZE = 1024


class AlertState(str, Enum):
    """Lifecycle state of one alert."""

    OK = "ok"
    FIRING = "firing"


@dataclass(frozen=True)
class AlertConfig:
    """Tuning knobs for one alert rule.

    ``threshold`` is kept as a backward-compatible shorthand: when the
    explicit ``fire_threshold`` / ``recover_threshold`` are not given,
    it sets both to the same value, reproducing the old single-line
    semantics.
    """

    name: str
    window_seconds: float
    threshold: float | None = None
    cooldown_seconds: float = 0.0
    fire_threshold: float | None = None
    recover_threshold: float | None = None
    min_samples: int = 1


@dataclass(frozen=True)
class AlertEvent:
    """A single evaluation result, kept for explainability.

    Carries the full input (``timestamp``/``violated`` plus the window
    and rule knobs) together with the verdict: previous/current state,
    whether a transition actually happened, whether the demanded
    transition was blocked by cooldown and whether the verdict was
    skipped because too few samples were available.
    """

    alert: str
    sequence: int
    timestamp: float
    violated: bool
    value: float
    threshold: float
    fire_threshold: float
    recover_threshold: float
    min_samples: int
    window_size: int
    previous: AlertState
    current: AlertState
    transitioned: bool
    suppressed_by_cooldown: bool
    insufficient_samples: bool


class WindowedAlert:
    """One alert rule over a sliding window of boolean observations."""

    def __init__(
        self, config: AlertConfig, history_size: int = DEFAULT_HISTORY_SIZE
    ) -> None:
        if config.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if config.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")
        if config.min_samples < 1:
            raise ValueError("min_samples must be >= 1")

        fire = config.fire_threshold
        recover = config.recover_threshold
        if fire is None:
            fire = config.threshold
        if fire is None:
            raise ValueError(
                "either threshold or fire_threshold/recover_threshold is required"
            )
        if recover is None:
            recover = config.threshold if config.threshold is not None else fire
        if not 0.0 < fire <= 1.0:
            raise ValueError("fire_threshold must be within (0, 1]")
        if not 0.0 <= recover <= 1.0 or recover > fire:
            raise ValueError(
                "require 0 <= recover_threshold <= fire_threshold <= 1"
            )

        self.config = config
        self.fire_threshold = fire
        self.recover_threshold = recover
        self.state: AlertState = AlertState.OK
        self._window: Deque[tuple[float, bool]] = deque()
        self._violated_in_window = 0
        self._last_transition: float | None = None
        self._sequence = 0
        self.history: Deque[AlertEvent] = deque(maxlen=history_size)

    def _evict(self, now: float) -> None:
        boundary = now - self.config.window_seconds
        while self._window and self._window[0][0] <= boundary:
            _, violated = self._window.popleft()
            if violated:
                self._violated_in_window -= 1

    def observe(self, timestamp: float, violated: bool) -> AlertEvent:
        """Record one observation, evaluate the window, return the verdict."""
        if self._window and timestamp < self._window[-1][0]:
            raise ValueError("observations must arrive in non-decreasing time")
        self._window.append((timestamp, violated))
        if violated:
            self._violated_in_window += 1
        self._evict(timestamp)

        window_size = len(self._window)
        value = self._violated_in_window / window_size
        insufficient = window_size < self.config.min_samples

        previous = self.state
        desired = previous
        if not insufficient:
            if previous is AlertState.OK:
                if value >= self.fire_threshold:
                    desired = AlertState.FIRING
            else:  # FIRING: only the lower recovery line can move it back
                if value < self.recover_threshold:
                    desired = AlertState.OK

        transitioned = False
        suppressed = False
        if desired is not previous:
            in_cooldown = (
                self._last_transition is not None
                and timestamp - self._last_transition
                < self.config.cooldown_seconds
            )
            if in_cooldown:
                suppressed = True
            else:
                self.state = desired
                self._last_transition = timestamp
                transitioned = True

        event = AlertEvent(
            alert=self.config.name,
            sequence=self._sequence,
            timestamp=timestamp,
            violated=violated,
            value=value,
            threshold=self.fire_threshold,
            fire_threshold=self.fire_threshold,
            recover_threshold=self.recover_threshold,
            min_samples=self.config.min_samples,
            window_size=window_size,
            previous=previous,
            current=self.state,
            transitioned=transitioned,
            suppressed_by_cooldown=suppressed,
            insufficient_samples=insufficient,
        )
        self._sequence += 1
        self.history.append(event)
        return event


class AlertManager:
    """Thread-safe collection of alert rules with structured logging."""

    def __init__(
        self,
        configs: list[AlertConfig],
        history_size: int = DEFAULT_HISTORY_SIZE,
    ) -> None:
        self._lock = threading.Lock()
        self._alerts: dict[str, WindowedAlert] = {
            c.name: WindowedAlert(c, history_size=history_size) for c in configs
        }

    def observe(self, name: str, timestamp: float, violated: bool) -> AlertEvent:
        """Feed one observation to a rule and log input plus verdict."""
        with self._lock:
            alert = self._alerts[name]
            event = alert.observe(timestamp, violated)

        fields = {
            "alert": event.alert,
            "sequence": event.sequence,
            "timestamp": event.timestamp,
            "violated": event.violated,
            "value": round(event.value, 6),
            "threshold": event.threshold,
            "fire_threshold": event.fire_threshold,
            "recover_threshold": event.recover_threshold,
            "min_samples": event.min_samples,
            "window_size": event.window_size,
            "previous": event.previous.value,
            "current": event.current.value,
            "transitioned": event.transitioned,
            "suppressed_by_cooldown": event.suppressed_by_cooldown,
            "insufficient_samples": event.insufficient_samples,
        }
        # Structured fields land in extra_data (picked up by the JSON
        # formatter); a human-readable summary is embedded in the message
        # so plain-text test logs also show every input and verdict.
        message = (
            f"alert evaluation {event.alert} seq={event.sequence} "
            f"ts={event.timestamp} violated={event.violated} "
            f"window={event.window_size}/{event.min_samples} "
            f"value={fields['value']} fire>={event.fire_threshold} "
            f"recover<{event.recover_threshold} "
            f"{event.previous.value}->{event.current.value} "
            f"transitioned={event.transitioned} "
            f"suppressed={event.suppressed_by_cooldown} "
            f"insufficient={event.insufficient_samples}"
        )
        # Emit through the observability logger. Fields go both into
        # ``extra_data`` (picked up by the JSON line formatter, so the
        # service's own structured output shows every input/verdict) and
        # as top-level LogRecord attributes for direct readers.
        logging.getLogger("observability.alerting").info(
            message,
            extra={
                "extra_data": {"event": "alert_evaluation", **fields},
                **fields,
            },
        )
        return event

    def states(self) -> dict[str, AlertState]:
        """Current state of every alert (deterministic after restart: all OK)."""
        with self._lock:
            return {name: alert.state for name, alert in self._alerts.items()}

    def history(self, name: str) -> list[AlertEvent]:
        """Evaluation history of one alert, for tests and debugging."""
        with self._lock:
            return list(self._alerts[name].history)

    def state_and_history(
        self, name: str, limit: int | None = None
    ) -> tuple[AlertState, list[AlertEvent]]:
        """Atomically snapshot one rule's current state and recent history.

        Taking both under the same lock guarantees readers never see a
        history that disagrees with the state: the last record's
        ``current`` is always the returned state. Records are returned
        newest-first and each sequence appears at most once, so nothing
        is lost or duplicated for concurrent readers.
        """
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        with self._lock:
            alert = self._alerts[name]
            records = list(alert.history)
            if limit is not None:
                records = records[-limit:]
            return alert.state, list(reversed(records))

    def names(self) -> list[str]:
        with self._lock:
            return list(self._alerts)
