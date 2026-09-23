"""Window- and threshold-based alerting with cooldown.

Semantics
---------
* Each alert rule keeps a sliding window of boolean observations
  (``(timestamp, violated)``). The window covers ``(now - window, now]``;
  observations at or before ``now - window`` are evicted.
* The evaluated value is ``violated / total`` inside the window.
* The alert fires when ``value >= threshold`` and recovers when
  ``value < threshold``.
* Cooldown: after a state transition at time ``t0``, no further
  transition is allowed before ``t0 + cooldown_seconds``. A demanded
  transition inside the cooldown is *suppressed* (the state stays as-is
  and the demand is dropped), so short bursts cannot flap the state and
  each state change happens at most once per cooldown period.
* Initial state is always ``OK``; a fresh instance after restart is
  therefore deterministic.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("observability.alerting")


class AlertState(str, Enum):
    """Lifecycle state of one alert."""

    OK = "ok"
    FIRING = "firing"


@dataclass(frozen=True)
class AlertConfig:
    """Tuning knobs for one alert rule."""

    name: str
    window_seconds: float
    threshold: float
    cooldown_seconds: float


@dataclass(frozen=True)
class AlertEvent:
    """A single evaluation result, kept for explainability."""

    alert: str
    timestamp: float
    value: float
    threshold: float
    window_size: int
    previous: AlertState
    current: AlertState
    transitioned: bool
    suppressed_by_cooldown: bool


class WindowedAlert:
    """One alert rule over a sliding window of boolean observations."""

    def __init__(self, config: AlertConfig) -> None:
        if config.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if not 0.0 < config.threshold <= 1.0:
            raise ValueError("threshold must be in (0, 1]")
        if config.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")
        self.config = config
        self.state: AlertState = AlertState.OK
        self._window: deque[tuple[float, bool]] = deque()
        self._violated_in_window = 0
        self._last_transition: float | None = None
        self.history: list[AlertEvent] = []

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

        value = self._violated_in_window / len(self._window)
        desired = (
            AlertState.FIRING if value >= self.config.threshold else AlertState.OK
        )

        previous = self.state
        transitioned = False
        suppressed = False
        if desired is not self.state:
            in_cooldown = (
                self._last_transition is not None
                and timestamp - self._last_transition < self.config.cooldown_seconds
            )
            if in_cooldown:
                suppressed = True
            else:
                self.state = desired
                self._last_transition = timestamp
                transitioned = True

        event = AlertEvent(
            alert=self.config.name,
            timestamp=timestamp,
            value=value,
            threshold=self.config.threshold,
            window_size=len(self._window),
            previous=previous,
            current=self.state,
            transitioned=transitioned,
            suppressed_by_cooldown=suppressed,
        )
        self.history.append(event)
        return event


class AlertManager:
    """Thread-safe collection of alert rules with structured logging."""

    def __init__(self, configs: list[AlertConfig]) -> None:
        self._lock = threading.Lock()
        self._alerts: dict[str, WindowedAlert] = {
            c.name: WindowedAlert(c) for c in configs
        }

    def observe(self, name: str, timestamp: float, violated: bool) -> AlertEvent:
        """Feed one observation to a rule and log input plus verdict."""
        with self._lock:
            alert = self._alerts[name]
            event = alert.observe(timestamp, violated)
        logger.info(
            "alert evaluation",
            extra={
                "alert": event.alert,
                "timestamp": event.timestamp,
                "violated": violated,
                "value": round(event.value, 6),
                "threshold": event.threshold,
                "window_size": event.window_size,
                "previous": event.previous.value,
                "current": event.current.value,
                "transitioned": event.transitioned,
                "suppressed_by_cooldown": event.suppressed_by_cooldown,
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
