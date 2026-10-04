"""Window- and threshold-based alerting with cooldown and hysteresis.

Semantics
---------
* Each alert rule keeps a sliding window of boolean observations
  (``(timestamp, violated)``). The window covers ``(now - window, now]``;
  observations at or before ``now - window`` are evicted.
* While the window contains fewer than ``min_samples`` observations the
  evidence is considered insufficient: the evaluation is *inconclusive*,
  no state is decided (the current state is kept) and no cooldown is
  consumed. The same rule applies to every alert, present or future.
* Once enough samples are available the evaluated value is
  ``violated / total`` inside the window and two distinct thresholds
  decide the verdict:

  - ``fire_threshold`` (the higher line): from ``ok`` the alert fires
    only when ``value >= fire_threshold``;
  - ``recover_threshold`` (the lower line): from ``firing`` the alert
    recovers only when ``value < recover_threshold``;
  - values inside the inclusive middle band
    ``[recover_threshold, fire_threshold)`` keep the current state, so
    jitter near a single line cannot flap the alert.

* Cooldown: after a state transition at time ``t0``, no further
  transition is allowed before ``t0 + cooldown_seconds``. A demanded
  transition inside the cooldown is *suppressed* (the state stays as-is
  and the demand is dropped), so short bursts cannot flap the state and
  each state change happens at most once per cooldown period.
* Initial state is always ``OK``; a fresh instance after restart is
  therefore deterministic.
* Every evaluation produces one :class:`AlertEvent` (inputs + verdict,
  including insufficient-sample and cooldown-suppressed cases), is kept
  in a bounded, append-only history and is emitted as one structured
  log line.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("observability.alerting")

#: Default cap on the per-alert evaluation history kept in memory.
DEFAULT_HISTORY_LIMIT = 1000


class AlertState(str, Enum):
    """Lifecycle state of one alert."""

    OK = "ok"
    FIRING = "firing"


@dataclass(frozen=True)
class AlertConfig:
    """Tuning knobs for one alert rule.

    ``threshold`` is kept as an alias for the common case where the
    firing and recovery lines coincide (``fire_threshold`` wins when a
    value is supplied explicitly). ``min_samples`` configures how many
    windowed samples must be present before any verdict is allowed.
    """

    name: str
    window_seconds: float
    threshold: float | None = None
    cooldown_seconds: float = 0.0
    min_samples: int = 1
    fire_threshold: float | None = None
    recover_threshold: float | None = None

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.min_samples < 1:
            raise ValueError("min_samples must be >= 1")
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be >= 0")
        fire = self.fire_threshold if self.fire_threshold is not None else self.threshold
        recover = (
            self.recover_threshold if self.recover_threshold is not None else self.threshold
        )
        if fire is None or recover is None:
            raise ValueError(
                "either threshold or both fire_threshold/recover_threshold are required"
            )
        if not 0.0 < fire <= 1.0:
            raise ValueError("fire threshold must be in (0, 1]")
        if not 0.0 < recover <= 1.0:
            raise ValueError("recover threshold must be in (0, 1]")
        if recover > fire:
            raise ValueError(
                "recover_threshold must be <= fire_threshold (higher line to fire, "
                "lower line to recover)"
            )
        # Frozen dataclass: write resolved values through object.__setattr__.
        object.__setattr__(self, "fire_threshold", float(fire))
        object.__setattr__(self, "recover_threshold", float(recover))


@dataclass(frozen=True)
class AlertEvent:
    """A single evaluation result, kept for explainability.

    Inputs of the decision: ``alert``, ``seq``, ``timestamp``,
    ``violated``, ``window_size``, ``value``, ``min_samples``,
    ``fire_threshold``, ``recover_threshold``.
    Verdict of the decision: ``previous``, ``current``,
    ``transitioned``, ``suppressed_by_cooldown``, ``inconclusive``
    (plus ``state_change`` as an explicit transition indicator).
    """

    alert: str
    seq: int
    timestamp: float
    violated: bool
    value: float
    window_size: int
    min_samples: int
    fire_threshold: float
    recover_threshold: float
    previous: AlertState
    current: AlertState
    transitioned: bool = False
    suppressed_by_cooldown: bool = False
    inconclusive: bool = False
    # Backwards-compatible alias of the legacy single-threshold field.
    threshold: float = field(default=0.0)

    def __post_init__(self) -> None:
        # ``transitioned`` and ``state_change`` always agree; the alias
        # name exists for readers that think in terms of state changes.
        object.__setattr__(self, "state_change", self.transitioned)


class WindowedAlert:
    """One alert rule over a sliding window of boolean observations."""

    def __init__(
        self, config: AlertConfig, history_limit: int = DEFAULT_HISTORY_LIMIT
    ) -> None:
        if history_limit < 1:
            raise ValueError("history_limit must be >= 1")
        self.config = config
        self.state: AlertState = AlertState.OK
        self._window: deque[tuple[float, bool]] = deque()
        self._violated_in_window = 0
        self._last_transition: float | None = None
        self.history: list[AlertEvent] = []
        self._history_limit = history_limit
        self._seq = 0

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
        value = self._violated_in_window / window_size if window_size else 0.0
        previous = self.state
        transitioned = False
        suppressed = False
        inconclusive = window_size < self.config.min_samples

        if inconclusive:
            # Not enough evidence yet: keep the current state, do not
            # touch the cooldown clock.
            desired: AlertState | None = None
        elif previous is AlertState.OK and value >= self.config.fire_threshold:
            desired = AlertState.FIRING
        elif (
            previous is AlertState.FIRING
            and value < self.config.recover_threshold
        ):
            desired = AlertState.OK
        else:
            # Inside the hysteresis band: hold the current state.
            desired = None

        if desired is not None and desired is not self.state:
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

        self._seq += 1
        event = AlertEvent(
            alert=self.config.name,
            seq=self._seq,
            timestamp=timestamp,
            violated=violated,
            value=value,
            window_size=window_size,
            min_samples=self.config.min_samples,
            fire_threshold=self.config.fire_threshold,
            recover_threshold=self.config.recover_threshold,
            previous=previous,
            current=self.state,
            transitioned=transitioned,
            suppressed_by_cooldown=suppressed,
            inconclusive=inconclusive,
            threshold=self.config.fire_threshold,
        )
        self.history.append(event)
        # Bounded history: oldest evaluations are dropped first.
        if len(self.history) > self._history_limit:
            del self.history[: len(self.history) - self._history_limit]
        return event

    def view(self, limit: int | None = None) -> tuple[AlertState, list[AlertEvent]]:
        """Return ``(state, history-copy)`` as one consistent snapshot."""
        history = list(self.history)
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be >= 0")
            history = history[-limit:] if limit else []
        return self.state, history


class AlertManager:
    """Thread-safe collection of alert rules with structured logging."""

    def __init__(
        self,
        configs: list[AlertConfig],
        history_limit: int = DEFAULT_HISTORY_LIMIT,
    ) -> None:
        self._lock = threading.Lock()
        self._history_limit = history_limit
        self._alerts: dict[str, WindowedAlert] = {
            c.name: WindowedAlert(c, history_limit=history_limit) for c in configs
        }

    def observe(self, name: str, timestamp: float, violated: bool) -> AlertEvent:
        """Feed one observation to a rule and log input plus verdict."""
        with self._lock:
            alert = self._alerts[name]
            event = alert.observe(timestamp, violated)
        # Structured payload: inputs of the decision...
        payload = {
            "event": "alert_evaluation",
            "alert": event.alert,
            "seq": event.seq,
            "timestamp": event.timestamp,
            "violated": event.violated,
            "window_size": event.window_size,
            "value": round(event.value, 6),
            "min_samples": event.min_samples,
            "fire_threshold": event.fire_threshold,
            "recover_threshold": event.recover_threshold,
            # ...and the verdict.
            "previous": event.previous.value,
            "current": event.current.value,
            "transitioned": event.transitioned,
            "state_change": event.transitioned,
            "suppressed_by_cooldown": event.suppressed_by_cooldown,
            "inconclusive": event.inconclusive,
        }
        logger.info("alert evaluation", extra=payload)
        return event

    def states(self) -> dict[str, AlertState]:
        """Current state of every alert (deterministic after restart: all OK)."""
        with self._lock:
            return {name: alert.state for name, alert in self._alerts.items()}

    def history(self, name: str) -> list[AlertEvent]:
        """Evaluation history of one alert, for tests and debugging."""
        with self._lock:
            return list(self._alerts[name].history)

    def names(self) -> list[str]:
        """Configured alert rule names, in registration order."""
        with self._lock:
            return list(self._alerts)

    def recent(
        self, name: str, limit: int | None = None
    ) -> tuple[AlertState, list[AlertEvent]]:
        """Read-only consistent view: ``(current state, recent events)``.

        The state and the returned history are read under the same lock,
        so concurrent evaluations cannot make the pair lose or repeat
        records relative to the current state.
        """
        with self._lock:
            alert = self._alerts.get(name)
            if alert is None:
                raise KeyError(name)
            return alert.view(limit)
