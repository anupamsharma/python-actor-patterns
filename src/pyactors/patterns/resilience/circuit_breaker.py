"""Circuit breaker actor."""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.exceptions import CircuitOpen
from pyactors.patterns.messages import GetStats

__all__ = ["BreakerState", "CircuitBreaker", "CircuitBreakerStats"]


class BreakerState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True, slots=True)
class CircuitBreakerStats:
    state: BreakerState
    failures: int
    successes: int
    rejected: int


class CircuitBreaker(Actor[Any, Any]):
    """Protect ``target`` with CLOSED / OPEN / HALF_OPEN states.

    * **CLOSED** — requests flow; consecutive failures count toward
      ``failure_threshold``.
    * **OPEN** — requests fail fast with :class:`CircuitOpen` until
      ``recovery_timeout`` elapses.
    * **HALF_OPEN** — up to ``half_open_max`` trial requests; success closes
      the circuit, failure re-opens it.
    """

    def __init__(
        self,
        target: Any,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max: int = 1,
        success_threshold: int = 1,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self._target = target
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max = half_open_max
        self._success_threshold = success_threshold
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._half_open_inflight = 0
        self._opened_at = 0.0
        self._successes = 0
        self._failures = 0
        self._rejected = 0

    @property
    def breaker_state(self) -> BreakerState:
        return self._state

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            self._maybe_half_open()
            return CircuitBreakerStats(self._state, self._failures, self._successes, self._rejected)
        self._maybe_half_open()
        if self._state is BreakerState.OPEN:
            self._rejected += 1
            raise ReplyError(CircuitOpen("circuit is open"))
        if self._state is BreakerState.HALF_OPEN:
            if self._half_open_inflight >= self._half_open_max:
                self._rejected += 1
                raise ReplyError(CircuitOpen("circuit is half-open; trial slots exhausted"))
            self._half_open_inflight += 1
        try:
            result = await self._target.ask(message)
        except Exception as exc:
            self._on_failure()
            raise ReplyError(exc) from exc
        else:
            self._on_success()
            return result
        finally:
            if self._state is BreakerState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)

    def _maybe_half_open(self) -> None:
        if self._state is BreakerState.OPEN:
            if time.monotonic() - self._opened_at >= self._recovery_timeout:
                self._state = BreakerState.HALF_OPEN
                self._half_open_successes = 0
                self._half_open_inflight = 0

    def _on_success(self) -> None:
        self._successes += 1
        self._consecutive_failures = 0
        if self._state is BreakerState.HALF_OPEN:
            self._half_open_successes += 1
            if self._half_open_successes >= self._success_threshold:
                self._state = BreakerState.CLOSED
        else:
            self._state = BreakerState.CLOSED

    def _on_failure(self) -> None:
        self._failures += 1
        self._consecutive_failures += 1
        half_open = self._state is BreakerState.HALF_OPEN
        if half_open or self._consecutive_failures >= self._failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = time.monotonic()
        self._half_open_inflight = 0
