"""Retry actor: forward with configurable retry / backoff."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.exceptions import RetryExhausted
from pyactors.patterns.messages import GetStats

__all__ = ["Retry", "RetryPolicy"]


@dataclass(slots=True)
class RetryPolicy:
    """Retry configuration.

    Operations are retried only when ``retry_on`` returns True for the raised
    exception.  By default all ``Exception`` subclasses are retried; pass a
    stricter predicate for non-idempotent work.
    """

    max_attempts: int = 3
    initial_backoff: float = 0.05
    max_backoff: float = 2.0
    multiplier: float = 2.0
    jitter: float = 0.1
    retry_on: Callable[[BaseException], bool] | None = None

    def should_retry(self, exc: BaseException) -> bool:
        if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            return False
        if self.retry_on is not None:
            return self.retry_on(exc)
        return isinstance(exc, Exception)


class Retry(Actor[Any, Any]):
    """Ask ``target`` with retries according to :class:`RetryPolicy`.

    Does **not** retry unless the failure matches the policy.  Non-idempotent
    operations should supply an explicit ``retry_on`` predicate.
    """

    def __init__(
        self,
        target: Any,
        *,
        policy: RetryPolicy | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self._target = target
        self._policy = policy or RetryPolicy()
        self._rng = rng or random.Random()
        self._attempts_total = 0
        self._successes = 0
        self._failures = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "attempts": self._attempts_total,
                "successes": self._successes,
                "failures": self._failures,
            }
        policy = self._policy
        delay = policy.initial_backoff
        last_error: BaseException | None = None
        for attempt in range(1, policy.max_attempts + 1):
            self._attempts_total += 1
            try:
                result = await self._target.ask(message)
            except BaseException as exc:
                last_error = exc
                if attempt >= policy.max_attempts or not policy.should_retry(exc):
                    self._failures += 1
                    raise ReplyError(RetryExhausted(exc, attempt)) from exc
                jitter = delay * policy.jitter * (self._rng.random() * 2 - 1)
                await asyncio.sleep(max(0.0, delay + jitter))
                delay = min(policy.max_backoff, delay * policy.multiplier)
            else:
                self._successes += 1
                return result
        assert last_error is not None
        self._failures += 1
        raise ReplyError(RetryExhausted(last_error, policy.max_attempts)) from last_error
