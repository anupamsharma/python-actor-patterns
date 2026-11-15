"""Token-bucket rate limiter actor."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import maybe_await
from pyactors.patterns.exceptions import RateLimited
from pyactors.patterns.messages import GetStats

__all__ = ["RateLimitMode", "RateLimiter", "RateLimiterStats"]


@dataclass(frozen=True, slots=True)
class RateLimiterStats:
    allowed: int
    rejected: int
    tokens: float


class RateLimitMode:
    REJECT = "reject"
    WAIT = "wait"


class RateLimiter(Actor[Any, Any]):
    """Allow up to ``rate`` messages per ``per`` seconds with burst ``burst``."""

    def __init__(
        self,
        rate: float,
        *,
        per: float = 1.0,
        burst: float | None = None,
        mode: str = RateLimitMode.REJECT,
        handler: Callable[[Any], Any] | None = None,
        emit_to: Any | None = None,
    ) -> None:
        if rate <= 0 or per <= 0:
            raise ValueError("rate and per must be > 0")
        self._rate = rate
        self._per = per
        self._burst = float(burst if burst is not None else rate)
        self._mode = mode
        self._handler = handler
        self._emit_to = emit_to
        self._tokens = self._burst
        self._updated = time.monotonic()
        self._allowed = 0
        self._rejected = 0
        self._waiters: list[tuple[Any, asyncio.Future[Any] | None]] = []
        self._timer: asyncio.TimerHandle | None = None

    async def on_stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        err = RateLimited("rate limiter stopped")
        for _, fut in self._waiters:
            if fut is not None and not fut.done():
                fut.set_exception(err)
        self._waiters.clear()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            self._refill()
            return RateLimiterStats(self._allowed, self._rejected, self._tokens)
        self._refill()
        if self._tokens >= 1.0:
            return await self._allow(message)
        if self._mode == RateLimitMode.REJECT:
            self._rejected += 1
            raise ReplyError(RateLimited("rate limit exceeded"))
        reply = self.context.detach_ask()
        self._waiters.append((message, reply))
        self._arm()
        return None

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._updated
        self._updated = now
        self._tokens = min(self._burst, self._tokens + elapsed * (self._rate / self._per))

    async def _allow(self, message: Any) -> Any:
        self._tokens -= 1.0
        self._allowed += 1
        result: Any = message
        if self._handler is not None:
            result = await maybe_await(self._handler(message))
        if self._emit_to is not None:
            await self._emit_to.send(message)
        return result

    def _arm(self) -> None:
        if self._timer is not None or not self._waiters:
            return
        need = 1.0 - self._tokens
        delay = max(0.0, need * (self._per / self._rate))
        self._timer = asyncio.get_running_loop().call_later(delay, self._schedule)

    def _schedule(self) -> None:
        self._timer = None
        asyncio.get_running_loop().create_task(self._drain())

    async def _drain(self) -> None:
        self._refill()
        while self._waiters and self._tokens >= 1.0:
            message, fut = self._waiters.pop(0)
            try:
                result = await self._allow(message)
            except Exception as exc:
                if fut is not None and not fut.done():
                    fut.set_exception(exc)
            else:
                if fut is not None and not fut.done():
                    fut.set_result(result)
        if self._waiters:
            self._arm()
