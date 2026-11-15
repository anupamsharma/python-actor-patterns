"""Throttle actor: ensure a minimum interval between emitted messages."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import OverflowPolicy, maybe_await
from pyactors.patterns.exceptions import PatternFull, RateLimited
from pyactors.patterns.messages import GetStats

__all__ = ["Throttle"]


class Throttle(Actor[Any, Any]):
    """Forward at most one message every ``interval`` seconds."""

    def __init__(
        self,
        interval: float,
        *,
        handler: Callable[[Any], Any] | None = None,
        emit_to: Any | None = None,
        overflow: OverflowPolicy = OverflowPolicy.WAIT,
        max_pending: int | None = None,
    ) -> None:
        if interval < 0:
            raise ValueError("interval must be >= 0")
        self._interval = interval
        self._handler = handler
        self._emit_to = emit_to
        self._overflow = overflow
        self._max_pending = max_pending
        self._last_emit = 0.0
        self._pending: deque[tuple[Any, asyncio.Future[Any] | None]] = deque()
        self._timer: asyncio.TimerHandle | None = None
        self._emitted = 0
        self._rejected = 0

    async def on_stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
        err = RateLimited("throttle stopped")
        while self._pending:
            _, fut = self._pending.popleft()
            if fut is not None and not fut.done():
                fut.set_exception(err)

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "pending": len(self._pending),
                "emitted": self._emitted,
                "rejected": self._rejected,
            }
        now = time.monotonic()
        wait = self._interval - (now - self._last_emit)
        if wait <= 0 and not self._pending:
            return await self._do_emit(message)
        if self._overflow is OverflowPolicy.REJECT and wait > 0 and not self._pending:
            self._rejected += 1
            raise ReplyError(RateLimited(f"throttled; retry after {wait:.3f}s"))
        if self._max_pending is not None and len(self._pending) >= self._max_pending:
            if self._overflow is OverflowPolicy.DROP_NEWEST:
                self._rejected += 1
                return None
            if self._overflow is OverflowPolicy.DROP_OLDEST and self._pending:
                _, old = self._pending.popleft()
                if old is not None and not old.done():
                    old.set_exception(RateLimited("dropped by throttle"))
                self._rejected += 1
            elif self._overflow is OverflowPolicy.REJECT:
                self._rejected += 1
                raise ReplyError(PatternFull("throttle pending queue is full"))
        reply = self.context.detach_ask()
        self._pending.append((message, reply))
        self._arm_timer(wait if wait > 0 else 0.0)
        return None

    def _arm_timer(self, delay: float) -> None:
        if self._timer is not None:
            return
        loop = asyncio.get_running_loop()
        self._timer = loop.call_later(max(0.0, delay), self._schedule_drain)

    def _schedule_drain(self) -> None:
        self._timer = None
        asyncio.get_running_loop().create_task(self._drain())

    async def _drain(self) -> None:
        if not self._pending:
            return
        now = time.monotonic()
        wait = self._interval - (now - self._last_emit)
        if wait > 0:
            self._arm_timer(wait)
            return
        message, fut = self._pending.popleft()
        try:
            result = await self._do_emit(message)
        except Exception as exc:
            if fut is not None and not fut.done():
                fut.set_exception(exc)
        else:
            if fut is not None and not fut.done():
                fut.set_result(result)
        if self._pending:
            self._arm_timer(self._interval)

    async def _do_emit(self, message: Any) -> Any:
        self._last_emit = time.monotonic()
        self._emitted += 1
        result: Any = message
        if self._handler is not None:
            result = await maybe_await(self._handler(message))
        if self._emit_to is not None:
            await self._emit_to.send(message)
        return result
