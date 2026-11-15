"""Trailing-edge debounce actor."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from pyactors.actor import Actor
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import Flush, GetStats

__all__ = ["Debounce"]


class Debounce(Actor[Any, Any]):
    """Emit only the latest message after a quiet period of ``interval`` seconds.

    Trailing-edge semantics: each new ``ask`` resets the timer and parks the
    caller; when the interval elapses, all parked askers receive the result
    for the latest value.
    """

    def __init__(
        self,
        interval: float,
        *,
        handler: Callable[[Any], Any] | None = None,
        emit_to: Any | None = None,
    ) -> None:
        if interval < 0:
            raise ValueError("interval must be >= 0")
        self._interval = interval
        self._handler = handler
        self._emit_to = emit_to
        self._pending: Any | None = None
        self._has_pending = False
        self._timer: asyncio.TimerHandle | None = None
        self._emitted = 0
        self._seen = 0
        self._waiters: list[asyncio.Future[Any]] = []

    async def on_stop(self) -> None:
        if self._has_pending:
            await self._emit()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "pending": self._has_pending,
                "emitted": self._emitted,
                "seen": self._seen,
            }
        if isinstance(message, Flush):
            if not self._has_pending:
                return None
            return await self._emit()
        self._seen += 1
        self._pending = message
        self._has_pending = True
        reply = self.context.detach_ask()
        if reply is not None:
            self._waiters.append(reply)
        if self._timer is not None:
            self._timer.cancel()
        loop = asyncio.get_running_loop()
        self._timer = loop.call_later(self._interval, self._schedule_emit)
        return None

    def _schedule_emit(self) -> None:
        asyncio.get_running_loop().create_task(self._emit())

    async def _emit(self) -> Any:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._has_pending:
            return None
        value = self._pending
        self._pending = None
        self._has_pending = False
        self._emitted += 1
        result: Any = value
        try:
            if self._handler is not None:
                result = await maybe_await(self._handler(value))
            if self._emit_to is not None:
                await self._emit_to.send(value)
        except Exception as exc:
            for waiter in self._waiters:
                if not waiter.done():
                    waiter.set_exception(exc)
            self._waiters.clear()
            raise
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(result)
        self._waiters.clear()
        return result
