"""Buffer actor: hold items until flush, capacity, or timeout."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import OverflowPolicy, maybe_await
from pyactors.patterns.exceptions import PatternFull
from pyactors.patterns.messages import Flush, GetStats

__all__ = ["Buffer", "BufferFlush", "Buffered"]


@dataclass(frozen=True, slots=True)
class Buffered:
    items: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class BufferFlush:
    """Alias retained for clarity; prefer :class:`~pyactors.patterns.messages.Flush`."""


class Buffer(Actor[Any, Any]):
    """Accumulate messages and emit them as a batch on flush triggers.

    Flush triggers:
    * buffer reaches ``capacity``
    * ``max_wait`` seconds since the first buffered item
    * explicit :class:`~pyactors.patterns.messages.Flush`

    ``handler``, if provided, receives the list of items; its return value is
    the ask result.  Otherwise askers receive :class:`Buffered`.
    """

    def __init__(
        self,
        *,
        capacity: int = 64,
        max_wait: float | None = None,
        overflow: OverflowPolicy = OverflowPolicy.REJECT,
        handler: Callable[[Sequence[Any]], Any] | None = None,
        emit_to: Any | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._max_wait = max_wait
        self._overflow = overflow
        self._handler = handler
        self._emit_to = emit_to
        self._buf: deque[Any] = deque()
        self._timer: asyncio.TimerHandle | None = None
        self._flushes = 0

    async def on_stop(self) -> None:
        if self._buf:
            await self._flush()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, (Flush, BufferFlush)):
            return await self._flush()
        if isinstance(message, GetStats):
            return {"size": len(self._buf), "flushes": self._flushes}
        if len(self._buf) >= self._capacity:
            if self._overflow is OverflowPolicy.DROP_NEWEST:
                return None
            if self._overflow is OverflowPolicy.DROP_OLDEST:
                self._buf.popleft()
            elif self._overflow is OverflowPolicy.REJECT:
                raise ReplyError(PatternFull("buffer is full"))
            else:
                await self._flush()
        self._buf.append(message)
        if len(self._buf) == 1 and self._max_wait is not None:
            loop = asyncio.get_running_loop()
            self._timer = loop.call_later(self._max_wait, self._schedule_flush)
        if len(self._buf) >= self._capacity:
            return await self._flush()
        return None

    def _schedule_flush(self) -> None:
        asyncio.get_running_loop().create_task(self._flush())

    async def _flush(self) -> Any:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._buf:
            return Buffered(items=())
        items = tuple(self._buf)
        self._buf.clear()
        self._flushes += 1
        result: Any = Buffered(items=items)
        if self._handler is not None:
            result = await maybe_await(self._handler(items))
        if self._emit_to is not None:
            await self._emit_to.send(result)
        return result
