"""Batch actor: group items by count and/or waiting time."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import Flush, GetStats

__all__ = ["Batch", "BatchReady"]


@dataclass(frozen=True, slots=True)
class BatchReady:
    """A completed batch of items."""

    items: tuple[Any, ...]
    reason: str  # "size" | "time" | "flush" | "shutdown"


class Batch(Actor[Any, Any]):
    """Accumulate items and invoke ``handler`` with each completed batch.

    A batch is emitted when:
    * ``max_size`` items have been collected, and/or
    * ``max_wait`` seconds have elapsed since the first item in the batch, or
    * :class:`~pyactors.patterns.messages.Flush` is received, or
    * the actor stops (shutdown flush — accepted items are never silently lost).

    At least one of ``max_size`` / ``max_wait`` must be set.
    """

    def __init__(
        self,
        handler: Callable[[Sequence[Any]], Any],
        *,
        max_size: int | None = 32,
        max_wait: float | None = None,
        emit_batches: bool = False,
    ) -> None:
        if max_size is None and max_wait is None:
            raise ValueError("at least one of max_size or max_wait is required")
        if max_size is not None and max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._handler = handler
        self._max_size = max_size
        self._max_wait = max_wait
        self._emit_batches = emit_batches
        self._buf: list[Any] = []
        self._timer: asyncio.TimerHandle | None = None
        self._batches = 0
        self._items_seen = 0

    async def on_stop(self) -> None:
        if self._buf:
            await self._emit("shutdown")

    async def receive(self, message: Any) -> Any:
        if isinstance(message, Flush):
            if not self._buf:
                return BatchReady(items=(), reason="flush") if self._emit_batches else None
            return await self._emit("flush")
        if isinstance(message, GetStats):
            return {
                "pending": len(self._buf),
                "batches": self._batches,
                "items_seen": self._items_seen,
            }
        self._buf.append(message)
        self._items_seen += 1
        if len(self._buf) == 1 and self._max_wait is not None:
            loop = asyncio.get_running_loop()
            self._timer = loop.call_later(self._max_wait, self._schedule_emit)
        if self._max_size is not None and len(self._buf) >= self._max_size:
            return await self._emit("size")
        return None

    def _schedule_emit(self) -> None:
        asyncio.get_running_loop().create_task(self._emit("time"))

    async def _emit(self, reason: str) -> Any:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._buf:
            return None
        items = tuple(self._buf)
        self._buf.clear()
        self._batches += 1
        result = await maybe_await(self._handler(items))
        if self._emit_batches:
            return BatchReady(items=items, reason=reason)
        return result
