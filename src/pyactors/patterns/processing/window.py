"""Window aggregator: tumbling and sliding windows."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import Flush, GetStats

__all__ = ["TimedEvent", "Window", "WindowKind", "WindowResult"]


class WindowKind:
    TUMBLING = "tumbling"
    SLIDING = "sliding"


@dataclass(frozen=True, slots=True)
class TimedEvent:
    """Event with an explicit timestamp (seconds, same domain as ``clock``)."""

    value: Any
    timestamp: float


@dataclass(frozen=True, slots=True)
class WindowResult:
    items: tuple[Any, ...]
    start: float
    end: float
    kind: str


class Window(Actor[Any, Any]):
    """Aggregate events into tumbling or sliding time windows.

    * **Tumbling** — non-overlapping windows of ``size`` seconds.
    * **Sliding** — windows of ``size`` seconds advancing every ``slide``
      seconds (``slide`` defaults to ``size`` for tumbling behaviour).

    Timestamps default to :func:`time.monotonic` when the message is not a
    :class:`TimedEvent`.  ``aggregator`` receives the list of values in the
    closed window.
    """

    def __init__(
        self,
        size: float,
        *,
        slide: float | None = None,
        kind: str = WindowKind.TUMBLING,
        aggregator: Callable[[Sequence[Any]], Any] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if size <= 0:
            raise ValueError("size must be > 0")
        self._size = size
        self._slide = size if slide is None else slide
        if self._slide <= 0:
            raise ValueError("slide must be > 0")
        self._kind = kind
        self._aggregator = aggregator or (lambda items: list(items))
        self._clock = clock or time.monotonic
        self._events: deque[tuple[float, Any]] = deque()
        self._window_start: float | None = None
        self._emitted = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "buffered": len(self._events),
                "emitted": self._emitted,
                "window_start": self._window_start,
            }
        if isinstance(message, Flush):
            return await self._flush_all()
        if isinstance(message, TimedEvent):
            ts, value = message.timestamp, message.value
        else:
            ts, value = self._clock(), message
        if self._window_start is None:
            self._window_start = ts
        self._events.append((ts, value))
        results = []
        while self._window_start is not None and ts >= self._window_start + self._size:
            results.append(await self._emit_window())
            if self._kind == WindowKind.TUMBLING:
                self._window_start += self._size
            else:
                self._window_start += self._slide
            self._drop_before(self._window_start)
            if not self._events:
                self._window_start = None
                break
        if len(results) == 1:
            return results[0]
        return results or None

    def _drop_before(self, cutoff: float) -> None:
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    async def _emit_window(self) -> Any:
        assert self._window_start is not None
        end = self._window_start + self._size
        items = tuple(v for ts, v in self._events if self._window_start <= ts < end)
        self._emitted += 1
        await maybe_await(self._aggregator(items))
        return WindowResult(items=items, start=self._window_start, end=end, kind=self._kind)

    async def _flush_all(self) -> Any:
        if not self._events or self._window_start is None:
            return None
        result = await self._emit_window()
        self._events.clear()
        self._window_start = None
        return result

    async def on_stop(self) -> None:
        if self._events:
            await self._flush_all()
