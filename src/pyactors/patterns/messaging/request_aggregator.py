"""Aggregate multiple related requests before emitting a combined result."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from typing import Any

from pyactors.actor import Actor
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import Flush, GetStats

__all__ = [
    "Aggregated",
    "PartialRequest",
    "RequestAggregator",
]


@dataclass(frozen=True, slots=True)
class PartialRequest:
    """One fragment of a multi-part request identified by ``group_id``."""

    group_id: Hashable
    part: Any
    expected_parts: int | None = None


@dataclass(slots=True)
class Aggregated:
    group_id: Hashable
    parts: list[Any] = field(default_factory=list)


@dataclass(slots=True)
class _Group:
    group_id: Hashable
    parts: list[Any]
    expected: int | None
    waiters: list[asyncio.Future[Any]]
    deadline: asyncio.TimerHandle | None


class RequestAggregator(Actor[Any, Any]):
    """Collect partial requests sharing a ``group_id`` until complete or timed out."""

    def __init__(
        self,
        *,
        timeout: float | None = 5.0,
        expected_parts: int | None = None,
        handler: Callable[[Aggregated], Any] | None = None,
        key: Callable[[Any], Hashable] | None = None,
    ) -> None:
        self._timeout = timeout
        self._expected = expected_parts
        self._handler = handler
        self._key = key
        self._groups: dict[Hashable, _Group] = {}
        self._completed = 0
        self._timed_out = 0

    async def on_stop(self) -> None:
        for group_id in list(self._groups):
            group = self._groups.pop(group_id)
            await self._complete(group, timed_out=True)

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "open_groups": len(self._groups),
                "completed": self._completed,
                "timed_out": self._timed_out,
            }
        if isinstance(message, Flush):
            results = []
            for group_id in list(self._groups):
                group = self._groups.pop(group_id)
                results.append(await self._complete(group, timed_out=False))
            return results
        if isinstance(message, PartialRequest):
            self._accept(message.group_id, message.part, message.expected_parts)
            return None
        if self._key is not None:
            self._accept(self._key(message), message, self._expected)
            return None
        raise TypeError("RequestAggregator expects PartialRequest (or a key extractor)")

    def _accept(self, group_id: Hashable, part: Any, expected: int | None) -> None:
        group = self._groups.get(group_id)
        if group is None:
            loop = asyncio.get_running_loop()
            group = _Group(
                group_id=group_id,
                parts=[],
                expected=expected if expected is not None else self._expected,
                waiters=[],
                deadline=None,
            )
            self._groups[group_id] = group
            if self._timeout is not None:
                group.deadline = loop.call_later(self._timeout, self._schedule_timeout, group_id)
        elif expected is not None and group.expected is None:
            group.expected = expected
        group.parts.append(part)
        reply = self.context.detach_ask()
        if reply is not None:
            group.waiters.append(reply)
        if group.expected is not None and len(group.parts) >= group.expected:
            self._groups.pop(group_id, None)
            asyncio.get_running_loop().create_task(self._complete(group, timed_out=False))

    def _schedule_timeout(self, group_id: Hashable) -> None:
        asyncio.get_running_loop().create_task(self._timeout_group(group_id))

    async def _timeout_group(self, group_id: Hashable) -> None:
        group = self._groups.pop(group_id, None)
        if group is None:
            return
        self._timed_out += 1
        await self._complete(group, timed_out=True)

    async def _complete(self, group: _Group, *, timed_out: bool) -> Any:
        if group.deadline is not None:
            group.deadline.cancel()
            group.deadline = None
        aggregated = Aggregated(group_id=group.group_id, parts=list(group.parts))
        result: Any = aggregated
        if self._handler is not None and group.parts:
            try:
                result = await maybe_await(self._handler(aggregated))
            except Exception as exc:
                for waiter in group.waiters:
                    if not waiter.done():
                        waiter.set_exception(exc)
                group.waiters.clear()
                group.parts.clear()
                raise
        if not timed_out:
            self._completed += 1
        for waiter in group.waiters:
            if not waiter.done():
                waiter.set_result(result)
        group.waiters.clear()
        group.parts.clear()
        return result
