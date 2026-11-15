"""Priority queue actor (lower number = higher priority)."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from dataclasses import dataclass, field
from heapq import heappop, heappush
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import OverflowPolicy
from pyactors.patterns.exceptions import PatternFull, PatternStopped
from pyactors.patterns.messages import GetStats

__all__ = ["PriorityConsume", "PriorityItem", "PriorityProduce", "PriorityQueueActor"]


@dataclass(frozen=True, slots=True)
class PriorityProduce:
    value: Any
    priority: int = 0
    wait: bool = True


@dataclass(frozen=True, slots=True)
class PriorityConsume:
    timeout: float | None = None


@dataclass(frozen=True, slots=True)
class PriorityItem:
    value: Any
    priority: int


@dataclass(order=True, slots=True)
class _Node:
    priority: int
    seq: int
    value: Any = field(compare=False)


class PriorityQueueActor(Actor[Any, Any]):
    """In-memory priority queue. Lower ``priority`` values are dequeued first.

    Equal priorities preserve insertion order (stable).  Overflow behaviour
    matches :class:`~pyactors.patterns.queues.queue_actor.QueueActor`.
    """

    def __init__(
        self,
        *,
        capacity: int | None = None,
        overflow: OverflowPolicy = OverflowPolicy.WAIT,
    ) -> None:
        if capacity is not None and capacity < 1:
            raise ValueError("capacity must be >= 1 or None")
        self._capacity = capacity
        self._overflow = overflow
        self._heap: list[_Node] = []
        self._seq = itertools.count()
        self._put_waiters: list[asyncio.Future[Any]] = []
        self._get_waiters: list[asyncio.Future[Any]] = []
        self._produced = 0
        self._consumed = 0
        self._closed = False

    async def on_stop(self) -> None:
        self._closed = True
        err = PatternStopped("priority queue stopping")
        for fut in self._put_waiters:
            if not fut.done():
                fut.set_exception(err)
        for fut in self._get_waiters:
            if not fut.done():
                fut.set_exception(err)
        self._put_waiters.clear()
        self._get_waiters.clear()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "size": len(self._heap),
                "produced": self._produced,
                "consumed": self._consumed,
            }
        if isinstance(message, PriorityProduce):
            return await self._produce(message.value, message.priority, message.wait)
        if isinstance(message, PriorityConsume):
            return await self._consume(message.timeout)
        if isinstance(message, tuple) and len(message) == 2:
            priority, value = message
            return await self._produce(value, int(priority), True)
        return await self._produce(message, 0, True)

    def _full(self) -> bool:
        return self._capacity is not None and len(self._heap) >= self._capacity

    async def _produce(self, value: Any, priority: int, wait: bool) -> None:
        if self._closed:
            raise ReplyError(PatternStopped("priority queue is stopped"))
        while self._full():
            if self._overflow is OverflowPolicy.DROP_NEWEST:
                return None
            if self._overflow is OverflowPolicy.DROP_OLDEST:
                if self._heap:
                    heappop(self._heap)
                break
            if self._overflow is OverflowPolicy.REJECT or not wait:
                raise ReplyError(PatternFull("priority queue is full"))
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._put_waiters.append(fut)
            await fut
        node = _Node(priority=priority, seq=next(self._seq), value=value)
        if self._get_waiters:
            waiter = self._get_waiters.pop(0)
            if not waiter.done():
                waiter.set_result(PriorityItem(value=value, priority=priority))
        else:
            heappush(self._heap, node)
        self._produced += 1
        return None

    async def _consume(self, timeout: float | None) -> PriorityItem:
        if self._heap:
            node = heappop(self._heap)
            self._consumed += 1
            self._wake_putter()
            return PriorityItem(value=node.value, priority=node.priority)
        fut: asyncio.Future[PriorityItem] = asyncio.get_running_loop().create_future()
        self._get_waiters.append(fut)
        try:
            if timeout is None:
                return await fut
            async with asyncio.timeout(timeout):
                return await fut
        except BaseException:
            if not fut.done():
                fut.cancel()
            with contextlib.suppress(ValueError):
                self._get_waiters.remove(fut)
            raise
        finally:
            if fut.done() and not fut.cancelled() and fut.exception() is None:
                self._consumed += 1
                self._wake_putter()

    def _wake_putter(self) -> None:
        while self._put_waiters:
            fut = self._put_waiters.pop(0)
            if not fut.done():
                fut.set_result(None)
                return
