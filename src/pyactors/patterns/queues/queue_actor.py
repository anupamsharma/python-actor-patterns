"""In-memory producer/consumer queue actor with optional acknowledgement."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import OverflowPolicy
from pyactors.patterns.exceptions import PatternFull, PatternStopped
from pyactors.patterns.messages import GetStats

__all__ = [
    "Ack",
    "Consume",
    "Offer",
    "Produce",
    "QueueActor",
    "QueueItem",
    "QueueStats",
]


@dataclass(frozen=True, slots=True)
class Produce:
    """Enqueue ``value``.  With ``wait=True`` (default) parks when full."""

    value: Any
    wait: bool = True


@dataclass(frozen=True, slots=True)
class Offer:
    """Non-blocking produce; returns ``True`` if accepted, ``False`` if full."""

    value: Any


@dataclass(frozen=True, slots=True)
class Consume:
    """Take the next item; parks until one is available (or timeout)."""

    timeout: float | None = None


@dataclass(frozen=True, slots=True)
class Ack:
    """Acknowledge successful processing of a previously consumed item."""

    item_id: str


@dataclass(frozen=True, slots=True)
class QueueItem:
    item_id: str
    value: Any


@dataclass(frozen=True, slots=True)
class QueueStats:
    size: int
    capacity: int | None
    produced: int
    consumed: int
    acked: int
    in_flight: int
    dropped: int


@dataclass(slots=True)
class _Pending:
    value: Any
    item_id: str


@dataclass(slots=True)
class _GetWaiter:
    future: asyncio.Future[Any]
    handle: asyncio.TimerHandle | None = None


class QueueActor(Actor[Any, Any]):
    """Generic in-memory FIFO queue with producer back-pressure.

    Delivery semantics
    ------------------
    * **At-most-once** when ``ack_required=False``.
    * **At-least-once within process** when ``ack_required=True`` until
      :class:`Ack`.  Not durable across process death.
    """

    def __init__(
        self,
        *,
        capacity: int | None = None,
        overflow: OverflowPolicy = OverflowPolicy.WAIT,
        ack_required: bool = False,
    ) -> None:
        if capacity is not None and capacity < 1:
            raise ValueError("capacity must be >= 1 or None (unbounded)")
        self._capacity = capacity
        self._overflow = overflow
        self._ack_required = ack_required
        self._items: deque[_Pending] = deque()
        self._put_waiters: deque[asyncio.Future[Any]] = deque()
        self._get_waiters: deque[_GetWaiter] = deque()
        self._in_flight: dict[str, _Pending] = {}
        self._produced = 0
        self._consumed = 0
        self._acked = 0
        self._dropped = 0
        self._closed = False

    async def on_stop(self) -> None:
        self._closed = True
        for pending in self._in_flight.values():
            self._items.appendleft(pending)
        self._in_flight.clear()
        err = PatternStopped("queue actor stopping")
        while self._put_waiters:
            fut = self._put_waiters.popleft()
            if not fut.done():
                fut.set_exception(err)
        while self._get_waiters:
            waiter = self._get_waiters.popleft()
            if waiter.handle is not None:
                waiter.handle.cancel()
            if not waiter.future.done():
                waiter.future.set_exception(err)

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return QueueStats(
                size=len(self._items),
                capacity=self._capacity,
                produced=self._produced,
                consumed=self._consumed,
                acked=self._acked,
                in_flight=len(self._in_flight),
                dropped=self._dropped,
            )
        if isinstance(message, Produce):
            return self._produce(message.value, wait=message.wait)
        if isinstance(message, Offer):
            return self._produce(message.value, wait=False, offer=True)
        if isinstance(message, Consume):
            return self._consume(message.timeout)
        if isinstance(message, Ack):
            return self._ack(message.item_id)
        return self._produce(message, wait=True)

    def _full(self) -> bool:
        return self._capacity is not None and len(self._items) >= self._capacity

    def _produce(self, value: Any, *, wait: bool, offer: bool = False) -> Any:
        if self._closed:
            raise ReplyError(PatternStopped("queue actor is stopped"))
        if self._full():
            if offer:
                self._dropped += 1
                return False
            if self._overflow is OverflowPolicy.DROP_NEWEST:
                self._dropped += 1
                return None
            if self._overflow is OverflowPolicy.DROP_OLDEST:
                if self._items:
                    self._items.popleft()
                    self._dropped += 1
            elif self._overflow is OverflowPolicy.REJECT or not wait:
                raise ReplyError(PatternFull("queue is full"))
            else:
                reply = self.context.detach_ask()
                if reply is None:
                    raise ReplyError(PatternFull("queue is full"))
                # Park producer; resume when space frees.
                self._put_waiters.append(reply)
                # Stash value on the future via a side table
                self._parked_puts: dict[asyncio.Future[Any], Any]
                if not hasattr(self, "_parked_puts"):
                    self._parked_puts = {}
                self._parked_puts[reply] = (value, offer)
                return None
        return self._enqueue(value, offer=offer)

    def _enqueue(self, value: Any, *, offer: bool) -> Any:
        pending = _Pending(value=value, item_id=uuid4().hex)
        if self._get_waiters:
            waiter = self._get_waiters.popleft()
            if waiter.handle is not None:
                waiter.handle.cancel()
            item = self._deliver(pending)
            if not waiter.future.done():
                waiter.future.set_result(item)
        else:
            self._items.append(pending)
        self._produced += 1
        return True if offer else pending.item_id

    def _consume(self, timeout: float | None) -> Any:
        if self._closed and not self._items:
            raise ReplyError(PatternStopped("queue actor is stopped"))
        if self._items:
            return self._deliver(self._items.popleft())
        reply = self.context.detach_ask()
        if reply is None:
            raise ReplyError(PatternStopped("Consume requires ask()"))
        waiter = _GetWaiter(future=reply)
        if timeout is not None:
            loop = asyncio.get_running_loop()

            def _timeout(w: _GetWaiter = waiter) -> None:
                with contextlib.suppress(ValueError):
                    self._get_waiters.remove(w)
                if not w.future.done():
                    w.future.set_exception(TimeoutError())

            waiter.handle = loop.call_later(timeout, _timeout)
        self._get_waiters.append(waiter)
        return None

    def _deliver(self, pending: _Pending) -> QueueItem:
        self._consumed += 1
        if self._ack_required:
            self._in_flight[pending.item_id] = pending
        self._wake_putter()
        return QueueItem(item_id=pending.item_id, value=pending.value)

    def _wake_putter(self) -> None:
        parked = getattr(self, "_parked_puts", None)
        while self._put_waiters and not self._full():
            fut = self._put_waiters.popleft()
            if fut.done():
                if parked is not None:
                    parked.pop(fut, None)
                continue
            if parked is not None and fut in parked:
                value, offer = parked.pop(fut)
                result = self._enqueue(value, offer=offer)
                fut.set_result(result)
                return
            fut.set_result(None)
            return

    def _ack(self, item_id: str) -> bool:
        pending = self._in_flight.pop(item_id, None)
        if pending is None:
            return False
        self._acked += 1
        return True
