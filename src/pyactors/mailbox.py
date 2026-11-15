"""Single-consumer FIFO mailbox with close semantics.

Why not ``asyncio.Queue``?  On Python 3.12 ``asyncio.Queue`` has no way to
close a queue: producers blocked in ``put()`` on a full queue can only be
released by cancelling their tasks, which we do not own.  An actor stop must
(a) reject new messages immediately, (b) wake every blocked producer with
``ActorStopped``/``ActorFailed`` and (c) never be blocked itself by a full
queue.  ``asyncio.Queue.shutdown()`` only exists from 3.13 onwards, so this
module implements the minimal equivalent: a ``deque`` plus one waiter Future
for the (single) consumer and a FIFO of waiter Futures for blocked producers.
It is also cheaper than ``asyncio.Queue`` because it skips the unused
``task_done``/``join`` bookkeeping.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable

from pyactors.envelope import Envelope
from pyactors.exceptions import MailboxFull

__all__ = ["Mailbox"]


class Mailbox:
    """FIFO queue of envelopes owned by exactly one actor.

    ``capacity == 0`` means unbounded.  Once closed, every enqueue attempt
    raises the exception produced by the ``closed_error`` factory given to
    :meth:`close`; items already queued stay available to the consumer until
    drained.
    """

    __slots__ = (
        "_capacity",
        "_closed_error",
        "_getter",
        "_items",
        "_loop",
        "_owner_id",
        "_putters",
    )

    def __init__(self, capacity: int, loop: asyncio.AbstractEventLoop, owner_id: str) -> None:
        self._items: deque[Envelope] = deque()
        self._capacity = capacity
        self._loop = loop
        self._owner_id = owner_id
        self._getter: asyncio.Future[None] | None = None
        self._putters: deque[asyncio.Future[None]] = deque()
        self._closed_error: Callable[[], Exception] | None = None

    @property
    def closed(self) -> bool:
        return self._closed_error is not None

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._items)

    # ------------------------------------------------------------------ producers

    def put_nowait(self, item: Envelope) -> None:
        """Enqueue without waiting; raise if closed or full."""
        if self._closed_error is not None:
            raise self._closed_error()
        if self._capacity and len(self._items) >= self._capacity:
            raise MailboxFull(self._owner_id)
        self._items.append(item)
        getter = self._getter
        if getter is not None and not getter.done():
            getter.set_result(None)

    async def put(self, item: Envelope) -> None:
        """Enqueue, waiting for capacity if the mailbox is bounded and full.

        Raises the close error if the mailbox is (or becomes) closed while
        waiting.  Cancellation leaves no waiter behind.
        """
        capacity = self._capacity
        if capacity:
            items = self._items
            while len(items) >= capacity and self._closed_error is None:
                waiter = self._loop.create_future()
                self._putters.append(waiter)
                try:
                    await waiter
                except BaseException:
                    waiter.cancel()
                    try:
                        self._putters.remove(waiter)
                    except ValueError:
                        # Already popped by a wake-up we will not use: pass it on.
                        if len(items) < capacity:
                            self._wake_putter()
                    raise
        self.put_nowait(item)

    # ------------------------------------------------------------------ consumer

    def pop_nowait(self) -> Envelope:
        """Remove the head item; the caller guarantees the mailbox is non-empty."""
        item = self._items.popleft()
        if self._putters:
            self._wake_putter()
        return item

    async def get(self) -> Envelope | None:
        """Return the next item, or ``None`` once the mailbox is closed and empty."""
        items = self._items
        while not items:
            if self._closed_error is not None:
                return None
            getter = self._loop.create_future()
            self._getter = getter
            try:
                await getter
            finally:
                self._getter = None
        return self.pop_nowait()

    # ------------------------------------------------------------------ control

    def close(self, closed_error: Callable[[], Exception]) -> bool:
        """Reject further enqueues and wake all waiters.  Idempotent.

        Returns ``True`` if this call closed the mailbox.
        """
        if self._closed_error is not None:
            return False
        self._closed_error = closed_error
        getter = self._getter
        if getter is not None and not getter.done():
            getter.set_result(None)
        self._wake_all_putters()
        return True

    def drain(self) -> list[Envelope]:
        """Remove and return every queued item."""
        drained = list(self._items)
        self._items.clear()
        self._wake_all_putters()
        return drained

    def _wake_putter(self) -> None:
        putters = self._putters
        while putters:
            waiter = putters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    def _wake_all_putters(self) -> None:
        putters = self._putters
        while putters:
            waiter = putters.popleft()
            if not waiter.done():
                waiter.set_result(None)
