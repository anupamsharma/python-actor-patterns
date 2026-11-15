"""Immutable public handle to an actor."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, NoReturn

from pyactors.envelope import Envelope

if TYPE_CHECKING:
    from pyactors.actor_cell import ActorCell
    from pyactors.lifecycle import LifecycleState
    from pyactors.mailbox import Mailbox

__all__ = ["ActorRef"]


class ActorRef[M, R]:
    """Lightweight, immutable reference used to message an actor.

    A ref is bound to one actor *incarnation*: if an actor stops and a new
    actor is spawned under the same name, old refs keep pointing at the
    stopped actor and raise :class:`~pyactors.ActorStopped`.

    Refs must only be used from the event loop that owns the actor system.
    """

    __slots__ = ("_cell", "_loop", "_mailbox")

    _cell: ActorCell
    _mailbox: Mailbox
    _loop: asyncio.AbstractEventLoop

    def __init__(self, cell: ActorCell) -> None:
        object.__setattr__(self, "_cell", cell)
        object.__setattr__(self, "_mailbox", cell.mailbox)
        object.__setattr__(self, "_loop", cell.loop)

    def __setattr__(self, name: str, value: object) -> NoReturn:
        raise AttributeError("ActorRef is immutable")

    def __delattr__(self, name: str) -> NoReturn:
        raise AttributeError("ActorRef is immutable")

    def __reduce__(self) -> NoReturn:
        raise TypeError("ActorRef cannot be pickled (local actors only)")

    def __repr__(self) -> str:
        return f"<ActorRef {self._cell.actor_id} {self._cell.state.value}>"

    @property
    def actor_id(self) -> str:
        """Unique identity of this actor incarnation, e.g. ``"counter#3"``."""
        return self._cell.actor_id

    @property
    def name(self) -> str:
        """Registered name (explicit or generated)."""
        return self._cell.name

    @property
    def is_alive(self) -> bool:
        """True while the actor accepts messages (``STARTING`` or ``RUNNING``)."""
        return not self._mailbox.closed

    @property
    def state(self) -> LifecycleState:
        """Current lifecycle state (diagnostic; may change at the next ``await``)."""
        return self._cell.state

    def tell(self, message: M) -> None:
        """Enqueue ``message`` without waiting (fire and forget).

        Raises :class:`~pyactors.MailboxFull` if a bounded mailbox is full and
        :class:`~pyactors.ActorStopped` if the actor no longer accepts
        messages.
        """
        self._mailbox.put_nowait(Envelope(message, None))

    async def send(self, message: M) -> None:
        """Enqueue ``message``, waiting for mailbox capacity if necessary.

        Returns once the message is *accepted* (not processed).  Raises
        :class:`~pyactors.ActorStopped` if the actor stops before or while
        waiting.  Cancelling the caller withdraws the pending enqueue.
        """
        await self._mailbox.put(Envelope(message, None))

    async def ask(self, message: M, timeout: float | None = None) -> R:
        """Send ``message`` and wait for the value returned by ``receive``.

        ``timeout`` (seconds) covers both waiting for mailbox capacity and
        waiting for the reply; on expiry ``TimeoutError`` is raised.  A
        timeout or cancellation never interrupts a handler that is already
        running; a request that is still queued when its caller gives up is
        dropped without being processed.

        Raises the handler's exception if ``receive`` fails,
        :class:`~pyactors.ActorFailed` if the actor failed on another message
        first, and :class:`~pyactors.ActorStopped` if the actor stops before
        handling the request.
        """
        reply: asyncio.Future[R] = self._loop.create_future()
        envelope = Envelope(message, reply)
        mailbox = self._mailbox
        if timeout is None:
            await mailbox.put(envelope)
            return await reply
        async with asyncio.timeout(timeout):
            await mailbox.put(envelope)
            return await reply

    async def stop(self, *, graceful: bool = True) -> None:
        """Shorthand for ``await system.stop(ref, graceful=graceful)``."""
        await self._cell.system.stop(self, graceful=graceful)

    @staticmethod
    def _cell_of(ref: ActorRef[Any, Any]) -> ActorCell:
        return ref._cell
