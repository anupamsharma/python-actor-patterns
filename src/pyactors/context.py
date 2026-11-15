"""Per-actor context exposed to actor code."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyactors.actor_cell import ActorCell
    from pyactors.actor_ref import ActorRef
    from pyactors.actor_system import ActorSystem

__all__ = ["ActorContext"]


class ActorContext:
    """Runtime services available to an actor via ``self.context``."""

    __slots__ = ("_cell",)

    def __init__(self, cell: ActorCell) -> None:
        self._cell = cell

    @property
    def ref(self) -> ActorRef[Any, Any]:
        """This actor's own :class:`~pyactors.ActorRef`."""
        return self._cell.ref

    @property
    def system(self) -> ActorSystem:
        """The owning :class:`~pyactors.ActorSystem`."""
        return self._cell.system

    @property
    def name(self) -> str:
        return self._cell.name

    @property
    def actor_id(self) -> str:
        return self._cell.actor_id

    def detach_ask(self) -> asyncio.Future[Any] | None:
        """Take ownership of the current ``ask`` Future, if any.

        After this call the runtime will not complete the Future when
        ``receive`` returns.  Pattern actors that park a request (barriers,
        queues, debouncers, …) must complete it later with ``set_result`` /
        ``set_exception``.  Returns ``None`` when the message was delivered
        with ``tell``/``send``.
        """
        return self._cell.detach_ask()

    def stop(self, *, graceful: bool = True) -> None:
        """Request this actor to stop; returns immediately (never deadlocks).

        The mailbox is closed at once.  The current ``receive`` call runs to
        completion; with ``graceful=True`` already-accepted messages are then
        processed, with ``graceful=False`` they are discarded (pending asks
        fail with ``ActorStopped``).  ``on_stop`` runs afterwards.
        """
        self._cell.request_stop(graceful)
