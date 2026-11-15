"""The :class:`ActorSystem`: actor creation, registry and shutdown."""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections.abc import Callable
from types import TracebackType
from typing import Any, Self

from pyactors.actor import Actor
from pyactors.actor_cell import ActorCell
from pyactors.actor_ref import ActorRef
from pyactors.exceptions import ActorNameTaken, ActorSystemClosed

__all__ = ["ActorSystem", "FailureHandler"]

logger = logging.getLogger("pyactors")

FailureHandler = Callable[[ActorRef[Any, Any], BaseException], None]
"""Called with ``(ref, exception)`` when ``receive()`` or ``on_stop()`` raises."""


def _log_failure(ref: ActorRef[Any, Any], exc: BaseException) -> None:
    logger.error("actor %s raised an exception", ref.actor_id, exc_info=exc)


class ActorSystem:
    """Creates, tracks and stops actors on one asyncio event loop.

    The system is **not thread-safe**: create it, spawn actors and use refs
    from a single event loop (the first ``spawn`` binds the system to the
    running loop).

    Use it as an async context manager to guarantee shutdown::

        async with ActorSystem() as system:
            ref = await system.spawn(Counter, name="counter")
            await ref.ask(1)

    Leaving the block shuts the system down gracefully (immediately if the
    block is exited via ``CancelledError``/``KeyboardInterrupt``).
    """

    __slots__ = ("_cells", "_closing", "_failure_handler", "_ids", "_loop", "name")

    def __init__(self, name: str = "pyactors", *, failure_handler: FailureHandler | None = None):
        self.name = name
        self._cells: dict[str, ActorCell] = {}
        self._ids = itertools.count(1)
        self._closing = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._failure_handler: FailureHandler = failure_handler or _log_failure

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        graceful = exc_type is None or issubclass(exc_type, Exception)
        await self.shutdown(graceful=graceful)

    def __repr__(self) -> str:
        return f"<ActorSystem {self.name!r} actors={len(self._cells)} closing={self._closing}>"

    @property
    def actor_count(self) -> int:
        """Number of registered actors that have not yet terminated."""
        return len(self._cells)

    @property
    def is_closing(self) -> bool:
        """True once ``shutdown()`` has been called."""
        return self._closing

    def get(self, name: str) -> ActorRef[Any, Any] | None:
        """Return the ref registered under ``name``, or ``None`` if there is none.

        Names are registered when ``spawn`` is called and released when the
        actor terminates, so a starting or stopping actor is still returned.
        """
        cell = self._cells.get(name)
        return None if cell is None else cell.ref

    async def spawn[M, R](
        self,
        actor_type: Callable[..., Actor[M, R]],
        /,
        *args: Any,
        name: str | None = None,
        mailbox_size: int = 0,
        **kwargs: Any,
    ) -> ActorRef[M, R]:
        """Create an actor, run its ``on_start`` and return its ref.

        ``actor_type(*args, **kwargs)`` builds the instance.  ``mailbox_size``
        of ``0`` means unbounded.  ``name`` defaults to a generated
        ``"$<n>"``; explicit names must not start with ``"$"``.

        Raises :class:`ActorSystemClosed` after shutdown began,
        :class:`ActorNameTaken` for duplicate names, whatever ``on_start``
        raised if startup failed, and :class:`ActorStopped` if the actor was
        stopped (e.g. by ``shutdown``) before startup completed.  If the
        caller is cancelled during startup the actor is stopped immediately.
        """
        if self._closing:
            raise ActorSystemClosed(f"actor system {self.name!r} is shut down")
        if mailbox_size < 0:
            raise ValueError("mailbox_size must be >= 0 (0 means unbounded)")
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("ActorSystem is bound to a different event loop")
        uid = next(self._ids)
        if name is None:
            name = f"${uid}"
        else:
            if not isinstance(name, str) or not name:
                raise ValueError("actor name must be a non-empty string")
            if name.startswith("$"):
                raise ValueError("actor names starting with '$' are reserved")
            if name in self._cells:
                raise ActorNameTaken(name)
        actor = actor_type(*args, **kwargs)
        if not isinstance(actor, Actor):
            raise TypeError(f"{actor_type!r} did not produce an Actor instance")
        if type(actor).receive is Actor.receive:
            raise TypeError(f"{type(actor).__name__} must implement receive()")
        # Constructing the actor may have run arbitrary code; re-check races.
        if self._closing:
            raise ActorSystemClosed(f"actor system {self.name!r} is shut down")
        if name in self._cells:
            raise ActorNameTaken(name)
        cell = ActorCell(self, actor, name, f"{name}#{uid}", mailbox_size, loop)
        self._cells[name] = cell
        cell.start()
        await cell.wait_started()
        return cell.ref

    async def stop(self, ref: ActorRef[Any, Any], *, graceful: bool = True) -> None:
        """Stop an actor and wait until it has terminated.

        * ``graceful=True``: reject new messages, process already-accepted
          ones (answering their asks), run ``on_stop``.
        * ``graceful=False``: reject new messages, discard queued ones (their
          asks fail with ``ActorStopped``), cancel the running handler, run
          ``on_stop``.  Cancellation is cooperative: code that blocks the
          event loop or swallows ``CancelledError`` cannot be interrupted.

        Stopping an already stopped actor is a no-op.  When called from the
        actor's own task (self-stop) it only initiates the stop and returns
        immediately instead of deadlocking.
        """
        cell = ActorRef._cell_of(ref)
        if cell.system is not self:
            raise ValueError(f"{ref!r} does not belong to this actor system")
        cell.request_stop(graceful)
        if cell.task is not asyncio.current_task():
            await cell.wait_terminated()

    async def shutdown(self, *, graceful: bool = True) -> None:
        """Stop every actor and reject new spawns.  Idempotent.

        Actors still starting are stopped too (their ``spawn`` raises
        ``ActorStopped``).  A later ``graceful=False`` call escalates an
        in-progress graceful shutdown.  When called from inside an actor, that
        actor is asked to stop but not awaited.
        """
        self._closing = True
        cells = list(self._cells.values())
        for cell in cells:
            cell.request_stop(graceful)
        current = asyncio.current_task()
        for cell in cells:
            if cell.task is not current:
                await cell.wait_terminated()

    # ------------------------------------------------------------------ runtime hooks

    def _unregister(self, cell: ActorCell) -> None:
        if self._cells.get(cell.name) is cell:
            del self._cells[cell.name]

    def _report_failure(self, ref: ActorRef[Any, Any], exc: BaseException) -> None:
        try:
            self._failure_handler(ref, exc)
        except Exception:
            logger.exception("failure handler raised while reporting %r for %s", exc, ref.actor_id)
