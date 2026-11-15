"""The user-facing :class:`Actor` base class."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyactors.context import ActorContext

__all__ = ["Actor"]


class Actor[M, R]:
    """Base class for actors.

    Subclass it and implement :meth:`receive`.  ``M`` is the message type and
    ``R`` the type returned to ``ask()`` callers::

        class Counter(Actor[int, int]):
            def __init__(self) -> None:
                self.total = 0

            async def receive(self, message: int) -> int:
                self.total += message
                return self.total

    Guarantees provided by the runtime:

    * ``receive`` is never executed concurrently for the same actor; messages
      are handled one at a time in mailbox (FIFO) order.
    * ``on_start`` completes before ``spawn()`` returns and before the first
      message is handled; ``on_stop`` runs exactly once if ``on_start``
      succeeded, after the last message.

    Actor state is just ordinary instance attributes.  The framework never
    hands the actor object to other code, but Python cannot stop you from
    deliberately sharing a mutable object (for example passing the same list
    to two actors' constructors or inside a message); doing so voids the
    isolation guarantee.

    The context (:attr:`context`) is attached after ``__init__`` returns, so
    use ``on_start`` for initialisation that needs it.
    """

    __slots__ = ("__weakref__", "_pyactors_context")

    _pyactors_context: ActorContext

    @property
    def context(self) -> ActorContext:
        """The actor's :class:`~pyactors.ActorContext` (self ref, system, self-stop)."""
        try:
            return self._pyactors_context
        except AttributeError:
            raise RuntimeError(
                "actor context is not available yet; it is attached after __init__ "
                "(use on_start for initialisation that needs the context)"
            ) from None

    async def on_start(self) -> None:
        """Called once before any message is processed.

        If it raises, ``spawn()`` re-raises the exception, the actor is
        unregistered and ``on_stop`` is **not** called.
        """

    async def receive(self, message: M) -> R:
        """Handle one message; the return value answers ``ask()``.

        If it raises, the exception is delivered to the asking caller (if
        any), the actor transitions to ``FAILED``, queued messages are
        discarded and the actor stops.  Catch exceptions inside ``receive``
        and return an error value if the actor should survive them.
        """
        raise NotImplementedError

    async def on_stop(self) -> None:
        """Called once after the last message (also after a ``receive`` failure).

        Exceptions are reported to the system's failure handler and do not
        prevent the actor from terminating.
        """
