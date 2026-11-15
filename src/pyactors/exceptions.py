"""Public exception types raised by pyactors."""

from __future__ import annotations

__all__ = [
    "ActorError",
    "ActorFailed",
    "ActorNameTaken",
    "ActorStopped",
    "ActorSystemClosed",
    "MailboxFull",
    "ReplyError",
]


class ActorError(Exception):
    """Base class for every exception raised by the pyactors runtime."""


class ActorStopped(ActorError):
    """The actor cannot accept or will never process the message.

    Raised by ``tell``/``send``/``ask`` when the target actor is stopping or
    terminated, by pending ``ask`` calls whose message was discarded, and by
    ``spawn`` when the actor was stopped before its startup completed.
    """

    def __init__(self, actor_id: str, *args: object) -> None:
        super().__init__(actor_id, *args)
        self.actor_id = actor_id

    def __str__(self) -> str:
        return f"actor {self.actor_id!r} is stopped"


class ActorFailed(ActorStopped):
    """The actor terminated because ``receive()`` raised.

    ``cause`` holds the original exception. Because this is a subclass of
    :class:`ActorStopped`, code that only cares whether a message can be
    delivered can catch ``ActorStopped``.
    """

    def __init__(self, actor_id: str, cause: BaseException | None = None) -> None:
        super().__init__(actor_id, cause)
        self.cause = cause
        self.__cause__ = cause

    def __str__(self) -> str:
        return f"actor {self.actor_id!r} failed: {self.cause!r}"


class MailboxFull(ActorError):
    """``tell()`` was called on a bounded mailbox that has no free capacity."""

    def __init__(self, actor_id: str, *args: object) -> None:
        super().__init__(actor_id, *args)
        self.actor_id = actor_id

    def __str__(self) -> str:
        return f"mailbox of actor {self.actor_id!r} is full"


class ActorNameTaken(ActorError):
    """``spawn()`` was given a name that is already registered."""

    def __init__(self, name: str, *args: object) -> None:
        super().__init__(name, *args)
        self.name = name

    def __str__(self) -> str:
        return f"actor name {self.name!r} is already in use"


class ActorSystemClosed(ActorError):
    """The actor system is shutting down (or has shut down) and rejects new actors."""


class ReplyError(ActorError):
    """Fail the current ``ask`` without failing the actor.

    Raise from ``receive`` to deliver ``cause`` to the asking caller while
    leaving the actor in ``RUNNING``.  Useful for reusable pattern actors
    (retry, circuit breaker, routers) that must survive target failures.
    Uncaught exceptions that are not :class:`ReplyError` still fail the actor.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(cause)
        self.cause = cause
        self.__cause__ = cause

    def __str__(self) -> str:
        return f"reply error: {self.cause!r}"
