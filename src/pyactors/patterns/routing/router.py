"""Generic router actor and routing strategies."""

from __future__ import annotations

import random
from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import maybe_await
from pyactors.patterns.exceptions import PatternRejected
from pyactors.patterns.messages import GetStats

__all__ = [
    "BroadcastRouter",
    "ConsistentHashRouter",
    "ContentBasedRouter",
    "RandomRouter",
    "RoundRobinRouter",
    "Router",
    "RouterStats",
    "SetRoutees",
]


@dataclass(frozen=True, slots=True)
class SetRoutees:
    """Replace the router's routee list."""

    routees: tuple[ActorRef[Any, Any], ...]


@dataclass(frozen=True, slots=True)
class RouterStats:
    routed: int
    failed: int
    routee_count: int


class Router(Actor[Any, Any]):
    """Forward each message to one routee chosen by :meth:`select`.

    Subclasses implement :meth:`select`.  Control messages
    :class:`SetRoutees` and :class:`~pyactors.patterns.messages.GetStats`
    are handled here.
    """

    def __init__(self, routees: Sequence[ActorRef[Any, Any]] | None = None) -> None:
        self._routees: list[ActorRef[Any, Any]] = list(routees or ())
        self._routed = 0
        self._failed = 0

    def select(self, message: Any) -> ActorRef[Any, Any]:
        """Return the routee that should receive *message*."""
        raise NotImplementedError

    async def receive(self, message: Any) -> Any:
        if isinstance(message, SetRoutees):
            self._routees = list(message.routees)
            return len(self._routees)
        if isinstance(message, GetStats):
            return RouterStats(self._routed, self._failed, len(self._routees))
        if not self._routees:
            raise ReplyError(PatternRejected("router has no routees"))
        try:
            routee = self.select(message)
        except Exception as exc:
            self._failed += 1
            raise ReplyError(exc) from exc
        self._routed += 1
        try:
            return await routee.ask(message)
        except Exception as exc:
            self._failed += 1
            raise ReplyError(exc) from exc


class RoundRobinRouter(Router):
    """Distribute messages to routees in cyclic order."""

    def __init__(self, routees: Sequence[ActorRef[Any, Any]] | None = None) -> None:
        super().__init__(routees)
        self._index = 0

    def select(self, message: Any) -> ActorRef[Any, Any]:
        routee = self._routees[self._index % len(self._routees)]
        self._index = (self._index + 1) % len(self._routees)
        return routee


class RandomRouter(Router):
    """Choose a routee uniformly at random for each message."""

    def __init__(
        self,
        routees: Sequence[ActorRef[Any, Any]] | None = None,
        *,
        rng: random.Random | None = None,
    ) -> None:
        super().__init__(routees)
        self._rng = rng or random.Random()

    def select(self, message: Any) -> ActorRef[Any, Any]:
        return self._rng.choice(self._routees)


class ConsistentHashRouter(Router):
    """Route by hashing a key extracted from the message.

    Messages with the same key always go to the same routee as long as the
    routee set is unchanged.  Uses Python's built-in :func:`hash` by default
    (process-local; not stable across interpreter restarts).
    """

    def __init__(
        self,
        routees: Sequence[ActorRef[Any, Any]] | None = None,
        *,
        key: Callable[[Any], Hashable] | None = None,
        hasher: Callable[[Hashable], int] | None = None,
    ) -> None:
        super().__init__(routees)
        self._key = key or (lambda m: m)
        self._hasher = hasher or hash

    def select(self, message: Any) -> ActorRef[Any, Any]:
        h = self._hasher(self._key(message))
        return self._routees[h % len(self._routees)]


class ContentBasedRouter(Router):
    """Route by evaluating ordered ``(predicate, routee)`` rules.

    The first matching predicate wins.  If none match and ``default`` is set,
    that routee is used; otherwise :class:`PatternRejected` is raised.
    """

    def __init__(
        self,
        rules: Sequence[tuple[Callable[[Any], bool], ActorRef[Any, Any]]] | None = None,
        *,
        default: ActorRef[Any, Any] | None = None,
    ) -> None:
        super().__init__([r for _, r in (rules or ())])
        self._rules = list(rules or ())
        self._default = default

    async def receive(self, message: Any) -> Any:
        if isinstance(message, SetRoutees):
            raise PatternRejected("ContentBasedRouter does not support SetRoutees; rebuild rules")
        if isinstance(message, GetStats):
            return RouterStats(self._routed, self._failed, len(self._rules))
        for predicate, routee in self._rules:
            if await maybe_await(predicate(message)):
                self._routed += 1
                try:
                    return await routee.ask(message)
                except Exception as exc:
                    self._failed += 1
                    raise ReplyError(exc) from exc
        if self._default is not None:
            self._routed += 1
            try:
                return await self._default.ask(message)
            except Exception as exc:
                self._failed += 1
                raise ReplyError(exc) from exc
        self._failed += 1
        raise ReplyError(PatternRejected("no matching route"))

    def select(self, message: Any) -> ActorRef[Any, Any]:
        raise NotImplementedError("ContentBasedRouter uses receive() directly")


class BroadcastRouter(Actor[Any, list[Any]]):
    """Send every message to all routees and return their replies as a list.

    Replies are collected concurrently.  If any routee raises, the first
    exception is re-raised after all asks complete (``asyncio.gather``
    default).  Use :class:`~pyactors.patterns.messaging.scatter_gather.ScatterGather`
    for partial-result policies.
    """

    def __init__(self, routees: Sequence[ActorRef[Any, Any]] | None = None) -> None:
        self._routees: list[ActorRef[Any, Any]] = list(routees or ())
        self._routed = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, SetRoutees):
            self._routees = list(message.routees)
            return len(self._routees)
        if isinstance(message, GetStats):
            return RouterStats(self._routed, 0, len(self._routees))
        if not self._routees:
            raise ReplyError(PatternRejected("broadcast router has no routees"))
        import asyncio

        self._routed += 1
        try:
            return list(await asyncio.gather(*(r.ask(message) for r in self._routees)))
        except Exception as exc:
            raise ReplyError(exc) from exc
