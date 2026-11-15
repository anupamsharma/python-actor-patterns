"""Typed event bus actor."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import GetStats

__all__ = ["Emit", "EventBus", "Off", "On"]


@dataclass(frozen=True, slots=True)
class On:
    """Register a handler callable or subscriber ref for an event type."""

    event_type: type
    handler: Callable[[Any], Any] | ActorRef[Any, Any]


@dataclass(frozen=True, slots=True)
class Off:
    event_type: type
    handler: Callable[[Any], Any] | ActorRef[Any, Any]


@dataclass(frozen=True, slots=True)
class Emit:
    event: Any


class EventBus(Actor[Any, Any]):
    """Dispatch events to handlers registered by concrete type.

    Handlers may be async/sync callables or :class:`ActorRef`s (``tell``).
    Inheritance is not walked — register the exact ``type(event)``.
    """

    def __init__(self) -> None:
        self._handlers: dict[type, list[Callable[[Any], Any] | ActorRef[Any, Any]]] = defaultdict(
            list
        )
        self._emitted = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "types": {t.__name__: len(h) for t, h in self._handlers.items()},
                "emitted": self._emitted,
            }
        if isinstance(message, On):
            handlers = self._handlers[message.event_type]
            if message.handler not in handlers:
                handlers.append(message.handler)
            return len(handlers)
        if isinstance(message, Off):
            existing = self._handlers.get(message.event_type)
            if not existing:
                return False
            try:
                existing.remove(message.handler)
            except ValueError:
                return False
            return True
        if isinstance(message, Emit):
            return await self._emit(message.event)
        # Bare objects are treated as events.
        return await self._emit(message)

    async def _emit(self, event: Any) -> int:
        self._emitted += 1
        handlers = list(self._handlers.get(type(event), ()))
        count = 0
        for handler in handlers:
            if isinstance(handler, ActorRef):
                try:
                    handler.tell(event)
                    count += 1
                except Exception:
                    continue
            else:
                await maybe_await(handler(event))
                count += 1
        return count
