"""Shared types and utilities used by reusable actor patterns.

Patterns deliver **at-most-once** processing of accepted messages. They do not
provide durable storage, exactly-once delivery, or cross-process coordination.
"""

from __future__ import annotations

import enum
import inspect
from collections.abc import Awaitable, Callable, Hashable
from typing import Any, TypeVar

__all__ = [
    "KeyExtractor",
    "OverflowPolicy",
    "Predicate",
    "Routee",
    "SyncOrAsyncHandler",
    "maybe_await",
]

M = TypeVar("M")
R = TypeVar("R")
K = TypeVar("K", bound=Hashable)


class OverflowPolicy(enum.Enum):
    """What to do when a bounded buffer or mailbox has no free capacity."""

    REJECT = "reject"
    """Raise :class:`~pyactors.patterns.exceptions.PatternFull` immediately."""

    DROP_NEWEST = "drop_newest"
    """Discard the incoming item and keep existing buffered items."""

    DROP_OLDEST = "drop_oldest"
    """Discard the oldest buffered item to make room for the new one."""

    WAIT = "wait"
    """Park the producer (via ``ask``) until capacity is available."""


SyncOrAsyncHandler = Callable[[Any], Any | Awaitable[Any]]
Predicate = Callable[[Any], bool]
KeyExtractor = Callable[[Any], Hashable]
Routee = Any  # ActorRef[Any, Any] — kept loose to avoid circular imports


async def maybe_await[T](value: T | Awaitable[T]) -> T:
    """Await *value* if it is awaitable, otherwise return it unchanged."""
    if inspect.isawaitable(value):
        return await value
    return value
