"""LRU + TTL cache actor."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.patterns.messages import GetStats

__all__ = ["Cache", "CacheDelete", "CacheGet", "CachePut", "CacheStats"]


@dataclass(frozen=True, slots=True)
class CacheGet:
    key: Hashable
    default: Any = None


@dataclass(frozen=True, slots=True)
class CachePut:
    key: Hashable
    value: Any
    ttl: float | None = None


@dataclass(frozen=True, slots=True)
class CacheDelete:
    key: Hashable


@dataclass(frozen=True, slots=True)
class CacheStats:
    size: int
    hits: int
    misses: int
    evictions: int


@dataclass(slots=True)
class _Entry:
    value: Any
    expires_at: float | None


class Cache(Actor[Any, Any]):
    """In-memory cache with optional max size (LRU) and per-entry TTL.

    Concurrent access is serialized by the actor.  Not durable.
    """

    def __init__(self, *, max_size: int | None = 1024, default_ttl: float | None = None) -> None:
        if max_size is not None and max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._data: OrderedDict[Hashable, _Entry] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            self._purge_expired()
            return CacheStats(len(self._data), self._hits, self._misses, self._evictions)
        if isinstance(message, CacheGet):
            return self._get(message.key, message.default)
        if isinstance(message, CachePut):
            return self._put(message.key, message.value, message.ttl)
        if isinstance(message, CacheDelete):
            return self._data.pop(message.key, None) is not None
        if isinstance(message, tuple) and len(message) == 2 and message[0] == "get":
            return self._get(message[1], None)
        raise TypeError(f"unsupported cache message: {type(message)!r}")

    def _purge_expired(self) -> None:
        now = time.monotonic()
        expired = [
            key
            for key, entry in self._data.items()
            if entry.expires_at is not None and entry.expires_at <= now
        ]
        for key in expired:
            del self._data[key]
            self._evictions += 1

    def _get(self, key: Hashable, default: Any) -> Any:
        self._purge_expired()
        entry = self._data.get(key)
        if entry is None:
            self._misses += 1
            return default
        self._data.move_to_end(key)
        self._hits += 1
        return entry.value

    def _put(self, key: Hashable, value: Any, ttl: float | None) -> None:
        self._purge_expired()
        effective = self._default_ttl if ttl is None else ttl
        expires = None if effective is None else time.monotonic() + effective
        self._data[key] = _Entry(value=value, expires_at=expires)
        self._data.move_to_end(key)
        if self._max_size is not None:
            while len(self._data) > self._max_size:
                self._data.popitem(last=False)
                self._evictions += 1
        return None
