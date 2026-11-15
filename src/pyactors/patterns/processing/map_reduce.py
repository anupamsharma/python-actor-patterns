"""Map-reduce coordinator actor."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import GetStats

__all__ = ["MapReduce", "MapReduceJob"]

Mapper = Callable[[Any], Any]
Reducer = Callable[[Any, Any], Any]


@dataclass(frozen=True, slots=True)
class MapReduceJob:
    items: tuple[Any, ...]
    mapper: Mapper | None = None
    reducer: Reducer | None = None
    initial: Any = None


class MapReduce(Actor[Any, Any]):
    """Map items (optionally via worker refs) then reduce to a single value.

    When ``workers`` are provided, map work is fanned out with ``ask`` using
    the job's items as messages (workers must understand the item type).
    Otherwise ``mapper`` runs locally (concurrently via gather for async
    mappers, sequentially for sync).
    """

    def __init__(
        self,
        *,
        mapper: Mapper | None = None,
        reducer: Reducer | None = None,
        workers: Sequence[ActorRef[Any, Any]] | None = None,
        initial: Any = None,
    ) -> None:
        self._mapper = mapper
        self._reducer = reducer
        self._workers = list(workers or ())
        self._initial = initial
        self._jobs = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {"jobs": self._jobs, "workers": len(self._workers)}
        if isinstance(message, MapReduceJob):
            items = message.items
            mapper = message.mapper or self._mapper
            reducer = message.reducer or self._reducer
            initial = self._initial if message.initial is None else message.initial
        elif isinstance(message, (list, tuple)):
            items = tuple(message)
            mapper, reducer, initial = self._mapper, self._reducer, self._initial
        else:
            raise TypeError("MapReduce expects MapReduceJob or a sequence of items")
        if reducer is None:
            raise ValueError("reducer is required")
        self._jobs += 1
        mapped = await self._map(items, mapper)
        return self._reduce(mapped, reducer, initial)

    async def _map(self, items: Sequence[Any], mapper: Mapper | None) -> list[Any]:
        if self._workers:
            tasks = [
                self._workers[i % len(self._workers)].ask(item) for i, item in enumerate(items)
            ]
            return list(await asyncio.gather(*tasks))
        if mapper is None:
            return list(items)
        results = []
        for item in items:
            results.append(await maybe_await(mapper(item)))
        return results

    @staticmethod
    def _reduce(values: Iterable[Any], reducer: Reducer, initial: Any) -> Any:
        iterator = iter(values)
        if initial is None:
            try:
                acc = next(iterator)
            except StopIteration:
                return None
        else:
            acc = initial
        for value in iterator:
            acc = reducer(acc, value)
        return acc
