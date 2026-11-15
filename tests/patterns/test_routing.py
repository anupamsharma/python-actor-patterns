from __future__ import annotations

import pytest

from pyactors import ActorSystem
from pyactors.patterns import (
    BroadcastRouter,
    ConsistentHashRouter,
    ContentBasedRouter,
    GetStats,
    PatternRejected,
    RandomRouter,
    RoundRobinRouter,
    SetRoutees,
)

from ..helpers import within
from .helpers import Echo


async def test_round_robin(system: ActorSystem) -> None:
    workers = [await system.spawn(Echo, name=f"w{i}") for i in range(3)]
    router = await system.spawn(RoundRobinRouter, workers)
    results = [await within(router.ask(i)) for i in range(6)]
    assert results == [0, 1, 2, 3, 4, 5]
    stats = await within(router.ask(GetStats()))
    assert stats.routed == 6


async def test_random_router_deterministic(system: ActorSystem) -> None:
    import random

    workers = [await system.spawn(Echo) for _ in range(4)]
    router = await system.spawn(RandomRouter, workers, rng=random.Random(0))
    results = [await within(router.ask(i)) for i in range(20)]
    assert len(set(results)) >= 1
    assert results == list(range(20))


async def test_consistent_hash_stable(system: ActorSystem) -> None:
    workers = [await system.spawn(Echo) for _ in range(5)]
    router = await system.spawn(ConsistentHashRouter, workers, key=lambda m: m["id"])
    a = await within(router.ask({"id": "cust-1", "n": 1}))
    b = await within(router.ask({"id": "cust-1", "n": 2}))
    assert a["id"] == b["id"] == "cust-1"


async def test_content_based(system: ActorSystem) -> None:
    odd = await system.spawn(Echo, name="odd")
    even = await system.spawn(Echo, name="even")
    router = await system.spawn(
        ContentBasedRouter,
        [(lambda m: m % 2 == 0, even), (lambda m: m % 2 == 1, odd)],
    )
    assert await within(router.ask(2)) == 2
    assert await within(router.ask(3)) == 3


async def test_content_based_no_match(system: ActorSystem) -> None:
    w = await system.spawn(Echo)
    router = await system.spawn(ContentBasedRouter, [(lambda m: False, w)])
    with pytest.raises(PatternRejected):
        await within(router.ask(1))
    assert router.is_alive


async def test_broadcast(system: ActorSystem) -> None:
    workers = [await system.spawn(Echo) for _ in range(3)]
    bcast = await system.spawn(BroadcastRouter, workers)
    assert await within(bcast.ask("hi")) == ["hi", "hi", "hi"]


async def test_set_routees(system: ActorSystem) -> None:
    a = await system.spawn(Echo)
    b = await system.spawn(Echo)
    router = await system.spawn(RoundRobinRouter, [a])
    await within(router.ask(SetRoutees((a, b))))
    stats = await within(router.ask(GetStats()))
    assert stats.routee_count == 2


async def test_empty_router_rejects(system: ActorSystem) -> None:
    router = await system.spawn(RoundRobinRouter, [])
    with pytest.raises(PatternRejected):
        await within(router.ask(1))
