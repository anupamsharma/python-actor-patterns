from __future__ import annotations

import asyncio
from typing import Any

from pyactors import Actor, ActorSystem

from .helpers import GET_LOG, Block, Counter, Probe, within


class Rendezvous(Actor[Any, str]):
    """Signals ``mine`` then waits for ``theirs``: only completes if both run at once."""

    def __init__(self, mine: asyncio.Event, theirs: asyncio.Event) -> None:
        self.mine = mine
        self.theirs = theirs

    async def receive(self, message: Any) -> str:
        self.mine.set()
        await self.theirs.wait()
        return "met"


async def test_two_actors_process_concurrently(system: ActorSystem) -> None:
    ev_a, ev_b = asyncio.Event(), asyncio.Event()
    a = await system.spawn(Rendezvous, ev_a, ev_b)
    b = await system.spawn(Rendezvous, ev_b, ev_a)
    assert list(await within(asyncio.gather(a.ask(1), b.ask(1)))) == ["met", "met"]


async def test_blocked_actor_does_not_block_others(system: ActorSystem) -> None:
    slow = await system.spawn(Probe)
    fast = await system.spawn(Probe)
    block = Block("slow")
    slow.tell(block)
    await within(block.entered.wait())
    for i in range(100):
        assert await within(fast.ask(i)) == i
    assert not block.finished
    block.release.set()
    assert await within(slow.ask(GET_LOG)) == ["start", "slow"]


class ConcurrencyTracker(Actor[Any, int]):
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0
        self.handled = 0

    async def receive(self, message: Any) -> int:
        if message == "stats":
            return self.max_active * 1_000_000 + self.handled
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.active -= 1
        self.handled += 1
        return self.handled


async def test_one_actor_never_processes_concurrently(system: ActorSystem) -> None:
    ref = await system.spawn(ConcurrencyTracker)

    async def producer() -> None:
        for i in range(50):
            if i % 3 == 0:
                await ref.ask(i)
            elif i % 3 == 1:
                await ref.send(i)
            else:
                ref.tell(i)

    await within(asyncio.gather(*(producer() for _ in range(20))))
    stats = await within(ref.ask("stats"))
    assert stats == 1 * 1_000_000 + 1000


async def test_thousands_of_messages(system: ActorSystem) -> None:
    ref = await system.spawn(Counter)
    for _ in range(20_000):
        ref.tell(1)
    assert await within(ref.ask("get"), 20) == 20_000


async def test_multiple_producers_lose_nothing_and_keep_per_producer_order(
    system: ActorSystem,
) -> None:
    ref = await system.spawn(Probe, mailbox_size=8)
    producers, per_producer = 25, 400

    async def produce(p: int) -> None:
        for i in range(per_producer):
            await ref.send((p, i))

    await within(asyncio.gather(*(produce(p) for p in range(producers))), 20)
    log = (await within(ref.ask(GET_LOG)))[1:]
    assert len(log) == producers * per_producer
    assert set(log) == {(p, i) for p in range(producers) for i in range(per_producer)}
    for p in range(producers):
        assert [i for q, i in log if q == p] == list(range(per_producer))


async def test_no_serialization_across_unrelated_actors(system: ActorSystem) -> None:
    """Each actor blocks until *all* actors are inside receive at the same time."""
    n = 200
    arrived = 0
    everyone = asyncio.Event()

    class Barrier(Actor[Any, int]):
        async def receive(self, message: Any) -> int:
            nonlocal arrived
            arrived += 1
            if arrived == n:
                everyone.set()
            await everyone.wait()
            return arrived

    refs = [await system.spawn(Barrier) for _ in range(n)]
    results = await within(asyncio.gather(*(r.ask(None) for r in refs)))
    assert results == [n] * n
