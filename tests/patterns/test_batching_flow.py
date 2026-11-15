from __future__ import annotations

import asyncio

import pytest

from pyactors import ActorSystem
from pyactors.patterns import (
    Batch,
    Debounce,
    Flush,
    OverflowPolicy,
    RateLimited,
    RateLimiter,
    Throttle,
)

from ..helpers import within


async def test_batch_by_size(system: ActorSystem) -> None:
    batches: list[tuple[object, ...]] = []

    async def handler(items):  # type: ignore[no-untyped-def]
        batches.append(tuple(items))
        return len(items)

    batch = await system.spawn(Batch, handler, max_size=3, max_wait=None)
    for i in range(6):
        await within(batch.ask(i))
    assert batches == [(0, 1, 2), (3, 4, 5)]


async def test_batch_flush_and_shutdown(system: ActorSystem) -> None:
    batches: list[tuple[object, ...]] = []

    async def handler(items):  # type: ignore[no-untyped-def]
        batches.append(tuple(items))

    batch = await system.spawn(Batch, handler, max_size=100, max_wait=None)
    await within(batch.ask(1))
    await within(batch.ask(2))
    await within(batch.ask(Flush()))
    assert batches == [(1, 2)]
    await within(batch.ask(3))
    await system.stop(batch)
    assert batches[-1] == (3,)


async def test_debounce_trailing(system: ActorSystem) -> None:
    emitted: list[int] = []

    async def handler(value: int) -> int:
        emitted.append(value)
        return value

    deb = await system.spawn(Debounce, 0.05, handler=handler)
    t1 = asyncio.create_task(deb.ask(1))
    await asyncio.sleep(0.01)
    t2 = asyncio.create_task(deb.ask(2))
    await asyncio.sleep(0.01)
    t3 = asyncio.create_task(deb.ask(3))
    results = await within(asyncio.gather(t1, t2, t3), 2)
    assert results == [3, 3, 3]
    assert emitted == [3]


async def test_debounce_flush(system: ActorSystem) -> None:
    deb = await system.spawn(Debounce, 10.0)
    pending = asyncio.create_task(deb.ask("x"))
    await asyncio.sleep(0)
    await within(deb.ask(Flush()))
    assert await within(pending) == "x"


async def test_throttle_spacing(system: ActorSystem) -> None:
    th = await system.spawn(Throttle, 0.05, overflow=OverflowPolicy.WAIT)
    t0 = asyncio.get_running_loop().time()
    await within(th.ask(1))
    await within(th.ask(2), 2)
    elapsed = asyncio.get_running_loop().time() - t0
    assert elapsed >= 0.04


async def test_rate_limiter_reject(system: ActorSystem) -> None:
    lim = await system.spawn(RateLimiter, rate=1, per=1.0, burst=1, mode="reject")
    assert await within(lim.ask("a")) == "a"
    with pytest.raises(RateLimited):
        await within(lim.ask("b"))
    assert lim.is_alive


async def test_rate_limiter_wait(system: ActorSystem) -> None:
    lim = await system.spawn(RateLimiter, rate=20, per=1.0, burst=1, mode="wait")
    await within(lim.ask(1))
    await within(lim.ask(2), 2)
