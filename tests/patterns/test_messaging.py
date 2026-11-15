from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import (
    CompletionPolicy,
    Flush,
    PartialRequest,
    RequestAggregator,
    ScatterGather,
    ScatterRequest,
)

from ..helpers import within
from .helpers import Echo


async def test_scatter_all(system: ActorSystem) -> None:
    workers = [await system.spawn(Echo) for _ in range(3)]
    sg = await system.spawn(ScatterGather, workers, timeout=2.0)
    result = await within(sg.ask("x"))
    assert result.successes == ["x", "x", "x"]
    assert not result.timed_out


async def test_scatter_any(system: ActorSystem) -> None:
    workers = [await system.spawn(Echo) for _ in range(3)]
    sg = await system.spawn(ScatterGather, workers, policy=CompletionPolicy.ANY, timeout=2.0)
    result = await within(sg.ask(ScatterRequest("y")))
    assert len(result.successes) >= 1


async def test_scatter_quorum(system: ActorSystem) -> None:
    workers = [await system.spawn(Echo) for _ in range(3)]
    sg = await system.spawn(
        ScatterGather, workers, policy=CompletionPolicy.QUORUM, quorum=2, timeout=2.0
    )
    result = await within(sg.ask("z"))
    assert len(result.successes) >= 2


async def test_scatter_timeout_partial(system: ActorSystem) -> None:
    class Slow(Echo):
        async def receive(self, message):  # type: ignore[no-untyped-def]
            await asyncio.Event().wait()

    slow = await system.spawn(Slow)
    fast = await system.spawn(Echo)
    sg = await system.spawn(ScatterGather, [fast, slow], timeout=0.05, policy=CompletionPolicy.ALL)
    result = await within(sg.ask("go"), 2)
    assert result.timed_out
    assert "go" in result.successes
    assert sg.is_alive


async def test_request_aggregator(system: ActorSystem) -> None:
    agg = await system.spawn(RequestAggregator, expected_parts=3, timeout=2.0)
    t1 = asyncio.create_task(agg.ask(PartialRequest("g", 1, 3)))
    t2 = asyncio.create_task(agg.ask(PartialRequest("g", 2, 3)))
    t3 = asyncio.create_task(agg.ask(PartialRequest("g", 3, 3)))
    results = await within(asyncio.gather(t1, t2, t3))
    assert all(r.group_id == "g" for r in results)
    assert sorted(results[0].parts) == [1, 2, 3]


async def test_request_aggregator_flush(system: ActorSystem) -> None:
    agg = await system.spawn(RequestAggregator, expected_parts=10, timeout=5.0)
    pending = asyncio.create_task(agg.ask(PartialRequest("g", "a")))
    await asyncio.sleep(0)
    await within(agg.ask(Flush()))
    result = await within(pending)
    assert result.parts == ["a"]
