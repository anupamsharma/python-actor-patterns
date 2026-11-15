from __future__ import annotations

from pyactors import ActorSystem
from pyactors.patterns import (
    MapReduce,
    MapReduceJob,
    Pipeline,
    TimedEvent,
    Window,
    WindowKind,
)

from ..helpers import within
from .helpers import Adder


async def test_pipeline_callables_and_refs(system: ActorSystem) -> None:
    adder = await system.spawn(Adder, 10)

    async def double(x: int) -> int:
        return x * 2

    pipe = await system.spawn(Pipeline, [double, adder])
    assert await within(pipe.ask(5)) == 20


async def test_map_reduce(system: ActorSystem) -> None:
    mr = await system.spawn(
        MapReduce,
        mapper=lambda x: x * 2,
        reducer=lambda a, b: a + b,
        initial=0,
    )
    assert await within(mr.ask(MapReduceJob(items=(1, 2, 3, 4)))) == 20


async def test_window_tumbling(system: ActorSystem) -> None:
    sums: list[int] = []

    def agg(items):  # type: ignore[no-untyped-def]
        total = sum(items)
        sums.append(total)
        return total

    win = await system.spawn(
        Window, size=10.0, kind=WindowKind.TUMBLING, aggregator=agg, clock=lambda: 0.0
    )
    # Use explicit timestamps
    await within(win.ask(TimedEvent(1, 0.0)))
    await within(win.ask(TimedEvent(2, 5.0)))
    result = await within(win.ask(TimedEvent(3, 10.0)))  # closes [0,10)
    assert result.items == (1, 2)
    assert sums == [3]
