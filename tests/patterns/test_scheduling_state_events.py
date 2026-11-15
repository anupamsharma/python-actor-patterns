from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import (
    Cache,
    CacheGet,
    CachePut,
    CancelTimer,
    Emit,
    Event,
    EventBus,
    GetState,
    On,
    Publish,
    PubSub,
    ScheduleIn,
    ScheduleOnce,
    StateMachine,
    Subscribe,
    TaskScheduler,
    Timer,
    Transition,
)

from ..helpers import within
from .helpers import Collector


async def test_timer_once(system: ActorSystem) -> None:
    sink = await system.spawn(Collector)
    timer = await system.spawn(Timer)
    tid = await within(timer.ask(ScheduleOnce(0.05, "tick", sink)))
    await asyncio.sleep(0.08)
    assert await within(sink.ask("get")) == ["tick"]
    assert await within(timer.ask(CancelTimer(tid.timer_id))) is False


async def test_task_scheduler(system: ActorSystem) -> None:
    done = asyncio.Event()
    values: list[int] = []

    async def job() -> None:
        values.append(1)
        done.set()

    sched = await system.spawn(TaskScheduler)
    await within(sched.ask(ScheduleIn(0.05, job)))
    await within(done.wait(), 2)
    assert values == [1]


async def test_state_machine(system: ActorSystem) -> None:
    sm = await system.spawn(
        StateMachine,
        "idle",
        [
            Transition("idle", "start", "running"),
            Transition("running", "stop", "idle"),
        ],
    )
    await within(sm.ask(Event("start")))
    assert await within(sm.ask(GetState())) == "running"
    await within(sm.ask(Event("stop")))
    assert await within(sm.ask(GetState())) == "idle"


async def test_cache_lru_and_ttl(system: ActorSystem) -> None:
    cache = await system.spawn(Cache, max_size=2, default_ttl=0.05)
    await within(cache.ask(CachePut("a", 1)))
    await within(cache.ask(CachePut("b", 2)))
    await within(cache.ask(CachePut("c", 3)))  # evicts a
    assert await within(cache.ask(CacheGet("a"))) is None
    assert await within(cache.ask(CacheGet("b"))) == 2
    await asyncio.sleep(0.06)
    assert await within(cache.ask(CacheGet("b"))) is None


async def test_pubsub(system: ActorSystem) -> None:
    a = await system.spawn(Collector)
    b = await system.spawn(Collector)
    hub = await system.spawn(PubSub)
    await within(hub.ask(Subscribe("t", a)))
    await within(hub.ask(Subscribe("t", b)))
    n = await within(hub.ask(Publish("t", "hello")))
    assert n == 2
    await asyncio.sleep(0)
    assert await within(a.ask("get")) == ["hello"]
    assert await within(b.ask("get")) == ["hello"]


async def test_event_bus(system: ActorSystem) -> None:
    seen: list[object] = []

    async def handler(event: object) -> None:
        seen.append(event)

    bus = await system.spawn(EventBus)
    await within(bus.ask(On(str, handler)))
    assert await within(bus.ask(Emit("hi"))) == 1
    assert seen == ["hi"]
