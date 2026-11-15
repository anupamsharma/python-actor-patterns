from __future__ import annotations

import asyncio

import pytest

from pyactors import Actor, ActorSystem
from pyactors.patterns import (
    Batch,
    Buffer,
    CircuitBreaker,
    Consume,
    Flush,
    Produce,
    Publish,
    PubSub,
    QueueActor,
    RateLimiter,
    Retry,
    RetryPolicy,
    RoundRobinRouter,
    ScheduleIn,
    Subscribe,
    TaskScheduler,
    spawn_worker_pool,
)

from ..helpers import within
from .helpers import Echo, Flaky


class Worker(Actor[int, int]):
    async def receive(self, message: int) -> int:
        return message + 1


async def test_queue_plus_worker_pool(system: ActorSystem) -> None:
    q = await system.spawn(QueueActor, capacity=100)
    pool = await spawn_worker_pool(system, Worker, size=3, name="qp")

    async def produce() -> None:
        for i in range(20):
            await q.ask(Produce(i))

    async def consume() -> None:
        for _ in range(20):
            item = await q.ask(Consume())
            await pool.ask(item.value)

    await within(asyncio.gather(produce(), consume()))


async def test_batch_plus_rate_limiter(system: ActorSystem) -> None:
    batches: list[int] = []

    async def handler(items):  # type: ignore[no-untyped-def]
        batches.append(len(items))
        return len(items)

    limiter = await system.spawn(RateLimiter, rate=1000, per=1.0, burst=100, mode="wait")
    batch = await system.spawn(Batch, handler, max_size=5, max_wait=None)
    for i in range(10):
        await within(limiter.ask(i))
        await within(batch.ask(i))
    assert batches == [5, 5]


async def test_router_plus_circuit_breaker(system: ActorSystem) -> None:
    ok = await system.spawn(Echo)
    bad = await system.spawn(Flaky, 100)
    breaker = await system.spawn(CircuitBreaker, bad, failure_threshold=1, recovery_timeout=5.0)
    router = await system.spawn(RoundRobinRouter, [ok, breaker])
    # even indices -> ok, odd -> breaker
    assert await within(router.ask(0)) == 0
    with pytest.raises(RuntimeError):
        await within(router.ask(1))
    assert await within(router.ask(2)) == 2
    assert router.is_alive
    assert breaker.is_alive


async def test_retry_plus_circuit_breaker(system: ActorSystem) -> None:
    target = await system.spawn(Flaky, 1)
    breaker = await system.spawn(CircuitBreaker, target, failure_threshold=5, recovery_timeout=5.0)
    retry = await system.spawn(
        Retry, breaker, policy=RetryPolicy(max_attempts=3, initial_backoff=0.01, jitter=0)
    )
    assert await within(retry.ask("x"), 2) == "x"


async def test_pubsub_plus_buffer(system: ActorSystem) -> None:
    buf = await system.spawn(Buffer, capacity=10)
    hub = await system.spawn(PubSub)
    await within(hub.ask(Subscribe("logs", buf)))
    await within(hub.ask(Publish("logs", "a")))
    await within(hub.ask(Publish("logs", "b")))
    await asyncio.sleep(0)
    result = await within(buf.ask(Flush()))
    assert result.items == ("a", "b")


async def test_pipeline_plus_batch(system: ActorSystem) -> None:
    batches: list[tuple[object, ...]] = []

    async def handler(items):  # type: ignore[no-untyped-def]
        batches.append(tuple(items))
        return items

    batch = await system.spawn(Batch, handler, max_size=3, max_wait=None)

    async def to_batch(x: int) -> int:
        await batch.ask(x)
        return x

    from pyactors.patterns import Pipeline

    pipe = await system.spawn(Pipeline, [lambda x: x * 2, to_batch])
    for i in range(3):
        await within(pipe.ask(i))
    assert batches == [(0, 2, 4)]


async def test_scheduler_plus_worker_pool(system: ActorSystem) -> None:
    pool = await spawn_worker_pool(system, Worker, size=2, name="sp")
    done = asyncio.Event()
    results: list[int] = []

    async def job() -> None:
        results.append(await pool.ask(10))
        done.set()

    sched = await system.spawn(TaskScheduler)
    await within(sched.ask(ScheduleIn(0.05, job)))
    await within(done.wait(), 2)
    assert results == [11]
