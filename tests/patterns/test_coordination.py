from __future__ import annotations

import asyncio

import pytest

from pyactors import Actor, ActorSystem
from pyactors.patterns import (
    Acquire,
    Arrive,
    Barrier,
    BreakerState,
    CircuitBreaker,
    CircuitOpen,
    GetStats,
    Release,
    Retry,
    RetryExhausted,
    RetryPolicy,
    RunSaga,
    SagaCoordinator,
    SagaFailed,
    SagaStep,
    Semaphore,
    spawn_worker_pool,
)

from ..helpers import within
from .helpers import Flaky


class Square(Actor[int, int]):
    async def receive(self, message: int) -> int:
        return message * message


async def test_worker_pool(system: ActorSystem) -> None:
    pool = await spawn_worker_pool(system, Square, size=4, name="pool")
    results = await within(asyncio.gather(*(pool.ask(i) for i in range(20))))
    assert results == [i * i for i in range(20)]


async def test_barrier(system: ActorSystem) -> None:
    barrier = await system.spawn(Barrier, 3)
    tasks = [asyncio.create_task(barrier.ask(Arrive(i))) for i in range(3)]
    results = await within(asyncio.gather(*tasks))
    assert all(sorted(r) == [0, 1, 2] for r in results)


async def test_semaphore(system: ActorSystem) -> None:
    sem = await system.spawn(Semaphore, 1)
    p1 = await within(sem.ask(Acquire()))
    waiting = asyncio.create_task(sem.ask(Acquire(timeout=2)))
    await asyncio.sleep(0)
    assert not waiting.done()
    await within(sem.ask(Release(p1.permit_id)))
    p2 = await within(waiting)
    await within(sem.ask(Release(p2.permit_id)))


async def test_saga_success_and_compensation(system: ActorSystem) -> None:
    log: list[str] = []

    async def step_a(ctx: object) -> str:
        log.append("a")
        return "A"

    async def step_b(ctx: object) -> str:
        log.append("b")
        raise RuntimeError("b failed")

    async def undo_a(ctx: object, result: object) -> None:
        log.append(f"undo:{result}")

    saga = await system.spawn(
        SagaCoordinator,
        [
            SagaStep("a", step_a, undo_a),
            SagaStep("b", step_b),
        ],
    )
    with pytest.raises(SagaFailed) as info:
        await within(saga.ask(RunSaga({})))
    assert info.value.failed_step == 1
    assert log == ["a", "b", "undo:A"]
    assert saga.is_alive


async def test_retry_succeeds_after_failures(system: ActorSystem) -> None:
    target = await system.spawn(Flaky, 2)
    retry = await system.spawn(
        Retry, target, policy=RetryPolicy(max_attempts=4, initial_backoff=0.01, jitter=0)
    )
    assert await within(retry.ask("ok"), 2) == "ok"
    assert retry.is_alive


async def test_retry_exhausted(system: ActorSystem) -> None:
    target = await system.spawn(Flaky, 10)
    retry = await system.spawn(
        Retry, target, policy=RetryPolicy(max_attempts=2, initial_backoff=0.01, jitter=0)
    )
    with pytest.raises(RetryExhausted):
        await within(retry.ask("x"), 2)
    assert retry.is_alive


async def test_circuit_breaker_opens(system: ActorSystem) -> None:
    target = await system.spawn(Flaky, 100)
    breaker = await system.spawn(
        CircuitBreaker, target, failure_threshold=2, recovery_timeout=0.05, half_open_max=1
    )
    with pytest.raises(RuntimeError):
        await within(breaker.ask(1))
    with pytest.raises(RuntimeError):
        await within(breaker.ask(2))
    with pytest.raises(CircuitOpen):
        await within(breaker.ask(3))
    stats = await within(breaker.ask(GetStats()))
    assert stats.state is BreakerState.OPEN
    assert breaker.is_alive
