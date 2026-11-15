from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pyactors import ActorFailed, ActorStopped, ActorSystem

from .conftest import Failures
from .helpers import GET_LOG, Block, Counter, Probe, Raise, within, yield_loop


async def test_ask_returns_result(system: ActorSystem) -> None:
    ref = await system.spawn(Counter)
    assert await within(ref.ask(3)) == 3


async def test_ask_without_timeout_waits(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    block = Block("slow-result")
    asker = asyncio.create_task(ref.ask(block))
    await within(block.entered.wait())
    await yield_loop()
    assert not asker.done()
    block.release.set()
    assert await within(asker) == "slow-result"


async def test_ask_times_out_and_late_result_is_safe(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("late")
    with pytest.raises(TimeoutError):
        await ref.ask(block, timeout=0.05)
    assert block.entered.is_set()
    assert not block.cancelled  # the running handler is not interrupted
    block.release.set()  # handler now completes and tries to answer a dead Future
    assert await within(ref.ask("next")) == "next"
    assert log == ["start", "late", "next"]
    assert ref.is_alive


async def test_timed_out_queued_ask_is_not_processed(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    with pytest.raises(TimeoutError):
        await ref.ask("queued", timeout=0.02)
    block.release.set()
    await within(ref.ask("after"))
    assert log == ["start", "blocker", "after"]


async def test_ask_handler_exception(system: ActorSystem, failures: Failures) -> None:
    ref = await system.spawn(Probe)
    with pytest.raises(ZeroDivisionError):
        await within(ref.ask(Raise(ZeroDivisionError("x"))))
    failures.clear()


async def test_ask_cancellation_while_running_is_safe(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("running")
    asker = asyncio.create_task(ref.ask(block))
    await within(block.entered.wait())
    asker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asker
    block.release.set()
    assert await within(ref.ask("ok")) == "ok"
    assert log == ["start", "running", "ok"]


async def test_ask_cancellation_while_queued_is_safe(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    asker = asyncio.create_task(ref.ask("queued"))
    await yield_loop()
    asker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asker
    block.release.set()
    assert await within(ref.ask("ok")) == "ok"
    assert "queued" not in log


async def test_actor_failure_releases_pending_asks(system: ActorSystem, failures: Failures) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    error = ValueError("handler failed")
    failing = asyncio.create_task(ref.ask(Raise(error)))
    others = [asyncio.create_task(ref.ask(i)) for i in range(5)]
    await yield_loop()
    block.release.set()
    with pytest.raises(ValueError, match="handler failed"):
        await within(failing)
    for task in others:
        with pytest.raises(ActorFailed) as info:
            await within(task)
        assert info.value.cause is error
    await within(system.stop(ref))
    assert log == ["start", "blocker", "stop"]
    assert [e for _, e in failures] == [error]
    failures.clear()


async def test_immediate_shutdown_releases_pending_asks(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    block = Block("in-flight")
    in_flight = asyncio.create_task(ref.ask(block))
    await within(block.entered.wait())
    queued = [asyncio.create_task(ref.ask(i)) for i in range(5)]
    await yield_loop()
    await within(system.shutdown(graceful=False))
    with pytest.raises(ActorStopped):
        await in_flight
    for task in queued:
        with pytest.raises(ActorStopped):
            await task
    assert block.cancelled


async def test_graceful_shutdown_completes_accepted_asks(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    block = Block("in-flight")
    in_flight = asyncio.create_task(ref.ask(block))
    await within(block.entered.wait())
    queued = [asyncio.create_task(ref.ask(i)) for i in range(5)]
    await yield_loop()
    shutdown = asyncio.create_task(system.shutdown())
    await yield_loop()
    with pytest.raises(ActorStopped):
        await ref.ask("rejected")
    block.release.set()
    await within(shutdown)
    assert await in_flight == "in-flight"
    assert [await t for t in queued] == list(range(5))


async def test_timeout_includes_mailbox_admission(system: ActorSystem) -> None:
    ref = await system.spawn(Probe, mailbox_size=1)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    ref.tell("fills-mailbox")
    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(TimeoutError):
        await ref.ask("never-admitted", timeout=0.05)
    assert loop.time() - started < 2
    assert not ref._mailbox._putters
    block.release.set()
    log = await within(ref.ask(GET_LOG))
    assert "never-admitted" not in log


async def test_ask_stopped_actor_fails_fast(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    await system.stop(ref)
    with pytest.raises(ActorStopped):
        await within(ref.ask("x"), 1)


async def test_many_concurrent_asks(system: ActorSystem) -> None:
    refs = [await system.spawn(Probe) for _ in range(10)]
    results = await within(
        asyncio.gather(*(refs[i % 10].ask(i, timeout=5) for i in range(5000))), 20
    )
    assert results == list(range(5000))
