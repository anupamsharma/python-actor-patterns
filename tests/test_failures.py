from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from pyactors import ActorFailed, ActorStopped, ActorSystem, LifecycleState

from .conftest import Failures
from .helpers import Block, Probe, Raise, within, yield_loop


async def test_receive_failure_semantics(system: ActorSystem, failures: Failures) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log, name="fragile", mailbox_size=3)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    error = RuntimeError("receive failed")
    failing = asyncio.create_task(ref.ask(Raise(error)))
    queued_ask = asyncio.create_task(ref.ask("queued-ask"))
    await yield_loop()
    ref.tell("queued-tell")
    blocked_sender = asyncio.create_task(ref.send("blocked"))
    await yield_loop()
    assert not blocked_sender.done()

    block.release.set()

    with pytest.raises(RuntimeError) as info:
        await within(failing)
    assert info.value is error  # the current asker receives the original exception
    with pytest.raises(ActorFailed) as failed:
        await within(queued_ask)  # other pending asks get ActorFailed
    assert failed.value.cause is error
    assert failed.value.__cause__ is error
    with pytest.raises(ActorFailed):
        await within(blocked_sender)  # blocked producers are released

    await within(system.stop(ref))  # waits for termination; no-op otherwise
    assert ref.state is LifecycleState.FAILED
    assert not ref.is_alive
    assert system.get("fragile") is None
    assert log == ["start", "blocker", "stop"]  # queued messages discarded; on_stop ran
    assert failures == [(ref, error)]  # original failure observable
    with pytest.raises(ActorFailed):
        ref.tell("after")
    with pytest.raises(ActorStopped):  # ActorFailed is an ActorStopped
        await ref.send("after")
    failures.clear()


async def test_tell_failure_is_reported(system: ActorSystem, failures: Failures) -> None:
    ref = await system.spawn(Probe)
    error = ValueError("from tell")
    ref.tell(Raise(error))
    await within(system.stop(ref))
    assert failures == [(ref, error)]
    assert ref.state is LifecycleState.FAILED
    failures.clear()


async def test_default_failure_handler_logs(caplog: pytest.LogCaptureFixture) -> None:
    system = ActorSystem()
    ref = await system.spawn(Probe)
    with caplog.at_level(logging.ERROR, logger="pyactors"):
        ref.tell(Raise(ValueError("logged")))
        await within(system.stop(ref))
    assert any(
        r.exc_info is not None and isinstance(r.exc_info[1], ValueError) for r in caplog.records
    )
    await system.shutdown()


async def test_failure_handler_errors_are_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def bad_handler(ref: Any, exc: BaseException) -> None:
        raise RuntimeError("handler broke")

    system = ActorSystem(failure_handler=bad_handler)
    ref = await system.spawn(Probe)
    with caplog.at_level(logging.ERROR, logger="pyactors"):
        ref.tell(Raise(ValueError("x")))
        await within(system.stop(ref))
    assert ref.state is LifecycleState.FAILED
    assert any("failure handler raised" in r.getMessage() for r in caplog.records)
    await system.shutdown()


async def test_on_stop_failure_after_receive_failure(
    system: ActorSystem, failures: Failures
) -> None:
    stop_error = RuntimeError("stop")
    recv_error = ValueError("recv")
    ref = await system.spawn(Probe, stop_error=stop_error)
    ref.tell(Raise(recv_error))
    await within(system.stop(ref))
    assert ref.state is LifecycleState.FAILED
    assert [e for _, e in failures] == [recv_error, stop_error]
    failures.clear()


async def test_failure_during_graceful_drain(system: ActorSystem, failures: Failures) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    error = ValueError("drain")
    ref.tell(Raise(error))
    later = asyncio.create_task(ref.ask("later"))
    await yield_loop()
    stopper = asyncio.create_task(system.stop(ref))
    await yield_loop()
    block.release.set()
    await within(stopper)
    with pytest.raises(ActorFailed):
        await later
    assert ref.state is LifecycleState.FAILED
    assert log == ["start", "blocker", "stop"]
    failures.clear()


async def test_no_retry_and_no_restart(system: ActorSystem, failures: Failures) -> None:
    attempts: list[int] = []

    class Once(Probe):
        async def receive(self, message: Any) -> Any:
            attempts.append(message)
            raise RuntimeError("nope")

    ref = await system.spawn(Once, name="once")
    with pytest.raises(RuntimeError):
        await within(ref.ask(1))
    await within(system.stop(ref))
    assert attempts == [1]
    assert system.get("once") is None
    failures.clear()
