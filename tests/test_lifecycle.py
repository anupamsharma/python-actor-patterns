from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pyactors import Actor, ActorStopped, ActorSystem, ActorSystemClosed, LifecycleState

from .conftest import Failures
from .helpers import GET_LOG, Block, Probe, within, yield_loop


async def test_state_transitions_are_observable(system: ActorSystem) -> None:
    states: list[LifecycleState] = []

    class Observer(Actor[Any, None]):
        async def on_start(self) -> None:
            states.append(self.context.ref.state)

        async def receive(self, message: Any) -> None:
            states.append(self.context.ref.state)

        async def on_stop(self) -> None:
            states.append(self.context.ref.state)

    ref = await system.spawn(Observer)
    await ref.ask(None)
    await system.stop(ref)
    states.append(ref.state)
    assert states == [
        LifecycleState.STARTING,
        LifecycleState.RUNNING,
        LifecycleState.STOPPING,
        LifecycleState.STOPPED,
    ]


async def test_messages_sent_during_startup_are_processed_after_it(system: ActorSystem) -> None:
    class SelfTell(Actor[Any, Any]):
        def __init__(self) -> None:
            self.log: list[Any] = []

        async def on_start(self) -> None:
            self.context.ref.tell("from-on-start")
            self.log.append("started")

        async def receive(self, message: Any) -> Any:
            if message == "log":
                return self.log
            self.log.append(message)
            return None

    ref = await system.spawn(SelfTell)
    assert await within(ref.ask("log")) == ["started", "from-on-start"]


async def test_graceful_stop_drains_accepted_messages(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    for i in range(10):
        ref.tell(i)
    stopper = asyncio.create_task(system.stop(ref))
    await yield_loop()
    assert not ref.is_alive
    assert ref.state is LifecycleState.STOPPING
    with pytest.raises(ActorStopped):
        ref.tell("rejected")
    assert not stopper.done()
    block.release.set()
    await within(stopper)
    assert log == ["start", "blocker", *range(10), "stop"]
    assert ref.state is LifecycleState.STOPPED


async def test_immediate_stop_discards_queued_messages(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    for i in range(10):
        ref.tell(i)
    await within(system.stop(ref, graceful=False))
    assert block.cancelled
    assert log == ["start", ("cancelled", "blocker"), "stop"]
    assert ref.state is LifecycleState.STOPPED
    assert system.actor_count == 0


async def test_on_stop_runs_and_registry_cleared(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log, name="p")
    await system.stop(ref)
    assert log == ["start", "stop"]
    assert not ref.is_alive
    assert system.get("p") is None
    assert system.actor_count == 0


async def test_on_stop_failure_is_reported_and_actor_terminates(
    system: ActorSystem, failures: Failures
) -> None:
    error = RuntimeError("cleanup failed")
    ref = await system.spawn(Probe, stop_error=error, name="p")
    await within(system.stop(ref))
    assert ref.state is LifecycleState.STOPPED
    assert system.get("p") is None
    assert failures == [(ref, error)]
    failures.clear()


async def test_repeated_stop_is_safe(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    await system.stop(ref)
    await system.stop(ref)
    await system.stop(ref, graceful=False)
    await ref.stop()
    assert log == ["start", "stop"]


async def test_repeated_shutdown_is_safe(failures: Failures) -> None:
    system = ActorSystem()
    log: list[Any] = []
    await system.spawn(Probe, log)
    await system.shutdown()
    await system.shutdown()
    await system.shutdown(graceful=False)
    assert log == ["start", "stop"]
    assert system.actor_count == 0


async def test_shutdown_escalation_from_graceful_to_immediate() -> None:
    system = ActorSystem()
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    ref.tell("queued")
    graceful = asyncio.create_task(system.shutdown())
    await yield_loop()
    assert not graceful.done()
    await within(system.shutdown(graceful=False))
    await within(graceful)
    assert log == ["start", ("cancelled", "blocker"), "stop"]


async def test_async_context_manager() -> None:
    log: list[Any] = []
    async with ActorSystem() as system:
        ref = await system.spawn(Probe, log)
        ref.tell("work")
    assert log == ["start", "work", "stop"]
    assert system.actor_count == 0
    assert system.is_closing


async def test_async_context_manager_shuts_down_on_error() -> None:
    log: list[Any] = []
    system = ActorSystem()

    async def body() -> None:
        async with system:
            await system.spawn(Probe, log)
            raise LookupError("body failed")

    with pytest.raises(LookupError):
        await body()
    assert log == ["start", "stop"]
    assert system.actor_count == 0


async def test_shutdown_rejects_new_spawns() -> None:
    system = ActorSystem()
    await system.shutdown()
    with pytest.raises(ActorSystemClosed):
        await system.spawn(Probe)
    assert system.actor_count == 0


async def test_shutdown_rejects_spawn_from_actor_on_stop() -> None:
    errors: list[BaseException] = []

    class SpawnOnStop(Actor[Any, None]):
        async def receive(self, message: Any) -> None:
            return None

        async def on_stop(self) -> None:
            try:
                await self.context.system.spawn(Probe)
            except ActorSystemClosed as exc:
                errors.append(exc)

    system = ActorSystem()
    await system.spawn(SpawnOnStop)
    await within(system.shutdown())
    assert len(errors) == 1
    assert system.actor_count == 0


async def test_name_can_be_reused_and_old_ref_is_dead(system: ActorSystem) -> None:
    old_log: list[Any] = []
    new_log: list[Any] = []
    old = await system.spawn(Probe, old_log, name="svc")
    await system.stop(old)
    new = await system.spawn(Probe, new_log, name="svc")
    assert old is not new
    assert old.actor_id != new.actor_id
    assert system.get("svc") is new
    with pytest.raises(ActorStopped):
        old.tell("to-old")
    with pytest.raises(ActorStopped):
        await old.ask("to-old")
    new.tell("to-new")
    assert await within(new.ask(GET_LOG)) == ["start", "to-new"]
    assert old_log == ["start", "stop"]


async def test_ref_is_immutable(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    with pytest.raises(AttributeError):
        setattr(ref, "_cell", None)  # noqa: B010
    with pytest.raises(AttributeError):
        setattr(ref, "extra", 1)  # noqa: B010
    with pytest.raises(AttributeError):
        delattr(ref, "_cell")


async def test_stop_rejects_foreign_ref(system: ActorSystem) -> None:
    other = ActorSystem("other")
    ref = await other.spawn(Probe)
    with pytest.raises(ValueError, match="does not belong"):
        await system.stop(ref)
    await other.shutdown()
