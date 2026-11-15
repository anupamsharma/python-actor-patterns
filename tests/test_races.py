from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pyactors import Actor, ActorStopped, ActorSystem, LifecycleState

from .helpers import GET_LOG, Block, Probe, within, yield_loop


async def test_graceful_stop_during_processing_waits_for_handler(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("busy")
    asker = asyncio.create_task(ref.ask(block))
    await within(block.entered.wait())
    stopper = asyncio.create_task(system.stop(ref))
    await yield_loop()
    assert not stopper.done()
    block.release.set()
    await within(stopper)
    assert await asker == "busy"
    assert log == ["start", "busy", "stop"]


async def test_immediate_stop_during_processing_cancels_handler(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    block = Block("busy")
    asker = asyncio.create_task(ref.ask(block))
    await within(block.entered.wait())
    await within(system.stop(ref, graceful=False))
    assert block.cancelled
    with pytest.raises(ActorStopped):
        await asker


@pytest.mark.parametrize("graceful", [True, False])
async def test_stop_while_send_waits_for_capacity(system: ActorSystem, graceful: bool) -> None:
    ref = await system.spawn(Probe, mailbox_size=1)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    ref.tell("fill")
    sender = asyncio.create_task(ref.send("waiting"))
    asker = asyncio.create_task(ref.ask("waiting-ask"))
    await yield_loop()
    stopper = asyncio.create_task(system.stop(ref, graceful=graceful))
    with pytest.raises(ActorStopped):
        await within(sender)
    with pytest.raises(ActorStopped):
        await within(asker)
    block.release.set()
    await within(stopper)
    assert not ref._mailbox._putters


async def test_stop_while_ask_waits_for_reply(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    waiting = [asyncio.create_task(ref.ask(i)) for i in range(10)]
    await yield_loop()
    await within(system.stop(ref, graceful=False))
    results = await asyncio.gather(*waiting, return_exceptions=True)
    assert all(isinstance(r, ActorStopped) for r in results)


@pytest.mark.parametrize("graceful", [True, False])
async def test_shutdown_during_actor_startup(graceful: bool) -> None:
    system = ActorSystem()
    log: list[Any] = []
    gate, entered = asyncio.Event(), asyncio.Event()
    spawn = asyncio.create_task(
        system.spawn(Probe, log, name="starting", start_gate=gate, start_entered=entered)
    )
    await within(entered.wait())
    shutdown = asyncio.create_task(system.shutdown(graceful=graceful))
    await yield_loop()
    if graceful:
        assert not shutdown.done()  # graceful shutdown lets on_start finish
        gate.set()
    with pytest.raises(ActorStopped):
        await within(spawn)
    await within(shutdown)
    if graceful:
        assert log == ["start", "stop"]
    else:
        assert log == ["start", "start-cancelled"]  # never started => no on_stop
    assert system.actor_count == 0
    assert system.get("starting") is None


async def test_shutdown_while_many_actors_active() -> None:
    system = ActorSystem()
    refs = [await system.spawn(Probe) for _ in range(100)]
    blocks = [Block(i) for i in range(100)]
    asks = [asyncio.create_task(r.ask(b)) for r, b in zip(refs, blocks, strict=True)]
    queued = [asyncio.create_task(r.ask("q")) for r in refs]
    await within(asyncio.gather(*(b.entered.wait() for b in blocks)))
    shutdown = asyncio.create_task(system.shutdown())
    await yield_loop()
    for b in blocks[::2]:
        b.release.set()
    await within(system.shutdown(graceful=False))
    await within(shutdown)
    results = await asyncio.gather(*asks, *queued, return_exceptions=True)
    assert all(t.done() for t in asks + queued)
    assert sum(1 for r in results if isinstance(r, ActorStopped)) >= 50
    assert system.actor_count == 0
    assert all(r.state.is_terminal for r in refs)


async def test_simultaneous_stop_requests(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("busy")
    ref.tell(block)
    await within(block.entered.wait())
    stoppers = [asyncio.create_task(system.stop(ref, graceful=i % 2 == 0)) for i in range(10)] + [
        asyncio.create_task(system.shutdown())
    ]
    await within(asyncio.gather(*stoppers))
    assert log.count("stop") == 1
    assert ref.state is LifecycleState.STOPPED


async def test_cancellation_during_startup(system: ActorSystem) -> None:
    log: list[Any] = []
    gate, entered = asyncio.Event(), asyncio.Event()
    spawn = asyncio.create_task(
        system.spawn(Probe, log, name="c", start_gate=gate, start_entered=entered)
    )
    await within(entered.wait())
    spawn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await spawn
    await yield_loop()
    assert log == ["start", "start-cancelled"]
    assert system.get("c") is None
    assert system.actor_count == 0


@pytest.mark.parametrize("graceful", [True, False])
async def test_stop_before_startup_begins(system: ActorSystem, graceful: bool) -> None:
    log: list[Any] = []
    spawn = asyncio.create_task(system.spawn(Probe, log, name="c"))
    await asyncio.sleep(0)  # spawn registered the cell; the actor task has not run yet
    ref = system.get("c")
    assert ref is not None
    assert ref.state is LifecycleState.CREATED
    # request_stop runs synchronously here, before the actor task's first step.
    await within(system.stop(ref, graceful=graceful))
    with pytest.raises(ActorStopped):
        await within(spawn)
    assert log == []  # on_start (and therefore on_stop) skipped entirely
    assert ref.state is LifecycleState.STOPPED
    assert system.get("c") is None


class SelfStopper(Actor[Any, Any]):
    def __init__(self, log: list[Any], mode: str) -> None:
        self.log = log
        self.mode = mode

    async def receive(self, message: Any) -> Any:
        if message == "stop":
            if self.mode == "context":
                self.context.stop()
            elif self.mode == "context-immediate":
                self.context.stop(graceful=False)
            elif self.mode == "system":
                await self.context.system.stop(self.context.ref)
            elif self.mode == "ref":
                await self.context.ref.stop(graceful=False)
            elif self.mode == "shutdown":
                await self.context.system.shutdown()
            self.log.append("stop-requested")
            return "stopping"
        self.log.append(message)
        return message

    async def on_stop(self) -> None:
        self.log.append("on_stop")
        await self.context.system.stop(self.context.ref)  # also must not deadlock


@pytest.mark.parametrize(
    ("mode", "drains"),
    [
        ("context", True),
        ("system", True),
        ("shutdown", True),
        ("context-immediate", False),
        ("ref", False),
    ],
)
async def test_actor_stopping_itself(system: ActorSystem, mode: str, drains: bool) -> None:
    log: list[Any] = []
    ref = await system.spawn(SelfStopper, log, mode)
    ref.tell("before")
    reply = asyncio.create_task(ref.ask("stop"))
    queued = asyncio.create_task(ref.ask("queued"))
    assert await within(reply) == "stopping"
    if drains:
        assert await within(queued) == "queued"
    else:
        with pytest.raises(ActorStopped):
            await within(queued)
    await within(system.stop(ref))
    expected = ["before", "stop-requested", *(["queued"] if drains else []), "on_stop"]
    assert log == expected
    assert ref.state is LifecycleState.STOPPED


async def test_self_stop_in_on_start_fails_spawn(system: ActorSystem) -> None:
    log: list[Any] = []

    class StopsInStart(Probe):
        async def on_start(self) -> None:
            self.context.stop()
            self.log.append("start")

    with pytest.raises(ActorStopped):
        await within(system.spawn(StopsInStart, log, name="s"))
    assert log == ["start", "stop"]
    assert system.get("s") is None


async def test_actor_stops_another_actor(system: ActorSystem) -> None:
    target_log: list[Any] = []
    target = await system.spawn(Probe, target_log)

    class Killer(Actor[Any, str]):
        async def receive(self, message: Any) -> str:
            await self.context.system.stop(target)
            return "done"

    killer = await system.spawn(Killer)
    target.tell("x")
    assert await within(killer.ask("go")) == "done"
    assert target_log == ["start", "x", "stop"]
    assert killer.is_alive


async def test_tell_racing_with_graceful_stop(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    accepted, rejected = [], []
    stopper = None
    for i in range(200):
        if i == 100:
            stopper = asyncio.create_task(system.stop(ref))
            await asyncio.sleep(0)
        try:
            ref.tell(i)
            accepted.append(i)
        except ActorStopped:
            rejected.append(i)
    assert stopper is not None
    await within(stopper)
    assert accepted == list(range(100))
    assert rejected == list(range(100, 200))
    assert log == ["start", *accepted, "stop"]


async def test_stop_escalation_cancels_drain(system: ActorSystem) -> None:
    log: list[Any] = []
    ref = await system.spawn(Probe, log)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    pending = asyncio.create_task(ref.ask("queued"))
    await yield_loop()
    graceful = asyncio.create_task(system.stop(ref))
    await yield_loop()
    await within(system.stop(ref, graceful=False))
    await within(graceful)
    with pytest.raises(ActorStopped):
        await pending
    assert log == ["start", ("cancelled", "blocker"), "stop"]


async def test_on_stop_is_not_cancelled_by_immediate_stop(system: ActorSystem) -> None:
    gate, entered = asyncio.Event(), asyncio.Event()
    done: list[str] = []

    class SlowStop(Actor[Any, None]):
        async def receive(self, message: Any) -> None:
            return None

        async def on_stop(self) -> None:
            entered.set()
            await gate.wait()
            done.append("cleaned")

    ref = await system.spawn(SlowStop)
    graceful = asyncio.create_task(system.stop(ref))
    await within(entered.wait())
    immediate = asyncio.create_task(system.stop(ref, graceful=False))
    await yield_loop()
    assert not immediate.done()
    gate.set()
    await within(asyncio.gather(graceful, immediate))
    assert done == ["cleaned"]


async def test_handler_swallowing_cancellation_still_stops(system: ActorSystem) -> None:
    log: list[Any] = []

    class Stubborn(Actor[Any, str]):
        async def receive(self, message: Any) -> str:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                log.append("swallowed")
            return "returned"

        async def on_stop(self) -> None:
            await asyncio.sleep(0)  # task must not be left in a cancelling state
            log.append("stop")

    ref = await system.spawn(Stubborn)
    asker = asyncio.create_task(ref.ask("x"))
    await yield_loop()
    await within(system.stop(ref, graceful=False))
    assert await asker == "returned"
    assert log == ["swallowed", "stop"]
    assert ref.state is LifecycleState.STOPPED


async def test_get_returns_starting_actor_and_messages_queue(system: ActorSystem) -> None:
    gate, entered = asyncio.Event(), asyncio.Event()
    spawn = asyncio.create_task(
        system.spawn(Probe, name="early", start_gate=gate, start_entered=entered)
    )
    await within(entered.wait())
    early = system.get("early")
    assert early is not None
    assert early.state is LifecycleState.STARTING
    early.tell("queued-during-start")
    gate.set()
    ref = await within(spawn)
    assert ref is early
    assert await within(ref.ask(GET_LOG)) == ["start", "queued-during-start"]
