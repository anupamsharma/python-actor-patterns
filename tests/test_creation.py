from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pyactors import Actor, ActorNameTaken, ActorSystem, LifecycleState

from .conftest import Failures
from .helpers import GET_LOG, Probe, within


async def test_spawn_one_actor(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    assert ref.is_alive
    assert ref.state is LifecycleState.RUNNING
    assert system.actor_count == 1
    assert await within(ref.ask("hello")) == "hello"


async def test_spawn_many_actors(system: ActorSystem) -> None:
    refs = [await system.spawn(Probe) for _ in range(1000)]
    assert system.actor_count == 1000
    assert len({r.actor_id for r in refs}) == 1000
    assert len({r.name for r in refs}) == 1000
    results = await within(asyncio.gather(*(r.ask(i) for i, r in enumerate(refs))))
    assert results == list(range(1000))


class WithArgs(Actor[str, tuple[Any, ...]]):
    def __init__(self, a: int, b: str, *, c: float = 0.0, d: list[int] | None = None) -> None:
        self.values = (a, b, c, d)

    async def receive(self, message: str) -> tuple[Any, ...]:
        return self.values


async def test_constructor_arguments(system: ActorSystem) -> None:
    ref = await system.spawn(WithArgs, 1, "two", c=3.0, d=[4], name="args")
    assert await within(ref.ask("values")) == (1, "two", 3.0, [4])


async def test_generated_identities(system: ActorSystem) -> None:
    a = await system.spawn(Probe)
    b = await system.spawn(Probe)
    assert a.name.startswith("$")
    assert b.name.startswith("$")
    assert a.name != b.name
    assert a.actor_id != b.actor_id
    assert system.get(a.name) is a
    assert system.get(b.name) is b


async def test_explicit_names(system: ActorSystem) -> None:
    ref = await system.spawn(Probe, name="worker")
    assert ref.name == "worker"
    assert ref.actor_id.startswith("worker#")
    assert system.get("worker") is ref
    assert system.get("missing") is None


async def test_duplicate_names_rejected(system: ActorSystem) -> None:
    log: list[Any] = []
    first = await system.spawn(Probe, log, name="dup")
    with pytest.raises(ActorNameTaken):
        await system.spawn(Probe, name="dup")
    assert system.actor_count == 1
    assert system.get("dup") is first
    assert await within(first.ask("still-works")) == "still-works"
    assert log == ["start", "still-works"]


async def test_duplicate_name_while_first_is_starting(system: ActorSystem) -> None:
    gate = asyncio.Event()
    entered = asyncio.Event()
    first = asyncio.create_task(
        system.spawn(Probe, name="x", start_gate=gate, start_entered=entered)
    )
    await within(entered.wait())
    with pytest.raises(ActorNameTaken):
        await system.spawn(Probe, name="x")
    gate.set()
    ref = await within(first)
    assert system.get("x") is ref


@pytest.mark.parametrize("bad", ["", "$reserved"])
async def test_invalid_names(system: ActorSystem, bad: str) -> None:
    with pytest.raises(ValueError, match="name"):
        await system.spawn(Probe, name=bad)
    assert system.actor_count == 0


async def test_negative_mailbox_size(system: ActorSystem) -> None:
    with pytest.raises(ValueError, match="mailbox_size"):
        await system.spawn(Probe, mailbox_size=-1)


async def test_startup_hook_runs_before_spawn_returns(system: ActorSystem) -> None:
    class Slow(Actor[str, bool]):
        def __init__(self) -> None:
            self.ready = False

        async def on_start(self) -> None:
            for _ in range(5):
                await asyncio.sleep(0)
            assert self.context.ref.state is LifecycleState.STARTING
            self.ready = True

        async def receive(self, message: str) -> bool:
            return self.ready

    ref = await system.spawn(Slow)
    assert ref.state is LifecycleState.RUNNING
    assert await within(ref.ask("ready?")) is True


async def test_context_available_in_on_start(system: ActorSystem) -> None:
    seen: list[Any] = []

    class Ctx(Actor[str, None]):
        async def on_start(self) -> None:
            seen.append((self.context.ref, self.context.system, self.context.name))

        async def receive(self, message: str) -> None:
            return None

    ref = await system.spawn(Ctx, name="ctx")
    assert seen == [(ref, system, "ctx")]


async def test_context_not_available_in_init(system: ActorSystem) -> None:
    class Early(Actor[str, None]):
        def __init__(self) -> None:
            _ = self.context

        async def receive(self, message: str) -> None:
            return None

    with pytest.raises(RuntimeError, match="context"):
        await system.spawn(Early)
    assert system.actor_count == 0


async def test_startup_failure_cleans_up(system: ActorSystem, failures: Failures) -> None:
    log: list[Any] = []
    error = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom") as info:
        await system.spawn(Probe, log, name="bad", start_error=error)
    assert info.value is error
    assert system.get("bad") is None
    assert system.actor_count == 0
    assert log == ["start"]  # on_stop is not called when on_start fails
    assert failures == []  # startup errors propagate instead of being reported
    # The name is free again.
    ref = await system.spawn(Probe, name="bad")
    assert ref.is_alive


async def test_constructor_failure_registers_nothing(system: ActorSystem) -> None:
    class Broken(Actor[str, None]):
        def __init__(self) -> None:
            raise ValueError("ctor")

        async def receive(self, message: str) -> None:
            return None

    with pytest.raises(ValueError, match="ctor"):
        await system.spawn(Broken, name="broken")
    assert system.get("broken") is None
    assert system.actor_count == 0


async def test_receive_must_be_implemented(system: ActorSystem) -> None:
    class NoReceive(Actor[Any, Any]):
        pass

    with pytest.raises(TypeError, match="receive"):
        await system.spawn(NoReceive)


async def test_factory_must_return_actor(system: ActorSystem) -> None:
    with pytest.raises(TypeError, match="Actor"):
        await system.spawn(lambda: object())  # type: ignore[arg-type, return-value]
    assert system.actor_count == 0


async def test_get_log_helper_sanity(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    ref.tell(1)
    assert await within(ref.ask(GET_LOG)) == ["start", 1]
