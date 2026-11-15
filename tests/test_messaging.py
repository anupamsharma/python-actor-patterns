from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from pyactors import Actor, ActorFailed, ActorSystem, LifecycleState

from .conftest import Failures
from .helpers import GET_LOG, Counter, Probe, Raise, within


async def test_tell_delivers(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    assert ref.tell("hi") is None  # type: ignore[func-returns-value]
    assert await within(ref.ask(GET_LOG)) == ["start", "hi"]


async def test_send_delivers(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    assert await within(ref.send("hi")) is None  # type: ignore[func-returns-value]
    assert await within(ref.ask(GET_LOG)) == ["start", "hi"]


async def test_ask_returns_result(system: ActorSystem) -> None:
    ref = await system.spawn(Counter)
    assert await within(ref.ask(5)) == 5
    assert await within(ref.ask(7)) == 12


async def test_fifo_order(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    for i in range(1000):
        ref.tell(i)
    assert await within(ref.ask(GET_LOG)) == ["start", *range(1000)]


async def test_fifo_order_mixed_operations(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    ref.tell(1)
    await ref.send(2)
    assert await within(ref.ask(3)) == 3
    ref.tell(4)
    await ref.send(5)
    assert await within(ref.ask(GET_LOG)) == ["start", 1, 2, 3, 4, 5]


async def test_none_is_valid_message(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    ref.tell(None)
    await ref.send(None)
    assert await within(ref.ask(None)) is None
    assert await within(ref.ask(GET_LOG)) == ["start", None, None, None]


async def test_none_is_valid_ask_result(system: ActorSystem) -> None:
    class ReturnsNone(Actor[int, None]):
        async def receive(self, message: int) -> None:
            return None

    ref = await system.spawn(ReturnsNone)
    assert await within(ref.ask(1)) is None  # type: ignore[func-returns-value]


async def test_receive_gets_original_message(system: ActorSystem) -> None:
    @dataclass(frozen=True)
    class Msg:
        x: int

    seen: list[Any] = []

    class Inspect(Actor[Any, Any]):
        async def receive(self, message: Any) -> Any:
            seen.append(message)
            return message

    ref = await system.spawn(Inspect)
    original = Msg(1)
    ref.tell(original)
    assert await within(ref.ask(original)) is original
    assert seen == [original, original]
    assert all(m is original for m in seen)


async def test_exception_propagates_to_asker(system: ActorSystem, failures: Failures) -> None:
    ref = await system.spawn(Probe)
    error = KeyError("missing")
    with pytest.raises(KeyError) as info:
        await within(ref.ask(Raise(error)))
    assert info.value is error
    await within(system.stop(ref))
    assert ref.state is LifecycleState.FAILED
    assert [e for _, e in failures] == [error]
    with pytest.raises(ActorFailed):
        ref.tell("after")
    failures.clear()


async def test_state_across_messages(system: ActorSystem) -> None:
    ref = await system.spawn(Counter, 10)
    for i in range(1, 11):
        ref.tell(i)
    assert await within(ref.ask("get")) == 10 + sum(range(1, 11))


async def test_independent_state(system: ActorSystem) -> None:
    a = await system.spawn(Counter)
    b = await system.spawn(Counter, 100)
    await a.ask(1)
    await a.ask(1)
    await b.ask(5)
    assert await within(a.ask("get")) == 2
    assert await within(b.ask("get")) == 105


async def test_concurrent_requests_do_not_corrupt_state(system: ActorSystem) -> None:
    ref = await system.spawn(Counter)
    results = await within(asyncio.gather(*(ref.ask(1) for _ in range(500))))
    assert await within(ref.ask("get")) == 500
    # Every intermediate total is observed exactly once: handlers never interleaved.
    assert sorted(results) == list(range(1, 501))
