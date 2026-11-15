from __future__ import annotations

import asyncio
import gc
import weakref
from typing import Any

import pytest

from pyactors import ActorStopped, ActorSystem

from .helpers import Block, Probe, Raise, within, yield_loop


def _actor_tasks() -> list[asyncio.Task[Any]]:
    return [t for t in asyncio.all_tasks() if t.get_name().startswith("pyactors:")]


async def test_no_leaked_tasks_after_shutdown() -> None:
    system = ActorSystem()
    refs = [await system.spawn(Probe) for _ in range(300)]
    for r in refs[:100]:
        r.tell("work")
    assert len(_actor_tasks()) == 300
    await within(system.shutdown())
    await yield_loop()
    assert _actor_tasks() == []


async def test_no_unresolved_futures_after_shutdown() -> None:
    system = ActorSystem()
    refs = [await system.spawn(Probe, mailbox_size=2) for _ in range(20)]
    blocks = [Block() for _ in refs]
    for r, b in zip(refs, blocks, strict=True):
        r.tell(b)
    await within(asyncio.gather(*(b.entered.wait() for b in blocks)))
    asks = [asyncio.create_task(r.ask(i)) for r in refs for i in range(5)]
    sends = [asyncio.create_task(r.send(i)) for r in refs for i in range(3)]
    await yield_loop()
    await within(system.shutdown(graceful=False))
    await yield_loop()
    pending = asks + sends
    assert all(t.done() for t in pending)
    results = await asyncio.gather(*pending, return_exceptions=True)
    assert all(isinstance(r, ActorStopped) for r in results)
    for r in refs:
        assert not r._mailbox._putters
        assert len(r._mailbox) == 0


async def test_registry_cleaned_on_every_termination_path() -> None:
    reported: list[BaseException] = []
    system = ActorSystem(failure_handler=lambda ref, exc: reported.append(exc))
    stopped = await system.spawn(Probe, name="stopped")
    await system.stop(stopped)
    assert system.get("stopped") is None

    failed = await system.spawn(Probe, name="failed")
    failed.tell(Raise(ValueError("x")))
    await within(system.stop(failed))
    assert system.get("failed") is None
    assert len(reported) == 1

    with pytest.raises(RuntimeError):
        await system.spawn(Probe, name="bad-start", start_error=RuntimeError("x"))
    assert system.get("bad-start") is None

    alive = await system.spawn(Probe, name="alive")
    assert system.actor_count == 1
    await system.shutdown()
    assert system.actor_count == 0
    assert system.get("alive") is None
    assert not alive.is_alive


async def test_actor_objects_are_released_after_termination() -> None:
    system = ActorSystem()
    holder: dict[str, Any] = {}

    class Tracked(Probe):
        async def on_start(self) -> None:
            holder["actor"] = weakref.ref(self)

    ref = await system.spawn(Tracked)
    await ref.ask("x")
    await system.stop(ref)
    del ref
    await yield_loop()
    gc.collect()
    assert holder["actor"]() is None
    await system.shutdown()


async def test_old_ref_cannot_reach_new_incarnation() -> None:
    system = ActorSystem()
    old_log: list[Any] = []
    new_log: list[Any] = []
    old = await system.spawn(Probe, old_log, name="reused")
    await system.shutdown()
    system2 = ActorSystem()
    new = await system2.spawn(Probe, new_log, name="reused")
    with pytest.raises(ActorStopped):
        old.tell("x")
    await new.ask("y")
    assert "x" not in new_log
    await system2.shutdown()
