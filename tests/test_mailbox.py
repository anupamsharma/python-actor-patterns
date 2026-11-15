from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pyactors import ActorRef, ActorStopped, ActorSystem, MailboxFull
from pyactors.envelope import Envelope
from pyactors.mailbox import Mailbox

from .helpers import GET_LOG, Block, Probe, within, yield_loop


def _mailbox(ref: ActorRef[Any, Any]) -> Mailbox:
    return ref._mailbox  # white-box access for leak assertions


async def _blocked_full(
    system: ActorSystem, capacity: int
) -> tuple[ActorRef[Any, Any], Block, list[Any]]:
    """Spawn a Probe whose handler is blocked and whose mailbox is full."""
    log: list[Any] = []
    ref = await system.spawn(Probe, log, mailbox_size=capacity)
    block = Block("blocker")
    ref.tell(block)
    await within(block.entered.wait())
    for i in range(capacity):
        ref.tell(("queued", i))
    return ref, block, log


async def test_unbounded_mailbox(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    block = Block("b")
    ref.tell(block)
    await within(block.entered.wait())
    for i in range(50_000):
        ref.tell(i)
    assert len(_mailbox(ref)) == 50_000
    block.release.set()
    log = await within(ref.ask(GET_LOG), 20)
    assert log == ["start", "b", *range(50_000)]


async def test_bounded_mailbox_and_tell_raises_mailbox_full(system: ActorSystem) -> None:
    ref, block, log = await _blocked_full(system, 3)
    with pytest.raises(MailboxFull):
        ref.tell("overflow")
    assert len(_mailbox(ref)) == 3
    block.release.set()
    await within(ref.ask(GET_LOG))
    assert log == ["start", "blocker", ("queued", 0), ("queued", 1), ("queued", 2)]
    ref.tell("fits-now")


async def test_send_waits_for_capacity(system: ActorSystem) -> None:
    ref, block, log = await _blocked_full(system, 2)
    sender = asyncio.create_task(ref.send("late"))
    await yield_loop()
    assert not sender.done()
    block.release.set()
    await within(sender)
    await within(ref.ask(GET_LOG))
    assert log[-1] == "late"
    assert log.index("late") > log.index(("queued", 1))


async def test_cancelled_send_does_not_leak(system: ActorSystem) -> None:
    ref, block, log = await _blocked_full(system, 1)
    sender = asyncio.create_task(ref.send("cancelled"))
    await yield_loop()
    assert len(_mailbox(ref)._putters) == 1
    sender.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sender
    assert len(_mailbox(ref)._putters) == 0
    block.release.set()
    await within(ref.ask(GET_LOG))
    assert "cancelled" not in log
    await within(ref.send("after"))


async def test_woken_then_cancelled_putter_passes_wakeup_on() -> None:
    loop = asyncio.get_running_loop()
    mailbox = Mailbox(1, loop, "unit")
    mailbox.put_nowait(Envelope("first", None))
    p1 = asyncio.create_task(mailbox.put(Envelope("p1", None)))
    p2 = asyncio.create_task(mailbox.put(Envelope("p2", None)))
    await yield_loop()
    assert len(mailbox._putters) == 2
    mailbox.pop_nowait()  # frees one slot and wakes p1 ...
    p1.cancel()  # ... which is cancelled before it can use the wake-up
    await within(asyncio.gather(p1, p2, return_exceptions=True))
    assert p1.cancelled()
    assert p2.done()
    assert p2.exception() is None
    assert [e.message for e in mailbox.drain()] == ["p2"]
    assert not mailbox._putters


async def test_stop_releases_blocked_senders(system: ActorSystem) -> None:
    ref, block, log = await _blocked_full(system, 1)
    senders = [asyncio.create_task(ref.send(("blocked", i))) for i in range(5)]
    await yield_loop()
    stopper = asyncio.create_task(system.stop(ref))  # graceful
    for s in senders:
        with pytest.raises(ActorStopped):
            await within(s)
    assert not stopper.done()  # still draining the accepted message
    block.release.set()
    await within(stopper)
    assert log == ["start", "blocker", ("queued", 0), "stop"]
    assert not _mailbox(ref)._putters


async def test_ordering_under_backpressure(system: ActorSystem) -> None:
    ref = await system.spawn(Probe, mailbox_size=2)
    for i in range(3000):
        await ref.send(i)
    assert (await within(ref.ask(GET_LOG)))[1:] == list(range(3000))


async def test_tell_to_stopped_actor_raises(system: ActorSystem) -> None:
    ref = await system.spawn(Probe)
    await system.stop(ref)
    with pytest.raises(ActorStopped):
        ref.tell("x")
    with pytest.raises(ActorStopped):
        await ref.send("x")


async def test_mailbox_unit_close_semantics() -> None:
    loop = asyncio.get_running_loop()
    mailbox = Mailbox(0, loop, "unit")
    mailbox.put_nowait(Envelope(1, None))
    assert mailbox.close(lambda: ActorStopped("unit")) is True
    assert mailbox.close(lambda: ActorStopped("unit")) is False
    with pytest.raises(ActorStopped):
        mailbox.put_nowait(Envelope(2, None))
    first = await mailbox.get()
    assert first is not None
    assert first.message == 1
    assert await mailbox.get() is None


async def test_mailbox_unit_close_wakes_consumer() -> None:
    loop = asyncio.get_running_loop()
    mailbox = Mailbox(0, loop, "unit")
    getter = asyncio.create_task(mailbox.get())
    await yield_loop()
    assert not getter.done()
    mailbox.close(lambda: ActorStopped("unit"))
    assert await within(getter) is None
