from __future__ import annotations

import asyncio

import pytest

from pyactors import ActorSystem
from pyactors.patterns import (
    Ack,
    Buffer,
    Buffered,
    Consume,
    Flush,
    Offer,
    OverflowPolicy,
    PatternFull,
    PriorityConsume,
    PriorityProduce,
    PriorityQueueActor,
    Produce,
    QueueActor,
)

from ..helpers import within


async def test_queue_produce_consume(system: ActorSystem) -> None:
    q = await system.spawn(QueueActor, capacity=10)
    await within(q.ask(Produce(1)))
    await within(q.ask(Produce(2)))
    assert (await within(q.ask(Consume()))).value == 1
    assert (await within(q.ask(Consume()))).value == 2


async def test_queue_backpressure_and_offer(system: ActorSystem) -> None:
    q = await system.spawn(QueueActor, capacity=1, overflow=OverflowPolicy.REJECT)
    await within(q.ask(Produce(1)))
    assert await within(q.ask(Offer(2))) is False
    with pytest.raises(PatternFull):
        await within(q.ask(Produce(3, wait=False)))


async def test_queue_consumer_waits(system: ActorSystem) -> None:
    q = await system.spawn(QueueActor)
    consumer = asyncio.create_task(q.ask(Consume()))
    await asyncio.sleep(0)
    assert not consumer.done()
    await within(q.ask(Produce("x")))
    item = await within(consumer)
    assert item.value == "x"


async def test_queue_ack(system: ActorSystem) -> None:
    q = await system.spawn(QueueActor, ack_required=True)
    await within(q.ask(Produce("job")))
    item = await within(q.ask(Consume()))
    assert await within(q.ask(Ack(item.item_id))) is True
    assert await within(q.ask(Ack("missing"))) is False


async def test_priority_queue_order(system: ActorSystem) -> None:
    q = await system.spawn(PriorityQueueActor)
    await within(q.ask(PriorityProduce("low", priority=10)))
    await within(q.ask(PriorityProduce("high", priority=1)))
    await within(q.ask(PriorityProduce("mid", priority=5)))
    assert (await within(q.ask(PriorityConsume()))).value == "high"
    assert (await within(q.ask(PriorityConsume()))).value == "mid"
    assert (await within(q.ask(PriorityConsume()))).value == "low"


async def test_buffer_flush_on_capacity(system: ActorSystem) -> None:
    seen: list[tuple[object, ...]] = []

    async def handler(items):  # type: ignore[no-untyped-def]
        seen.append(tuple(items))
        return len(items)

    buf = await system.spawn(Buffer, capacity=3, handler=handler)
    assert await within(buf.ask(1)) is None
    assert await within(buf.ask(2)) is None
    assert await within(buf.ask(3)) == 3
    assert seen == [(1, 2, 3)]


async def test_buffer_explicit_flush(system: ActorSystem) -> None:
    buf = await system.spawn(Buffer, capacity=100)
    await within(buf.ask("a"))
    await within(buf.ask("b"))
    result = await within(buf.ask(Flush()))
    assert isinstance(result, Buffered)
    assert result.items == ("a", "b")
