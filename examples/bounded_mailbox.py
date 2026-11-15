"""Bounded mailboxes: tell() fails fast, send() applies back-pressure."""

from __future__ import annotations

import asyncio

from pyactors import Actor, ActorSystem, MailboxFull


class SlowConsumer(Actor[int, None]):
    def __init__(self) -> None:
        self.processed = 0

    async def receive(self, message: int) -> None:
        await asyncio.sleep(0.001)
        self.processed += 1


async def main() -> None:
    async with ActorSystem() as system:
        consumer = await system.spawn(SlowConsumer, name="consumer", mailbox_size=10)

        accepted = rejected = 0
        for i in range(100):
            try:
                consumer.tell(i)
                accepted += 1
            except MailboxFull:
                rejected += 1
        print(f"tell(): accepted={accepted} rejected={rejected} (mailbox_size=10)")

        loop = asyncio.get_running_loop()
        started = loop.time()
        for i in range(100):
            await consumer.send(i)  # waits whenever the mailbox is full
        print(f"send(): 100 messages accepted in {loop.time() - started:.3f}s with back-pressure")


if __name__ == "__main__":
    asyncio.run(main())
