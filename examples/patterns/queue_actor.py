"""Producer/consumer queue with back-pressure."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Consume, Produce, QueueActor


async def main() -> None:
    async with ActorSystem() as system:
        q = await system.spawn(QueueActor, capacity=8)

        async def producer() -> None:
            for i in range(20):
                await q.ask(Produce(i))

        async def consumer() -> None:
            for _ in range(20):
                item = await q.ask(Consume())
                print("got", item.value)

        await asyncio.gather(producer(), consumer())


if __name__ == "__main__":
    asyncio.run(main())
