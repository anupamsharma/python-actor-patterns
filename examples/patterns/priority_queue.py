"""Priority queue: lower number = higher priority."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import PriorityConsume, PriorityProduce, PriorityQueueActor


async def main() -> None:
    async with ActorSystem() as system:
        q = await system.spawn(PriorityQueueActor)
        await q.ask(PriorityProduce("low", 10))
        await q.ask(PriorityProduce("high", 1))
        print((await q.ask(PriorityConsume())).value)
        print((await q.ask(PriorityConsume())).value)


if __name__ == "__main__":
    asyncio.run(main())
