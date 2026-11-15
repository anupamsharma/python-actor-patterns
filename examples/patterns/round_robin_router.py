"""Round-robin distribution across workers."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import RoundRobinRouter


class Worker(Actor[int, int]):
    async def receive(self, message: int) -> int:
        return message * 2


async def main() -> None:
    async with ActorSystem() as system:
        workers = [await system.spawn(Worker) for _ in range(4)]
        router = await system.spawn(RoundRobinRouter, workers)
        print([await router.ask(i) for i in range(8)])


if __name__ == "__main__":
    asyncio.run(main())
