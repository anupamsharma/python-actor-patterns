"""Random router demo."""

import asyncio
import random

from pyactors import Actor, ActorSystem
from pyactors.patterns import RandomRouter


class Echo(Actor[int, int]):
    async def receive(self, message: int) -> int:
        return message


async def main() -> None:
    async with ActorSystem() as system:
        workers = [await system.spawn(Echo) for _ in range(3)]
        router = await system.spawn(RandomRouter, workers, rng=random.Random(42))
        print([await router.ask(i) for i in range(5)])


if __name__ == "__main__":
    asyncio.run(main())
