"""Content-based routing by message type."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import ContentBasedRouter


class Odd(Actor[int, str]):
    async def receive(self, message: int) -> str:
        return f"odd:{message}"


class Even(Actor[int, str]):
    async def receive(self, message: int) -> str:
        return f"even:{message}"


async def main() -> None:
    async with ActorSystem() as system:
        odd, even = await system.spawn(Odd), await system.spawn(Even)
        router = await system.spawn(
            ContentBasedRouter, [(lambda m: m % 2 == 0, even), (lambda m: True, odd)]
        )
        print(await router.ask(2), await router.ask(3))


if __name__ == "__main__":
    asyncio.run(main())
