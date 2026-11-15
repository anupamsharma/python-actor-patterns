"""Generic router using round-robin strategy."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import RoundRobinRouter


class Echo(Actor[str, str]):
    async def receive(self, message: str) -> str:
        return f"{self.context.name}:{message}"


async def main() -> None:
    async with ActorSystem() as system:
        workers = [await system.spawn(Echo, name=f"w{i}") for i in range(3)]
        router = await system.spawn(RoundRobinRouter, workers)
        for i in range(6):
            print(await router.ask(f"msg-{i}"))


if __name__ == "__main__":
    asyncio.run(main())
