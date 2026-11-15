"""Broadcast a message to all routees."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import BroadcastRouter


class Worker(Actor[str, str]):
    async def receive(self, message: str) -> str:
        return f"{self.context.name}:{message}"


async def main() -> None:
    async with ActorSystem() as system:
        workers = [await system.spawn(Worker, name=f"n{i}") for i in range(3)]
        bcast = await system.spawn(BroadcastRouter, workers)
        print(await bcast.ask("ping"))


if __name__ == "__main__":
    asyncio.run(main())
