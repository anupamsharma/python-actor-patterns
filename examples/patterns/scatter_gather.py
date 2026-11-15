"""Aggregate responses from multiple workers."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import CompletionPolicy, ScatterGather


class Worker(Actor[int, int]):
    async def receive(self, message: int) -> int:
        return message + 1


async def main() -> None:
    async with ActorSystem() as system:
        workers = [await system.spawn(Worker) for _ in range(5)]
        sg = await system.spawn(ScatterGather, workers, policy=CompletionPolicy.QUORUM, quorum=3)
        result = await sg.ask(10)
        print("successes:", result.successes)


if __name__ == "__main__":
    asyncio.run(main())
