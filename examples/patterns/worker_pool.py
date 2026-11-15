"""Process jobs using a worker pool."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import spawn_worker_pool


class JobWorker(Actor[int, int]):
    async def receive(self, message: int) -> int:
        await asyncio.sleep(0.01)
        return message * message


async def main() -> None:
    async with ActorSystem() as system:
        pool = await spawn_worker_pool(system, JobWorker, size=8, name="jobs")
        results = await asyncio.gather(*(pool.ask(i) for i in range(20)))
        print(results)


if __name__ == "__main__":
    asyncio.run(main())
