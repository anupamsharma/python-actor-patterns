"""Limit concurrent work with a semaphore."""

from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Acquire, Release, Semaphore


async def main() -> None:
    async with ActorSystem() as system:
        sem = await system.spawn(Semaphore, 2)

        async def work(i: int) -> None:
            permit = await sem.ask(Acquire())
            print("start", i)
            await asyncio.sleep(0.05)
            print("end", i)
            await sem.ask(Release(permit.permit_id))

        await asyncio.gather(*(work(i) for i in range(5)))


if __name__ == "__main__":
    asyncio.run(main())
