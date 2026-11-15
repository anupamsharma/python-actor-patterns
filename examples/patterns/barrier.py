"""Synchronize N participants at a barrier."""

from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Arrive, Barrier


async def main() -> None:
    async with ActorSystem() as system:
        barrier = await system.spawn(Barrier, 3)

        async def worker(i: int) -> None:
            payloads = await barrier.ask(Arrive(i))
            print(i, "released with", payloads)

        await asyncio.gather(*(worker(i) for i in range(3)))


if __name__ == "__main__":
    asyncio.run(main())
