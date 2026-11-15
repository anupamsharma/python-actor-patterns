"""Batching log events by size."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Batch, Flush


async def main() -> None:
    async def write_batch(items: tuple[str, ...] | list[str]) -> int:
        print(f"flush {len(items)} logs:", list(items))
        return len(items)

    async with ActorSystem() as system:
        batch = await system.spawn(Batch, write_batch, max_size=5, max_wait=1.0)
        for i in range(12):
            await batch.ask(f"log-{i}")
        await batch.ask(Flush())


if __name__ == "__main__":
    asyncio.run(main())
