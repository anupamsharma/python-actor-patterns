"""Throttle emissions to at most one per interval."""

import asyncio
import time

from pyactors import ActorSystem
from pyactors.patterns import Throttle


async def main() -> None:
    async with ActorSystem() as system:
        th = await system.spawn(Throttle, 0.05)
        t0 = time.monotonic()
        for i in range(5):
            await th.ask(i)
            print(i, f"+{time.monotonic() - t0:.3f}s")


if __name__ == "__main__":
    asyncio.run(main())
