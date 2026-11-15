"""Buffer messages until capacity or flush."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Buffer, Flush


async def main() -> None:
    async with ActorSystem() as system:
        buf = await system.spawn(Buffer, capacity=100)
        for i in range(5):
            await buf.ask(i)
        print(await buf.ask(Flush()))


if __name__ == "__main__":
    asyncio.run(main())
