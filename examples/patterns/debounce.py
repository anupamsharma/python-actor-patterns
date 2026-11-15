"""Debounce rapid user input."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Debounce


async def main() -> None:
    async def search(query: str) -> str:
        print("searching:", query)
        return query

    async with ActorSystem() as system:
        deb = await system.spawn(Debounce, 0.1, handler=search)
        tasks = [asyncio.create_task(deb.ask(q)) for q in ("a", "ab", "abc")]
        print("result:", await asyncio.gather(*tasks))


if __name__ == "__main__":
    asyncio.run(main())
