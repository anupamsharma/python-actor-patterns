"""Rate-limit requests with a token bucket."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import RateLimited, RateLimiter


async def main() -> None:
    async with ActorSystem() as system:
        lim = await system.spawn(RateLimiter, rate=2, per=1.0, burst=2, mode="reject")
        for i in range(4):
            try:
                print(await lim.ask(i))
            except RateLimited as exc:
                print("rejected", i, exc)


if __name__ == "__main__":
    asyncio.run(main())
