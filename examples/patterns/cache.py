"""Cache: LRU eviction plus per-entry time-to-live."""

from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Cache, CacheDelete, CacheGet, CachePut, GetStats


async def main() -> None:
    async with ActorSystem() as system:
        cache = await system.spawn(Cache, max_size=2, name="cache")

        await cache.ask(CachePut("a", 1))
        await cache.ask(CachePut("b", 2))
        await cache.ask(CacheGet("a"))  # touch "a" so "b" becomes least recent
        await cache.ask(CachePut("c", 3))  # evicts "b"
        print("a:", await cache.ask(CacheGet("a")))
        print("b:", await cache.ask(CacheGet("b", default="evicted")))

        print("deleted a:", await cache.ask(CacheDelete("a")))

        await cache.ask(CachePut("session", "token", ttl=0.1))
        print("session before ttl:", await cache.ask(CacheGet("session")))
        await asyncio.sleep(0.15)
        print("session after ttl: ", await cache.ask(CacheGet("session", default="expired")))

        print("stats:", await cache.ask(GetStats()))


if __name__ == "__main__":
    asyncio.run(main())
