"""Route by customer id with consistent hashing."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import ConsistentHashRouter


class Shard(Actor[dict[str, str], str]):
    async def receive(self, message: dict[str, str]) -> str:
        return f"{self.context.name} handled {message['id']}"


async def main() -> None:
    async with ActorSystem() as system:
        shards = [await system.spawn(Shard, name=f"shard-{i}") for i in range(4)]
        router = await system.spawn(ConsistentHashRouter, shards, key=lambda m: m["id"])
        for cid in ("cust-a", "cust-b", "cust-a"):
            print(await router.ask({"id": cid}))


if __name__ == "__main__":
    asyncio.run(main())
