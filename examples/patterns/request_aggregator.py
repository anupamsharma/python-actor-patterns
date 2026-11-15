"""Aggregate partial requests by group id."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import PartialRequest, RequestAggregator


async def main() -> None:
    async with ActorSystem() as system:
        agg = await system.spawn(RequestAggregator, expected_parts=3, timeout=2.0)
        tasks = [
            asyncio.create_task(agg.ask(PartialRequest("order-1", part)))
            for part in ("a", "b", "c")
        ]
        results = await asyncio.gather(*tasks)
        print(results[0].parts)


if __name__ == "__main__":
    asyncio.run(main())
