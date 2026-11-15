"""MapReduce: count words per document (map), then merge the counts (reduce)."""

from __future__ import annotations

import asyncio
from collections import Counter

from pyactors import ActorSystem
from pyactors.patterns import MapReduce, MapReduceJob


def count_words(doc: str) -> Counter[str]:
    return Counter(doc.lower().split())


def merge(total: Counter[str], part: Counter[str]) -> Counter[str]:
    return total + part


async def main() -> None:
    docs = ("the cat sat", "the dog sat down", "a cat and a dog")
    async with ActorSystem() as system:
        job = await system.spawn(MapReduce, mapper=count_words, reducer=merge, name="wc")
        totals = await job.ask(list(docs))
        print("word counts:", dict(totals.most_common(4)))

        # Mapper and reducer can also be supplied per job.
        longest = await job.ask(MapReduceJob(items=docs, mapper=lambda d: len(d), reducer=max))
        print("longest document has", longest, "characters")


if __name__ == "__main__":
    asyncio.run(main())
