"""Pipeline: push a stream of records through ordered processing stages."""

from __future__ import annotations

import asyncio
from typing import Any

from pyactors import Actor, ActorSystem
from pyactors.patterns import GetStats, Pipeline, PipelineStage


class Enricher(Actor[dict[str, Any], dict[str, Any]]):
    """A stage can be an actor as well as a plain function."""

    async def receive(self, message: dict[str, Any]) -> dict[str, Any]:
        tier = "gold" if message["amount"] >= 100 else "standard"
        return {**message, "tier": tier}


def parse(line: str) -> dict[str, Any]:
    user, amount = line.split(",")
    return {"user": user.strip(), "amount": int(amount)}


async def render(record: dict[str, Any]) -> str:
    return f"{record['user']:<6} {record['amount']:>4}  {record['tier']}"


async def main() -> None:
    stream = ["ada, 250", "bob, 40", "cyd, 100", "dee, 7"]
    async with ActorSystem() as system:
        enricher = await system.spawn(Enricher, name="enricher")
        pipeline = await system.spawn(
            Pipeline,
            [
                PipelineStage("parse", parse),
                PipelineStage("enrich", enricher),
                PipelineStage("render", render),
            ],
            name="pipeline",
        )
        for line in stream:
            print(await pipeline.ask(line))
        print("stats:", await pipeline.ask(GetStats()))


if __name__ == "__main__":
    asyncio.run(main())
