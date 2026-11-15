"""One-shot timer delivery."""

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import ScheduleOnce, Timer


class Sink(Actor[str, None]):
    async def receive(self, message: str) -> None:
        print("received:", message)


async def main() -> None:
    async with ActorSystem() as system:
        sink = await system.spawn(Sink)
        timer = await system.spawn(Timer)
        await timer.ask(ScheduleOnce(0.1, "tick", sink))
        await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())
