"""Schedule a callable after a delay."""

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import ScheduleIn, TaskScheduler


async def main() -> None:
    done = asyncio.Event()

    async def job() -> None:
        print("job ran")
        done.set()

    async with ActorSystem() as system:
        sched = await system.spawn(TaskScheduler)
        await sched.ask(ScheduleIn(0.1, job))
        await done.wait()


if __name__ == "__main__":
    asyncio.run(main())
