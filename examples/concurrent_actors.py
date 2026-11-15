"""Many actors working concurrently on one event loop."""

from __future__ import annotations

import asyncio
from typing import Any

from pyactors import Actor, ActorRef, ActorSystem


class Worker(Actor[int, int]):
    """Simulates I/O-bound work; each worker handles one job at a time."""

    def __init__(self) -> None:
        self.jobs_done = 0

    async def receive(self, job: int) -> int:
        await asyncio.sleep(0.01)  # e.g. a network call
        self.jobs_done += 1
        return job * job


async def main() -> None:
    workers_n, jobs_n = 100, 1000
    async with ActorSystem() as system:
        workers: list[ActorRef[Any, int]] = [
            await system.spawn(Worker, name=f"worker-{i}") for i in range(workers_n)
        ]
        loop = asyncio.get_running_loop()
        started = loop.time()
        results = await asyncio.gather(
            *(workers[job % workers_n].ask(job) for job in range(jobs_n))
        )
        elapsed = loop.time() - started
        assert results == [j * j for j in range(jobs_n)]
        serial = jobs_n * 0.01
        print(
            f"{jobs_n} jobs on {workers_n} actors took {elapsed:.2f}s "
            f"(a single actor would need ~{serial:.0f}s)"
        )


if __name__ == "__main__":
    asyncio.run(main())
