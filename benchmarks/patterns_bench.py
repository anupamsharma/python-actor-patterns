"""Benchmarks for selected actor patterns.

Run::

    python benchmarks/patterns_bench.py [--quick]
"""

from __future__ import annotations

import argparse
import asyncio
import platform
import sys
import time
from typing import Any

from pyactors import Actor, ActorSystem
from pyactors.patterns import (
    Batch,
    Consume,
    Pipeline,
    Produce,
    QueueActor,
    RateLimiter,
    RoundRobinRouter,
    spawn_worker_pool,
)


class Echo(Actor[Any, Any]):
    async def receive(self, message: Any) -> Any:
        return message


class Worker(Actor[int, int]):
    async def receive(self, message: int) -> int:
        return message * message


def record(name: str, **fields: Any) -> None:
    parts = "  ".join(
        f"{k}={v:,.2f}"
        if isinstance(v, float)
        else f"{k}={v:,}"
        if isinstance(v, int)
        else f"{k}={v}"
        for k, v in fields.items()
    )
    print(f"  {name:<28} {parts}", flush=True)


async def bench_router(messages: int) -> None:
    print("\n[round-robin router]")
    async with ActorSystem() as system:
        workers = [await system.spawn(Echo) for _ in range(8)]
        router = await system.spawn(RoundRobinRouter, workers)
        t0 = time.perf_counter()
        for i in range(messages):
            await router.ask(i)
        elapsed = time.perf_counter() - t0
        record("router_ask", messages=messages, asks_per_s=messages / elapsed)


async def bench_queue(messages: int) -> None:
    print("\n[queue actor]")
    async with ActorSystem() as system:
        q = await system.spawn(QueueActor, capacity=10_000)

        async def producer() -> None:
            for i in range(messages):
                await q.ask(Produce(i, wait=True))

        async def consumer() -> None:
            for _ in range(messages):
                await q.ask(Consume())

        t0 = time.perf_counter()
        await asyncio.gather(producer(), consumer())
        elapsed = time.perf_counter() - t0
        record("queue_throughput", messages=messages, msgs_per_s=messages / elapsed)


async def bench_batch(messages: int) -> None:
    print("\n[batch actor]")
    batches = 0

    async def handler(items: Any) -> int:
        nonlocal batches
        batches += 1
        return len(items)

    async with ActorSystem() as system:
        batch = await system.spawn(Batch, handler, max_size=64, max_wait=None)
        t0 = time.perf_counter()
        for i in range(messages):
            await batch.ask(i)
        from pyactors.patterns import Flush

        await batch.ask(Flush())
        elapsed = time.perf_counter() - t0
        record(
            "batch_by_size",
            messages=messages,
            batches=batches,
            msgs_per_s=messages / elapsed,
        )


async def bench_rate_limiter(messages: int) -> None:
    print("\n[rate limiter wait mode]")
    async with ActorSystem() as system:
        limiter = await system.spawn(RateLimiter, rate=50_000, per=1.0, burst=50_000, mode="wait")
        t0 = time.perf_counter()
        for i in range(messages):
            await limiter.ask(i)
        elapsed = time.perf_counter() - t0
        record("rate_limiter_wait", messages=messages, msgs_per_s=messages / elapsed)


async def bench_worker_pool(messages: int) -> None:
    print("\n[worker pool]")
    async with ActorSystem() as system:
        pool = await spawn_worker_pool(system, Worker, size=8, name="bench-pool")
        t0 = time.perf_counter()
        await asyncio.gather(*(pool.ask(i) for i in range(messages)))
        elapsed = time.perf_counter() - t0
        record("worker_pool_ask", messages=messages, asks_per_s=messages / elapsed)


async def bench_pipeline(messages: int) -> None:
    print("\n[pipeline]")

    async def double(x: int) -> int:
        return x * 2

    async def add_one(x: int) -> int:
        return x + 1

    async with ActorSystem() as system:
        pipe = await system.spawn(Pipeline, [double, add_one])
        t0 = time.perf_counter()
        for i in range(messages):
            await pipe.ask(i)
        elapsed = time.perf_counter() - t0
        record("pipeline_ask", messages=messages, asks_per_s=messages / elapsed)


async def main(quick: bool) -> None:
    print("pyactors pattern benchmarks")
    print(f"  python: {sys.version.split()[0]}")
    print(f"  os: {platform.platform()}")
    n = 5_000 if quick else 50_000
    await bench_router(n)
    await bench_queue(n)
    await bench_batch(n)
    await bench_rate_limiter(min(n, 10_000))
    await bench_worker_pool(n // 2)
    await bench_pipeline(n)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    asyncio.run(main(parser.parse_args().quick))
