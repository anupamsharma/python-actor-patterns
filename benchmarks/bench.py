"""pyactors benchmark suite.

Run::

    python benchmarks/bench.py            # full run
    python benchmarks/bench.py --quick    # smaller sizes, for smoke testing
    python benchmarks/bench.py --json results.json

Every number printed is measured on the current machine; nothing here is a
claim about expected performance.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyactors import Actor, ActorRef, ActorSystem, MailboxFull

# ---------------------------------------------------------------- message types


@dataclass(slots=True)
class DataMsg:
    """Small (mutable) dataclass message."""

    seq: int
    value: float


IMMUTABLE = ("event", 1, 2.5)  # small immutable object (tuple)

MESSAGE_FACTORIES: dict[str, Callable[[int], Any]] = {
    "int": lambda i: i,
    "tuple": lambda i: IMMUTABLE,
    "dataclass": lambda i: DataMsg(i, 1.5),
}

# ---------------------------------------------------------------- actors


class Sink(Actor[Any, int]):
    """Counts messages; replies with the count."""

    def __init__(self) -> None:
        self.count = 0

    async def receive(self, message: Any) -> int:
        self.count += 1
        return self.count


class Echo(Actor[Any, Any]):
    async def receive(self, message: Any) -> Any:
        return message


class LatencyRecorder(Actor[Any, Any]):
    """Records enqueue->handle latency for messages carrying a perf_counter_ns stamp."""

    def __init__(self) -> None:
        self.samples: list[int] = []

    async def receive(self, message: Any) -> Any:
        if message is None:
            return self.samples
        self.samples.append(time.perf_counter_ns() - message)
        return None


class Idle(Actor[Any, None]):
    async def receive(self, message: Any) -> None:
        return None


# ---------------------------------------------------------------- reporting

Result = dict[str, Any]
RESULTS: list[Result] = []


def record(benchmark: str, **fields: Any) -> None:
    row: Result = {"benchmark": benchmark, **fields}
    RESULTS.append(row)
    parts = []
    for key, value in fields.items():
        if isinstance(value, float):
            parts.append(f"{key}={value:,.2f}")
        elif isinstance(value, int):
            parts.append(f"{key}={value:,}")
        else:
            parts.append(f"{key}={value}")
    print(f"  {benchmark:<28} " + "  ".join(parts), flush=True)


def percentiles_us(samples_ns: list[int]) -> dict[str, float]:
    qs = statistics.quantiles(samples_ns, n=100, method="inclusive")
    return {
        "p50_us": qs[49] / 1000,
        "p95_us": qs[94] / 1000,
        "p99_us": qs[98] / 1000,
        "max_us": max(samples_ns) / 1000,
    }


def cpu_info() -> str:
    try:
        if sys.platform == "darwin":
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        if sys.platform.startswith("linux"):
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return platform.processor() or "unknown"


def environment() -> dict[str, Any]:
    return {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "os": platform.platform(),
        "cpu": cpu_info(),
        "logical_cpus": os.cpu_count(),
    }


def peak_rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024


# ---------------------------------------------------------------- benchmarks


async def bench_creation(counts: list[int]) -> None:
    print("\n[actor creation / startup]")
    for n in counts:
        async with ActorSystem() as system:
            gc.collect()
            t0 = time.perf_counter()
            for _ in range(n):
                await system.spawn(Idle)
            elapsed = time.perf_counter() - t0
            record(
                "spawn_sequential",
                actors=n,
                total_ms=elapsed * 1000,
                us_per_actor=elapsed / n * 1e6,
                actors_per_s=n / elapsed,
            )
        async with ActorSystem() as system:
            gc.collect()
            t0 = time.perf_counter()
            await asyncio.gather(*(system.spawn(Idle) for _ in range(n)))
            elapsed = time.perf_counter() - t0
            record(
                "spawn_concurrent",
                actors=n,
                total_ms=elapsed * 1000,
                actors_per_s=n / elapsed,
            )
        async with ActorSystem() as system:
            for _ in range(n):
                await system.spawn(Idle)
            t0 = time.perf_counter()
            await system.shutdown()
            elapsed = time.perf_counter() - t0
            record("shutdown", actors=n, total_ms=elapsed * 1000, actors_per_s=n / elapsed)


async def bench_memory(counts: list[int]) -> None:
    print("\n[memory per idle actor] (tracemalloc; includes actor, cell, ref, mailbox, task)")
    for n in counts:
        system = ActorSystem()
        await system.spawn(Idle)  # warm up lazily created structures
        gc.collect()
        tracemalloc.start()
        before, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        refs = [await system.spawn(Idle) for _ in range(n)]
        await asyncio.sleep(0)
        gc.collect()
        after, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        record(
            "idle_actor_memory",
            actors=n,
            bytes_per_actor=(after - before) / n,
            total_kib=(after - before) / 1024,
            peak_kib=(peak - before) / 1024,
        )
        del refs
        await system.shutdown()


async def _drain(ref: ActorRef[Any, Any]) -> None:
    await ref.ask(None)


async def bench_tell(messages: int) -> None:
    print("\n[tell throughput, 1 actor] (enqueue all, then wait until processed)")
    for kind, factory in MESSAGE_FACTORIES.items():
        async with ActorSystem() as system:
            ref = await system.spawn(Sink)
            payload = [factory(i) for i in range(messages)]
            gc.collect()
            t0 = time.perf_counter()
            tell = ref.tell
            for m in payload:
                tell(m)
            enqueued = time.perf_counter() - t0
            await _drain(ref)
            elapsed = time.perf_counter() - t0
            record(
                "tell_throughput",
                msg_type=kind,
                actors=1,
                messages=messages,
                msgs_per_s=messages / elapsed,
                enqueue_ns_per_msg=enqueued / messages * 1e9,
            )


async def bench_tell_interleaved(messages: int) -> None:
    print("\n[tell throughput, producer yields every 100 msgs] (actor idle/wake cycles)")
    async with ActorSystem() as system:
        ref = await system.spawn(Sink)
        gc.collect()
        t0 = time.perf_counter()
        for i in range(messages):
            ref.tell(i)
            if i % 100 == 99:
                await asyncio.sleep(0)
        await _drain(ref)
        elapsed = time.perf_counter() - t0
        record("tell_interleaved", messages=messages, msgs_per_s=messages / elapsed)


async def bench_ask(messages: int) -> None:
    print("\n[ask throughput, 1 actor]")
    for kind, factory in MESSAGE_FACTORIES.items():
        async with ActorSystem() as system:
            ref = await system.spawn(Echo)
            payload = [factory(i) for i in range(messages)]
            gc.collect()
            t0 = time.perf_counter()
            ask = ref.ask
            for m in payload:
                await ask(m)
            elapsed = time.perf_counter() - t0
            record(
                "ask_sequential",
                msg_type=kind,
                messages=messages,
                asks_per_s=messages / elapsed,
            )
    for callers in (10, 100):
        async with ActorSystem() as system:
            ref = await system.spawn(Echo)
            per_caller = messages // callers

            async def caller(r: ActorRef[Any, Any] = ref, k: int = per_caller) -> None:
                for i in range(k):
                    await r.ask(i)

            gc.collect()
            t0 = time.perf_counter()
            await asyncio.gather(*(caller() for _ in range(callers)))
            elapsed = time.perf_counter() - t0
            total = per_caller * callers
            record("ask_concurrent", callers=callers, messages=total, asks_per_s=total / elapsed)
    async with ActorSystem() as system:
        ref = await system.spawn(Echo)
        gc.collect()
        t0 = time.perf_counter()
        for i in range(messages):
            await ref.ask(i, timeout=10)
        elapsed = time.perf_counter() - t0
        record("ask_with_timeout", messages=messages, asks_per_s=messages / elapsed)


async def bench_latency(messages: int) -> None:
    print("\n[latency]")
    async with ActorSystem() as system:
        ref = await system.spawn(Echo)
        samples: list[int] = []
        for i in range(messages):
            t0 = time.perf_counter_ns()
            await ref.ask(i)
            samples.append(time.perf_counter_ns() - t0)
        record("ask_round_trip", messages=messages, **percentiles_us(samples))
    async with ActorSystem() as system:
        ref = await system.spawn(LatencyRecorder)
        for _ in range(messages):
            ref.tell(time.perf_counter_ns())
            await asyncio.sleep(0)  # idle actor: measures wake-up latency
        samples = await ref.ask(None)
        record("tell_to_receive_idle", messages=messages, **percentiles_us(samples))
    async with ActorSystem() as system:
        ref = await system.spawn(LatencyRecorder)
        for _ in range(messages):
            ref.tell(time.perf_counter_ns())
        samples = await ref.ask(None)
        record("tell_to_receive_burst", messages=messages, **percentiles_us(samples))


async def bench_multi_actor(counts: list[int], messages: int) -> None:
    print("\n[throughput across many actors] (tell, round-robin, then drain all)")
    for n in counts:
        async with ActorSystem() as system:
            refs = [await system.spawn(Sink) for _ in range(n)]
            total = max(messages, n)
            gc.collect()
            t0 = time.perf_counter()
            for i in range(total):
                refs[i % n].tell(i)
            await asyncio.gather(*(_drain(r) for r in refs))
            elapsed = time.perf_counter() - t0
            record(
                "multi_actor_tell",
                actors=n,
                messages=total,
                msgs_per_s=total / elapsed,
            )
        async with ActorSystem() as system:
            refs = [await system.spawn(Echo) for _ in range(n)]
            total = max(messages // 4, n)
            gc.collect()
            t0 = time.perf_counter()
            await asyncio.gather(*(refs[i % n].ask(i) for i in range(total)))
            elapsed = time.perf_counter() - t0
            record("multi_actor_ask", actors=n, messages=total, asks_per_s=total / elapsed)


async def bench_bounded(messages: int) -> None:
    print("\n[bounded mailbox]")
    for capacity in (0, 16, 1024):
        async with ActorSystem() as system:
            ref = await system.spawn(Sink, mailbox_size=capacity)
            gc.collect()
            t0 = time.perf_counter()
            send = ref.send
            for i in range(messages):
                await send(i)
            await _drain(ref)
            elapsed = time.perf_counter() - t0
            record(
                "send_backpressure",
                capacity=capacity or "unbounded",
                messages=messages,
                msgs_per_s=messages / elapsed,
            )
    async with ActorSystem() as system:
        ref = await system.spawn(Sink, mailbox_size=1024)
        accepted = rejected = 0
        gc.collect()
        t0 = time.perf_counter()
        for i in range(messages):
            try:
                ref.tell(i)
                accepted += 1
            except MailboxFull:
                rejected += 1
                await asyncio.sleep(0)
        await _drain(ref)
        elapsed = time.perf_counter() - t0
        record(
            "tell_with_mailbox_full",
            capacity=1024,
            attempts=messages,
            accepted=accepted,
            rejected=rejected,
            accepted_per_s=accepted / elapsed,
        )
    for producers in (10, 100):
        async with ActorSystem() as system:
            ref = await system.spawn(Sink, mailbox_size=64)
            per = messages // producers

            async def produce(r: ActorRef[Any, Any] = ref, k: int = per) -> None:
                for i in range(k):
                    await r.send(i)

            gc.collect()
            t0 = time.perf_counter()
            await asyncio.gather(*(produce() for _ in range(producers)))
            await _drain(ref)
            elapsed = time.perf_counter() - t0
            total = per * producers
            record(
                "send_many_producers",
                capacity=64,
                producers=producers,
                messages=total,
                msgs_per_s=total / elapsed,
            )


async def main(args: argparse.Namespace) -> dict[str, Any]:
    env = environment()
    print("pyactors benchmarks")
    for key, value in env.items():
        print(f"  {key}: {value}")
    counts = [1, 100, 1000, 10_000]
    messages = 20_000 if args.quick else 200_000
    latency_messages = 5_000 if args.quick else 50_000
    if args.quick:
        counts = [1, 100, 1000]

    t0 = time.perf_counter()
    await bench_creation(counts)
    await bench_memory(counts)
    await bench_tell(messages)
    await bench_tell_interleaved(messages)
    await bench_ask(messages // 4)
    await bench_latency(latency_messages)
    await bench_multi_actor(counts, messages)
    await bench_bounded(messages // 4)
    env["peak_rss_mib"] = round(peak_rss_mb(), 1)
    env["wall_time_s"] = round(time.perf_counter() - t0, 1)
    print(f"\npeak RSS: {env['peak_rss_mib']} MiB, total time {env['wall_time_s']} s")
    return env


def write_json(path: str, env: dict[str, Any]) -> None:
    with open(path, "w") as f:
        json.dump({"environment": env, "results": RESULTS}, f, indent=2)
    print(f"results written to {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true", help="smaller sizes")
    parser.add_argument("--json", help="write results to this JSON file")
    arguments = parser.parse_args()
    environment_info = asyncio.run(main(arguments))
    if arguments.json:
        write_json(arguments.json, environment_info)
