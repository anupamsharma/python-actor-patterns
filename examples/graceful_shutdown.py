"""Graceful vs immediate stop, on_start/on_stop hooks and self-stop."""

from __future__ import annotations

import asyncio

from pyactors import Actor, ActorStopped, ActorSystem


class Writer(Actor[str, None]):
    def __init__(self, label: str) -> None:
        self.label = label
        self.buffer: list[str] = []

    async def on_start(self) -> None:
        print(f"[{self.label}] started")

    async def receive(self, message: str) -> None:
        if message == "quit":
            self.context.stop()  # self-stop: returns immediately, never deadlocks
            return
        await asyncio.sleep(0.01)
        self.buffer.append(message)

    async def on_stop(self) -> None:
        print(f"[{self.label}] on_stop: flushing {len(self.buffer)} line(s)")


async def main() -> None:
    system = ActorSystem()

    graceful = await system.spawn(Writer, "graceful")
    for i in range(5):
        graceful.tell(f"line {i}")
    await system.stop(graceful)  # processes all 5 queued lines first
    try:
        graceful.tell("too late")
    except ActorStopped:
        print("[graceful] rejects messages after stop")

    immediate = await system.spawn(Writer, "immediate")
    for i in range(5):
        immediate.tell(f"line {i}")
    await asyncio.sleep(0.015)
    await system.stop(immediate, graceful=False)  # discards whatever is still queued

    quitter = await system.spawn(Writer, "self-stop")
    quitter.tell("only line")
    quitter.tell("quit")
    await system.stop(quitter)  # waits for termination (already in progress)

    await system.spawn(Writer, "left-running")
    await system.shutdown()  # stops everything that is still running
    await system.shutdown()  # idempotent
    print(f"actors remaining: {system.actor_count}")


if __name__ == "__main__":
    asyncio.run(main())
