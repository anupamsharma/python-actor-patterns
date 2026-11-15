"""Ping-pong: two actors exchange messages via refs carried inside messages."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from pyactors import Actor, ActorRef, ActorSystem


@dataclass(frozen=True, slots=True)
class Ball:
    hits: int
    reply_to: ActorRef[Ball, None]


class Player(Actor[Ball, None]):
    def __init__(self, name: str, rounds: int, done: asyncio.Future[int]) -> None:
        self.name = name
        self.rounds = rounds
        self.done = done

    async def receive(self, message: Ball) -> None:
        if message.hits >= self.rounds:
            if not self.done.done():
                self.done.set_result(message.hits)
            return
        if message.hits < 3 or message.hits >= self.rounds - 2:
            print(f"{self.name} hits the ball (hit #{message.hits + 1})")
        # Never await an ask to the other player here: two actors waiting on
        # each other's replies would deadlock.  tell() is fire-and-forget.
        message.reply_to.tell(Ball(message.hits + 1, self.context.ref))


async def main() -> None:
    rounds = 10_000
    loop = asyncio.get_running_loop()
    done: asyncio.Future[int] = loop.create_future()
    async with ActorSystem() as system:
        ping: ActorRef[Any, None] = await system.spawn(Player, "ping", rounds, done, name="ping")
        pong: ActorRef[Any, None] = await system.spawn(Player, "pong", rounds, done, name="pong")
        started = loop.time()
        ping.tell(Ball(0, pong))
        hits = await asyncio.wait_for(done, timeout=30)
        elapsed = loop.time() - started
        print(f"{hits} hits in {elapsed:.3f}s ({hits / elapsed:,.0f} messages/s)")


if __name__ == "__main__":
    asyncio.run(main())
