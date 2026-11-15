"""Counter actor: state lives inside the actor, updates are serialized."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from pyactors import Actor, ActorSystem


@dataclass(frozen=True, slots=True)
class Add:
    amount: int


@dataclass(frozen=True, slots=True)
class Get:
    pass


class Counter(Actor[Add | Get, int]):
    def __init__(self) -> None:
        self.value = 0

    async def receive(self, message: Add | Get) -> int:
        if isinstance(message, Add):
            self.value += message.amount
        return self.value


async def main() -> None:
    async with ActorSystem() as system:
        counter = await system.spawn(Counter, name="counter")
        for _ in range(1000):
            counter.tell(Add(1))  # fire-and-forget
        # 100 concurrent tasks; the actor still applies one update at a time.
        await asyncio.gather(*(counter.ask(Add(10)) for _ in range(100)))
        total = await counter.ask(Get())
        print(f"counter value: {total}")
        assert total == 2000


if __name__ == "__main__":
    asyncio.run(main())
