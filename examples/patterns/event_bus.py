"""EventBus: handlers register for an event type and run when it is emitted."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from pyactors import Actor, ActorSystem
from pyactors.patterns import Emit, EventBus, GetStats, Off, On


@dataclass(frozen=True)
class UserSignedUp:
    name: str


@dataclass(frozen=True)
class OrderPlaced:
    order_id: int


class Auditor(Actor[object, list[object]]):
    def __init__(self) -> None:
        self.seen: list[object] = []

    async def receive(self, message: object) -> list[object]:
        if message != "dump":
            self.seen.append(message)
        return self.seen


async def main() -> None:
    def welcome(event: UserSignedUp) -> None:
        print(f"welcome email queued for {event.name}")

    async def reserve(event: OrderPlaced) -> None:
        print(f"inventory reserved for order {event.order_id}")

    async with ActorSystem() as system:
        bus = await system.spawn(EventBus, name="bus")
        auditor = await system.spawn(Auditor, name="auditor")

        await bus.ask(On(UserSignedUp, welcome))
        await bus.ask(On(UserSignedUp, auditor))  # handlers may be actors too
        await bus.ask(On(OrderPlaced, reserve))

        print("handlers run:", await bus.ask(Emit(UserSignedUp("ada"))))
        await bus.ask(Emit(OrderPlaced(7)))
        await bus.ask(Off(UserSignedUp, welcome))
        await bus.ask(Emit(UserSignedUp("grace")))  # only the auditor sees this

        print("auditor saw:", await auditor.ask("dump"))
        print("stats:", await bus.ask(GetStats()))


if __name__ == "__main__":
    asyncio.run(main())
