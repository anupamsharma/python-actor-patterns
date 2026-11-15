"""PubSub: publishers send to a topic, every subscriber of that topic gets a copy."""

from __future__ import annotations

import asyncio

from pyactors import Actor, ActorSystem
from pyactors.patterns import GetStats, Publish, PubSub, Subscribe, Unsubscribe


class Listener(Actor[str, list[str]]):
    def __init__(self) -> None:
        self.inbox: list[str] = []

    async def receive(self, message: str) -> list[str]:
        if message != "dump":
            self.inbox.append(message)
        return self.inbox


async def main() -> None:
    async with ActorSystem() as system:
        hub = await system.spawn(PubSub, name="hub")
        alice = await system.spawn(Listener, name="alice")
        bob = await system.spawn(Listener, name="bob")

        await hub.ask(Subscribe("news", alice))
        await hub.ask(Subscribe("news", bob))
        await hub.ask(Subscribe("sports", bob))

        print("news delivered to", await hub.ask(Publish("news", "launch day")), "subscribers")
        await hub.ask(Publish("sports", "final score 3-2"))
        await hub.ask(Unsubscribe("news", bob))
        await hub.ask(Publish("news", "follow-up"))

        print("alice inbox:", await alice.ask("dump"))
        print("bob inbox:  ", await bob.ask("dump"))
        print("stats:", await hub.ask(GetStats()))


if __name__ == "__main__":
    asyncio.run(main())
