"""Request/response with ask(): results, timeouts and error handling."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from pyactors import Actor, ActorFailed, ActorRef, ActorSystem


@dataclass(frozen=True, slots=True)
class Lookup:
    key: str


@dataclass(frozen=True, slots=True)
class Slow:
    seconds: float


@dataclass(frozen=True, slots=True)
class Crash:
    pass


Request = Lookup | Slow | Crash


class KeyValueService(Actor[Request, str | None]):
    def __init__(self, data: dict[str, str]) -> None:
        self.data = dict(data)

    async def receive(self, message: Request) -> str | None:
        match message:
            case Lookup(key):
                # Expected "errors" are returned as values so the actor survives.
                return self.data.get(key)
            case Slow(seconds):
                await asyncio.sleep(seconds)
                return "slow done"
            case Crash():
                raise RuntimeError("unexpected bug")  # fails the actor


def report(ref: ActorRef[Any, Any], exc: BaseException) -> None:
    print(f"[failure] {ref}: {exc!r}")


async def main() -> None:
    async with ActorSystem(failure_handler=report) as system:
        kv = await system.spawn(KeyValueService, {"a": "apple"}, name="kv")

        print("lookup a ->", await kv.ask(Lookup("a")))
        print("lookup z ->", await kv.ask(Lookup("z")))

        try:
            await kv.ask(Slow(1.0), timeout=0.1)
        except TimeoutError:
            print("slow request timed out (the handler keeps running)")

        try:
            await kv.ask(Crash())
        except RuntimeError as exc:
            print("asker saw the handler's exception:", repr(exc))

        try:
            await kv.ask(Lookup("a"))
        except ActorFailed as exc:
            print("actor is now failed:", exc)


if __name__ == "__main__":
    asyncio.run(main())
