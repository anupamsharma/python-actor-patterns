"""Retry a flaky dependency with backoff."""

import asyncio

from pyactors import Actor, ActorSystem, ReplyError
from pyactors.patterns import Retry, RetryPolicy


class Flaky(Actor[str, str]):
    def __init__(self) -> None:
        self.n = 0

    async def receive(self, message: str) -> str:
        self.n += 1
        if self.n < 3:
            raise ReplyError(RuntimeError("transient"))
        return f"ok:{message}"


async def main() -> None:
    async with ActorSystem() as system:
        target = await system.spawn(Flaky)
        retry = await system.spawn(
            Retry, target, policy=RetryPolicy(max_attempts=5, initial_backoff=0.05, jitter=0)
        )
        print(await retry.ask("job"))


if __name__ == "__main__":
    asyncio.run(main())
