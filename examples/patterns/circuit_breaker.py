"""CircuitBreaker: fail fast while a target is unhealthy, then probe recovery."""

from __future__ import annotations

import asyncio
from typing import Any

from pyactors import ActorSystem
from pyactors.patterns import CircuitBreaker, CircuitOpen, GetStats


class Backend:
    """Anything with an async ``ask`` can sit behind the breaker."""

    healthy = False

    async def ask(self, message: Any) -> str:
        if not self.healthy:
            raise ConnectionError("backend down")
        return f"backend handled {message}"


async def call(breaker: Any, label: str) -> None:
    try:
        print(f"{label}:", await breaker.ask(label))
    except CircuitOpen:
        print(f"{label}: rejected, circuit is open")
    except ConnectionError as exc:
        print(f"{label}: target failed ({exc})")


async def main() -> None:
    backend = Backend()
    async with ActorSystem() as system:
        breaker = await system.spawn(
            CircuitBreaker, backend, failure_threshold=2, recovery_timeout=0.2, name="breaker"
        )
        for n in range(1, 5):
            await call(breaker, f"call-{n}")
        print("stats:", await breaker.ask(GetStats()))

        backend.healthy = True
        await asyncio.sleep(0.25)  # recovery window passes -> half-open trial
        await call(breaker, "trial-call")
        print("stats:", await breaker.ask(GetStats()))


if __name__ == "__main__":
    asyncio.run(main())
