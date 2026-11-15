"""Window: group timestamped events into tumbling time windows."""

from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import Flush, TimedEvent, Window, WindowResult


def show(result: WindowResult) -> None:
    print(
        f"window [{result.start:.0f}s, {result.end:.0f}s): sum={sum(result.items)} {result.items}"
    )


async def main() -> None:
    async with ActorSystem() as system:
        window = await system.spawn(Window, 10.0, name="windows")
        readings = [(1, 5), (4, 7), (9, 3), (12, 10), (18, 2), (25, 8)]
        for ts, value in readings:
            closed = await window.ask(TimedEvent(value, float(ts)))
            for result in closed if isinstance(closed, list) else [closed]:
                if result is not None:
                    show(result)
        show(await window.ask(Flush()))  # close the window still open


if __name__ == "__main__":
    asyncio.run(main())
