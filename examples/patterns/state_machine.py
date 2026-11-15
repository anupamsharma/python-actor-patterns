"""StateMachine: an order lifecycle with guards, actions and rejected events."""

from __future__ import annotations

import asyncio

from pyactors import ActorSystem
from pyactors.patterns import (
    Event,
    GetState,
    PatternRejected,
    StateMachine,
    Transition,
)

TRANSITIONS = [
    Transition("new", "pay", "paid", guard=lambda amount: amount and amount > 0),
    Transition("paid", "ship", "shipped", action=lambda _: "label printed"),
    Transition("shipped", "deliver", "delivered"),
    Transition("new", "cancel", "cancelled"),
]


async def main() -> None:
    def log(old: str, event: str, new: str, payload: object) -> None:
        print(f"  {old} --{event}--> {new}")

    async with ActorSystem() as system:
        order = await system.spawn(
            StateMachine, "new", TRANSITIONS, on_transition=log, name="order"
        )
        for name, payload in [("pay", 0), ("pay", 25), ("ship", None), ("cancel", None)]:
            try:
                reply = await order.ask(Event(name, payload))
                print(f"{name}({payload}) ok, action result: {reply['result']}")
            except PatternRejected as exc:
                print(f"{name}({payload}) rejected: {exc}")
        await order.ask(Event("deliver"))
        print("final state:", await order.ask(GetState()))


if __name__ == "__main__":
    asyncio.run(main())
