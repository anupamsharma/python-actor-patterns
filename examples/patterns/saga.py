"""Saga: run steps in order and undo completed ones when a later step fails."""

from __future__ import annotations

import asyncio
from typing import Any

from pyactors import ActorSystem
from pyactors.patterns import RunSaga, SagaCoordinator, SagaFailed, SagaStep


def step(name: str) -> SagaStep:
    def forward(order: dict[str, Any]) -> str:
        if order.get("fail_at") == name:
            raise RuntimeError(f"{name} unavailable")
        print(f"  do   {name}")
        return f"{name}-ok"

    def compensate(order: dict[str, Any], result: str) -> None:
        print(f"  undo {name} (had {result})")

    return SagaStep(name, forward, compensate)


async def main() -> None:
    steps = [step("reserve-stock"), step("charge-card"), step("ship-order")]
    async with ActorSystem() as system:
        saga = await system.spawn(SagaCoordinator, steps, name="order-saga")

        print("order 1 (everything works):")
        result = await saga.ask(RunSaga({"id": 1}))
        print("  results:", result.results)

        print("order 2 (shipping fails):")
        try:
            await saga.ask(RunSaga({"id": 2, "fail_at": "ship-order"}))
        except SagaFailed as exc:
            print(f"  saga failed at step {exc.failed_step}: {exc.cause}")


if __name__ == "__main__":
    asyncio.run(main())
