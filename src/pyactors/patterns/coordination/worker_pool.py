"""Worker pool: distribute tasks across a fixed set of worker actors."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.actor_system import ActorSystem
from pyactors.patterns.messages import GetStats
from pyactors.patterns.routing.router import RoundRobinRouter

__all__ = ["Submit", "WorkerPool", "spawn_worker_pool"]


@dataclass(frozen=True, slots=True)
class Submit:
    """Submit a task payload to the pool."""

    payload: Any


class WorkerPool(RoundRobinRouter):
    """Round-robin distributor over a fixed set of worker :class:`ActorRef`s.

    Workers are ordinary actors — one consumer task each — not a new asyncio
    task per message.  Use :func:`spawn_worker_pool` to create workers and the
    pool in one step.
    """

    def __init__(self, workers: Sequence[ActorRef[Any, Any]]) -> None:
        if not workers:
            raise ValueError("WorkerPool requires at least one worker")
        super().__init__(workers)
        self._submitted = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            base = await super().receive(message)
            return {"submitted": self._submitted, "router": base}
        if isinstance(message, Submit):
            self._submitted += 1
            return await super().receive(message.payload)
        self._submitted += 1
        return await super().receive(message)


async def spawn_worker_pool[M, R](
    system: ActorSystem,
    worker_factory: Callable[..., Actor[M, R]],
    *,
    size: int,
    mailbox_size: int = 0,
    name: str | None = None,
    worker_args: tuple[Any, ...] = (),
    worker_kwargs: dict[str, Any] | None = None,
) -> ActorRef[Any, Any]:
    """Spawn ``size`` workers and a :class:`WorkerPool` that routes to them."""
    if size < 1:
        raise ValueError("size must be >= 1")
    kwargs = dict(worker_kwargs or ())
    workers = []
    prefix = name or "pool"
    for i in range(size):
        workers.append(
            await system.spawn(
                worker_factory,
                *worker_args,
                name=f"{prefix}-worker-{i}",
                mailbox_size=mailbox_size,
                **kwargs,
            )
        )
    return await system.spawn(WorkerPool, workers, name=prefix)
