"""Barrier actor: release N waiters together."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.patterns.messages import GetStats

__all__ = ["Arrive", "Barrier", "Reset"]


@dataclass(frozen=True, slots=True)
class Arrive:
    """Signal arrival; asker is released when the party is complete."""

    payload: Any = None


@dataclass(frozen=True, slots=True)
class Reset:
    """Reset the barrier to a new party size (default: keep current)."""

    parties: int | None = None


class Barrier(Actor[Any, Any]):
    """Block until ``parties`` arrivals, then release all waiters together.

    Each :class:`Arrive` (or bare message) via ``ask`` parks the caller until
    the party is complete.  Uses :meth:`ActorContext.detach_ask` so the
    mailbox keeps processing.
    """

    def __init__(self, parties: int) -> None:
        if parties < 1:
            raise ValueError("parties must be >= 1")
        self._parties = parties
        self._waiters: list[tuple[Any, asyncio.Future[Any]]] = []
        self._generations = 0

    async def on_stop(self) -> None:
        for _, fut in self._waiters:
            if not fut.done():
                fut.set_exception(asyncio.CancelledError())
        self._waiters.clear()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "waiting": len(self._waiters),
                "parties": self._parties,
                "generations": self._generations,
            }
        if isinstance(message, Reset):
            if message.parties is not None:
                if message.parties < 1:
                    raise ValueError("parties must be >= 1")
                self._parties = message.parties
            return self._parties
        payload = message.payload if isinstance(message, Arrive) else message
        reply = self.context.detach_ask()
        if reply is None:
            # tell/send: count as arrival without a waiter future
            self._waiters.append((payload, asyncio.get_running_loop().create_future()))
        else:
            self._waiters.append((payload, reply))
        if len(self._waiters) >= self._parties:
            generation = list(self._waiters)
            self._waiters = []
            self._generations += 1
            payloads = [p for p, _ in generation]
            for _, waiter in generation:
                if not waiter.done():
                    waiter.set_result(payloads)
        return None
