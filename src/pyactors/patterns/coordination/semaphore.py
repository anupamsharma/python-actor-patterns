"""Semaphore actor: limit concurrent holders."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.exceptions import PatternRejected
from pyactors.patterns.messages import GetStats

__all__ = ["Acquire", "Release", "Semaphore", "SemaphorePermit"]


@dataclass(frozen=True, slots=True)
class Acquire:
    """Acquire a permit; parks the asker until one is available."""

    timeout: float | None = None


@dataclass(frozen=True, slots=True)
class Release:
    """Release a previously acquired permit."""

    permit_id: str


@dataclass(frozen=True, slots=True)
class SemaphorePermit:
    permit_id: str


@dataclass(slots=True)
class _Waiter:
    future: asyncio.Future[Any]
    handle: asyncio.TimerHandle | None = None


class Semaphore(Actor[Any, Any]):
    """Counting semaphore as an actor.

    ``ask(Acquire())`` parks until a permit is available (or ``timeout``).
    Callers must ``ask``/``tell`` :class:`Release` when finished.
    """

    def __init__(self, value: int) -> None:
        if value < 0:
            raise ValueError("value must be >= 0")
        self._value = value
        self._available = value
        self._issued: set[str] = set()
        self._waiters: deque[_Waiter] = deque()

    async def on_stop(self) -> None:
        err = PatternRejected("semaphore stopping")
        while self._waiters:
            waiter = self._waiters.popleft()
            if waiter.handle is not None:
                waiter.handle.cancel()
            if not waiter.future.done():
                waiter.future.set_exception(err)

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "available": self._available,
                "issued": len(self._issued),
                "waiting": len(self._waiters),
            }
        if isinstance(message, Acquire) or message == "acquire":
            timeout = message.timeout if isinstance(message, Acquire) else None
            if self._available > 0:
                return self._issue()
            reply = self.context.detach_ask()
            if reply is None:
                raise ReplyError(PatternRejected("Acquire requires ask()"))
            waiter = _Waiter(future=reply)
            if timeout is not None:
                loop = asyncio.get_running_loop()

                def _timeout(w: _Waiter = waiter) -> None:
                    with contextlib.suppress(ValueError):
                        self._waiters.remove(w)
                    if not w.future.done():
                        w.future.set_exception(TimeoutError())

                waiter.handle = loop.call_later(timeout, _timeout)
            self._waiters.append(waiter)
            return None
        if isinstance(message, Release):
            return self._release(message.permit_id)
        raise TypeError(f"unsupported semaphore message: {message!r}")

    def _issue(self) -> SemaphorePermit:
        self._available -= 1
        permit_id = uuid4().hex
        self._issued.add(permit_id)
        return SemaphorePermit(permit_id=permit_id)

    def _release(self, permit_id: str) -> bool:
        if permit_id not in self._issued:
            return False
        self._issued.remove(permit_id)
        self._available += 1
        while self._waiters and self._available > 0:
            waiter = self._waiters.popleft()
            if waiter.handle is not None:
                waiter.handle.cancel()
            if waiter.future.done():
                continue
            waiter.future.set_result(self._issue())
            break
        return True
