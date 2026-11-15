"""Deterministic test actors and utilities."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Any

from pyactors import Actor

TIMEOUT = 5.0


async def within[T](aw: Awaitable[T], timeout: float = TIMEOUT) -> T:
    """Await with a bound so a broken runtime fails instead of hanging."""
    async with asyncio.timeout(timeout):
        return await aw


async def yield_loop(n: int = 10) -> None:
    """Let other ready tasks run ``n`` scheduling rounds."""
    for _ in range(n):
        await asyncio.sleep(0)


@dataclass(eq=False)
class Block:
    """Message: signal ``entered``, wait for ``release``, then return ``value``."""

    value: Any = None
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False
    finished: bool = False


@dataclass(eq=False)
class Raise:
    """Message: raise ``exc`` from ``receive``."""

    exc: BaseException


GET_LOG = "__get_log__"


class Probe(Actor[Any, Any]):
    """Scriptable actor that records what happens to it in ``log``."""

    def __init__(
        self,
        log: list[Any] | None = None,
        *,
        start_gate: asyncio.Event | None = None,
        start_entered: asyncio.Event | None = None,
        start_error: BaseException | None = None,
        stop_error: BaseException | None = None,
    ) -> None:
        self.log: list[Any] = [] if log is None else log
        self.start_gate = start_gate
        self.start_entered = start_entered
        self.start_error = start_error
        self.stop_error = stop_error

    async def on_start(self) -> None:
        self.log.append("start")
        if self.start_entered is not None:
            self.start_entered.set()
        if self.start_gate is not None:
            try:
                await self.start_gate.wait()
            except asyncio.CancelledError:
                self.log.append("start-cancelled")
                raise
        if self.start_error is not None:
            raise self.start_error

    async def receive(self, message: Any) -> Any:
        if isinstance(message, Block):
            message.entered.set()
            try:
                await message.release.wait()
            except asyncio.CancelledError:
                message.cancelled = True
                self.log.append(("cancelled", message.value))
                raise
            message.finished = True
            self.log.append(message.value)
            return message.value
        if isinstance(message, Raise):
            raise message.exc
        if isinstance(message, str) and message == GET_LOG:
            return list(self.log)
        self.log.append(message)
        return message

    async def on_stop(self) -> None:
        self.log.append("stop")
        if self.stop_error is not None:
            raise self.stop_error


class Counter(Actor[Any, int]):
    """Adds integers; ``"get"`` returns the total.  Yields mid-update on purpose."""

    def __init__(self, start: int = 0) -> None:
        self.total = start

    async def receive(self, message: Any) -> int:
        if message == "get":
            return self.total
        current = self.total
        await asyncio.sleep(0)  # would expose lost updates if handlers interleaved
        self.total = current + message
        return self.total
