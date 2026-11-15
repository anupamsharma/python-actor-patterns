"""Shared helpers for pattern tests."""

from __future__ import annotations

from typing import Any

from pyactors import Actor, ReplyError


class Echo(Actor[Any, Any]):
    async def receive(self, message: Any) -> Any:
        return message


class Adder(Actor[int, int]):
    def __init__(self, n: int = 1) -> None:
        self.n = n

    async def receive(self, message: int) -> int:
        return message + self.n


class Flaky(Actor[Any, Any]):
    """Fails the first ``fail_times`` calls, then succeeds (actor stays alive)."""

    def __init__(self, fail_times: int = 1, error: BaseException | None = None) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.error = error or RuntimeError("flaky")

    async def receive(self, message: Any) -> Any:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ReplyError(RuntimeError("flaky"))
        return message


class Collector(Actor[Any, list[Any]]):
    def __init__(self) -> None:
        self.items: list[Any] = []

    async def receive(self, message: Any) -> list[Any]:
        if message == "get":
            return list(self.items)
        self.items.append(message)
        return list(self.items)
