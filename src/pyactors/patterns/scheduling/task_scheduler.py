"""Task scheduler: run callables after a delay or at an absolute monotonic time."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pyactors.actor import Actor
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import GetStats

__all__ = ["CancelTask", "ScheduleAt", "ScheduleIn", "ScheduledId", "TaskScheduler"]

Job = Callable[[], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class ScheduleIn:
    delay: float
    job: Job


@dataclass(frozen=True, slots=True)
class ScheduleAt:
    """Schedule at an absolute :func:`time.monotonic` timestamp."""

    when: float
    job: Job


@dataclass(frozen=True, slots=True)
class CancelTask:
    task_id: str


@dataclass(frozen=True, slots=True)
class ScheduledId:
    task_id: str


class TaskScheduler(Actor[Any, Any]):
    """In-process scheduler for delayed callables.

    Jobs run on the actor's event loop (via ``call_later`` + a short task).
    Not a distributed cron; process restart loses scheduled work.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, asyncio.TimerHandle] = {}
        self._running: set[asyncio.Task[None]] = set()
        self._completed = 0
        self._failed = 0

    async def on_stop(self) -> None:
        for handle in self._jobs.values():
            handle.cancel()
        self._jobs.clear()
        for task in list(self._running):
            task.cancel()
        if self._running:
            await asyncio.gather(*self._running, return_exceptions=True)
        self._running.clear()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "scheduled": len(self._jobs),
                "running": len(self._running),
                "completed": self._completed,
                "failed": self._failed,
            }
        if isinstance(message, ScheduleIn):
            return self._schedule(message.delay, message.job)
        if isinstance(message, ScheduleAt):
            delay = max(0.0, message.when - time.monotonic())
            return self._schedule(delay, message.job)
        if isinstance(message, CancelTask):
            handle = self._jobs.pop(message.task_id, None)
            if handle is None:
                return False
            handle.cancel()
            return True
        raise TypeError(f"unsupported scheduler message: {type(message)!r}")

    def _schedule(self, delay: float, job: Job) -> ScheduledId:
        task_id = uuid4().hex
        loop = asyncio.get_running_loop()

        def fire() -> None:
            self._jobs.pop(task_id, None)
            task = loop.create_task(self._run(job))
            self._running.add(task)
            task.add_done_callback(self._running.discard)

        self._jobs[task_id] = loop.call_later(max(0.0, delay), fire)
        return ScheduledId(task_id=task_id)

    async def _run(self, job: Job) -> None:
        try:
            await maybe_await(job())
            self._completed += 1
        except Exception:
            self._failed += 1
