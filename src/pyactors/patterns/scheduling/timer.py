"""Timer actor: delayed and periodic message delivery."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pyactors.actor import Actor
from pyactors.patterns.messages import GetStats

__all__ = ["CancelTimer", "ScheduleOnce", "SchedulePeriodic", "Timer", "TimerId"]


@dataclass(frozen=True, slots=True)
class ScheduleOnce:
    delay: float
    message: Any
    target: Any | None = None  # ActorRef; default: reply to asker via stored callback


@dataclass(frozen=True, slots=True)
class SchedulePeriodic:
    interval: float
    message: Any
    target: Any
    initial_delay: float | None = None


@dataclass(frozen=True, slots=True)
class CancelTimer:
    timer_id: str


@dataclass(frozen=True, slots=True)
class TimerId:
    timer_id: str


@dataclass(slots=True)
class _Entry:
    handle: asyncio.TimerHandle | None
    task: asyncio.Task[None] | None
    periodic: bool


class Timer(Actor[Any, Any]):
    """Schedule one-shot or periodic delivery of messages to a target ref.

    Cooperative cancellation: :class:`CancelTimer` cancels the pending timer.
    On stop, all timers are cancelled.  Periodic timers do not compensate for
    handler overruns (fixed interval from schedule time).
    """

    def __init__(self) -> None:
        self._timers: dict[str, _Entry] = {}
        self._fired = 0

    async def on_stop(self) -> None:
        for entry in list(self._timers.values()):
            self._cancel_entry(entry)
        self._timers.clear()

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {"active": len(self._timers), "fired": self._fired}
        if isinstance(message, ScheduleOnce):
            return self._once(message)
        if isinstance(message, SchedulePeriodic):
            return self._periodic(message)
        if isinstance(message, CancelTimer):
            entry = self._timers.pop(message.timer_id, None)
            if entry is None:
                return False
            self._cancel_entry(entry)
            return True
        raise TypeError(f"unsupported timer message: {type(message)!r}")

    def _once(self, msg: ScheduleOnce) -> TimerId:
        if msg.target is None:
            raise ValueError("ScheduleOnce requires a target ActorRef")
        target = msg.target
        payload = msg.message
        timer_id = uuid4().hex
        loop = asyncio.get_running_loop()

        def fire() -> None:
            self._timers.pop(timer_id, None)
            self._fired += 1
            with contextlib.suppress(Exception):
                target.tell(payload)

        handle = loop.call_later(max(0.0, msg.delay), fire)
        self._timers[timer_id] = _Entry(handle=handle, task=None, periodic=False)
        return TimerId(timer_id=timer_id)

    def _periodic(self, msg: SchedulePeriodic) -> TimerId:
        timer_id = uuid4().hex
        delay = msg.initial_delay if msg.initial_delay is not None else msg.interval
        loop = asyncio.get_running_loop()

        def schedule(next_delay: float) -> None:
            def fire() -> None:
                entry = self._timers.get(timer_id)
                if entry is None:
                    return
                self._fired += 1
                try:
                    msg.target.tell(msg.message)
                except Exception:
                    self._timers.pop(timer_id, None)
                    return
                schedule(msg.interval)

            handle = loop.call_later(max(0.0, next_delay), fire)
            entry = self._timers.get(timer_id)
            if entry is not None:
                entry.handle = handle

        self._timers[timer_id] = _Entry(handle=None, task=None, periodic=True)
        schedule(delay)
        return TimerId(timer_id=timer_id)

    @staticmethod
    def _cancel_entry(entry: _Entry) -> None:
        if entry.handle is not None:
            entry.handle.cancel()
        if entry.task is not None:
            entry.task.cancel()
