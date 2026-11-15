"""In-memory saga coordinator (forward steps + compensations)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import maybe_await
from pyactors.patterns.exceptions import SagaFailed
from pyactors.patterns.messages import GetStats

__all__ = ["RunSaga", "SagaCoordinator", "SagaResult", "SagaStep"]

Forward = Callable[[Any], Any | Awaitable[Any]]
Compensate = Callable[[Any, Any], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class SagaStep:
    """One forward operation and its optional compensation.

    ``compensate(context, forward_result)`` undoes the forward step.
    Compensation receives the value returned by ``forward``.
    """

    name: str
    forward: Forward
    compensate: Compensate | None = None


@dataclass(frozen=True, slots=True)
class RunSaga:
    """Start a saga with the given input context."""

    context: Any = None


@dataclass(frozen=True, slots=True)
class SagaResult:
    context: Any
    results: tuple[Any, ...]
    compensated: bool = False


class SagaCoordinator(Actor[Any, SagaResult]):
    """Run a sequence of forward steps; compensate in reverse on failure.

    This is **in-memory coordination only**.  It is not a durable distributed
    transaction engine: process crash mid-saga loses progress, compensations
    are best-effort, and there is no recovery log.
    """

    def __init__(self, steps: Sequence[SagaStep]) -> None:
        if not steps:
            raise ValueError("saga requires at least one step")
        self._steps = list(steps)
        self._runs = 0
        self._failures = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {"runs": self._runs, "failures": self._failures, "steps": len(self._steps)}
        context = message.context if isinstance(message, RunSaga) else message
        self._runs += 1
        results: list[Any] = []
        try:
            for step in self._steps:
                results.append(await maybe_await(step.forward(context)))
        except Exception as exc:
            self._failures += 1
            compensation_errors: list[BaseException] = []
            for step, result in zip(
                reversed(self._steps[: len(results)]), reversed(results), strict=True
            ):
                if step.compensate is None:
                    continue
                try:
                    await maybe_await(step.compensate(context, result))
                except Exception as comp_exc:
                    compensation_errors.append(comp_exc)
            raise ReplyError(SagaFailed(len(results), exc, compensation_errors)) from exc
        return SagaResult(context=context, results=tuple(results), compensated=False)
