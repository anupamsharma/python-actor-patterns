"""Pipeline actor: pass a value through ordered stages."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import maybe_await
from pyactors.patterns.messages import GetStats

__all__ = ["Pipeline", "PipelineStage"]

StageFn = Callable[[Any], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class PipelineStage:
    name: str
    stage: StageFn | ActorRef[Any, Any]


class Pipeline(Actor[Any, Any]):
    """Apply stages sequentially; each stage receives the previous output.

    A stage is either a callable or an :class:`ActorRef` (invoked via
    ``ask``).  Failure in any stage aborts the pipeline and propagates.
    """

    def __init__(self, stages: Sequence[StageFn | ActorRef[Any, Any] | PipelineStage]) -> None:
        if not stages:
            raise ValueError("pipeline requires at least one stage")
        self._stages: list[PipelineStage] = []
        for i, stage in enumerate(stages):
            if isinstance(stage, PipelineStage):
                self._stages.append(stage)
            else:
                self._stages.append(PipelineStage(name=f"stage-{i}", stage=stage))
        self._processed = 0
        self._failed = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {
                "stages": len(self._stages),
                "processed": self._processed,
                "failed": self._failed,
            }
        value = message
        try:
            for stage in self._stages:
                target = stage.stage
                if isinstance(target, ActorRef):
                    value = await target.ask(value)
                else:
                    value = await maybe_await(target(value))
        except Exception as exc:
            self._failed += 1
            raise ReplyError(exc) from exc
        self._processed += 1
        return value
