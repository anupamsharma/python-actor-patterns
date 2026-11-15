"""Scatter-gather: fan-out a request and aggregate replies."""

from __future__ import annotations

import asyncio
import enum
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from pyactors.actor import Actor
from pyactors.actor_ref import ActorRef
from pyactors.exceptions import ReplyError
from pyactors.patterns.exceptions import PatternRejected
from pyactors.patterns.messages import GetStats

__all__ = [
    "CompletionPolicy",
    "ScatterGather",
    "ScatterGatherResult",
    "ScatterRequest",
]


class CompletionPolicy(enum.Enum):
    """When a scatter-gather request is considered complete."""

    ALL = "all"
    """Wait for every routee (or timeout)."""

    ANY = "any"
    """Complete when the first successful reply arrives."""

    QUORUM = "quorum"
    """Complete when ``quorum`` successful replies arrive."""


@dataclass(frozen=True, slots=True)
class ScatterRequest:
    """Fan-out payload plus per-request overrides."""

    payload: Any
    timeout: float | None = None
    policy: CompletionPolicy | None = None
    quorum: int | None = None


@dataclass(slots=True)
class ScatterGatherResult:
    """Aggregated replies from a scatter-gather round."""

    successes: list[Any] = field(default_factory=list)
    failures: list[BaseException] = field(default_factory=list)
    timed_out: bool = False

    @property
    def complete(self) -> bool:
        return not self.timed_out and not self.failures

    @property
    def partial(self) -> bool:
        return bool(self.successes) and (bool(self.failures) or self.timed_out)


class ScatterGather(Actor[Any, ScatterGatherResult]):
    """Ask every routee and aggregate replies under a completion policy.

    Default policy is :attr:`CompletionPolicy.ALL`.  Per-request overrides
    are supplied via :class:`ScatterRequest`.  Plain messages are treated as
    the payload with the actor's defaults.
    """

    def __init__(
        self,
        routees: Sequence[ActorRef[Any, Any]],
        *,
        timeout: float | None = 5.0,
        policy: CompletionPolicy = CompletionPolicy.ALL,
        quorum: int = 1,
        aggregator: Callable[[ScatterGatherResult], Any] | None = None,
    ) -> None:
        if not routees:
            raise ValueError("ScatterGather requires at least one routee")
        self._routees = list(routees)
        self._timeout = timeout
        self._policy = policy
        self._quorum = max(1, quorum)
        self._aggregator = aggregator
        self._rounds = 0

    async def receive(self, message: Any) -> Any:
        if isinstance(message, GetStats):
            return {"rounds": self._rounds, "routees": len(self._routees)}
        if isinstance(message, ScatterRequest):
            payload = message.payload
            timeout = self._timeout if message.timeout is None else message.timeout
            policy = self._policy if message.policy is None else message.policy
            quorum = self._quorum if message.quorum is None else message.quorum
        else:
            payload, timeout, policy, quorum = (
                message,
                self._timeout,
                self._policy,
                self._quorum,
            )
        self._rounds += 1
        result = await self._scatter(payload, timeout, policy, quorum)
        if self._aggregator is not None:
            return self._aggregator(result)
        return result

    async def _scatter(
        self,
        payload: Any,
        timeout: float | None,
        policy: CompletionPolicy,
        quorum: int,
    ) -> ScatterGatherResult:
        result = ScatterGatherResult()
        tasks = [asyncio.create_task(r.ask(payload)) for r in self._routees]
        pending: set[asyncio.Task[Any]] = set(tasks)
        done_ok = 0

        async def _consume(task: asyncio.Task[Any]) -> None:
            nonlocal done_ok
            try:
                value = await task
            except BaseException as exc:
                result.failures.append(exc)
            else:
                result.successes.append(value)
                done_ok += 1

        try:
            async with asyncio.timeout(timeout):
                while pending:
                    finished, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in finished:
                        await _consume(task)
                    if policy is CompletionPolicy.ANY and done_ok >= 1:
                        break
                    if policy is CompletionPolicy.QUORUM and done_ok >= quorum:
                        break
                    if policy is CompletionPolicy.ALL and not pending:
                        break
        except TimeoutError:
            result.timed_out = True
        finally:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        if policy is CompletionPolicy.ALL and (result.failures or result.timed_out):
            if not result.successes and result.failures:
                raise ReplyError(result.failures[0])
        if policy is CompletionPolicy.ANY and not result.successes:
            if result.failures:
                raise ReplyError(result.failures[0])
            raise ReplyError(PatternRejected("scatter-gather: no successful reply"))
        if policy is CompletionPolicy.QUORUM and done_ok < quorum:
            raise ReplyError(
                PatternRejected(
                    f"scatter-gather: quorum {quorum} not reached ({done_ok} success(es))"
                )
            )
        return result
