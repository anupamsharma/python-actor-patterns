"""Typed state-machine actor."""

from __future__ import annotations

from collections.abc import Callable, Hashable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from pyactors.actor import Actor
from pyactors.exceptions import ReplyError
from pyactors.patterns.common import maybe_await
from pyactors.patterns.exceptions import PatternRejected
from pyactors.patterns.messages import GetStats

__all__ = ["Event", "GetState", "StateMachine", "Transition"]

S = TypeVar("S", bound=Hashable)
E = TypeVar("E", bound=Hashable)


@dataclass(frozen=True, slots=True)
class Transition[S: Hashable, E: Hashable]:
    """A single transition: ``source --event[guard]/action--> target``."""

    source: S
    event: E
    target: S
    guard: Callable[[Any], bool] | None = None
    action: Callable[[Any], Any] | None = None


@dataclass(frozen=True, slots=True)
class Event[E: Hashable]:
    name: E
    payload: Any = None


@dataclass(frozen=True, slots=True)
class GetState:
    pass


class StateMachine[S: Hashable, E: Hashable](Actor[Any, Any]):
    """Actor-backed finite state machine.

    Messages are :class:`Event` instances (or bare event names if they match
    configured events).  Unmatched events raise :class:`PatternRejected`
    unless ``ignore_unknown`` is True.
    """

    def __init__(
        self,
        initial: S,
        transitions: Sequence[Transition[S, E]],
        *,
        ignore_unknown: bool = False,
        on_transition: Callable[[S, E, S, Any], Any] | None = None,
    ) -> None:
        self._state = initial
        self._ignore_unknown = ignore_unknown
        self._on_transition = on_transition
        self._table: dict[tuple[S, E], list[Transition[S, E]]] = {}
        for tr in transitions:
            self._table.setdefault((tr.source, tr.event), []).append(tr)
        self._steps = 0

    @property
    def current_state(self) -> S:
        return self._state

    async def receive(self, message: Any) -> Any:
        if isinstance(message, (GetState, GetStats)) or message == "state":
            if isinstance(message, GetStats):
                return {"state": self._state, "steps": self._steps}
            return self._state
        if isinstance(message, Event):
            event_name: E = message.name
            payload = message.payload
        else:
            event_name = cast("E", message)
            payload = None
        candidates = self._table.get((self._state, event_name), ())
        for tr in candidates:
            if tr.guard is not None and not tr.guard(payload):
                continue
            previous = self._state
            action_result = None
            if tr.action is not None:
                action_result = await maybe_await(tr.action(payload))
            self._state = tr.target
            self._steps += 1
            if self._on_transition is not None:
                await maybe_await(self._on_transition(previous, event_name, tr.target, payload))
            return {"from": previous, "to": self._state, "result": action_result}
        if self._ignore_unknown:
            return None
        raise ReplyError(
            PatternRejected(f"no transition for state={self._state!r} event={event_name!r}")
        )
