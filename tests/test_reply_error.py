"""Tests for ReplyError: fail ask without failing the actor."""

from __future__ import annotations

from typing import Any

import pytest

from pyactors import Actor, ActorSystem, LifecycleState, ReplyError

from .helpers import within


class SometimesFails(Actor[Any, Any]):
    async def receive(self, message: Any) -> Any:
        if message == "boom":
            raise ReplyError(ValueError("handled"))
        return message


async def test_reply_error_fails_ask_but_actor_survives(system: ActorSystem) -> None:
    ref = await system.spawn(SometimesFails)
    with pytest.raises(ValueError, match="handled"):
        await within(ref.ask("boom"))
    assert ref.is_alive
    assert ref.state is LifecycleState.RUNNING
    assert await within(ref.ask("ok")) == "ok"


async def test_reply_error_on_tell_is_swallowed(system: ActorSystem) -> None:
    ref = await system.spawn(SometimesFails)
    ref.tell("boom")
    assert await within(ref.ask("still-alive")) == "still-alive"
