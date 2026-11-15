from __future__ import annotations

import asyncio
import gc
from collections.abc import AsyncIterator
from typing import Any

import pytest

from pyactors import ActorRef, ActorSystem

Failures = list[tuple[ActorRef[Any, Any], BaseException]]


@pytest.fixture(autouse=True)
async def loop_guard() -> AsyncIterator[list[dict[str, Any]]]:
    """Fail the test on unhandled loop errors or leaked actor tasks.

    Catches "Task exception was never retrieved", "Future exception was never
    retrieved" and any exception reported through the loop's handler.
    """
    loop = asyncio.get_running_loop()
    errors: list[dict[str, Any]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))
    try:
        yield errors
        for _ in range(5):
            await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
        leaked = [
            t for t in asyncio.all_tasks() if t.get_name().startswith("pyactors:") and not t.done()
        ]
        assert not leaked, f"leaked actor tasks: {leaked}"
        assert not errors, f"unhandled loop errors: {errors}"
    finally:
        loop.set_exception_handler(previous)


@pytest.fixture
def failures() -> Failures:
    """Failures reported by the system; tests that expect some must clear() them."""
    return []


@pytest.fixture
async def system(failures: Failures) -> AsyncIterator[ActorSystem]:
    sys = ActorSystem("test", failure_handler=lambda ref, exc: failures.append((ref, exc)))
    yield sys
    async with asyncio.timeout(5):
        await sys.shutdown(graceful=False)
    assert sys.actor_count == 0
    assert not failures, f"unexpected actor failures: {failures}"
