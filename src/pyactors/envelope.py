"""Internal message envelope."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

__all__ = ["Envelope"]


@dataclass(slots=True)
class Envelope:
    """A user message plus the optional Future used to answer an ``ask``.

    The actor's ``receive()`` only ever sees ``message``; the envelope never
    leaks to user code.
    """

    message: Any
    reply: asyncio.Future[Any] | None
