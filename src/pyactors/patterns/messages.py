"""Common control messages understood by several patterns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "Flush",
    "GetStats",
    "Ping",
    "ShutdownHint",
]


@dataclass(frozen=True, slots=True)
class Flush:
    """Ask a buffering / batching / debounce pattern to emit pending work now."""


@dataclass(frozen=True, slots=True)
class GetStats:
    """Ask a pattern for a snapshot of diagnostic counters."""


@dataclass(frozen=True, slots=True)
class Ping:
    """Liveness probe; patterns that recognise it reply with ``"pong"``."""


@dataclass(frozen=True, slots=True)
class ShutdownHint:
    """Optional hint that a pattern should finish pending work before stop.

    Sending this does not stop the actor; call ``system.stop(ref)`` afterwards.
    ``drain`` requests that buffered items be processed first.
    """

    drain: bool = True
    payload: Any = None
