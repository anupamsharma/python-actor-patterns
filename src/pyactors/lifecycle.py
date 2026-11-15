"""Actor lifecycle states."""

from __future__ import annotations

import enum

__all__ = ["LifecycleState"]


class LifecycleState(enum.Enum):
    """Lifecycle of a single actor.

    Normal path::

        CREATED -> STARTING -> RUNNING -> STOPPING -> STOPPED

    ``FAILED`` is terminal and is entered when ``on_start()`` or ``receive()``
    raises. ``STOPPED`` and ``FAILED`` are the only terminal states.
    """

    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"
    STOPPED = "stopped"

    @property
    def is_terminal(self) -> bool:
        """True for ``STOPPED`` and ``FAILED``."""
        return self is LifecycleState.STOPPED or self is LifecycleState.FAILED
