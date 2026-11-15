"""Exceptions raised by reusable actor patterns."""

from __future__ import annotations

from pyactors.exceptions import ActorError

__all__ = [
    "CircuitOpen",
    "PatternError",
    "PatternFull",
    "PatternRejected",
    "PatternStopped",
    "RateLimited",
    "RetryExhausted",
    "SagaFailed",
]


class PatternError(ActorError):
    """Base class for pattern-specific errors."""


class PatternFull(PatternError):
    """A bounded pattern buffer rejected an item because it is full."""


class PatternRejected(PatternError):
    """A pattern rejected a message (policy, open circuit, etc.)."""


class PatternStopped(PatternError):
    """The pattern actor is stopping or has stopped."""


class RateLimited(PatternRejected):
    """A rate limiter or throttle rejected or deferred a message."""


class CircuitOpen(PatternRejected):
    """A circuit breaker is open and is not forwarding requests."""


class RetryExhausted(PatternError):
    """All retry attempts failed.

    ``last_error`` is the exception from the final attempt.
    """

    def __init__(self, last_error: BaseException, attempts: int) -> None:
        super().__init__(last_error, attempts)
        self.last_error = last_error
        self.attempts = attempts
        self.__cause__ = last_error

    def __str__(self) -> str:
        return f"retry exhausted after {self.attempts} attempt(s): {self.last_error!r}"


class SagaFailed(PatternError):
    """A saga forward step failed; compensation was attempted.

    ``failed_step`` is the zero-based index of the failed forward step.
    ``cause`` is the original exception. ``compensation_errors`` lists any
    errors raised while running compensations (in reverse order).
    """

    def __init__(
        self,
        failed_step: int,
        cause: BaseException,
        compensation_errors: list[BaseException] | None = None,
    ) -> None:
        super().__init__(failed_step, cause)
        self.failed_step = failed_step
        self.cause = cause
        self.compensation_errors = list(compensation_errors or ())
        self.__cause__ = cause

    def __str__(self) -> str:
        return f"saga failed at step {self.failed_step}: {self.cause!r}"
