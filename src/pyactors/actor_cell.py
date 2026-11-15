"""Internal per-actor runtime: mailbox, consumer task and lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pyactors.actor_ref import ActorRef
from pyactors.context import ActorContext
from pyactors.exceptions import ActorFailed, ActorStopped, ReplyError
from pyactors.lifecycle import LifecycleState
from pyactors.mailbox import Mailbox

if TYPE_CHECKING:
    from pyactors.actor import Actor
    from pyactors.actor_system import ActorSystem

__all__ = ["ActorCell"]

logger = logging.getLogger("pyactors")

_CREATED = LifecycleState.CREATED
_STARTING = LifecycleState.STARTING
_RUNNING = LifecycleState.RUNNING
_STOPPING = LifecycleState.STOPPING
_FAILED = LifecycleState.FAILED
_STOPPED = LifecycleState.STOPPED


class ActorCell:
    """Owns one actor instance and the single task that drives it.

    All lifecycle transitions happen inside :meth:`_run` except the
    *requests* made by :meth:`request_stop`, which only close the mailbox,
    discard queued work (immediate mode) and, when safe, cancel the task.

    Pending-request bookkeeping is implicit: every unanswered ask Future is
    either inside a queued envelope (failed by :meth:`_discard_queued`), the
    one currently being handled (``_current_reply``), or still held by a
    producer blocked in ``Mailbox.put`` (released by ``Mailbox.close``).
    This avoids a per-message set insertion/removal on the hot path.
    """

    __slots__ = (
        "_aborting",
        "_current_reply",
        "_failure",
        "_finalizing",
        "_reply_detached",
        "_start_error",
        "_started",
        "_started_ok",
        "_terminated",
        "actor",
        "actor_id",
        "loop",
        "mailbox",
        "name",
        "ref",
        "state",
        "system",
        "task",
    )

    def __init__(
        self,
        system: ActorSystem,
        actor: Actor[Any, Any],
        name: str,
        actor_id: str,
        mailbox_size: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.system = system
        self.actor = actor
        self.name = name
        self.actor_id = actor_id
        self.loop = loop
        self.state = _CREATED
        self.mailbox = Mailbox(mailbox_size, loop, actor_id)
        self.ref: ActorRef[Any, Any] = ActorRef(self)
        self.task: asyncio.Task[None] | None = None
        self._started: asyncio.Future[None] = loop.create_future()
        self._terminated = asyncio.Event()
        self._started_ok = False
        self._aborting = False
        self._finalizing = False
        self._failure: BaseException | None = None
        self._start_error: BaseException | None = None
        self._current_reply: asyncio.Future[Any] | None = None
        self._reply_detached = False
        actor._pyactors_context = ActorContext(self)

    def detach_ask(self) -> asyncio.Future[Any] | None:
        """Hand the current ask Future to user code; the cell will not complete it."""
        reply = self._current_reply
        if reply is None:
            return None
        self._current_reply = None
        self._reply_detached = True
        return reply

    # ------------------------------------------------------------------ control API

    def start(self) -> None:
        self.task = self.loop.create_task(self._run(), name=f"pyactors:{self.actor_id}")

    async def wait_started(self) -> None:
        """Wait until ``on_start`` succeeded; re-raise its error otherwise.

        If the caller is cancelled, the actor is stopped immediately.
        """
        try:
            await self._started
        except asyncio.CancelledError:
            self.request_stop(graceful=False)
            raise

    async def wait_terminated(self) -> None:
        await self._terminated.wait()

    @property
    def terminated(self) -> bool:
        return self._terminated.is_set()

    def request_stop(self, graceful: bool) -> None:
        """Begin stopping; never blocks and is safe to call repeatedly.

        A later ``graceful=False`` request escalates an in-progress graceful
        stop.  ``on_stop`` itself is never cancelled by this method.
        """
        if self._terminated.is_set():
            return
        self.mailbox.close(self._stopped_error)
        if self.state is _RUNNING:
            self.state = _STOPPING
        if graceful:
            return
        self._discard_queued(self._stopped_error)
        if self._finalizing or self._aborting or self.state is _CREATED:
            # CREATED: _run sees the closed mailbox and skips on_start.
            return
        task = self.task
        if task is None or task is asyncio.current_task():
            # Self-stop: the current receive() finishes, then the loop sees
            # an empty, closed mailbox and terminates.
            return
        self._aborting = True
        task.cancel()

    # ------------------------------------------------------------------ internals

    def _stopped_error(self) -> Exception:
        return ActorStopped(self.actor_id)

    def _failed_error(self) -> Exception:
        return ActorFailed(self.actor_id, self._failure)

    def _discard_queued(self, error: Callable[[], Exception]) -> None:
        dropped = self.mailbox.drain()
        if not dropped:
            return
        for envelope in dropped:
            reply = envelope.reply
            if reply is not None and not reply.done():
                reply.set_exception(error())
        logger.debug("actor %s discarded %d queued message(s)", self.actor_id, len(dropped))

    async def _run(self) -> None:
        task = self.task
        assert task is not None
        reraise: BaseException | None = None
        try:
            if not self.mailbox.closed:
                self.state = _STARTING
                await self.actor.on_start()
                self._started_ok = True
                if self.mailbox.closed:
                    # Stop requested during startup: drain then stop; spawn() fails.
                    self.state = _STOPPING
                else:
                    self.state = _RUNNING
                    if not self._started.done():
                        self._started.set_result(None)
                await self._process()
        except asyncio.CancelledError as exc:
            if not self._aborting:
                reraise = exc
        except Exception as exc:
            if self._started_ok:
                self._record_failure(exc)
            else:
                self._start_error = exc
        except BaseException as exc:  # KeyboardInterrupt, SystemExit
            reraise = exc
        if self._aborting and task.cancelling():
            task.uncancel()
        try:
            await self._finalize()
        except asyncio.CancelledError as exc:
            reraise = reraise or exc
        if reraise is not None:
            raise reraise

    async def _process(self) -> None:
        """Message loop: returns when the mailbox is closed and empty."""
        mailbox = self.mailbox
        items = mailbox._items
        receive = self.actor.receive
        while True:
            if items:
                envelope = mailbox.pop_nowait()
            else:
                next_envelope = await mailbox.get()
                if next_envelope is None:
                    return
                envelope = next_envelope
            reply = envelope.reply
            if reply is None:
                with contextlib.suppress(ReplyError):
                    await receive(envelope.message)
                continue
            if reply.done():
                # The asker timed out or was cancelled while the request was queued.
                continue
            self._current_reply = reply
            self._reply_detached = False
            try:
                result = await receive(envelope.message)
            except ReplyError as exc:
                if not self._reply_detached and not reply.done():
                    reply.set_exception(exc.cause)
                self._current_reply = None
                continue
            except Exception as exc:
                if not self._reply_detached and not reply.done():
                    reply.set_exception(exc)
                raise
            self._current_reply = None
            if not self._reply_detached and not reply.done():
                reply.set_result(result)

    def _record_failure(self, exc: BaseException) -> None:
        self._failure = exc
        self.state = _FAILED
        self.mailbox.close(self._failed_error)
        self.system._report_failure(self.ref, exc)

    async def _finalize(self) -> None:
        self._finalizing = True
        error = self._failed_error if self._failure is not None else self._stopped_error
        self.mailbox.close(error)
        self._discard_queued(error)
        reply = self._current_reply
        self._current_reply = None
        if reply is not None and not reply.done():
            reply.set_exception(error())
        cancelled: asyncio.CancelledError | None = None
        if self._started_ok:
            if self.state is not _FAILED:
                self.state = _STOPPING
            try:
                await self.actor.on_stop()
            except asyncio.CancelledError as exc:
                cancelled = exc
            except Exception as exc:
                self.system._report_failure(self.ref, exc)
        failed = self._failure is not None or self._start_error is not None
        self.state = _FAILED if failed else _STOPPED
        self.system._unregister(self)
        started = self._started
        if not started.done():
            if self._start_error is not None:
                started.set_exception(self._start_error)
            else:
                started.set_exception(ActorStopped(self.actor_id))
        self._terminated.set()
        if cancelled is not None:
            raise cancelled
