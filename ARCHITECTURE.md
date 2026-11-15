# pyactors architecture (v0.2)

## Components

```
user code ──► ActorRef ──► Mailbox ──► ActorCell._run (one task per actor) ──► Actor.receive
                 │                           │
                 └──────── ActorSystem ──────┘  (registry: name -> ActorCell)
```

| module | role |
| --- | --- |
| `actor.py` | `Actor[M, R]` base class: `receive`, `on_start`, `on_stop`, `context` |
| `actor_ref.py` | `ActorRef`: immutable, `__slots__`; holds the cell, its mailbox and loop |
| `actor_cell.py` | `ActorCell`: owns actor, mailbox, task, state, identity, system ref |
| `mailbox.py` | `Mailbox`: closable single-consumer FIFO with back-pressure |
| `envelope.py` | `Envelope(message, reply)`: `dataclass(slots=True)` |
| `context.py` | `ActorContext`: self ref, system, self-stop, `detach_ask` |
| `actor_system.py` | `ActorSystem`: spawn, registry, stop, shutdown, failure reporting |
| `lifecycle.py` | `LifecycleState` enum |
| `exceptions.py` | public exceptions including `ReplyError` |
| `patterns/` | Thirty reusable in-process patterns (see `docs/PATTERNS.md`) |

## Why a custom mailbox instead of `asyncio.Queue`

Stopping an actor has to (1) reject new messages at once, (2) wake every
producer blocked on a full mailbox with `ActorStopped`/`ActorFailed`, and (3)
never be blocked itself by a full queue. On Python 3.12, `asyncio.Queue` has
no close operation (`Queue.shutdown()` arrived in 3.13), and the runtime
cannot cancel producer tasks it does not own. `Mailbox` is the minimal
replacement: a `deque`, one waiter Future for the single consumer, and a FIFO
of waiter Futures for blocked producers. `close()` sets a flag and wakes every
waiter. Woken producers re-check the flag and raise. Stop control is
out-of-band: it is a flag, not a message, so it can never wait behind a full
queue. Because it skips `asyncio.Queue`'s `task_done`/`join` bookkeeping, it
is also cheaper.

A producer that is cancelled after being woken passes the wake-up to the next
waiter, so cancellation never strands capacity.

## The actor task

`ActorCell._run` is the only code that runs actor callbacks:

1. If the mailbox is already closed (stopped before the task ever ran), skip
   to finalisation without calling `on_start`. Cancelling a task that has not
   started would skip its `try`/`finally`, so stop requests in `CREATED`
   never cancel. They only close the mailbox.
2. `STARTING`: `await on_start()`. On success, resolve the `spawn()` Future
   (unless a stop was requested meanwhile).
3. `RUNNING`: the message loop. Hot path:
   `if items: pop_nowait() else: await get()`, then `await receive(msg)`. It
   uses no per-message tasks, locks, reflection, or set bookkeeping.
   `receive` is bound once.
4. `_finalize()` (always runs): close the mailbox, fail queued asks, fail the
   in-flight ask if it was interrupted, `await on_stop()` (only if
   `on_start` succeeded), set the terminal state, unregister, resolve
   `spawn()` if still pending, set the termination event.

### Stop requests (`ActorCell.request_stop`)

These are synchronous and idempotent:

- Always: close the mailbox (releases blocked producers); `RUNNING` →
  `STOPPING`.
- Graceful: nothing else. The loop drains the mailbox, and `get()` returns
  `None` when the mailbox is closed and empty.
- Immediate: drain and fail queued asks now (prompt failure even if the
  handler ignores cancellation), then `task.cancel()`. Exceptions: no cancel
  while finalising (`on_stop` is never cancelled), in `CREATED`, or when
  called from the actor's own task (self-stop). In the self-stop case the
  emptied, closed mailbox ends the loop after the current `receive` returns.
- The cell sets `_aborting` before cancelling, so `_run` can tell its own
  cancellation from an external one (for example event-loop teardown, which
  is cleaned up and then re-raised). After its own cancellation it calls
  `task.uncancel()` so `on_stop` runs in a clean, non-cancelling task (so
  `asyncio.timeout` inside `on_stop` behaves).

### Pending-request bookkeeping

There is no per-request registry. Every unanswered ask Future is in exactly
one of three places:

- in a queued envelope, failed by `_discard_queued`;
- the in-flight one (`_current_reply`), failed in `_finalize`;
- held by a producer blocked in `Mailbox.put`, released by `close()`.

This keeps the hot path free of set insert/remove calls while still
guaranteeing no request waits forever. Every completion is guarded with
`if not fut.done()`, so late results after a timeout or cancellation never
raise `InvalidStateError`.

### Ask

`ask` creates one loop Future and one envelope, awaits `mailbox.put`, then
the Future, both inside a single `asyncio.timeout` (if given). If the caller
is cancelled or times out, the Future is cancelled. The actor then skips that
envelope if it is still queued, or discards the result if the handler was
already running.

## Termination and shutdown

`stop()` awaits the cell's `asyncio.Event` (cancel-safe: each waiter gets its
own Future). `shutdown()` sets `_closing` (spawn checks it before and after
constructing the actor), snapshots all cells (including starting ones),
requests stops, and awaits each, skipping the caller's own cell. Names are
released only when an actor terminates. Refs bind to a cell, not a name, so
an old ref can never reach a new actor that reuses the name.

## Failure observability

`receive` and `on_stop` exceptions go to `ActorSystem(failure_handler=...)`,
which by default logs at `ERROR` on the `"pyactors"` logger. Errors raised by
the handler itself are logged, not propagated. `ActorFailed.cause` and
`ref.state is LifecycleState.FAILED` give programmatic access.

## Design decisions where requirements interact

- **Ask exception vs. actor failure.** The spec requires both "a handler
  exception completes the Future exceptionally" and "if receive fails, the
  actor transitions to FAILED". Both apply: an exception during an ask fails
  the actor *and* is delivered to that asker.
- **`asyncio.Queue` vs. shutdown semantics.** Replaced by `Mailbox` (above).
- **Graceful shutdown during startup.** `on_start` is allowed to finish (user
  code is not interrupted), then the actor stops gracefully and `spawn()`
  raises `ActorStopped`. Immediate shutdown cancels `on_start` instead.
- **Queued asks whose caller gave up are dropped.** This avoids work nobody
  waits for. Handlers that already started are never interrupted, as
  required.
- **Self-stop** is non-blocking by definition (`context.stop()`), and
  `system.stop`/`shutdown` detect the caller's own task to avoid awaiting
  their own termination.
- **`ReplyError`.** Raising `ReplyError(cause)` from `receive` completes the
  current `ask` with `cause` but leaves the actor `RUNNING`. Pattern actors
  (retry, circuit breaker, routers) use this so a target failure does not
  kill the pattern actor. Uncaught non-`ReplyError` exceptions still fail
  the actor.
- **`context.detach_ask()`.** Hands the current ask Future to user code so
  `receive` can return immediately while the caller remains parked. Required
  for barriers, queues, debouncers and similar patterns that must keep
  processing the mailbox while an asker waits. The pattern must complete the
  Future later with `set_result` / `set_exception`.

## Patterns package

`src/pyactors/patterns/` is organised by concern: `routing`, `messaging`,
`queues`, `batching`, `flow_control`, `coordination`, `scheduling`,
`resilience`, `state`, `events`, `processing`. Patterns compose via ordinary
`ActorRef`s; they add no runtime dependencies. Catalog: `docs/PATTERNS.md`.

## Performance notes

- One task, one mailbox, one ref, one context and one Event per actor. All
  internal classes use `__slots__`.
- `tell` costs one `Envelope` allocation, a `deque.append`, and an optional
  waiter wake-up.
- Registry lookup is a dict lookup, O(1).
- No polling: all waiting is on Futures.
