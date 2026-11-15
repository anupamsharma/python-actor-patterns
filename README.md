# pyactors

A small, fast, Akka-inspired **actor runtime for Python 3.12+**, built on `asyncio`
with zero runtime dependencies.

The core provides a single-process actor model: actors, refs, `tell`/`send`/`ask`,
FIFO mailboxes (bounded or unbounded), lifecycle hooks, failure propagation and
graceful/immediate shutdown.

**v0.2** adds thirty reusable in-process patterns (routers, queues, batching,
flow control, worker pools, circuit breakers, sagas, pub/sub, pipelines, and
more). See [docs/PATTERNS.md](docs/PATTERNS.md). Supervision trees, persistence,
multiprocessing and distribution remain out of scope (see
[Limitations](#limitations-and-extension-points)).

```python
import asyncio
from pyactors import Actor, ActorSystem


class Counter(Actor[int, int]):
    def __init__(self) -> None:
        self.total = 0

    async def receive(self, message: int) -> int:
        self.total += message
        return self.total


async def main() -> None:
    async with ActorSystem() as system:
        counter = await system.spawn(Counter, name="counter")
        counter.tell(1)  # fire and forget
        await counter.send(2)  # waits for mailbox capacity
        print(await counter.ask(3))  # request/response -> 6


asyncio.run(main())
```

## Installation

```bash
pip install .            # or: uv pip install .
pip install -e '.[dev]'  # pytest, pytest-asyncio, pytest-timeout, ruff, mypy
```

## API

### `Actor[M, R]`

Subclass and implement `async receive(message: M) -> R`. Optional hooks:
`async on_start()` and `async on_stop()`. State is ordinary instance
attributes. `self.context` (available from `on_start` onward, not in
`__init__`) exposes:

| member | meaning |
| --- | --- |
| `context.ref` | the actor's own `ActorRef` |
| `context.system` | the owning `ActorSystem` |
| `context.name`, `context.actor_id` | identity |
| `context.stop(graceful=True)` | non-blocking self-stop |

### `ActorRef[M, R]`

Immutable, lightweight handle; the actor object itself is never exposed.

| member | behaviour |
| --- | --- |
| `tell(msg)` | synchronous, never blocks. Raises `MailboxFull` (bounded mailbox full) or `ActorStopped` |
| `await send(msg)` | waits for capacity; returns when the message is **accepted**, not processed. Raises `ActorStopped` if the actor stops while waiting. Cancel-safe |
| `await ask(msg, timeout=None)` | returns the value returned by `receive`. `timeout` covers mailbox admission **and** the reply; raises `TimeoutError` |
| `await stop(graceful=True)` | shorthand for `system.stop(ref)` |
| `actor_id` | unique per incarnation, e.g. `"counter#3"` |
| `name` | registered name |
| `is_alive` | `True` while the actor accepts messages (`CREATED`/`STARTING`/`RUNNING`) |
| `state` | current `LifecycleState` (diagnostic) |

### `ActorSystem`

| member | behaviour |
| --- | --- |
| `await spawn(actor_type, *args, name=None, mailbox_size=0, **kwargs)` | builds `actor_type(*args, **kwargs)`, runs `on_start`, returns the ref. `mailbox_size=0` means unbounded. Generated names look like `"$7"`; explicit names may not start with `$` |
| `get(name)` | the registered ref, or **`None`** if no such actor |
| `await stop(ref, graceful=True)` | stops and waits for termination; idempotent |
| `await shutdown(graceful=True)` | stops all actors, rejects new spawns; idempotent; a later `graceful=False` call escalates |
| `actor_count` | number of registered, not-yet-terminated actors |
| `async with ActorSystem() as system:` | graceful shutdown on exit (immediate if exiting via `CancelledError`/`KeyboardInterrupt`) |

`ActorSystem(name="pyactors", failure_handler=None)`: `failure_handler(ref, exc)`
is called when `receive()` or `on_stop()` raises. The default logs at `ERROR`
on the `"pyactors"` logger.

### Exceptions

```
ActorError
├── ActorStopped        actor not accepting / message discarded / stopped during startup
│   └── ActorFailed     actor terminated because receive() raised (.cause = original)
├── MailboxFull         tell() on a full bounded mailbox
├── ActorNameTaken      duplicate name in spawn()
├── ActorSystemClosed   spawn() after shutdown began
└── ReplyError          fail the current ask without failing the actor (.cause)
```
Timeouts raise the built-in `TimeoutError`. Pattern-specific errors
(`RateLimited`, `CircuitOpen`, `RetryExhausted`, …) live under
`pyactors.patterns.exceptions`.

## Patterns

```python
from pyactors.patterns import RoundRobinRouter, Batch, spawn_worker_pool
```

Thirty composable patterns are documented in [docs/PATTERNS.md](docs/PATTERNS.md).
Examples live under `examples/patterns/`.

## Guarantees

- **Sequential execution.** At most one `receive()` runs at a time per actor;
  each actor is driven by exactly one consumer task (no task per message).
- **Concurrency.** Different actors run concurrently on the same event loop.
- **Ordering.** Messages are handled in mailbox (FIFO) order. Messages from a
  single producer that enqueues sequentially keep their order (also across
  `tell`/`send`/`ask`). Concurrent producers have no defined global order.
- **Startup.** `on_start` completes before `spawn()` returns and before the
  first message is handled. Messages accepted during startup (e.g. sent by
  `on_start` itself) are queued and processed afterwards.
- **Isolation.** Refs never expose actor state. Python cannot prevent you from
  deliberately sharing a mutable object between actors (via constructor
  arguments or inside messages); prefer immutable messages.
- **No retries, no restarts.** A failed message is never re-delivered; a
  failed actor stays failed.
- **Single event loop, not thread-safe.** Use a system and its refs from the
  loop that created them. Use `loop.call_soon_threadsafe` to hand messages in
  from other threads.

## Lifecycle and failure semantics

```
CREATED -> STARTING -> RUNNING -> STOPPING -> STOPPED
              |           |
              +-----------+------> FAILED   (on_start or receive raised)
```

| event | what happens |
| --- | --- |
| `on_start` raises | `spawn()` re-raises it, the name is unregistered, `on_stop` is **not** called, state `FAILED` |
| `receive` raises | state `FAILED`; the current asker gets the original exception; other queued asks get `ActorFailed`; queued tells are discarded; blocked `send`/`ask` producers get `ActorFailed`; `on_stop` runs; the failure goes to `failure_handler` |
| `on_stop` raises | reported to `failure_handler`; termination still completes (`STOPPED`, or `FAILED` if already failed) |
| stop before/during startup | `spawn()` raises `ActorStopped`. Graceful: an in-flight `on_start` finishes, then `on_stop` runs. Immediate: `on_start` is cancelled and `on_stop` is skipped |

A failing `receive` always fails the actor, even when the message came from
`ask`. Catch exceptions inside `receive` and return an error value if the
actor should survive them.

### Ask semantics

- `receive` sees the original message; its return value (including `None`)
  completes the asker's Future, its exception fails it.
- `timeout` spans waiting for mailbox capacity plus waiting for the reply.
- A timeout or cancellation **never interrupts a handler that already
  started**; its late result is discarded safely.
- A request that is still **queued** when its caller times out or is
  cancelled is dropped without calling `receive` (its Future is already
  done). Do not use `ask(timeout=...)` as "fire and forget".
- If the actor stops before handling a queued ask, the ask fails promptly with
  `ActorStopped`/`ActorFailed`; no request waits forever.

### Stop and shutdown

| | graceful (`graceful=True`) | immediate (`graceful=False`) |
| --- | --- | --- |
| new messages | rejected (`ActorStopped`) | rejected |
| blocked `send`/`ask` producers | released with `ActorStopped` | released with `ActorStopped` |
| current handler | runs to completion | cancelled (`CancelledError` raised inside it) |
| accepted, queued messages | processed, asks answered | discarded, asks fail with `ActorStopped` |
| `on_stop` | runs | runs (never cancelled by the runtime) |

- Cancellation is cooperative. Code that blocks the event loop (CPU loops,
  blocking I/O) or swallows `CancelledError` cannot be interrupted; the stop
  completes when the handler returns.
- `stop()` and `shutdown()` are idempotent and safe to call concurrently. An
  immediate request escalates a graceful one that is in progress.
- **Self-stop never deadlocks.** `context.stop()` only initiates the stop.
  `await system.stop(own_ref)` or `await system.shutdown()` called from the
  actor's own task returns without waiting for its own termination; the
  current `receive` finishes first and then the stop proceeds.
- Awaiting `system.stop(other)` from inside an actor blocks that actor until
  `other` terminates. Two actors stopping (or asking) each other this way can
  deadlock, just as mutual `ask` can.

## Examples

```bash
python examples/counter.py
python examples/ping_pong.py
python examples/request_response.py
python examples/concurrent_actors.py
python examples/bounded_mailbox.py
python examples/graceful_shutdown.py
```

## Development

```bash
uv venv --python 3.12 .venv && uv pip install -e '.[dev]'
.venv/bin/pytest            # 111 tests
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/mypy              # strict
.venv/bin/python benchmarks/bench.py [--quick] [--json out.json]
```

The test suite fails any test that leaks actor tasks, leaves unretrieved
task/Future exceptions, or reports an unexpected actor failure.

See [ARCHITECTURE.md](ARCHITECTURE.md) for runtime internals and
[benchmarks/results.txt](benchmarks/results.txt) for the latest measured
benchmark output.

## Limitations and extension points

- Single process, single event loop; not thread-safe.
- No supervision, restart, persistence, remote actors, clustering or
  routing (future phases). Natural extension points: a supervisor hook where
  `ActorCell` records a failure, mailbox variants behind the `Mailbox`
  interface, and the ref/cell split for location transparency.
- Asks whose caller already gave up are dropped (see above).
- `on_stop` is not time-bounded; a hanging `on_stop` delays `stop()`/`shutdown()`.
- Self-stop detection recognises the actor's own task only. If an actor awaits a
  helper task that in turn awaits `system.stop(<that actor>)`, it deadlocks; use
  `context.stop()` instead.
- Producers waiting on a full mailbox are woken in FIFO order, but a `tell`
  that arrives just as a slot frees up can take that slot first (same as
  `asyncio.Queue`). Per-producer ordering is unaffected.
