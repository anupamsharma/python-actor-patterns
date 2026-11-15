# Pattern catalog

Reusable actor patterns built on the core single-process runtime. Every pattern
is in-memory, provides **at-most-once** delivery of accepted messages unless
noted otherwise, and is neither durable nor distributed.

Import from `pyactors.patterns`:

```python
from pyactors.patterns import RoundRobinRouter, Batch, WorkerPool
```

## Messaging and routing

| Pattern | Module | Summary |
| --- | --- | --- |
| Router | `patterns.routing` | Base router; subclasses choose a routee |
| Round Robin Router | `RoundRobinRouter` | Cycle through routees |
| Random Router | `RandomRouter` | Uniform random routee |
| Consistent Hash Router | `ConsistentHashRouter` | Same key → same routee |
| Content-Based Router | `ContentBasedRouter` | First matching predicate wins |
| Broadcast | `BroadcastRouter` | Ask all routees; return list of replies |
| Scatter-Gather | `ScatterGather` | Fan-out with ALL / ANY / QUORUM policies |
| Request Aggregator | `RequestAggregator` | Combine partial requests by group id |

## Queues and flow control

| Pattern | Module | Summary |
| --- | --- | --- |
| Queue Actor | `QueueActor` | FIFO producer/consumer; optional ack |
| Priority Queue | `PriorityQueueActor` | Lower priority number dequeued first |
| Buffer | `Buffer` | Hold until capacity, timeout, or flush |
| Batch | `Batch` | Emit by max size and/or max wait |
| Debounce | `Debounce` | Trailing-edge; latest after quiet period |
| Throttle | `Throttle` | Minimum interval between emissions |
| Rate Limiter | `RateLimiter` | Token bucket; reject or wait |
| Retry | `Retry` | Forward with backoff and predicates |

## Coordination and scheduling

| Pattern | Module | Summary |
| --- | --- | --- |
| Worker Pool | `WorkerPool` / `spawn_worker_pool` | Fixed workers, round-robin distribution |
| Barrier | `Barrier` | Release N waiters together |
| Semaphore | `Semaphore` | Counting permits via acquire/release |
| Saga Coordinator | `SagaCoordinator` | Forward steps + reverse compensation |
| Circuit Breaker | `CircuitBreaker` | CLOSED / OPEN / HALF_OPEN |
| Timer | `Timer` | One-shot and periodic `tell` |
| Task Scheduler | `TaskScheduler` | Delayed callables on the event loop |

## State, events, and processing

| Pattern | Module | Summary |
| --- | --- | --- |
| State Machine | `StateMachine` | Typed states, events, guards, actions |
| Cache | `Cache` | LRU + optional TTL |
| Pub/Sub | `PubSub` | Topic subscriptions |
| Event Bus | `EventBus` | Handlers by concrete event type |
| Pipeline | `Pipeline` | Ordered stages (callables or refs) |
| Map-Reduce | `MapReduce` | Map items then reduce |
| Window Aggregator | `Window` | Tumbling / sliding time windows |

## Conventions

* **Control messages:** `Flush`, `GetStats`, and pattern-specific dataclasses.
* **Overflow:** `OverflowPolicy` — `REJECT`, `DROP_NEWEST`, `DROP_OLDEST`, `WAIT`.
* **Surviving target failures:** pattern actors raise `ReplyError` so an `ask`
  fails without moving the pattern actor to `FAILED`.
* **Queue acknowledgements:** with `ack_required=True`, delivery is at-least-once
  *within the process* until `Ack`; not durable across restarts.
* **Saga:** in-memory only — not a durable distributed transaction engine.

## Examples

See `examples/patterns/` for one runnable script per pattern.
