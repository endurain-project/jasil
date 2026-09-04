# Durable jobs

The event bus is best-effort: a failed handler is logged and the event is gone.
Durable jobs are the other option — every `(event, subscriber)` pair becomes a
database row that is retried with backoff and dead-lettered if it never succeeds.

Requires the `jobs` extra and a database.

```python
jasil_settings.configure(
    jasil_settings.JasilSettings(
        jobs=jasil_settings.JobSettings(enabled=True),
    )
)
```

## The pipeline

```
publish(..., db=session)
      │
      ▼
 event_outbox ─────relay─────▶ processing_jobs ─────worker─────▶ subscriber
  (one row per         (one row per                    (claim, run,
   event)               event × subscriber)             complete or fail)
```

Two stages, deliberately. The producer writes **one** row and returns; fanning
out to N subscribers happens later, off the request path. The fan-out is
idempotent, so re-running the relay is harmless.

## Registering a subscriber

```python
import jasil.jobs.registry as jobs_registry

jobs_registry.registry.register("order.created", "invoice.render", render_invoice)
```

The `subscriber_id` is a stable identifier independent of the Python module
path — it is stored on every job row, so renaming a module must not orphan
queued work.

The handler must **raise** on failure. That is what drives retry.

Every registration has a named queue. Existing calls use `default`; select one
when the subscriber is registered when independent worker capacity matters:

```python
jobs_registry.registry.register(
    "order.created",
    "invoice.render",
    render_invoice,
    queue="campaign",
)
```

Queue ownership belongs to the subscriber rather than the event. One event can
fan out to several subscribers on different queues, and the relay stamps each
subscriber's queue on its own job. Names are 1–100 lowercase ASCII characters:
letters, digits, `.`, `_`, and `-`; an invalid name raises before persistence.

## The job state machine

```
                 ┌──────────────── backoff ◀────────────┐
                 ▼                                      │
enqueue ──▶ pending ──claim──▶ claimed ──success──▶ completed
                                  │
                                  ├──failure, attempts left──▶ pending
                                  ├──failure, ceiling hit───▶ dead_letter
                                  └──lease expired──────────▶ pending | dead_letter
```

| Status | Meaning |
|---|---|
| `pending` | Waiting to be claimed, no earlier than `available_at`. |
| `claimed` | Leased to a worker until `lease_expires_at`. |
| `completed` | Terminal. Prunable. |
| `dead_letter` | Terminal. Kept for operator review; never pruned. |

### Claiming

A worker claims a batch of due jobs, taking a time-bounded lease. On PostgreSQL
the claim uses `FOR UPDATE SKIP LOCKED`, so concurrent workers take disjoint
batches with no coordinating lock.

A worker can select a non-empty queue allowlist. Omitting it consumes every
queue, preserving the original behavior. A selective claim includes the queue
predicate in the locked query, so it cannot lease work from another queue.
Workers consuming several queues rotate their queue cursor between batches; a
permanently busy queue cannot starve another one indefinitely.
Rotation considers only the worker's allowlist. Queues outside it are never
claimed by that worker and need another matching worker.

!!! note "The attempt is counted at claim time"
    Not at completion. A worker that crashes mid-run still consumes an attempt,
    which is what bounds a crash loop — otherwise a job that reliably kills its
    worker would be retried forever.

### Failure and backoff

A failed job is rescheduled with an exponentially growing delay:
`base_seconds * 2 ** (attempts - 1)`, clamped to `backoff_max_seconds`.

**Equal jitter** is applied: the delay is randomised to between 50% and 100% of
the computed value. Without it, a batch of jobs that failed together during a
downstream outage would all retry at the same instant and stampede the recovering
dependency.

Once `attempts` reaches `max_attempts`, the job becomes `dead_letter`.

### Lease reclamation

A worker that dies holds its lease until it expires. The reaper returns those
jobs to `pending` — or dead-letters them if they have no attempts left — so work
is never stranded by a crash.

## Idempotency

`(event_id, subscriber_id)` is **unique in the database**. A repeated enqueue is a
no-op, so a subscriber never runs twice for the same event even if the relay
overlaps with another replica's.

This is a database constraint rather than relay logic on purpose: it holds under
concurrency, restarts, and manual intervention.

## Reconciliation nets

Durable is not the same as guaranteed. A Redis-Streams consumer can drop a
message, a provider can be briefly down, and some write paths persist rows
without publishing anything at all. A subscriber that writes **durable** derived
state therefore needs a scheduled backfill that re-derives whatever the create
path missed.

Declare one per subscriber:

```python
from jasil.jobs.reconciliation import DurableSubscriberNet

NETS = [
    DurableSubscriberNet("invoice.render", backfill=backfill_missing_invoices),
    DurableSubscriberNet("cache.warm", backfill=None, exempt_reason="rebuilt on read"),
]
```

Exactly one of `backfill` / `exempt_reason` must be set — neither is refused at
construction. A subscriber with no net and no stated reason is one whose derived
state goes missing silently, which is the failure the type exists to prevent.

Hold every subscriber to it with one conformance test:

```python
import jasil.jobs.registry as jobs_registry
from jasil.jobs.reconciliation import assert_nets_complete


def test_every_durable_subscriber_declares_a_net():
    assert_nets_complete(ALL_NETS, registry=jobs_registry.registry)
```

Import every subscriber module first, or the registry will be empty and the test
will pass by vacuum. `undeclared_subscribers` is the same check as a plain query
when you want to report the gap rather than fail on it.

## Running the workers

### Local: one process and SQLite

```python
import jasil.jobs.service as jobs_service

jobs_service.start_job_worker()  # in-process worker thread
jobs_service.schedule_job_maintenance(scheduler)  # relay + reaper on APScheduler
```

This worker consumes all named queues serially and rotates the starting queue
between batches for process-lifetime fairness. SQLite supports exactly this
topology: one API process and one in-process consumer. JASIL refuses
an in-process SQLite worker when `web_workers != 1`, and refuses a standalone
SQLite worker because it has enough information to know those shapes are unsafe.

### Distributed: API plumbing and PostgreSQL workers

The API process schedules the relay and reaper but does not start a domain-job
consumer:

```python
jobs_service.schedule_job_maintenance(scheduler)
```

Standalone worker processes use the supported blocking entry point. It must run
in a dedicated worker process, never on an API request or event-loop thread:

```python
import signal
import threading

import jasil.jobs.service as jobs_service

stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())

jobs_service.run_job_worker(
    queues=("campaign", "intake"),
    role="domain-worker",
    label="campaign-intake-1",
    metadata={"zone": "eu-1"},
    stop=stop,
)
```

Pass one queue per process group when each queue needs independent concurrency.
Run several processes with the same queue to get competing consumers; PostgreSQL
claims remain `FOR UPDATE SKIP LOCKED`, so each job is processed once per claim.
Omit `queues` to consume all queues, and never pass an empty allowlist.

During a rolling upgrade, an old relay can still write the database-side
`default` queue. A selective new worker also adopts those legacy rows when the
subscriber is now registered to one of its selected queues. It does not adopt
rows belonging to a subscriber that is genuinely registered to `default`, so
queue isolation remains intact while old writers drain. Conversely, a selective
`default` worker excludes legacy rows for subscribers now assigned to a named
queue; the two workers cannot race for the same compatibility row.

The relay and reaper may run on every API replica. Their locking and the
idempotent `(event_id, subscriber_id)` fan-out make a single-runner lock
unnecessary. Durable jobs use the database only; Redis is not a job-queue
dependency.

## Dead letters

Dead-lettered jobs are rare and human-actionable, so they are never pruned. Once
the cause is fixed:

```python
import jasil.admin as jasil_admin

jasil_admin.replay_dead_letter_job(job_id)
```

which returns the job to `pending` with a fresh attempt budget.

`jasil_admin.get_jobs_summary()` gives counts by status and queue, recent
throughput, backlog age, and the dead-letter list for an operations dashboard.
`jasil_admin.get_workers_summary()` reports running, stale, and gracefully
stopped worker instances one bounded page at a time; pass its `next_cursor` into
the next call. See [Observability](observability.md#worker-registry).

!!! note "Why `jasil.admin` and not `jasil.jobs.crud`"
    The CRUD layer is where these queries live, but it is the wrong thing to wire
    a route to: it reaches a model at import time (so it cannot be imported
    before `map_models` has run) and every function commits the session it is
    given. `jasil.admin` is importable from anywhere and takes no session, so an
    admin route cannot accidentally hand JASIL its own open transaction.

## Tuning

| Setting | Raise it when | Lower it when |
|---|---|---|
| `batch_size` | Throughput is the bottleneck | Individual jobs are slow or heavy |
| `lease_seconds` | Jobs legitimately run long | Crashed workers strand work too long |
| `max_attempts` | Failures are usually transient | Failures are usually permanent |
| `backoff_base_seconds` | The dependency needs time to recover | Retries should be prompt |
| `poll_interval_seconds` | The queue is usually empty | Latency matters |
| `heartbeat_interval_seconds` | Telemetry write volume matters | Faster stale detection matters |

`lease_seconds` must exceed the slowest realistic job duration. If a lease
expires while a job is still running, the reaper requeues it and it runs twice
concurrently — which your handler must tolerate anyway, but is wasteful. A late
worker finalizes with its worker id and attempt generation, so it cannot overwrite
the replacement worker's newer claim.

Queue rotation is intentionally in-memory and resets when a worker process
restarts. It prevents starvation across batches in a healthy process; it is not
a global scheduling guarantee across repeated crashes or deployments. Use
queue-specific process groups when queues require independent capacity.
