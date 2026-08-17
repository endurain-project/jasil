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

## Running the workers

```python
import jasil.jobs.service as jobs_service

jobs_service.start_job_worker()  # in-process worker thread
jobs_service.register_scheduled_jobs(scheduler)  # relay + reaper on APScheduler
```

The relay and the reaper run on **every** replica. Both use `SKIP LOCKED`, and
the fan-out is idempotent, so no single-runner lock is needed.

## Dead letters

Dead-lettered jobs are rare and human-actionable, so they are never pruned. Once
the cause is fixed:

```python
import jasil.jobs.crud as jobs_crud

jobs_crud.replay_dead_letter_job(job_id, now=clock.now(), db=db)
```

which returns the job to `pending` with a fresh attempt budget.

`jobs_crud.get_jobs_summary(db)` gives counts by status, recent throughput, and
the dead-letter list, for an operations dashboard.

## Tuning

| Setting | Raise it when | Lower it when |
|---|---|---|
| `batch_size` | Throughput is the bottleneck | Individual jobs are slow or heavy |
| `lease_seconds` | Jobs legitimately run long | Crashed workers strand work too long |
| `max_attempts` | Failures are usually transient | Failures are usually permanent |
| `backoff_base_seconds` | The dependency needs time to recover | Retries should be prompt |
| `poll_interval_seconds` | The queue is usually empty | Latency matters |

`lease_seconds` must exceed the slowest realistic job duration. If a lease
expires while a job is still running, the reaper requeues it and it runs twice
concurrently — which your handler must tolerate anyway, but is wasteful.
