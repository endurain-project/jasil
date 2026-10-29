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
 jasil_event_outbox ─────relay─────▶ jasil_processing_jobs ─────worker─────▶ subscriber
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

The handler must **raise** on failure. That is what drives retry. Returning
normally completes the job. A handler that is only *waiting* (a rate limit that
clears in a few minutes) raises [`DeferJob`](#deferring-a-job) instead, which
reschedules the job without failing it.

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
letters, digits, `.`, `_`, and `-`, starting with a letter or a digit; an invalid
name raises before persistence. Registering a `subscriber_id` again replaces its
handler, queue and callback, and moves it to the new event type if it was
registered under another one.

### Attempt counts

A durable handler's `Event` carries the attempt it is running, computed from the
job row by the same comparison that dead-letters the job:

| Field | Meaning |
|---|---|
| `event.attempt` | The 1-based attempt: `1` on the first run. Equal to `event.retry_count` inside a durable handler. `None` outside one. |
| `event.max_attempts` | The ceiling stamped on the job when it was enqueued. A later change to the `max_attempts` setting does not alter jobs already queued. |
| `event.is_last_attempt` | `True` when a failure now dead-letters the job (`attempt >= max_attempts`). |
| `event.deferral_count` | How many earlier runs of this job the handler [deferred](#deferring-a-job). They are not attempts, so they are not in `attempt` or `retry_count`. `0` on a first run. |

Use `event.is_last_attempt` rather than comparing `retry_count` with a setting.
A replay (`replay_dead_letter_job`) restarts both counts.

### Deferring a job

A handler that meets a transient, time-bounded condition can say *when* to run
again instead of failing. Raise `DeferJob`:

```python
from datetime import UTC, datetime, timedelta

from jasil.jobs import DeferJob


def send_digest(event):
    try:
        provider.send(event.payload)
    except ProviderRateLimited as limited:
        # The provider says its limit clears in N seconds.
        raise DeferJob(
            datetime.now(UTC) + timedelta(seconds=limited.retry_after),
            reason=f"provider rate limited for {limited.retry_after}s",
        ) from limited
```

It is **not a failure**. The runner returns the job to `pending`, available from
`retry_at`, with its lease cleared and `last_error` set to `reason`. The claim
does **not** count against `max_attempts`, so a job may be deferred more often
than it has attempts, and a deferral never dead-letters or runs `on_dead_letter`.
It is recorded as a `deferred` [attempt](observability.md#job-attempts) and logged
at `INFO` (`retry_at`, `reason` and `deferrals` in the log context), never `ERROR`.

Guard rails, so a bad value cannot park or spin a job:

- `retry_at` is clamped to between now and `jobs.defer_max_seconds` ahead
  (default 86400). A naive `datetime` is read as UTC.
- Up to 10% of the delay is added as jitter, so jobs deferred to the same instant
  do not wake together. The ceiling is applied after the jitter.
- After `jobs.max_deferrals` deferrals (default 100) the next `DeferJob` is
  treated as an ordinary failure, with the usual backoff and dead-lettering, so a
  job cannot defer forever. `0` disables deferral.
- Like any write by the runner it applies only while the worker still owns the
  claim; a lost claim is logged and changes nothing.

The handler's side effects before it raises are not rolled back, so the handler
stays idempotent, as for any retry. Do not sleep in a handler to wait out a
condition: that holds a lease and a worker slot. Defer instead.

### Reacting to a dead letter

A handler that drives user-visible state, such as an import that shows as "queued",
needs that state to reach a terminal value when JASIL gives up on the job. Register
an `on_dead_letter` callback beside the handler:

```python
import jasil.jobs.registry as jobs_registry


def import_profile(event):
    run_import(event.payload["import_id"])  # raises on failure


def close_import(event, error):
    # Idempotent: set a terminal state, never append or increment.
    with session_factory() as db:
        record = db.get(ProfileImport, event.payload["import_id"])
        if record is not None and record.state not in {"done", "failed"}:
            record.state = "failed"
            record.detail = error or "gave up"
            db.commit()


jobs_registry.registry.register(
    "profile.import.requested",
    "profile.import",
    import_profile,
    on_dead_letter=close_import,
)
```

`callback(event, error)` is called after the job has become `dead_letter`,
whichever way it got there: the handler raised on its last attempt, the lease
expired on the last attempt, or the reaper recovered a worker that died.
`event` is the job's envelope with its final `retry_count` and `max_attempts`;
`error` is the stored last failure reason (`str | None`); for an exception it is
`"<ExceptionType>: <message>"`, so it names the failure even when the message is empty.

* **At least once.** Delivery is recorded in `dead_letter_notified_at` only after the
  callback returns. A crash, or a callback that raises, leaves the callback owed and a
  later reaper pass calls it again, after a backoff that starts at 60 seconds and
  doubles per failure up to an hour. Each delivery first claims the job with a
  5-minute lease, so a worker and a reaper (or two reapers) do not run it together.
  The count and the last failure are kept in `dead_letter_attempts` and
  `dead_letter_callback_error`. **Make the callback idempotent.**
* **After the transition, outside the handler.** It runs in the worker or reaper
  process, after the dead-letter is committed and in no session of the failed handler.
  An exception from it is logged and swallowed; the job stays `dead_letter`.
* **It runs inline.** The worker (or reaper) waits for it, after the failure has been
  logged, so keep it short and give any network call its own timeout. A callback that
  outlasts its 5-minute lease may be started a second time by another process.
* **Check your own state.** A handler that finishes after its lease was taken still
  writes its result, but the job stays `dead_letter`. The callback may then run for
  work that succeeded, so read the app's record before overwriting it.
* **Replay.** `replay_dead_letter_job` clears the record, and the callback runs
  again only if the job dead-letters again.
* **Register it where the reaper runs.** The callback lives in the process-local
  registry. A process that did not register it never delivers or consumes the
  notification, so register subscribers, callbacks included, in every process that
  runs a worker or the reaper. A job whose subscriber has no callback in the process
  doing the sweep is skipped without a log line.
* **Dead letters before this feature** are not called back; the migration marks them
  as delivered.

A subscriber registered without `on_dead_letter` behaves exactly as before.

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

!!! warning "Moving a subscriber to another queue"
    A job keeps the queue it was enqueued on. Only rows carrying the database
    default `default` queue follow their subscriber to its currently assigned
    queue. If a subscriber moves from queue `a` to `b` and no worker still selects
    `a`, its pending rows on `a` wait forever. Keep a worker on the old queue until
    it drains, or reassign the rows once:
    `UPDATE jasil_processing_jobs SET queue = 'b' WHERE queue = 'a' AND subscriber_id = '<id>' AND status = 'pending'`.

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

Only real attempts count. A run the handler [deferred](#deferring-a-job) with
`DeferJob` is neither a failure nor an attempt: `jasil_processing_jobs.attempts`
still counts every claim (it identifies the claim), `deferrals` counts the
deferred ones, and the budget spent against `max_attempts`, and the exponent of
the backoff above, is `attempts - deferrals`. The reaper uses the same budget.

### Lease reclamation

A worker that dies holds its lease until it expires. The reaper returns those
jobs to `pending` — or dead-letters them if they have no attempts left — so work
is never stranded by a crash.

A *running* worker does not lose its leases. Its heartbeat renews every lease it
holds, the running job's and the waiting jobs' of its batch, on each beat, so
`lease_seconds` is how long a worker may go **silent**, not how long a handler may
run. See [Long-running handlers](#long-running-handlers).

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
topology: one process and one in-process consumer. JASIL refuses to start a
consumer on SQLite when the configured deployment is multi-process — several
`web_workers`, `profile=distributed`, or `job_worker_mode=standalone` — because it
has enough information to know those shapes are unsafe. `start_job_worker()`
requires `job_worker_mode=in_process`, which is the default.

### Distributed: API plumbing and PostgreSQL workers

The API process schedules the relay and reaper but does not start a domain-job
consumer:

```python
jobs_service.schedule_job_maintenance(scheduler)
```

Declare the shape with `job_worker_mode=standalone`, and load it in the API
processes as well as the workers: a worker is one more process reaching state,
events, the lock and mail, and the API's own startup checks can only refuse a
wiring the workers cannot share if it knows they exist. Standalone worker
processes then use the supported blocking entry point, which requires that mode.
It must run in a dedicated worker process, never on an API request or event-loop
thread:

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

Both rules are decided from the registrations of the worker process that makes
the claim, so **every worker process must register every durable subscriber**,
even those it will not consume, before it starts. A worker that has not
registered a subscriber cannot tell that a legacy `default` row belongs to
another queue, and will claim it, find no handler, and dead-letter it. Routing
reads the registrations on every claim, but the one-time retag of legacy rows
runs on the worker's first batch, so register subscribers before starting the
worker. Alternatively, let the `default` queue drain before rolling out.

The relay and reaper may run on every API replica. Their locking and the
idempotent `(event_id, subscriber_id)` fan-out make a single-runner lock
unnecessary. The relay fans an event out to the durable subscribers registered
**in its own process**, so register every durable subscriber in each process
that runs it, the API processes included. An event it finds no subscriber for is
marked relayed with no jobs and logged as a warning. Durable jobs use the database only; Redis is not a job-queue
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
throughput, backlog age, handler durations, and the dead-letter list (each with
the worker that ran its last attempt) for an operations dashboard.
`jasil_admin.get_job(job_id)` returns a dead letter's whole history: which worker
ran each attempt, how long it took, and why it failed.
`jasil_admin.get_workers_summary()` reports running, stale, and gracefully
stopped worker instances one bounded page at a time; pass its `next_cursor` into
the next call. See [Observability](observability.md#worker-registry).

!!! note "Why `jasil.admin` and not `jasil.jobs.crud`"
    The CRUD layer is where these queries live, but it is the wrong thing to wire
    a route to: it reaches a model at import time (so it cannot be imported
    before `map_models` has run) and every function commits the session it is
    given. `jasil.admin` is importable from anywhere and takes no session, so an
    admin route cannot accidentally hand JASIL its own open transaction.

## Sending mail

There is no separate mail queue: durable jobs are the queue. Publish an event,
and have a durable subscriber call `platform.mail.send(...)`. A raised
`MailError` gets backoff, retries and the dead-letter queue like any other
failure.

- **Give mail its own named queue.** Register the mail subscriber with
  `queue="mail"` (or similar), so a slow relay cannot hold up other subscribers'
  jobs. The isolation comes from a dedicated worker group calling
  `run_job_worker(queues=("mail",))`: the single local consumer still runs mail
  serially with everything else.
- **Mail needs a real relay once workers are standalone.** The handler runs in
  the worker process, so that process's `mail.uri` is the one that sends.
  `memory://` there would record every message in that process's memory and
  complete the job with nothing delivered. With
  [`job_worker_mode=standalone`](deployment-profiles.md#job-workers-are-processes-too)
  declared, the consistency checks refuse that wiring at startup, in the API and
  in the workers.
- **An unconfigured mail backend is retried too.** `MailNotConfiguredError` is a
  `MailError`, so a job on a deployment with no `mail.uri` uses its whole retry
  budget before dead-lettering. Catch it in the handler if that is not wanted.
- **Delivery is at-least-once.** A retry after a lost acknowledgement sends the
  message twice.
- **Size `jobs.max_runtime_seconds` for the whole handler, not one send.** One
  SMTP send can take up to 90 seconds, and the DNS lookups before it are not
  counted against that deadline. A handler that sends N messages can take N times
  that. The worker's heartbeat keeps the job's lease alive while it runs, so the
  lease itself no longer needs to be that long; the cap is
  `max_runtime_seconds` (default 30 minutes). Past it the job is abandoned and a
  retry may deliver the same message again.
- **A lost lease stops the send.** The mail backends check
  [`jasil.jobs.lease_lost()`](#long-running-handlers) before connecting, so a
  handler that loops over recipients does not keep sending after its job has moved
  to another worker. Give a dedicated mail worker group a small `jobs.batch_size`,
  so one process does not hold claimed messages while the rest of the group sits
  idle.
- **Do not put secrets or rendered bodies in the event payload.** Payloads are
  stored verbatim and dead-lettered rows are never pruned. Carry identifiers and
  build the message inside the handler.

## Tuning

| Setting | Raise it when | Lower it when |
|---|---|---|
| `batch_size` | Throughput is the bottleneck | Individual jobs are slow or heavy |
| `lease_seconds` | Heartbeats are slow or the database is flaky | Crashed workers strand work too long |
| `max_runtime_seconds` | Handlers legitimately run for a long time | A hung handler should be abandoned sooner |
| `max_attempts` | Failures are usually transient | Failures are usually permanent |
| `backoff_base_seconds` | The dependency needs time to recover | Retries should be prompt |
| `poll_interval_seconds` | The queue is usually empty | Latency matters |
| `heartbeat_interval_seconds` | Heartbeat write volume matters | Faster stale detection, or a shorter lease, matters |

`lease_seconds` is how long a worker may go silent. One heartbeat renews leases
and proves the worker alive, so it must be at least **three**
`heartbeat_interval_seconds` (two missed beats must not expire a lease), and
`max_runtime_seconds` must not be below it; `build_runner` refuses anything else
at startup. A worker is therefore reported stale no later than its leases start
expiring.

A late worker finalizes with its worker id and attempt generation, so even a
handler that outlives its lease cannot overwrite the replacement worker's newer
claim.

### Long-running handlers

A handler may run far longer than `lease_seconds`. While it does, the worker's
heartbeat renews the job's lease, so the reaper leaves it alone and it is not run
twice. Three things bound that:

- **`max_runtime_seconds`.** Past it the job is **abandoned**: it is no longer
  renewed, the handler is signalled, an `ERROR` is logged, and the rest of the
  worker's batch is released back to the queue (no attempt is spent on jobs that
  never started) instead of waiting behind a worker that is stuck. Python cannot
  kill a thread, so a truly hung handler still occupies its worker; the log is the
  operator's signal.
- **Self-fencing.** If renewals fail for almost a whole lease (the database is
  unreachable), the handler is signalled *before* another worker can have
  reclaimed the job.
- **A stolen lease.** If the job is found to belong to another worker, the handler
  is signalled and its result is discarded.

An abandoned job is signalled but not fenced: if the handler returns while this
worker still holds the claim, its result is recorded as usual (a mail send made
after the signal raises `MailError`, so that job is retried with backoff).

The signal is cooperative. A handler that loops, or does several side effects,
should check it between units of work and return early:

```python
import jasil.jobs


def send_digest(event):
    for recipient in recipients_of(event):
        if jasil.jobs.lease_lost():
            return  # another worker owns this job now
        platform.mail.send(build_message(recipient))
```

`lease_lost()` is `False` outside a durable job. The mail backends check it
themselves. A `ContextVar` does not follow work a handler hands to its own
threads unless it copies the context (`contextvars.copy_context()`).

Delivery remains at-least-once: a handler that ignores the signal mid-send, or a
database partition, can still produce a duplicate side effect, so handlers must
stay idempotent.

Queue rotation is intentionally in-memory and resets when a worker process
restarts. It prevents starvation across batches in a healthy process; it is not
a global scheduling guarantee across repeated crashes or deployments. Use
queue-specific process groups when queues require independent capacity.
