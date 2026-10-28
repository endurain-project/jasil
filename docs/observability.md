# Observability

JASIL owns five operational tables — and four more for the [audit trail](audit.md) — every one named with a `jasil_` prefix so none can collide with a table of your own. The event/outbox/job tables are append-only;
the worker registry updates one row per restart-unique instance. This page covers
what goes in them, how to create them, and how to bound their growth.

| Table | Written by | Purpose |
|---|---|---|
| `jasil_event_log` | The bus and the publish facade | One row per event, recording its lifecycle. |
| `jasil_event_outbox` | The publish facade | Staged events awaiting relay. |
| `jasil_processing_jobs` | The relay and the worker | One row per `(event, subscriber)`. |
| `jasil_job_workers` | Durable workers | Identity, queue selection, heartbeat, stop time, and metadata. |
| `jasil_job_attempts` | Durable workers | One row per attempt to run a job. See [Job attempts](#job-attempts). |

The audit trail's `jasil_audit_log`, `jasil_audit_actions`, `jasil_audit_checkpoints` and `jasil_instance` are covered in [Audit trail](audit.md); they share the migrations below and are *not* pruned by `jasil.retention`.

## The event log

```python
jasil_settings.configure(
    jasil_settings.JasilSettings(
        event_log=jasil_settings.EventLogSettings(enabled=True),
    )
)
```

Off by default, because it writes to your database.

Each row records what was published, whether it was processed, by which worker,
how long it took, and why it failed:

| Status | Meaning |
|---|---|
| `published` | Written, dispatch not finished. |
| `queued` | Handed to the durable job queue (terminal here; execution is tracked in `jasil_processing_jobs` and `jasil_job_attempts`). |
| `processing` | A consumer picked it up. |
| `completed` | Handlers succeeded. |
| `failed` | A handler raised. |
| `dead_letter` | Abandoned after retries. |

!!! note "Best-effort by design"
    Every write opens its own short-lived session and swallows any storage error.
    A database hiccup degrades observability rather than breaking event
    processing — the trail is never the source of truth.

Because the durable route bypasses the bus, an event handed to the outbox is
recorded `queued` explicitly. Otherwise the dashboard would go dark exactly when
durable jobs are switched on.

!!! note "A durable event stays `queued`"
    The event log is a publication log: one row per event, written when it is
    published. A durable event fans out to one job per subscriber, on different
    queues and workers, so there is no single status its row could honestly hold,
    and every job finishing would update the same row. Its outcome is the state
    of those jobs: call `jasil.admin.get_event_jobs(event_id)` to see, for each
    subscriber, which worker ran it and how it ended.

`jasil.admin.get_event_log_summary()` gives counts by type and status,
recent failures, and throughput over a window. It is the counterpart of
`jasil.admin.get_jobs_summary()`, and like it takes no session — see
[durable jobs](durable-jobs.md#dead-letters).

## Worker registry

JASIL owns worker identity and persistence because it already owns claims,
sessions, clocks, and worker start/stop. Each in-process or standalone worker
gets a UUIDv4 instance id on startup and records:

- start, latest heartbeat, and graceful-stop timestamps;
- its selected queues (`null` means all queues);
- optional host-supplied role, label, and neutral metadata, capped at 16 KiB of
    serialized JSON.

Public summaries derive active claim counts directly from `jasil_processing_jobs`, so
reaping a crashed worker cannot leave phantom load on the dashboard.

A dedicated heartbeat thread beats every `heartbeat_interval_seconds`, not on
every empty poll, and keeps running while a handler is blocked. Each beat does two
independent things: it renews the leases of the jobs the worker holds, and it
refreshes the worker's registry row. One signal therefore means both "this worker
is alive" and "this worker still holds these jobs", so a worker reported `stale`
is one whose leases are about to expire. The registry write is best-effort: a
database error is logged and job execution continues.

```python
import jasil.admin as jasil_admin

workers = jasil_admin.get_workers_summary(limit=100)
next_page = jasil_admin.get_workers_summary(limit=100, cursor=workers.next_cursor)
queues = jasil_admin.get_jobs_summary().by_queue
```

Queue counts use the requested summary window. Backlog age considers every
currently pending or claimed job, so a queue with only older unfinished work
still appears with zero windowed jobs and a non-null `oldest_pending_seconds`.

The admin facade uses synchronous database sessions. A synchronous framework
route may call it directly; an asynchronous route must run it through the
framework's thread-pool helper so database I/O does not block the event loop.

Status is derived when read:

| Status | Meaning |
|---|---|
| `running` | No graceful stop and heartbeat age is within the threshold. |
| `stale` | No graceful stop and heartbeat age exceeds the threshold. |
| `stopped` | The worker recorded a graceful stop. |

The default stale threshold is three configured heartbeat intervals; an
operator can pass `stale_after_seconds=` explicitly. Status totals cover every
retained worker, while `workers` is a cursor-paginated page of 100 by default and
500 at most. This is telemetry, not a health policy. The host owns HTTP routes,
authentication, authorization, UI, alerts, and whether any worker state affects
a container health endpoint. JASIL ships no route or UI and never changes host
health automatically.

## Job attempts

`jasil_processing_jobs` says where a job is *now*. `jasil_job_attempts` keeps what happened:
one row for every attempt to run it. A worker opens the row in the transaction
that claims the job, and whichever path ends that claim closes it in the same
transaction as the job's own state change, so the history can never disagree with
the job.

| Outcome | Meaning |
|---|---|
| `completed` | The handler succeeded and this claim recorded it. |
| `failed` | The handler raised; the job was rescheduled with backoff. |
| `deferred` | The handler raised `DeferJob`: the job was rescheduled for its `retry_at` and the attempt was not consumed. Not a failure, so alert on `failed`, not on this. |
| `dead_letter` | The handler raised on the last attempt. |
| `lease_expired` | The lease ran out with no result: the worker died or went silent. |
| `superseded` | The handler finished **after** its job had been taken: it ran twice. |
| `released` | Claimed but never run; returned to `pending` with the attempt refunded. |
| `abandoned` | Exceeded `max_runtime_seconds`; signalled and left to the reaper. |

`superseded` is the one to alert on. It is the only durable record that a side
effect (an email) may have happened twice, and it keeps the late handler's
duration and error. Each row records the job, event, subscriber and queue (copied,
so statistics survive the job being pruned), the attempt number, the worker
instance, and when it was claimed, started and finished. `duration_ms` is the
handler's own time, from the monotonic clock. A replayed dead letter starts again
at attempt 1; order history by claim time.

```python
import jasil.admin as jasil_admin

job = jasil_admin.get_job(job_id)  # the job and its attempts, newest first
for attempt in job.history:
    print(attempt.attempt, attempt.worker_id, attempt.outcome, attempt.duration_ms, attempt.error)

event = jasil_admin.get_event_jobs(event_id)  # every subscriber's job for one event
page = jasil_admin.get_worker_attempts(instance_id, limit=100)  # what one worker has done
more = jasil_admin.get_worker_attempts(instance_id, cursor=page.next_cursor)
```

The summaries carry it too. `get_jobs_summary()` adds, per subscriber and queue,
`avg_duration_ms`, `max_duration_ms`, `duration_buckets` and `superseded` (over
the window), and each dead letter's `last_worker_id`. `get_workers_summary()`
adds each worker's `completed`, `failed`, `dead_lettered`, `lease_expired`, `superseded`,
`released` and `abandoned` counts, `last_finished_at`, and `claimed_by_stale_workers`: jobs
waiting out a lease behind a worker that stopped beating, which is the number to
watch during a crash.

**Duration buckets.** An average hides a slow tail and a maximum shows only that
it exists, so completed attempts are also counted in six fixed ranges:

| Field | Range |
|---|---|
| `under_100ms` | below 100 ms |
| `under_1s` | 100 ms up to 1 s |
| `under_10s` | 1 s up to 10 s |
| `under_1m` | 10 s up to 1 min |
| `under_5m` | 1 min up to 5 min |
| `over_5m` | 5 min and over |

The boundaries are fixed, so two deployments' dashboards are comparable, and they
sit near the 90-second SMTP deadline and the default 5-minute lease. There are no
percentiles: they are not portable SQL across SQLite, PostgreSQL and MySQL/MariaDB.
A host that needs them computes them from the `duration_ms` log field.

**Turning it off.** `jobs.record_attempts` (on by default) can be set to `False`
for a very high-volume deployment. Nothing is written then and job processing is
unaffected, and the reads say so: attempt-derived summary fields are `None`
rather than `0`, and `attempts_recorded` is `False`, so "not recorded" is never
mistaken for "nothing happened". The reaper still closes any attempt left open
from before the switch.

**Metrics.** JASIL exports none. The admin reads are bounded and importable
anywhere, so a host maps them into its own metrics system (for example
`superseded` and `claimed_by_stale_workers` as alerts) from a scheduled job or a
metrics route.

## Migrations

The tables ship as packaged Alembic revisions behind the `migrations` extra:

```python
import jasil.orm as jasil_orm
from jasil import migrations

jasil_orm.map_models(Base)  # the metadata must exist first
migrations.adopt_existing_schema(engine)  # validate legacy unversioned tables
migrations.upgrade(engine)  # create or upgrade JASIL's tables
migrations.verify_schema_current(engine)  # fail fast if not migrated
```

| Function | Use |
|---|---|
| `upgrade(engine)` | Create or upgrade to head. Run at deploy time. |
| `downgrade(engine, "base")` | Drop JASIL's tables. |
| `adopt_existing_schema(engine)` | Validate and adopt unversioned JASIL tables at head or an earlier revision. |
| `head_revision()` | The newest revision shipped in this package. |
| `db_revision(engine)` | What the database currently records. |
| `verify_schema_current(engine)` | Raise unless the database is at head. |

`verify_schema_current` is a useful fail-fast at startup — it turns "forgot to
migrate" into a clear message at boot rather than a confusing query error later.

!!! note "Revision `rev0006` (0.14): the audit trail"
    Adds `jasil_audit_log`, `jasil_audit_actions`, `jasil_audit_checkpoints` and `jasil_instance`. On PostgreSQL
    (13 or newer) it also partitions the log by day, installs the append-only triggers and creates the two
    `SECURITY DEFINER` functions, so run it as the role that should own them. Processes of the previous release
    are unaffected. Details in [Audit trail → Upgrading](audit.md#upgrading).

!!! note "It will not touch your tables"
    JASIL's migrations use their own version table, `jasil_alembic_version`, so
    they never collide with your Alembic history. Every operation is scoped to
    JASIL's own tables, so autogenerate cannot propose dropping yours — even
    though both live in the same registry.

### Upgrading from 0.8 or earlier: the `jasil_` prefix

Up to 0.8 the tables had no prefix (`event_log`, `event_outbox`,
`processing_jobs`, `job_workers`, `job_attempts`). Revision `rev0003` renames each
table, its indexes and its unique constraint to the `jasil_` names. No column or
row changes.

- **Stop every process running 0.8 or earlier before `upgrade()`.** The old code
  looks the old names up, so a rolling deployment across this revision is not
  possible. This is the one revision that is not forward-compatible with the
  previous release.
- PostgreSQL renames in place and is instant. SQLite and MySQL rename the table
  and recreate its indexes; on SQLite the table carrying the unique constraint is
  rebuilt.
- A **fresh** database is created with the final names directly, so a host that
  already has a table called `event_log` of its own can install JASIL. The rename
  only ever touches an unprefixed table when no prefixed one exists.
- `adopt_existing_schema()` recognises an unversioned pre-0.9 schema (at least two
  of `event_log`, `event_outbox` and `processing_jobs`) and adopts it at its old
  revision; `upgrade()` then renames it. A lone unprefixed `event_log` is taken to
  be yours and is left alone.
- Anything of yours that names the old tables (a reporting query, a dashboard)
  must be updated. Downgrading `rev0003` restores the old names, and fails if a
  table of yours has since taken one.

### Adopting existing tables

`adopt_existing_schema()` is only for a database where JASIL's tables already
exist but `jasil_alembic_version` does not record a revision. It compares their
columns, types, nullability, primary keys, unique constraints, and required
indexes with the schema of the installed migration head. If that does not match
exactly, it tries each earlier revision's schema, newest first, and records the
first exact match (the newest, if two describe the same schema). It returns
`True` only when this call recorded a revision. PostgreSQL adoption also
requires the metadata GIN index and its PostgreSQL-specific definition.

Adopting an older revision leaves the database behind head, so always call
`upgrade()` next: it applies the revisions after the recorded one. That is how a
host whose own migrations created the tables at an earlier JASIL revision (for
example `event_log`, `event_outbox` and `processing_jobs` in the `rev0001`
shape) catches up without copying JASIL's DDL into its own history.

An empty database is left unversioned for `upgrade()` to create. A schema that
matches no revision raises `SchemaCompatibilityError` without changing it or
recording a revision; the message names the closest revision and lists its
differences. Unexpected tables and columns are incompatible; additional ordinary
non-unique indexes are allowed because they do not change JASIL's write
contract. JASIL never repairs an incompatible schema automatically.

!!! note "Tables your host created itself"
    Create them in exactly the shape of a revision JASIL ships (same columns,
    types, nullability, primary keys, unique constraints and index names), at
    that revision or at head. If your host evolved them differently, migrate
    them to a shipped revision's shape in your own history first; adoption never
    guesses and there is no public way to stamp an arbitrary revision. A host
    that copied a later revision's DDL by hand can delete that copy: adoption
    plus `upgrade()` now applies it.

Do not guess or hardcode a JASIL revision identifier. The adoption API owns the
shipped revisions and all expected schema details. Calling it again after a
successful adoption is a no-op.

### Prefer one unified history?

Point your own `env.py` at your `Base.metadata` and add JASIL's `versions`
directory to your `version_locations`. The self-contained runner above needs no
host wiring, but it is not mandatory.

One limit of the unified history: only `jasil.migrations.upgrade()` creates a
fresh database with the `jasil_` names directly. Run from your own history,
`rev0001` creates the unprefixed tables and `rev0003` renames them, so a table of
yours called `event_log` (or another old name) still collides on a fresh
install. Create JASIL's tables with `upgrade()` first if that applies to you.

## Retention

The append-only rows and retained worker instances grow without bound unless
pruned. Pruning runs on a schedule:

```python
jasil_settings.configure(
    jasil_settings.JasilSettings(
        event_log=jasil_settings.EventLogSettings(retention_days=30),
        jobs=jasil_settings.JobSettings(retention_days=30),
    )
)
```

```python
import jasil.retention as jasil_retention

jasil_retention.schedule_retention_maintenance(scheduler)
```

That registers a daily prune on your APScheduler instance and runs one pass as
the scheduler starts — without the startup pass, a daily interval means a process
that is redeployed daily never prunes at all. Pass `run_at_startup=False` to skip
it, or call `jasil_retention.prune_expired_records()` yourself if you schedule
work some other way.

Register it on **every** replica, like the durable-job maintenance. The two are
separate calls because retention also prunes the `jasil_event_log`, so it applies to a
deployment that never enabled durable jobs.

The two windows are independent, and `<= 0` disables either.

### What is deleted, and what never is

| Deleted | Kept |
|---|---|
| Any `jasil_event_log` row past the window | Unrelayed `jasil_event_outbox` rows — pending work |
| Relayed `jasil_event_outbox` rows | `pending` / `claimed` jobs — in flight |
| `completed` jobs | `dead_letter` jobs — human-actionable |
| Stopped or stale worker rows past job retention | Recent/running worker rows |
| `jasil_job_attempts` past the window, and orphans of pruned jobs | Attempts of `pending`, `claimed` and `dead_letter` jobs |

Every `jasil_event_log` row is prunable regardless of status: it is a safe-to-lose
observability trail, and nothing in it is a source of truth. The job tables are
the opposite — pruning an in-flight row would silently drop derived work, so
status is checked on every delete.

Deletes run in **bounded batches**, each committed separately, so a prune pass
never holds locks on a hot table long enough to block the relay or a worker.
There is also a cap on batches per pass; a pathological backlog is drained across
several passes rather than blocking the scheduler.

Pruning takes the platform lock, so only one replica does the work — the deletes
are idempotent, but duplicating them is pointless load.

## Logging

JASIL uses `logging.getLogger(__name__)` throughout — no configuration, no
adapter. Records propagate to whatever handlers you have set up, and structured
fields arrive via `extra`:

```python
{"event_type": ..., "event_id": ..., "subscriber": ..., "event_metadata": {...}}
```

Every job-outcome record from `jasil.jobs.runner` also carries `job_id`,
`worker_id`, `queue`, `attempts` and, once the handler has run, `duration_ms`,
so a log line names the worker that ran the job. The reaper's "Reaped N expired
job lease(s)" warning carries `workers`, a `{instance id: count}` breakdown, so a
crash names the worker that went silent. The keys are not covered by SemVer, but
alerts are built on them, so they are listed here.

Loggers are named after their module (`jasil.publisher`, `jasil.jobs.runner`,
`jasil._core.network`, …), so you can raise or lower verbosity per subsystem.

One worth routing deliberately: `jasil._core.network` logs at INFO every time the
SSRF allowlist permits a private destination. That is an audit trail — it tells
you what the exception is being used for.
