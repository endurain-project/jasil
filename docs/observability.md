# Observability

JASIL owns four operational tables. The event/outbox/job tables are append-only;
the worker registry updates one row per restart-unique instance. This page covers
what goes in them, how to create them, and how to bound their growth.

| Table | Written by | Purpose |
|---|---|---|
| `event_log` | The bus and the publish facade | One row per event, recording its lifecycle. |
| `event_outbox` | The publish facade | Staged events awaiting relay. |
| `processing_jobs` | The relay and the worker | One row per `(event, subscriber)`. |
| `job_workers` | Durable workers | Identity, queue selection, heartbeat, stop time, and metadata. |

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
| `queued` | Handed to the durable job queue (terminal here; execution is tracked in `processing_jobs`). |
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

Public summaries derive active claim counts directly from `processing_jobs`, so
reaping a crashed worker cannot leave phantom load on the dashboard.

A dedicated heartbeat thread writes at `heartbeat_interval_seconds`, not on
every empty poll, and keeps running while a handler is blocked. Heartbeat writes
are best-effort: a database error is logged and job execution continues.

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

## Migrations

The tables ship as packaged Alembic revisions behind the `migrations` extra:

```python
import jasil.orm as jasil_orm
from jasil import migrations

jasil_orm.map_models(Base)  # the metadata must exist first
migrations.upgrade(engine)  # create or upgrade JASIL's tables
```

| Function | Use |
|---|---|
| `upgrade(engine)` | Create or upgrade to head. Run at deploy time. |
| `downgrade(engine, "base")` | Drop JASIL's tables. |
| `stamp(engine)` | Mark an existing database as at head, without running anything. |
| `head_revision()` | The newest revision shipped in this package. |
| `db_revision(engine)` | What the database currently records. |
| `verify_schema_current(engine)` | Raise unless the database is at head. |

`verify_schema_current` is a useful fail-fast at startup — it turns "forgot to
migrate" into a clear message at boot rather than a confusing query error later.

!!! note "It will not touch your tables"
    JASIL's migrations use their own version table, `jasil_alembic_version`, so
    they never collide with your Alembic history. Every operation is scoped to
    JASIL's four tables, so autogenerate cannot propose dropping yours — even
    though both live in the same registry.

### Already created the tables with `create_all`?

```python
migrations.stamp(engine)
```

This records head without running the baseline, so future `upgrade()` calls
apply only genuinely new revisions.

### Prefer one unified history?

Point your own `env.py` at your `Base.metadata` and add JASIL's `versions`
directory to your `version_locations`. The self-contained runner above needs no
host wiring, but it is not mandatory.

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
separate calls because retention also prunes the `event_log`, so it applies to a
deployment that never enabled durable jobs.

The two windows are independent, and `<= 0` disables either.

### What is deleted, and what never is

| Deleted | Kept |
|---|---|
| Any `event_log` row past the window | Unrelayed `event_outbox` rows — pending work |
| Relayed `event_outbox` rows | `pending` / `claimed` jobs — in flight |
| `completed` jobs | `dead_letter` jobs — human-actionable |
| Stopped or stale worker rows past job retention | Recent/running worker rows |

Every `event_log` row is prunable regardless of status: it is a safe-to-lose
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

Loggers are named after their module (`jasil.publisher`, `jasil.jobs.runner`,
`jasil._core.network`, …), so you can raise or lower verbosity per subsystem.

One worth routing deliberately: `jasil._core.network` logs at INFO every time the
SSRF allowlist permits a private destination. That is an audit trail — it tells
you what the exception is being used for.
