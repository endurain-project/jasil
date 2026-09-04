# Observability

JASIL owns three append-only tables. This page covers what goes in them, how to
create them, and how to stop them growing forever.

| Table | Written by | Purpose |
|---|---|---|
| `event_log` | The bus and the publish facade | One row per event, recording its lifecycle. |
| `event_outbox` | The publish facade | Staged events awaiting relay. |
| `processing_jobs` | The relay and the worker | One row per `(event, subscriber)`. |

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
| `adopt_existing_schema(engine)` | Validate and adopt complete, unversioned JASIL tables. |
| `head_revision()` | The newest revision shipped in this package. |
| `db_revision(engine)` | What the database currently records. |
| `verify_schema_current(engine)` | Raise unless the database is at head. |

`verify_schema_current` is a useful fail-fast at startup — it turns "forgot to
migrate" into a clear message at boot rather than a confusing query error later.

!!! note "It will not touch your tables"
    JASIL's migrations use their own version table, `jasil_alembic_version`, so
    they never collide with your Alembic history. Every operation is scoped to
    JASIL's three tables, so autogenerate cannot propose dropping yours — even
    though both live in the same registry.

### Adopting existing tables

`adopt_existing_schema()` is only for a database where all three JASIL-owned
tables already exist but `jasil_alembic_version` does not record a revision. It
compares their columns, types, nullability, primary keys, unique constraints,
and required indexes with the schema expected by the
installed JASIL migration head. PostgreSQL adoption also requires the metadata
GIN index and its PostgreSQL-specific definition.

An empty database is left unversioned for `upgrade()` to create. A partial or
incompatible schema raises `SchemaCompatibilityError` without changing it or
recording a revision. Unexpected columns are incompatible; additional ordinary
non-unique indexes are allowed because they do not change JASIL's write
contract. JASIL never repairs an incompatible schema automatically.

Do not guess or hardcode a JASIL revision identifier. The adoption API owns the
installed head and all expected schema details. Calling it again after a
successful adoption is a no-op.

### Prefer one unified history?

Point your own `env.py` at your `Base.metadata` and add JASIL's `versions`
directory to your `version_locations`. The self-contained runner above needs no
host wiring, but it is not mandatory.

## Retention

All three tables are append-only, so they grow without bound. Pruning runs on a
schedule:

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
