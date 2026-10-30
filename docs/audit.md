# Audit trail

`jasil.audit` is a complete, append-only record of *who did what to which thing, and how it ended*. An audit
row is written **synchronously on the caller's own SQLAlchemy session**, in the same transaction as the change
it describes, so the two commit or roll back together. Nothing else is involved: no bus, no outbox, no Redis.
The behaviour is therefore identical in a single process and in a deployment with the outbox, Redis, workers
and replicas.

## Not the event log, not a domain event

| | Event log / domain events | Audit trail |
|---|---|---|
| Written | Asynchronously, after the fact | In the caller's transaction |
| Delivery | At least once; may be dropped on failure | Exactly as durable as the change itself |
| Misses denials | Yes: a refused request publishes nothing | No: `record_detached` records them |
| Failure to write | Swallowed (observability is best-effort) | `stage` raises and the change rolls back |
| Retention | Prunable at will | Bounded by a database-side floor |
| Tamper protection | None | Triggers, partition-level retention, signed checkpoints |

Use events to *react* to something. Use the audit trail to *prove* it happened.

## Quick start

```python
import jasil.audit as jasil_audit

# 1. Declare the actions, once, at startup. Ids are <module>.<resource>.<verb> and stable forever.
PASSWORD_CHANGE = jasil_audit.get_catalog().register(
    jasil_audit.AuditAction(
        id="credential.password.change",
        category=jasil_audit.Category.CREDENTIAL,
        severity=jasil_audit.Severity.HIGH,
        description_key="audit.credential.password.change",
        description_default="A password was changed",
    )
)

# 2. Say who is acting, once per request (or use jasil_audit.audit_context(...) in a worker or CLI).
jasil_audit.set_context(
    jasil_audit.AuditContext(actor_type="user", actor_id="42", ip_address="203.0.113.7", source="api")
)


# 3. Stage the row in the transaction that makes the change.
def change_password(db, user, new_hash):
    user.password_hash = new_hash
    jasil_audit.stage(db, PASSWORD_CHANGE, target_type="user", target_id=str(user.id), target_owner_id=str(user.id))
    db.commit()  # the change and its audit row become durable together


# 4. Where there is no transaction to join, record on a session of its own.
LOGIN = jasil_audit.get_catalog().register(
    jasil_audit.AuditAction(
        id="authentication.session.login",
        category=jasil_audit.Category.AUTHENTICATION,
        severity=jasil_audit.Severity.LOW,
        description_key="audit.authentication.session.login",
        description_default="A login was attempted",
    )
)


def failed_login(user_id):
    jasil_audit.record_detached(LOGIN, outcome=jasil_audit.Outcome.FAILURE, target_type="user", target_id=user_id)
```

Then run [maintenance](#maintenance) on a schedule and publish the catalog with `jasil_audit.sync_catalog()` at
startup.

## The two write paths

| | `stage(db, action, ...)` | `record_detached(action, outcome=..., ...)` |
|---|---|---|
| Session | The caller's | Its own, short-lived, committed |
| Atomic with the change | **Yes** | No |
| When it is wrong to use | Where the transaction rolls back or does not exist | Where the change and the row must stand or fall together |
| On a failure | **Fails closed**: the exception propagates and the caller must roll back | **Fails open**: logged at error level, counted, never raised |
| Use for | Every successful or failed change made inside a transaction | A failed login, a denied request, a refresh-token reuse, a CLI command |

`stage` adds the row with `Session.add` and nothing more. It does **not** commit, and it does not flush: the row
is flushed whenever the session next flushes (autoflush, an explicit flush, or commit), and becomes durable when
the caller commits. A storage error therefore surfaces from that flush or commit, which is exactly where the
caller's transaction fails. The first audited write in a process reads the deployment's
[instance id](#storage) on that same session, without flushing it. Only a deployment's very first write, before
`jasil_instance` holds its row, creates it on a short session of its own on the same engine; call
`jasil_audit.sync_catalog()` at startup to do that outside a request.

`stage_many(db, [AuditRecord(...), ...])` validates every record before adding any, so one bad record stages
nothing (and [`capture_audit`](#testing) captures nothing).

**Let every exception from `stage` or `stage_many` propagate, and roll back.** A validation error is raised
*before* any row is added, so a caller that catches it and commits anyway persists the change with no audit row.
A storage error surfaces from the flush or commit, which is where the transaction fails on its own.

!!! note "`publish(db=...)` commits the session"
    In durable-jobs mode `jasil.publisher.publish(..., db=db)` commits the caller's session after staging the
    outbox row. That commits any audit row staged earlier in the same transaction with the change and the
    outbox row; one staged *after* it needs its own commit. Stage before you publish.

### A denial is a different path

An operation that is refused before it changes anything has no transaction. Record it with `record_detached` and
`outcome=jasil_audit.Outcome.DENIED`. Do not stage it on a session that is about to roll back: the row would go
with it.

`record_detached` can only report a failure by logging and counting. Read the counter with
`jasil_audit.get_audit_health()` (`write_failures`, `last_failure_at`, `furthest_partition_date`,
`last_checkpoint_at`) and alert on it: a trail that has silently stopped recording is worse than none.
`write_failures` and `last_failure_at` count this process's writes. The partition and checkpoint figures are what
the latest maintenance run recorded in `jasil_instance`, on whichever replica won its lock, so every replica reports
the same ones. The call reads that one row, so it raises when the database cannot be read.

### What goes in `details`

Each action declares a `details_model`, a `VersionedPayload` like the ones event payloads use, and `details` are
validated against it before the row is staged. The row stores the model's `SCHEMA_VERSION` as `details_version`,
so a reader can tell shapes apart as the model evolves.

**`details` carry ids and the names of the fields that changed. Never a secret, a token, a password, a key, or a
value that would be one.** The row is meant to be read by operators, kept for months and copied to evidence
storage. Keys the model does not declare are dropped, not stored: `AuditAction` refuses a `details_model` that sets
`extra="allow"`, and the default (`ignore`) and `forbid` are both accepted. As a guard against a payload that has grown
into a data dump, a serialized payload over `audit.max_details_bytes` (4096 by default) is refused with
`AuditDetailsError`.

When validation fails, `stage` raises the Pydantic `ValidationError`; `record_detached` logs the error locations
and types only, never the input.

### Disabling

With `audit.enabled = False` both paths still validate their arguments (so a misspelt id is found in development)
and then write nothing; maintenance does nothing. The test helpers still capture.

## The catalog

An `AuditAction` is declared once and registered in the process-wide catalog (`jasil_audit.get_catalog()`):

| Field | Meaning |
|---|---|
| `id` | `<module>.<resource>.<verb>`, three lower-case snake_case segments, at most 100 characters. Stable forever: it is stored in rows that outlive the code. |
| `category` | One of `authentication`, `credential`, `authorization`, `identity`, `admin`, `data_change`, `data_transfer`, `integration`, `system`. |
| `severity` | `info`, `low`, `medium`, `high`. |
| `description_key`, `description_default` | A translation key owned by the host's i18n, and the text used when the key has no translation. |
| `details_model` | The versioned model `details` are validated against. Defaults to `NoDetails`. |
| `deprecated` | The action is no longer emitted. |

Registering a duplicate id raises. Staging an id that is not registered raises `UnknownAuditActionError`. The
category and severity are copied onto each row so it stands alone, even if the action is later changed.

An id is never renamed or deleted. To retire one, stop emitting it and set `deprecated=True`; the rows that carry it
still need describing.

JASIL registers the actions it emits itself: `audit.retention.prune`, `audit.retention.change`,
`audit.integrity.violation` and `audit.catalog.sync`.

### Publishing the catalog

`jasil_audit.sync_catalog()` writes the registered catalog to `jasil_audit_actions`: new actions are inserted,
changed ones updated, and actions that are no longer registered are marked `deprecated`. Rows are never deleted.
This is how the observability product describes rows without importing your code (`jasil_audit.list_catalog()`).

Call it at startup, after registering every action, and **only from the application that owns the catalog**: a
process with an empty catalog would mark everything deprecated. A change is itself audited as
`audit.catalog.sync`. [Maintenance](#maintenance) never syncs the catalog, so it can run in a process that
registers only JASIL's own actions.

## The ambient context

`AuditContext` carries what a call site should not have to repeat:

| Field | |
|---|---|
| `actor_type` | `user`, `api_key`, `system` or `anonymous` (the default) |
| `actor_id`, `credential_type`, `credential_id`, `on_behalf_of_id` | Who, through what, and for whom. The credential's *id*, never its value. |
| `source` | `api` (the default), `worker`, `cli` or `migration` |
| `ip_address`, `user_agent` | The client. `user_agent` is clipped to 512 characters with a trailing `...`; `ip_address` to 45. |
| `event_id` | The domain event being handled, when running in a handler |

`request_id` is not part of it: it is always read from [`jasil.correlation`](configuration.md#correlation-ids), so
there is one source of truth.

`set_context()` stores it in a `ContextVar`, so every thread and every asyncio task has its own. A value lives as
long as its thread or task, though: a worker thread reused across requests keeps the previous request's actor until
the next call, so set the context at the start of every request (`set_context(None)` for an anonymous one) or use
`audit_context(...)` per request. A host that
already carries this information installs a reader instead, with `jasil_audit.configure_provider(callable)`. Workers and CLI
commands, which have no request middleware, use the block form:

```python
with jasil_audit.audit_context(actor_type="system", source="worker"):
    ...
```

A `with audit_context(...)` block wins over an installed provider, so a scheduled job can attribute its rows to the
system in a host whose provider only knows about requests. With no context set the row says `anonymous`/`api`. That is wrong for anything but an unauthenticated request:
set the context in every entry point.

## Storage

| Table | Holds |
|---|---|
| `jasil_audit_log` | The trail. Append-only. |
| `jasil_audit_actions` | The catalog, as last synchronized. |
| `jasil_audit_checkpoints` | The sealed, chained [checkpoints](#tamper-evidence). |
| `jasil_instance` | One row: a random UUID generated on first use, stamped on every audit row as `instance_id`, and the retention settings maintenance last saw. |

`jasil_audit_log` rows have a UUIDv7 `id` (time-ordered) and an `occurred_at` that together form the primary key.
`occurred_at` is taken from the platform clock when the row is staged. Indexes: `(occurred_at)`,
`(action_id, occurred_at)`, `(actor_id, occurred_at)`, `(target_type, target_id)` and `(request_id)`.

### On PostgreSQL

The migration (`rev0006`, PostgreSQL 13 or newer) builds the log differently from every other dialect:

- It is **range-partitioned by `occurred_at`, one partition per UTC day**.
- A `BEFORE UPDATE OR DELETE` row trigger and a `BEFORE TRUNCATE` statement trigger raise an exception. Statement
  triggers are not inherited, so each partition gets its own `TRUNCATE` trigger.
- `jasil_audit_ensure_partitions(days_ahead)` (`SECURITY DEFINER`) creates the partitions from yesterday to
  `days_ahead` from today.
- `jasil_audit_drop_partitions(cutoff, prune_at)` (`SECURITY DEFINER`, `search_path` pinned; `prune_at` defaults to the database's `now()`) refuses a cutoff younger than
  `now() - <retention floor>`, drops **whole partitions only** (a partition is dropped only if it ends at or before
  the cutoff), and inserts its own `audit.retention.prune` row, stamped `prune_at`. Retention never deletes row by row. Dropping a
  partition is DDL, so the triggers do not stand in its way.
- Both functions are closed to `PUBLIC`; [grant](#roles-and-grants) `EXECUTE` to the roles that need it.
- Both wait at most 10 seconds for a lock (`lock_timeout`), then fail; maintenance logs the failure and
  retries on its next run. Their advisory lock is one any role that can connect may hold, see
  [the threat model](security/threat-model.md#tampering-with-the-audit-trail).

**A row whose day has no partition is rejected**, and `stage` fails closed, so the request fails. Keep
[maintenance](#maintenance) running; `audit.partition_days_ahead` (7 by default) is the margin by which it may
stop, and `get_audit_health()["furthest_partition_date"]` is what to alert on.

### On other dialects

SQLite and MySQL get a plain, unpartitioned table with the same columns (and `create_all` builds the same, on any
dialect). Nothing prevents `UPDATE` or `DELETE`, and retention is a bounded `DELETE`. This is for test
environments; do not run a production audit trail on it.

### The retention floor

The floor is a constant inside `jasil_audit_drop_partitions`, baked in when the migration runs from
`audit.retention_floor_days` (30 by default). That the guard lives in the database is the point: an application
that is compromised, or misconfigured to a retention of one day, cannot shorten the trail below it.

Changing the setting afterwards does **not** change the function. To change it, run the statement
`jasil_audit.retention_floor_sql(floor_days, schema=...)` returns as the role that owns the function. `schema` is required
and must be the schema the migration ran in (its `current_schema()`): the function names its schema in its body,
so there is no default that is safe. It replaces the body only; the owner and grants stay.

```python
import sqlalchemy as sa

with engine.begin() as connection:  # connected as the migrating (owner) role
    connection.execute(sa.text(jasil_audit.retention_floor_sql(90, schema="public")))
```

(Use `sa.text` or `psql`: the statement contains `%`, which a raw driver call would read as a placeholder.)

## Roles and grants

JASIL names no role. `jasil_audit.grants_sql(runtime_role, reader_role, *, maintenance_role=None, schema=None)`
returns the statements; review them and run them as the table owner:

```python
for statement in jasil_audit.grants_sql("app_runtime", "app_observability", maintenance_role="app_maintenance"):
    print(statement + ";")
```

| Role | Gets | Cannot |
|---|---|---|
| **Runtime** (the application) | `INSERT` on `jasil_audit_log`; `SELECT`, `INSERT` on `jasil_instance`; `SELECT`, `INSERT`, `UPDATE` on `jasil_audit_actions`; `EXECUTE` on `jasil_audit_ensure_partitions` | Read, update, delete or truncate the log; touch the checkpoints; drop a partition |
| **Reader** (the observability product) | `SELECT` on all four tables | Write anything; execute either function |
| **Maintenance** (optional) | `SELECT`, `INSERT` on the log; `SELECT`, `INSERT` on the checkpoints; `SELECT`, `INSERT`, `UPDATE` on the catalog and the instance row; `EXECUTE` on both functions | Update or delete a log row |

Every statement first revokes whatever the role held on these tables, so running the list converges. The reader
must differ from the runtime and the maintenance roles (`ValueError` otherwise): it would otherwise be able to write
rows or checkpoints, or the application to read the trail it writes.

**Sealing a checkpoint means reading the log**, so the process that runs `run_audit_maintenance()` needs `SELECT`
on it. If that is the application's own role, the application can read the trail it writes. Either accept that, or
run maintenance in a separate process (a worker, a scheduled job) that connects as the maintenance role. Passing
the runtime role as `maintenance_role` is the explicit way to choose the former.

## Maintenance

`run_audit_maintenance()` is **owned by JASIL and scheduled by the host**: JASIL never starts a scheduler. It is a
separate function from [`prune_expired_records`](observability.md#retention) with its own lock
(`jasil_audit_maintenance`), so neither can block the other. Under the platform's `LockProvider`, in order:

1. **Record a retention change**: if `audit.retention_days` or `audit.legal_hold` differs from what the last run
   stored in `jasil_instance`, stage `audit.retention.change` (the first run only records the baseline).
2. **Ensure the partitions** for the next `partition_days_ahead` days (PostgreSQL).
3. **Seal the checkpoints** that are due.
4. **Apply retention**, unless `legal_hold` is set or `retention_days <= 0`, and no further than the newest sealed
   checkpoint allows: the cutoff is the earlier of `now - retention_days` and that checkpoint's `window_end` minus the
   `retention_days` it was sealed under; with no such checkpoint (none yet, or one sealed under legal hold) nothing
   is pruned. On PostgreSQL call `jasil_audit_drop_partitions(cutoff, now)` with the platform clock's `now`, which writes the
   `audit.retention.prune` row itself, so the row belongs to the window that clock is sealing. On other dialects, `DELETE` rows older than the cutoff (floored to the start of the sealed window holding
   it, as a partition drop is) and write that row from Python.

Each step runs on its own sessions, and a failure in one is logged and does not skip the others: a partition problem
does not stop checkpoints. Retention never prunes a row that is not sealed, so while sealing fails (a bad signing
key, a sink outage or conflict) or works off a backlog, it prunes less or nothing and the log grows; alert on
`last_checkpoint_at`. The run is idempotent and safe on every replica at once; the lock makes all but one a
no-op. The process needs the [maintenance grants](#roles-and-grants).

```python
import jasil.audit as jasil_audit

jasil_audit.schedule_audit_maintenance(scheduler)  # on every replica; runs once at startup, then every 5 minutes
# or: call jasil_audit.run_audit_maintenance() from whatever scheduler you have
```

`schedule_audit_maintenance(scheduler, run_at_startup=True)` registers it on an APScheduler instance, mirroring
`schedule_retention_maintenance`. Checkpoints are sealed once a window and its grace period have passed, so the
interval bounds how far behind they run.

### Retention

`retention_days` (90) is how long a partition is kept; `<= 0` keeps everything. `legal_hold` suspends pruning
entirely. The hold binds JASIL's maintenance, not the database: `jasil_audit_drop_partitions` refuses only a cutoff
inside the retention floor, so any role with `EXECUTE` on it can still drop older partitions while the hold is set.
For a hold the database enforces, revoke that right for its duration
(`REVOKE EXECUTE ON FUNCTION jasil_audit_drop_partitions(timestamptz, timestamptz) FROM <maintenance role>`) and grant it back
afterwards; maintenance does not call it while `legal_hold` is set. `retention_floor_days` (30) is the database-side minimum described [above](#the-retention-floor); a
positive `retention_days` below it is a configuration error. The host maps its own deployment profiles onto
`retention_days`; JASIL supplies only the neutral default.

## Tamper evidence

Database permissions stop everyone but the table owner. Checkpoints detect a change by someone who is not stopped,
and keep the evidence where that person cannot reach it.

Time is cut into windows of `checkpoint_window_minutes` (15; it must divide a day). A window is **sealed** only
`checkpoint_grace_minutes` (5) after its end, because a row's `occurred_at` is set when it is staged but the row is
only visible once its transaction commits: sealing at the end would let a slow transaction look like a later
insertion. Choose a grace period longer than your longest audited transaction plus the clock skew between replicas.

To seal a window, JASIL serializes its rows canonically, ordered by `(occurred_at, id)`, and computes
`SHA-256(serialization || previous checkpoint's hash)`, so the checkpoints form a chain. It then signs the
deployment's instance id, the window, row count, hash, previous hash and the `retention_days` and `legal_hold` in
force with Ed25519 (if configured),
stores the checkpoint in `jasil_audit_checkpoints` (window start and end, row count, hash, previous hash, retention
days, legal hold, signature, key id), and copies it to the external sink (if configured). Empty
windows are sealed too, so a row inserted into one later is detected.

A window ends on the `checkpoint_window_minutes` grid; after the setting changes, the first window is shortened to
rejoin it. If the newest checkpoint ends after now (one inserted ahead of time, or a clock that went back), sealing
raises on every run instead of resuming from it, until someone investigates.

There is deliberately **no per-row hash chain**: it would serialize every insert across replicas.

### Signing

```python
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

key = Ed25519PrivateKey.generate()
private = key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
print(base64.b64encode(private).decode())  # audit.signing_key: keep it out of the database
print(base64.b64encode(public).decode())  # give this to whoever verifies
```

Install `jasil[audit]` for `cryptography`. Without `audit.signing_key` checkpoints are still hash-chained but
unsigned, with a warning: whoever can rewrite the rows can then recompute the chain. The key id on each checkpoint is
a fingerprint of the public key. After a rotation, pass every public key the trail was signed with, as a list:
each checkpoint is checked against the key its key id names, and the windows retention pruned under the new key stay
verifiable alongside the ones the old key sealed.

### The external sink

`audit.external_sink_uri` writes each checkpoint to a store the database's operators cannot rewrite:

| URI | Backend | Notes |
|---|---|---|
| `file:///absolute/directory` | A directory | Files are created exclusively and read-only. Point it at a mount the application can write but not rewrite. Each copy is published with a hard link, so the directory's file system must support them; on one that does not (some network shares), every seal fails. No host, query or fragment. Refused under the `distributed` profile, whose nodes share no disk. |
| `s3://bucket/prefix?region=…&endpoint_url=…` | The S3 storage backend | Use a *separate* bucket with Object Lock in compliance mode **and a default retention period** that covers how long checkpoints must live (the sink sets no per-object retention, so an unlocked default means unlocked objects); verify it before relying on it. Put it in another account if you can, with credentials that may `GetObject` and `PutObject` on the prefix but not delete, and a bucket policy that denies any `PutObject` without `If-None-Match: *` (the `s3:if-none-match` condition key), so no holder of the credentials can overwrite a checkpoint. The sink writes every checkpoint with that header; a store that does not support conditional writes fails every seal. Needs `jasil[s3]`. |

Each object is named `checkpoint-<instance id>-<window start>.json`, so deployments sharing a sink do not collide.
The contract (`jasil.providers.AuditSink`) is that an object is never replaced: `write_once` with identical bytes
is a no-op, with different bytes it raises `AuditSinkConflictError`, and `read` returns what is stored. The sink is written **before** the database row, so if it
is unreachable the checkpoint is not stored and the window is retried on the next run; the external copy is never
missing a checkpoint the database has. Sealing the same rows gives the same bytes (Ed25519 signatures are
deterministic), apart from the `retention_days` and `legal_hold` taken from the settings in force. So when a retry
after a crash between the two writes finds the window already in the sink, and those settings changed meanwhile,
the window is resealed under the values the copy records and stored if that reproduces the copy byte for byte;
anything else is an `AuditSinkConflictError`. The S3 sink's "never replace" is a conditional `PutObject`
(`If-None-Match: *`): S3 refuses it when the key holds an object, and the sink then reads that object to tell a retry
from a conflict. Object Lock keeps a locked version from being deleted or overwritten in place, but without the
bucket policy above a `PutObject` that omits the header could still create a new current version, and a delete
marker can hide the locked one; the policy, credentials without delete rights and a default compliance-mode
retention are what close that. `verify_checkpoints` reads
only the database and does not compare the external copy: reconcile the sink yourself.

### Verifying

```python
report = jasil_audit.verify_checkpoints(public_key, from_ts, to_ts)  # a key, a list of keys, or None for hashes only
for window in report.windows:
    if window.status is not jasil_audit.VerificationStatus.OK:
        jasil_audit.record_detached(
            jasil_audit.INTEGRITY_VIOLATION,
            outcome=jasil_audit.Outcome.FAILURE,
            details=jasil_audit.IntegrityViolationDetails(
                window_start=window.window_start, window_end=window.window_end, status=window.status.value
            ),
        )
```

JASIL provides the function; the host or the observability product decides when to call it. One call covers at most 366 days, since it reads and hashes every row in the range; verify a longer history in consecutive ranges. Each window in the range
is reported as one of the statuses below. A gap reaching into the range is reported even when every checkpoint inside
the range was deleted, and rows dated before the first checkpoint are reported as one `row_mismatch` window ending
where that checkpoint starts. Time after the newest checkpoint is not sealed yet, so it is not reported: alert on
`last_checkpoint_at` from `get_audit_health()` to catch sealing that has stopped.

| Status | Meaning |
|---|---|
| `ok` | Rows, count, chain link and (if keys were given) signature all match |
| `row_mismatch` | The rows of the window are not the rows that were sealed: one was modified, deleted or inserted; or rows are dated before the first checkpoint |
| `missing_window` | There is a gap between two checkpoints (a checkpoint was deleted) |
| `broken_chain` | The checkpoint's `prev_hash` is not the previous checkpoint's hash |
| `bad_signature` | The signature is absent, or does not verify with the key its key id names among the keys given (also for a checkpoint signed for another deployment) |

Dropped windows are expected **only** where a checkpoint allows them. A window whose rows are all gone is reported
`ok` with `pruned=True` if some checkpoint not sealed under legal hold ends at least the `retention_days` it was sealed
under after the window's end, and, when keys are given, that checkpoint's signature verifies under one of them; only the window's chain
link and signature are then checked. Otherwise missing rows are a `row_mismatch`. The retention a checkpoint allows
is the application's, under the application's signature: `audit.retention.prune` rows are a record of what retention
did, not evidence, since anyone able to insert into the log could write one. A legal hold stops checkpoints sealed
during it from allowing anything; windows that checkpoints sealed before it already allowed stay allowed. Without a
key the recorded retention is taken as it stands, like the rest of an unsigned chain.

### What this does not do

- **Nothing stops a database superuser** (or the table owner). They can disable the triggers, rewrite rows, and
  rewrite the checkpoints and the chain with them. The goal is prevention for everyone else, plus *detection with
  evidence kept outside the database*, and against the owner that evidence is the **external sink**: reconcile it
  with `jasil_audit_checkpoints` from somewhere the database's operators do not control. A checkpoint in the sink that
  is missing or different in the database, or an `AuditSinkConflictError` while sealing, is the sign of a rewrite.
- **The signing key alone does not detect the owner.** The application holds the key and signs whatever the database
  holds when it seals: if a period's checkpoints are deleted and its rows rewritten, the next maintenance run seals
  and signs the rewritten rows, and `verify_checkpoints` reports them `ok`. The signature stops anyone *without* the
  key from forging or altering a checkpoint, including the retention it records; only the sink keeps the original.
- Without a signing key or a sink, the chain proves only that the rows and checkpoints are consistent with each
  other.
- Deleting the **newest** checkpoints is not a violation the chain can show, and maintenance then re-seals those
  windows from what the database holds; only the sink shows it. Deleting the **oldest** ones is reported as
  `broken_chain` for the first one left.
- A sealed window protects what it contained. Rows are not covered until their window is sealed, which is the window
  length plus the grace plus the maintenance interval later.
- `occurred_at` is stamped by the application's clock. A host whose replicas disagree by more than the grace period
  can have a row land in a window that is already sealed; keep the clocks synchronized.
- Pruned rows are gone; the checkpoints that covered them stay (they are small, about one row per window, and are
  never pruned).

## Reading the trail

Only the observability product reads the trail; the host application exposes no read route. The query surface lives
in `jasil.audit` and, like [`jasil.admin`](observability.md), takes no session: each call opens a short one of its
own, so run it from a thread in an asynchronous route.

```python
from datetime import UTC, datetime

import jasil.audit as jasil_audit

filters = jasil_audit.AuditFilters(
    actor_id="42",
    categories=["credential"],
    outcomes=["denied"],
    from_ts=datetime(2026, 1, 1, tzinfo=UTC),
    to_ts=datetime(2026, 1, 2, tzinfo=UTC),
)
page = jasil_audit.list_entries(filters, limit=100)

# A cursor restores only the position, not the filters or the time bounds: pass the same filters and fix
# from_ts/to_ts so the window does not move between pages. No cursor means no further page.
while page.next_cursor is not None:
    page = jasil_audit.list_entries(filters, cursor=page.next_cursor, limit=100)
```

| Call | Returns |
|---|---|
| `list_entries(filters, cursor, limit)` | A page of `AuditEntry`, newest first, with `next_cursor` |
| `get_entry(entry_id, occurred_at)` | One entry (the timestamp lets PostgreSQL go to one partition) |
| `summary(hours)` | Counts by category, outcome and severity, and the top actions |
| `list_catalog()` | The synchronized catalog, deprecated actions included |
| `verify_checkpoints(public_key, from_ts, to_ts)` | A `VerificationReport` |

Filters: `action_ids`, `categories`, `severities`, `outcomes`, `actor_id`, `actor_type`, `target_type`, `target_id`,
`target_owner_id`, `request_id`, `event_id`, `source`, `from_ts`, `to_ts` (both inclusive). Pagination is keyset
over `(occurred_at, id)`, never offset. The time range is **always bounded** so partition pruning applies: it
defaults to the 24 hours before `to_ts`, which defaults to now, and may cover at most 31 days (`summary(hours)`
takes 1 to 744); an inverted or longer range raises `ValueError`, so read a longer period one range at a time.
`limit` is 1 to 500.

## Testing

```python
import jasil.testing as jasil_testing


def test_changing_a_password_is_audited(db, user):
    with jasil_testing.capture_audit() as audit:
        change_password(db, user, "new-hash")

    audit.assert_audited("credential.password.change", outcome="success", target_id=str(user.id))
```

`capture_audit()` collects every entry staged or recorded in the block, after validation and before persistence, so
it works with `audit.enabled = False`, without a database, and the same whether durable jobs are enabled or not:
audit rows never go through the bus or the outbox. `assert_audited` also matches any other `AuditEntry` field
(`actor_id="42"`) and returns the entry; `assert_not_audited(action_id)` and `matching(...)` complete it. An entry
is captured when it is *staged*, so one whose transaction later rolled back is listed; assert on the database to
test that. `jasil_testing.reset_all()` also resets the catalog, the context and the cached instance id.

## Upgrading

Migration `rev0006` creates the four tables and, on PostgreSQL, the partitions, triggers and functions. See
[Migrations](observability.md#migrations).

- Run it as the role that should **own** the table and the functions: `SECURITY DEFINER` runs as the owner, and only
  the owner can drop partitions.
- The retention floor is baked in from `audit.retention_floor_days` as configured when it runs.
- Nothing else reads or writes the new tables, so processes of the previous release are unaffected during a rolling
  deploy.
- Downgrading drops the tables, **and the audit trail with them**.

## For the host's architecture tests

If the host enforces that it imports only JASIL's public surface, treat `jasil.audit` (everything it exports) as
public, and `jasil._core` and the underscore modules of `jasil.audit` as private. `jasil.audit` is safe to import at
any point, before `jasil.orm.map_models`; the models load on first use.
