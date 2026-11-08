# Events & outbox

Every event goes through one publish seam, so changing *how* events are delivered
is a configuration change rather than an edit to every call site.

## The envelope

```python
from jasil.events import Event, new_event

event = new_event(
    "order.created",
    {"order_id": 42},
    source="api:create_order",
    metadata={"tenant_id": 7},
)
```

| Field | Meaning |
|---|---|
| `event_id` | UUIDv4 unless the producer supplies one (see [Publishing with a known id](#publishing-with-a-known-id)); at most 36 characters; stable across retries — also the deduplication key. |
| `event_type` | Dot-notation channel, owned by the publishing domain. |
| `source` | Where it originated. |
| `timestamp` | ISO-8601 UTC, of the first publish. The publish functions stamp the platform clock's instant; `new_event` takes it as `now` and uses the system clock without one. |
| `payload` | Domain data, homogeneous per `event_type`. |
| `metadata` | Correlation context. |
| `schema_version` | Which version of the payload's shape this carries. |
| `retry_count` | Processing attempts so far: 0 on first publish; in a durable handler, the attempt being run, from 1. Read `attempt` and `is_last_attempt` rather than comparing it. |
| `max_attempts` | The durable job's attempt ceiling; `None` outside a durable delivery. |
| `deferral_count` | How many earlier runs of this durable job deferred with `DeferJob`; not counted in `retry_count`. |
| `continuation_count` | How many earlier runs of this durable job ended with `ContinueJob`; `retry_count` counts only the runs since the last of them. |

The envelope is frozen. Handlers share one instance, so a mutation would be
visible to every other handler.

Event types and payload shapes are **owned by the publishing domain**, not
defined here. JASIL only knows the envelope. The metadata keys it defines are
`META_REQUEST_ID` and `META_SCOPE`; everything else in `metadata` is yours to
name. Each value is a string or absent: `new_event` raises `TypeError` for anything
else (`publish()` logs it and returns `None`), because a durable handler runs under the
first as its correlation id and the second as its [audit scope](audit.md#scope).
A `META_SCOPE` longer than 64 characters or holding a NUL character raises
`ValueError`. The publisher stamps the ambient request id for you; it never sets
`META_SCOPE`.

### What not to put in one

An event is not a private in-memory message. Know where the two open fields end
up before you fill them:

| Field | Where it goes |
|---|---|
| `payload` | Written verbatim to `jasil_event_outbox` and `jasil_processing_jobs`, to `jasil_event_log` when the trail is enabled, and serialized onto the Redis stream on the `redis://` bus. |
| `metadata` | The same three tables and the same stream — **and** into log records, whole, whenever a best-effort subscriber fails. |

So:

**No secrets.** No tokens, passwords, API keys, or session identifiers. A
correlation id is fine; a bearer token is not. Metadata in particular is logged
in full by [`best_effort`](#subscribing), so anything you put there should be
safe to read in a log aggregator.

**Think about personal data.** Whatever you publish inherits the retention window
of the tables it lands in, not the retention rules of the record it describes —
and a dead-lettered job keeps its payload until an operator clears it. Publishing
an id and having the subscriber re-read the row keeps deletion in one place.

**Keep it small.** JASIL does not cap payload size: the value is stored as JSON
in every row it reaches, and each durable subscriber gets its own copy. Reference
a blob through the [storage provider](providers-and-backends.md) rather than
inlining it.

Identifiers *are* bounded, because they are indexed columns: `event_type` is
capped at 100 characters, `source` at 50, and a durable `subscriber_id` at 200.
Going over raises at `new_event` rather than failing at the write. The functions
tied to your commit (`publish_committing`, `publish_on_commit` and their batch
forms) raise it before committing or staging anything, on the durable and the
best-effort route alike; `publish` logs and swallows it with every other failure.

## Publishing

```python
from jasil.publisher import publish

publish("order.created", {"order_id": 42}, source="api:create_order", db=db)
```

!!! note "Publishing never raises"
    Delivery failures are logged and swallowed. Your domain row is the source of
    truth, and a publish that breaks the request that produced it would be worse
    than a missed side effect. Every subscriber therefore needs a reconciliation
    path — a backfill or sweeper that re-derives missed work. See
    [Reconciliation nets](durable-jobs.md#reconciliation-nets).

`publish` returns the event's id, or `None` when it swallowed a failure. With
`db=` and durable delivery it commits your session, so when it refuses the
event (an invalid `event_id`, or an id already in the outbox with a different
`event_type` or payload) it commits nothing and swallows the error: your pending
changes stay uncommitted and the transaction stays open, so roll it back or
commit it yourself.

The ambient correlation id is stamped automatically; see
[Configuration](configuration.md#correlation-ids).

### Committing variants

`publish` assumes you have already committed. When you have not, and want the
event to be atomic with your domain write, hand over the commit:

```python
from jasil.publisher import publish_committing

publish_committing(
    "order.created",
    {"order_id": 42},
    source="api:create_order",
    db=db,
    commit=db.commit,
)
```

Behaviour depends on the delivery route:

- **Durable** — the outbox row is staged on your session *without* committing,
  then `commit()` flushes your domain rows and the outbox row in one
  transaction. The event cannot be lost relative to the change that produced it.
  A staging failure propagates so the whole unit of work rolls back.
- **Best-effort** — `commit()` runs *first*, so the domain row is durable
  regardless, then the event is dispatched and any dispatch failure is swallowed.

`publish_many_committing` is the batch form, for producers that touch many rows
in one unit of work. An empty batch still commits exactly once.

`publish_committing` and `publish_on_commit` return the event's id; the batch
forms return one entry per payload, in order. An entry is `None` for an event
the best-effort route could not publish during the call (the failure is
logged); on the durable route a failure raises, so every entry is an id.
`publish_many_committing([])` and `publish_many_on_commit([])` return `[]`.

### Publishing without owning the commit

`publish_committing` makes the publisher own the commit, so a function that
publishes cannot be composed with other writes. When the transaction belongs to
someone else, stage the event and let that owner commit:

```python
from jasil.publisher import publish_many_on_commit, publish_on_commit

publish_on_commit(
    "order.created",
    {"order_id": 42},
    source="api:create_order",
    db=db,
)
# ... more writes, more events ...
db.commit()  # whoever owns the transaction
```

- **Durable** — the outbox row is staged on your session immediately and commits
  or rolls back atomically with your domain rows. A staging failure raises.
- **Best-effort** — the event is held on the session and dispatched **after the
  outermost commit**, in publish order. A rolled-back (or closed) transaction, or a
  rolled-back savepoint, discards its events; events from a released savepoint
  wait for the outermost commit. Dispatch failures are logged and swallowed.

Details worth knowing: the envelope (and ambient request id) is captured when you
publish, not when you commit, including a copy of the payload, so changing the dict
afterwards does not change the event; publishing starts the session's transaction if none
is active; "outermost" means the session's outermost transaction, so a session
joined to an external transaction (`Session(bind=connection)` over a connection
that already has one open, in any `join_transaction_mode`) dispatches when the
session commits, before the external transaction does: if that one then rolls
back, the event has already gone out. Publish on a session that owns its
transaction when that matters; an event published from a subscriber during
dispatch waits for the next commit. `publish_many_on_commit([])` is a no-op that
returns `[]`.
Only synchronous `Session` objects are supported; anything else (an `AsyncSession`,
a `scoped_session` proxy) raises `TypeError`.

On the best-effort route the returned id is minted before the commit that
dispatches the event: it is not a promise the event goes out, because a rollback
discards it.

### Publishing with a known id

Every publish function takes `event_id=` (the batch forms take `event_id_for=`,
a callable from a payload to an id or `None`, like `metadata_for`), and returns
the id. Pass one to answer a request with the id of the work it started:

```python
event_id = publish_committing(
    "report.requested",
    {"report_id": 42},
    source="api:request_report",
    db=db,
    commit=db.commit,
)
return {"status": "accepted", "event_id": event_id}, 202
```

or to make a producer idempotent. A scheduler that ticks on every replica
derives the same id from what makes the tick the same event, here the user and
the minute:

```python
from uuid import NAMESPACE_URL, uuid5

bucket = now.strftime("%Y-%m-%dT%H:%M")
publish_committing(
    "digest.due",
    {"user_id": user_id},
    source="scheduler",
    event_id=str(uuid5(NAMESPACE_URL, f"digest.due:{user_id}:{bucket}")),
    db=db,
    commit=db.commit,
)
```

On the durable route `jasil_event_outbox` has a unique index on `event_id`.
Staging an id that is already there writes nothing (no outbox row and no
`queued` row in the event log) and returns the id, provided the stored row has
the same `event_type` and payload. The payload is compared as it reads back from
the JSON column, so a tuple equals a list; metadata, source, timestamp and
schema version are not compared. A different `event_type` or payload under the
same id raises `ValueError` before the commit, so your unit of work rolls back
(`publish` logs and swallows it). The message names the field, never the
payload. The comparison is a standard-library JSON round trip, so a payload
published under an id that is already stored must be JSON-native (`dict`,
`list`, `str`, `int`, `float`, `bool`, `None`); anything else raises
`TypeError` there.

An id is at most 36 characters, starts with a lowercase letter or digit, and
contains only `a-z`, `0-9`, `.`, `_` and `-`: the rule durable queue names
follow, so that two ids are equal on every database (MySQL's default collation
ignores case and accents) and an id is safe in a log line or a URL. A UUID
string passes. Any other id raises `ValueError` before anything is committed or
staged, on both routes; `publish` logs and swallows it.

Know the limits:

- **Dedup lasts as long as the outbox row.** Relayed rows are pruned after
  `jobs.retention_days`; after that the same id is a new event. The fan-out's
  `(event_id, subscriber_id)` uniqueness holds only while the completed jobs are
  retained, which the same setting prunes.
- **The bus route does not dedup.** A reused id is dispatched again and the
  subscriber runs again. The event log keeps one row per id (its primary key):
  the first event's type, source and payload, with the status of the latest
  processing of an event of that type. A durable event's `queued` row, or a row
  of another `event_type`, is never updated by a bus event that reuses its id.
- **The payload must be deterministic for the id.** A per-replica timestamp in
  the payload makes the repeats differ, and they are refused.

## Delivery routing

An event takes the durable route only when **all three** are true:

1. `jobs.enabled` is set, and
2. at least one durable subscriber is registered for its `event_type`, and
3. the caller supplied a session.

Otherwise it goes on the event bus. Writing to the outbox with nothing to relay
to would strand the row forever, so the registry check is not optional.

```
publish(...)
    │
    ├─ durable jobs on + subscriber registered + db given
    │      └─▶ jasil_event_outbox ──relay──▶ jasil_processing_jobs ──▶ subscriber
    │
    └─ otherwise
           └─▶ event bus ──▶ subscriber (in-process, or via Redis Streams)
```

With `events_uri="none://"` the event bus is disabled: an event that takes the event-bus route is dropped, and
best-effort subscribers never run (`subscribe` raises `CapabilityDisabledError`). The outbox route is unaffected.

## Subscribing

A durable handler must **raise** on failure, so the runner can retry and
eventually dead-letter. A bus subscriber must **not**, so derived work cannot
fail the request that produced the event. Write the raising core once and wrap it:

```python
from jasil.subscribers import best_effort


def render_invoice(event: Event) -> None: ...  # raises on failure


on_order_created = best_effort(render_invoice)  # bus subscriber
```

`best_effort` logs the event type, id, subscriber name and the whole metadata
dict, then swallows. The raising original stays available for durable
registration.

## Payload versioning

Events outlive the code that wrote them — in the outbox, in a Redis stream, and
during a rolling deploy where old and new replicas run at once. A consumer that
silently ignores unknown keys would read a renamed field as its default and do
the wrong thing quietly.

```python
from typing import ClassVar
from jasil.event_versioning import VersionedPayload, parse_payload


class OrderCreated(VersionedPayload):
    SCHEMA_VERSION: ClassVar[int] = 2
    UPGRADERS: ClassVar[dict] = {
        1: lambda p: {**p, "currency": "EUR"},  # v1 had no currency
    }

    order_id: int
    currency: str


def handle(event: Event) -> None:
    payload = parse_payload(OrderCreated, event)
```

- **Older payload** — walked forward one version at a time through `UPGRADERS`.
  Evolving 1 → 3 needs a 1→2 and a 2→3 entry, not a 1→3 jump.
- **Newer payload** — refused with `UnsupportedEventVersionError`. On the bus
  this is logged and swallowed; in a durable job it drives retry and eventually
  dead-lettering, so the event waits for the replica that understands it.
- **Missing upgrader** — also refused. An evolution that shipped without its
  migration fails loudly instead of corrupting data.

Bump `SCHEMA_VERSION` when a field's shape or meaning changes. Purely additive
optional fields do not need one.

## Delivery guarantees, honestly

`publish` is **best-effort from the producer's perspective**. Without the
committing variants, the outbox write is not atomic with the domain change, so a
crash between them can drop an event.

"Durable" means *retryable once written*, not *never lost*. Use
`publish_committing` or `publish_on_commit` when you need the stronger guarantee, and give every
subscriber a [reconciliation net](durable-jobs.md#reconciliation-nets) regardless.
