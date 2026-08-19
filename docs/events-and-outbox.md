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
| `event_id` | UUIDv4, stable across retries — also the deduplication key. |
| `event_type` | Dot-notation channel, owned by the publishing domain. |
| `source` | Where it originated. |
| `timestamp` | ISO-8601 UTC, of the first publish. |
| `payload` | Domain data, homogeneous per `event_type`. |
| `metadata` | Correlation context. |
| `schema_version` | Which version of the payload's shape this carries. |

The envelope is frozen. Handlers share one instance, so a mutation would be
visible to every other handler.

Event types and payload shapes are **owned by the publishing domain**, not
defined here. JASIL only knows the envelope. The one metadata key it defines is
`META_REQUEST_ID`; everything else in `metadata` is yours to name.

### What not to put in one

An event is not a private in-memory message. Know where the two open fields end
up before you fill them:

| Field | Where it goes |
|---|---|
| `payload` | Written verbatim to `event_outbox` and `processing_jobs`, to `event_log` when the trail is enabled, and serialized onto the Redis stream on the `redis://` bus. |
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
Going over raises at `new_event` rather than failing at the write.

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

## Delivery routing

An event takes the durable route only when **both** are true:

1. `jobs.enabled` is set, and
2. at least one durable subscriber is registered for its `event_type`, and
3. the caller supplied a session.

Otherwise it goes on the event bus. Writing to the outbox with nothing to relay
to would strand the row forever, so the registry check is not optional.

```
publish(...)
    │
    ├─ durable jobs on + subscriber registered + db given
    │      └─▶ event_outbox ──relay──▶ processing_jobs ──▶ subscriber
    │
    └─ otherwise
           └─▶ event bus ──▶ subscriber (in-process, or via Redis Streams)
```

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
`publish_committing` when you need the stronger guarantee, and give every
subscriber a [reconciliation net](durable-jobs.md#reconciliation-nets) regardless.
