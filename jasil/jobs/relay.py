"""The outbox relay — turns published events into per-subscriber durable jobs.

Runs on every replica (scheduled). One pass is one transaction: ``list_unrelayed``
claims a batch with ``SELECT ... FOR UPDATE SKIP LOCKED``, each row is fanned out
into one job per durable subscriber of its event type and stamped relayed, and
the whole batch commits at the end. Holding the transaction for the fan-out is
what makes the lock mean anything — released at the select, it would leave every
relayer racing over the same rows.

That gives concurrent relayers disjoint batches without a single-runner lock.
Where the dialect has no ``SKIP LOCKED`` the batches can overlap, and two other
properties keep it harmless: the fan-out is idempotent (jobs dedup on
``event_id + subscriber_id``) and the stamp is a plain overwrite. The same two
properties cover a crash mid-pass, which rolls the batch back and re-relays it on
the next one — at-least-once, never at-most-once.
"""

from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.orm import Session

import jasil.jobs.crud as jobs_crud
import jasil.jobs.outbox as jobs_outbox
from jasil.events import Event
from jasil.jobs.models import EventOutbox
from jasil.jobs.registry import JobHandlerRegistry
from jasil.providers import ClockProvider


@dataclass(frozen=True)
class _OutboxSnapshot:
    id: str
    event_id: str
    event_type: str
    source: str
    timestamp: str
    payload: dict
    metadata: dict | None
    schema_version: int


def relay_outbox_once(
    *,
    registry: JobHandlerRegistry,
    clock: ClockProvider,
    session_factory: Callable[[], Session],
    max_attempts: int,
    batch_size: int,
) -> int:
    """
    Relay one batch of unrelayed outbox rows into durable jobs, in one transaction.

    Args:
        registry: The durable-subscriber registry (event type -> subscribers).
        clock: Time source for job/outbox timestamps.
        session_factory: Opens the session the pass runs in.
        max_attempts: Attempt ceiling stamped on each enqueued job.
        batch_size: Maximum number of outbox rows to relay in this pass.

    Returns:
        The number of outbox rows relayed.
    """
    with session_factory() as db:
        rows = jobs_outbox.list_unrelayed(limit=batch_size, db=db)
        snapshots = [_snapshot(row) for row in rows]
        now = clock.now()
        for snapshot in snapshots:
            event = _event_from(snapshot)
            for subscriber_id in registry.subscribers_for(event.event_type):
                jobs_crud.enqueue_job(event, subscriber_id, max_attempts=max_attempts, now=now, db=db, commit=False)
            jobs_outbox.mark_relayed(snapshot.id, now=now, db=db, commit=False)
        db.commit()
    return len(snapshots)


def _event_from(snapshot: _OutboxSnapshot) -> Event:
    return Event(
        event_id=snapshot.event_id,
        event_type=snapshot.event_type,
        source=snapshot.source,
        timestamp=snapshot.timestamp,
        payload=snapshot.payload,
        metadata=snapshot.metadata or {},
        retry_count=0,
        schema_version=snapshot.schema_version,
    )


def _snapshot(row: EventOutbox) -> _OutboxSnapshot:
    return _OutboxSnapshot(
        id=row.id,
        event_id=row.event_id,
        event_type=row.event_type,
        source=row.source,
        timestamp=row.timestamp,
        payload=dict(row.payload),
        metadata=dict(row.event_metadata) if row.event_metadata else None,
        schema_version=row.schema_version,
    )
