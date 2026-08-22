"""Async outbox relay — turns published events into per-subscriber durable jobs.

The async twin of :mod:`jasil.jobs.relay`. The semantics are identical and the
design rationale in that module applies unchanged here; this docstring records
the points specific to the async implementation.

**Single-transaction fan-out (critical correctness property):**

``list_unrelayed`` claims a batch with ``SELECT ... FOR UPDATE SKIP LOCKED``,
which holds **only while the transaction remains open**.  Releasing the session
— by committing, rolling back, or exiting the ``async with`` block — before the
fan-out would drop the lock and let every concurrent relayer race over the same
rows, potentially duplicating work.  To hold the lock for the full fan-out this
function keeps its ``AsyncSession`` open across the entire loop: each
``enqueue_job`` and ``mark_relayed`` call is made with ``commit=False`` (flush
only, no commit), and a single ``await db.commit()`` lands the batch atomically
at the end.  This is the same single-commit batch semantics as the sync relay.

The idempotent fan-out (jobs dedup on ``event_id + subscriber_id``) and the
overwrite-safe relayed stamp mean a crash mid-pass — which rolls the transaction
back — is harmless: the next pass re-relays the same batch.  At-least-once,
never at-most-once.

**Concurrent relayers and dialects without SKIP LOCKED:**

Where the dialect supports ``FOR UPDATE SKIP LOCKED``, concurrent relayers
automatically take disjoint batches without any single-runner lock.  Where it
does not, the batches can overlap, but the two idempotency properties above make
that harmless too.

**AsyncJobHandlerRegistry:**

The relay only calls ``registry.subscribers_for(event_type)`` to enumerate the
fan-out targets; it never resolves or calls a handler.  The async registry is
used here (rather than the sync ``JobHandlerRegistry``) because the async face
registers all of its subscribers — fan-out IDs and handlers — on
``async_registry``, and the relay must fan out only the subscribers that the
async runner knows how to dispatch to.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

import jasil.jobs.crud_async as jobs_crud
import jasil.jobs.outbox_async as jobs_outbox
from jasil.events import Event
from jasil.jobs.models import EventOutbox
from jasil.jobs.runner_async import AsyncJobHandlerRegistry
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)


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


async def relay_outbox_once(
    *,
    registry: AsyncJobHandlerRegistry,
    clock: ClockProvider,
    session_factory: Callable[[], AsyncSession],
    max_attempts: int,
    batch_size: int,
) -> int:
    """
    Relay one batch of unrelayed outbox rows into durable jobs, in one transaction.

    The ``AsyncSession`` stays open across the entire fan-out so the
    ``FOR UPDATE SKIP LOCKED`` claim in ``list_unrelayed`` holds for the duration.
    Each ``enqueue_job`` and ``mark_relayed`` call uses ``commit=False`` (flush
    only), and a single ``await db.commit()`` commits the entire batch.

    Args:
        registry: The async durable-subscriber registry (event type → subscriber ids).
        clock: Time source for job/outbox timestamps.
        session_factory: Opens the session the pass runs in.
        max_attempts: Attempt ceiling stamped on each enqueued job.
        batch_size: Maximum number of outbox rows to relay in this pass.

    Returns:
        The number of outbox rows relayed.
    """
    async with session_factory() as db:
        rows = await jobs_outbox.list_unrelayed(limit=batch_size, db=db)
        snapshots = [_snapshot(row) for row in rows]
        now = clock.now()
        for snapshot in snapshots:
            event = _event_from(snapshot)
            for subscriber_id in registry.subscribers_for(event.event_type):
                await jobs_crud.enqueue_job(
                    event, subscriber_id, max_attempts=max_attempts, now=now, db=db, commit=False
                )
            await jobs_outbox.mark_relayed(snapshot.id, now=now, db=db, commit=False)
        # One commit lands the whole batch: all enqueued jobs and all relayed
        # stamps atomically.  If this process crashes before here, the
        # transaction rolls back and the next relay pass re-processes the rows.
        await db.commit()
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
