"""Async CRUD for ``event_outbox`` — the asynchronous twin of :mod:`jasil.jobs.outbox`.

Same staging table, same guarantees, same statements (from
:mod:`jasil.jobs.statements`); only the execution differs. In particular the
``FOR UPDATE SKIP LOCKED`` claim in :func:`list_unrelayed` holds only for as long
as the caller keeps the transaction open, which is why the async relay must keep
its session open across the whole fan-out exactly as the sync one does.
"""

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

import jasil._core.pruning as pruning
import jasil.jobs.statements as statements
from jasil._core.dialects import supports_skip_locked
from jasil._core.sessions import commit_or_flush_async
from jasil.events import Event
from jasil.jobs.models import EventOutbox


async def add_to_outbox(event: Event, *, now: datetime, db: AsyncSession, commit: bool = True) -> str:
    """
    Persist an event to the outbox on the caller's session.

    With ``commit=True`` (default) the write is committed immediately. Callers on
    the ingestion path pass ``commit=False`` so the outbox row is only flushed and
    joins their open transaction — the domain change and the outbox row then commit
    together (atomic delivery; see :func:`jasil.publisher.apublish_committing`).
    When committed independently, the outbox write is *not* atomic with the domain
    change, so a crash between them can drop the event and the subscriber's
    reconciliation net is the safety net.

    Args:
        event: The event envelope to persist.
        now: Current instant (the outbox write time).
        db: Active async database session (the producer's).
        commit: When True, commit immediately; when False, flush only and leave the
            row in the caller's open transaction.

    Returns:
        The new outbox row id.
    """
    row = statements.new_outbox_row(event, now=now)
    db.add(row)
    # The id is read before the commit deliberately: it was assigned in Python
    # when the row was built, and reading it afterwards would touch a
    # (potentially) expired instance, which asyncio cannot refresh implicitly.
    outbox_id: str = row.id
    await commit_or_flush_async(db, commit)
    return outbox_id


async def list_unrelayed(*, limit: int, db: AsyncSession) -> list[EventOutbox]:
    """
    Fetch the oldest not-yet-relayed outbox rows, locking them for this relayer.

    On a database with row-level locking the rows are claimed with ``FOR UPDATE
    SKIP LOCKED``, so concurrent relayers across replicas take disjoint batches
    (no single-runner lock needed) — but only for as long as the caller holds the
    transaction. :func:`jasil.jobs.relay_async.relay_outbox_once` therefore keeps
    it open across the whole fan-out. Where the clause is unavailable the batches
    can overlap, and the idempotent fan-out is what keeps that harmless.

    Args:
        limit: Maximum number of rows to return.
        db: Active async database session, whose transaction must stay open for as
            long as the claim needs to hold.

    Returns:
        Unrelayed outbox rows, oldest-first.
    """
    stmt = statements.unrelayed_stmt(limit=limit, skip_locked=supports_skip_locked(db.bind))
    return list((await db.execute(stmt)).scalars().all())


async def mark_relayed(outbox_id: str, *, now: datetime, db: AsyncSession, commit: bool = True) -> None:
    """
    Stamp an outbox row as relayed.

    Args:
        outbox_id: The outbox row id.
        now: Current instant.
        db: Active async database session.
        commit: When True, commit immediately; when False, flush only and leave
            the stamp in the caller's open transaction, so a whole relayed batch
            lands under one commit.

    Returns:
        None.
    """
    await db.execute(statements.mark_relayed_stmt(outbox_id, now=now))
    await commit_or_flush_async(db, commit)


async def delete_relayed_before(
    cutoff: datetime,
    *,
    db: AsyncSession,
    batch_size: int = pruning.PRUNE_BATCH_SIZE,
) -> int:
    """
    Delete relayed outbox rows older than ``cutoff``, in bounded batches.

    Only rows that have been relayed (``relayed_at`` set) and whose relay is older
    than ``cutoff`` are removed; unrelayed rows are pending work and are never
    touched. A relayed row's only remaining value is audit — the per-subscriber
    jobs it fanned out into are the source of truth — so it is safe to prune.

    Args:
        cutoff: Delete rows whose ``relayed_at`` is strictly before this instant.
        db: Active async database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    return await pruning.bounded_delete_async(
        EventOutbox,
        *statements.outbox_prune_condition(cutoff),
        db=db,
        batch_size=batch_size,
    )
