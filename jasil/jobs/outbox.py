"""CRUD for ``event_outbox`` — the durable-delivery staging table.

``add_to_outbox`` persists an event and commits it on the caller's session. Note
that the ingestion path commits per-CRUD, so this is *not* one transaction with
the domain change — the domain row is the source of truth and each subscriber's
reconciliation net (backfill/sweeper) recovers anything dropped by a crash
between the domain commit and the outbox write. ``list_unrelayed`` and
``mark_relayed`` are used by the relay; combined with the idempotent job fan-out
(dedup on ``event_id + subscriber_id``) and ``SELECT ... FOR UPDATE SKIP LOCKED``,
that makes concurrent relayers safe and re-relaying a row harmless.

The statements themselves live in :mod:`jasil.jobs.statements`, shared with the
async twin (:mod:`jasil.jobs.outbox_async`); what remains here is the execution.
"""

from datetime import datetime

from sqlalchemy.orm import Session

import jasil._core.pruning as pruning
import jasil.jobs.statements as statements
from jasil._core.dialects import supports_skip_locked
from jasil._core.sessions import commit_or_flush
from jasil.events import Event
from jasil.jobs.models import EventOutbox


def add_to_outbox(event: Event, *, now: datetime, db: Session, commit: bool = True) -> str:
    """
    Persist an event to the outbox on the caller's session.

    With ``commit=True`` (default) the write is committed immediately. Callers on
    the ingestion path pass ``commit=False`` so the outbox row is only flushed and
    joins their open transaction — the domain change and the outbox row then commit
    together (atomic delivery; see :func:`jasil.publisher.publish_committing`).
    When committed independently (the historical best-effort seam), the outbox
    write is *not* atomic with the domain change, so a crash between them can drop
    the event and the subscriber's reconciliation net is the safety net.

    Args:
        event: The event envelope to persist.
        now: Current instant (the outbox write time).
        db: Active database session (the producer's).
        commit: When True, commit immediately; when False, flush only and leave the
            row in the caller's open transaction.

    Returns:
        The new outbox row id.
    """
    row = statements.new_outbox_row(event, now=now)
    db.add(row)
    commit_or_flush(db, commit)
    outbox_id: str = row.id
    return outbox_id


def list_unrelayed(*, limit: int, db: Session) -> list[EventOutbox]:
    """
    Fetch the oldest not-yet-relayed outbox rows, locking them for this relayer.

    On a database with row-level locking the rows are claimed with ``FOR UPDATE
    SKIP LOCKED``, so concurrent relayers across replicas take disjoint batches
    (no single-runner lock needed) — but only for as long as the caller holds the
    transaction. :func:`jasil.jobs.relay.relay_outbox_once` therefore keeps it
    open across the whole fan-out. Where the clause is unavailable the batches can
    overlap, and the idempotent fan-out is what keeps that harmless.

    Args:
        limit: Maximum number of rows to return.
        db: Active database session, whose transaction must stay open for as long
            as the claim needs to hold.

    Returns:
        Unrelayed outbox rows, oldest-first.
    """
    stmt = statements.unrelayed_stmt(limit=limit, skip_locked=supports_skip_locked(db.bind))
    return list(db.execute(stmt).scalars().all())


def mark_relayed(outbox_id: str, *, now: datetime, db: Session, commit: bool = True) -> None:
    """
    Stamp an outbox row as relayed.

    Args:
        outbox_id: The outbox row id.
        now: Current instant.
        db: Active database session.
        commit: When True, commit immediately; when False, flush only and leave
            the stamp in the caller's open transaction, so a whole relayed batch
            lands under one commit.

    Returns:
        None.
    """
    db.execute(statements.mark_relayed_stmt(outbox_id, now=now))
    commit_or_flush(db, commit)


def delete_relayed_before(cutoff: datetime, *, db: Session, batch_size: int = pruning.PRUNE_BATCH_SIZE) -> int:
    """
    Delete relayed outbox rows older than ``cutoff``, in bounded batches.

    Only rows that have been relayed (``relayed_at`` set) and whose relay is older
    than ``cutoff`` are removed; unrelayed rows are pending work and are never
    touched. A relayed row's only remaining value is audit — the per-subscriber
    jobs it fanned out into are the source of truth — so it is safe to prune.

    Args:
        cutoff: Delete rows whose ``relayed_at`` is strictly before this instant.
        db: Active database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    return pruning.bounded_delete(
        EventOutbox,
        *statements.outbox_prune_condition(cutoff),
        db=db,
        batch_size=batch_size,
    )
