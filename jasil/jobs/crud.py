"""CRUD for ``processing_jobs`` — the durable, database-as-truth work queue.

**Internal.** Not covered by the API-stability contract, and it reaches a model
at import time, so it cannot be imported before ``jasil.orm.map_models`` has run.
Hosts wanting the dashboard aggregates or dead-letter replay should use
:mod:`jasil.admin`, which is importable from anywhere and opens its own session.

**Every function here commits the session it is given** — except where a
``commit`` flag says otherwise (``enqueue_job``, for the relay's single-
transaction fan-out). The claim, the terminal state writes and the reaper all
have to be durable before the caller moves on, so they are not optional. Pass a
session JASIL owns, not one carrying a caller's uncommitted work.

Nothing here builds a query. The claim's ``SELECT ... FOR UPDATE SKIP LOCKED``
plus compare-and-set, the reaper's double-guarded requeue and the dialect
insert-or-ignore all live in :mod:`jasil.jobs.statements`, so this module and its
async twin (:mod:`jasil.jobs.crud_async`) execute the same SQL and cannot drift.
Callers pass an explicit ``now`` (from the ``ClockProvider``) so lease, backoff,
and reaping are deterministic under test.
"""

from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import CursorResult
from sqlalchemy.orm import Session

import jasil._core.pruning as pruning
import jasil.jobs.schema as jobs_schema
import jasil.jobs.statements as statements
from jasil._core.dialects import supports_skip_locked
from jasil._core.sessions import commit_or_flush
from jasil.events import Event
from jasil.jobs.models import ProcessingJob

# Re-exported so callers (the runner's dead-letter check, the admin surface) keep
# importing their status constants from the CRUD module they already use.
STATUS_PENDING = statements.STATUS_PENDING
STATUS_CLAIMED = statements.STATUS_CLAIMED
STATUS_COMPLETED = statements.STATUS_COMPLETED
STATUS_DEAD_LETTER = statements.STATUS_DEAD_LETTER


def enqueue_job(
    event: Event,
    subscriber_id: str,
    *,
    max_attempts: int,
    now: datetime,
    db: Session,
    available_at: datetime | None = None,
    commit: bool = True,
) -> ProcessingJob | None:
    """
    Enqueue one durable job for ``(event, subscriber_id)``, idempotently.

    The ``(event_id, subscriber_id)`` uniqueness makes a repeat enqueue a no-op,
    so the same subscriber never runs twice for the same event (idempotent
    consumer). Because each call mints a fresh job id, the freshly inserted row
    is returned only when this call actually inserted it.

    Args:
        event: The originating event envelope.
        subscriber_id: The durable subscriber this job runs.
        max_attempts: Attempt ceiling before the job is dead-lettered.
        now: Current instant (used for created/updated/available timestamps).
        db: Active database session.
        available_at: Earliest claim instant; defaults to ``now``.
        commit: When True, commit immediately; when False, flush only and leave
            the row in the caller's open transaction (the relay fans a whole
            batch out under one commit).

    Returns:
        The inserted job, or ``None`` when a job for this ``(event, subscriber)``
        already existed.
    """
    values = statements.job_values(
        event,
        subscriber_id,
        max_attempts=max_attempts,
        now=now,
        available_at=available_at,
    )
    db.execute(statements.insert_ignoring_duplicate(values, _dialect_name(db)))
    commit_or_flush(db, commit)
    # Returns the row only when this call inserted it: a skipped conflict leaves
    # our unique job_id absent, so ``get`` yields None for a duplicate enqueue.
    return db.get(ProcessingJob, values["id"])


def claim_jobs(
    *,
    worker_id: str,
    limit: int,
    lease_seconds: int,
    now: datetime,
    db: Session,
) -> list[ProcessingJob]:
    """
    Atomically claim up to ``limit`` due jobs, taking a time-bounded lease.

    Selects ``pending`` rows whose ``available_at`` has passed and marks them
    ``claimed`` (incrementing ``attempts`` and stamping the lease). Where the
    dialect supports it the rows are locked with ``FOR UPDATE SKIP LOCKED`` so
    concurrent workers select disjoint sets; where it does not, the update is a
    compare-and-set on ``status = 'pending'`` and only the worker that wins the
    race is handed the rows. The attempt is counted at claim time so a worker that
    crashes mid-run still consumes an attempt, bounding crash loops.

    Args:
        worker_id: Identifier of the claiming worker (the lease holder).
        limit: Maximum number of jobs to claim.
        lease_seconds: Lease duration; the reaper requeues jobs past it.
        now: Current instant.
        db: Active database session.

    Returns:
        The jobs this call actually claimed, oldest-available first.
    """
    id_stmt = statements.due_job_ids_stmt(now=now, limit=limit, skip_locked=supports_skip_locked(db.bind))
    job_ids = list(db.execute(id_stmt).scalars().all())
    if not job_ids:
        db.commit()  # end the transaction so FOR UPDATE locks are released
        return []
    db.execute(statements.claim_update_stmt(job_ids, worker_id=worker_id, now=now, lease_seconds=lease_seconds))
    db.commit()
    claimed = db.execute(statements.claimed_jobs_stmt(job_ids, worker_id=worker_id)).scalars().all()
    return list(claimed)


def mark_job_completed(job_id: str, *, now: datetime, db: Session) -> None:
    """
    Mark a claimed job ``completed`` and release its lease.

    Args:
        job_id: The job to complete.
        now: Current instant.
        db: Active database session.

    Returns:
        None.
    """
    db.execute(statements.mark_job_completed_stmt(job_id, now=now))
    db.commit()


def mark_job_failed(
    job_id: str,
    error_message: str,
    *,
    base_seconds: float,
    max_seconds: float,
    now: datetime,
    db: Session,
) -> str:
    """
    Record a failed attempt: reschedule with backoff, or dead-letter if exhausted.

    Args:
        job_id: The job that failed.
        error_message: The failure reason (truncated for storage).
        base_seconds: Backoff base delay.
        max_seconds: Backoff ceiling.
        now: Current instant.
        db: Active database session.

    Returns:
        The job's new status (``pending`` when rescheduled, ``dead_letter`` when
        the attempt ceiling was reached), or the empty string when the job was
        not found.
    """
    job = db.get(ProcessingJob, job_id)
    if job is None:
        return ""
    if job.attempts >= job.max_attempts:
        db.execute(statements.mark_job_dead_letter_stmt(job_id, error_message, now=now))
        db.commit()
        return STATUS_DEAD_LETTER
    db.execute(
        statements.reschedule_stmt(
            job_id,
            error_message,
            attempts=job.attempts,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
            now=now,
        )
    )
    db.commit()
    return STATUS_PENDING


def reclaim_expired_leases(*, now: datetime, db: Session, limit: int = 100) -> int:
    """
    Return jobs whose lease expired (a crashed worker) to a runnable state.

    Exhausted rows (``attempts >= max_attempts``) are dead-lettered; the rest are
    made ``pending`` and immediately available again.

    Args:
        now: Current instant.
        db: Active database session.
        limit: Maximum number of expired leases to reclaim in one pass.

    Returns:
        The number of jobs this call reclaimed (requeued plus dead-lettered).
    """
    id_stmt = statements.expired_lease_ids_stmt(now=now, limit=limit, skip_locked=supports_skip_locked(db.bind))
    job_ids = list(db.execute(id_stmt).scalars().all())
    if not job_ids:
        db.commit()
        return 0
    dead_lettered = cast(CursorResult[Any], db.execute(statements.reap_dead_letter_stmt(job_ids, now=now)))
    requeued = cast(CursorResult[Any], db.execute(statements.reap_requeue_stmt(job_ids, now=now)))
    db.commit()
    return dead_lettered.rowcount + requeued.rowcount


def get_job(job_id: str, db: Session) -> ProcessingJob | None:
    """
    Fetch a job by id.

    Args:
        job_id: The job identifier.
        db: Active database session.

    Returns:
        The job, or ``None`` when it does not exist.
    """
    return db.get(ProcessingJob, job_id)


def get_jobs_summary(db: Session, *, hours: int = 24, dead_letter_limit: int = 50) -> jobs_schema.JobsSummary:
    """
    Aggregate ``processing_jobs`` into the admin-dashboard payload.

    Args:
        db: Active database session.
        hours: Look-back window for the status/subscriber counts.
        dead_letter_limit: Maximum dead-letter jobs to return for inspection.

    Returns:
        The durable-jobs summary — window counts, per-subscriber breakdown,
        oldest pending age, and the current dead-letter queue.
    """
    now = datetime.now(UTC)
    window_start = now - timedelta(hours=hours)
    return statements.build_jobs_summary(
        hours=hours,
        now=now,
        count_rows=db.execute(statements.subscriber_counts_stmt(window_start)).all(),
        oldest_pending=db.execute(statements.oldest_pending_stmt()).scalar(),
        dead_letter_rows=db.execute(statements.dead_letter_list_stmt(dead_letter_limit)).scalars().all(),
    )


def replay_dead_letter_job(job_id: str, *, now: datetime, db: Session) -> bool:
    """
    Requeue a dead-lettered job for a fresh run.

    Resets the job to ``pending`` with a full attempt budget so the worker picks
    it up again. Only a job currently in ``dead_letter`` is affected.

    Args:
        job_id: The job to replay.
        now: Current instant (the new availability time).
        db: Active database session.

    Returns:
        True when a dead-letter job was requeued; False when none matched.
    """
    job = db.get(ProcessingJob, job_id)
    if job is None or job.status != STATUS_DEAD_LETTER:
        return False
    db.execute(statements.replay_stmt(job_id, now=now))
    db.commit()
    return True


def delete_completed_jobs_before(cutoff: datetime, *, db: Session, batch_size: int = pruning.PRUNE_BATCH_SIZE) -> int:
    """
    Delete ``completed`` jobs older than ``cutoff``, in bounded batches.

    Only terminal ``completed`` rows are pruned. In-flight rows (``pending`` /
    ``claimed``) are never touched, and ``dead_letter`` rows are deliberately kept
    for operator review (they are rare and human-actionable). Rows whose
    ``completed_at`` is older than the retention window are removed; the relayed
    outbox row won't re-create them (idempotent fan-out), so this is safe.

    Args:
        cutoff: Delete rows whose ``completed_at`` is strictly before this instant.
        db: Active database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    return pruning.bounded_delete(
        ProcessingJob,
        *statements.jobs_prune_condition(cutoff),
        db=db,
        batch_size=batch_size,
    )


def _dialect_name(db: Session) -> str:
    """Return the session bind's dialect name, or the empty string when unbound."""
    bind = db.bind
    return bind.dialect.name if bind is not None else ""
