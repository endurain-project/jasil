"""Async CRUD for ``processing_jobs`` — the asynchronous twin of :mod:`jasil.jobs.crud`.

**Internal.** Not covered by the API-stability contract, and it reaches a model
at import time, so it cannot be imported before ``jasil.orm.map_models`` has run.

The correctness-critical parts of the durable queue — the claim's ``SELECT ...
FOR UPDATE SKIP LOCKED`` plus compare-and-set, the reaper's double-guarded
requeue, the dialect insert-or-ignore, the backoff arithmetic — are **not**
restated here. They live in :mod:`jasil.jobs.statements` and are shared verbatim
with the synchronous module, because two hand-maintained copies of lease and
claim logic would drift, and the drift would be silent: both copies would still
pass their own tests while disagreeing about who owns a job.

**Every function here commits the session it is given** — except where a
``commit`` flag says otherwise (``enqueue_job``, for the relay's single-
transaction fan-out) — matching the sync module exactly.

Configure the async sessionmaker with ``expire_on_commit=False``. SQLAlchemy
expires instances on commit by default, and a later attribute read on an expired
instance triggers a *lazy refresh*, which under asyncio raises
``MissingGreenlet`` rather than quietly issuing a query. The functions here are
ordered so nothing is read after a commit, but a caller holding a returned job
across its own commit would hit it.
"""

from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

import jasil._core.pruning as pruning
import jasil.jobs.schema as jobs_schema
import jasil.jobs.statements as statements
from jasil._core.dialects import supports_skip_locked
from jasil._core.sessions import commit_or_flush_async
from jasil.events import Event
from jasil.jobs.models import ProcessingJob

# Re-exported so async callers keep importing their status constants from the
# CRUD module they already use, exactly as the sync callers do.
STATUS_PENDING = statements.STATUS_PENDING
STATUS_CLAIMED = statements.STATUS_CLAIMED
STATUS_COMPLETED = statements.STATUS_COMPLETED
STATUS_DEAD_LETTER = statements.STATUS_DEAD_LETTER


async def enqueue_job(
    event: Event,
    subscriber_id: str,
    *,
    max_attempts: int,
    now: datetime,
    db: AsyncSession,
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
        db: Active async database session.
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
    await db.execute(statements.insert_ignoring_duplicate(values, _dialect_name(db)))
    await commit_or_flush_async(db, commit)
    # Returns the row only when this call inserted it: a skipped conflict leaves
    # our unique job_id absent, so ``get`` yields None for a duplicate enqueue.
    return await db.get(ProcessingJob, values["id"])


async def claim_jobs(
    *,
    worker_id: str,
    limit: int,
    lease_seconds: int,
    now: datetime,
    db: AsyncSession,
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
        db: Active async database session.

    Returns:
        The jobs this call actually claimed, oldest-available first.
    """
    id_stmt = statements.due_job_ids_stmt(now=now, limit=limit, skip_locked=supports_skip_locked(db.bind))
    job_ids = list((await db.execute(id_stmt)).scalars().all())
    if not job_ids:
        await db.commit()  # end the transaction so FOR UPDATE locks are released
        return []
    await db.execute(statements.claim_update_stmt(job_ids, worker_id=worker_id, now=now, lease_seconds=lease_seconds))
    await db.commit()
    # Read back *after* the commit, so the instances handed to the caller are
    # freshly loaded rather than expired — an expired instance would need a lazy
    # refresh, which is exactly what asyncio cannot do implicitly.
    claimed = (await db.execute(statements.claimed_jobs_stmt(job_ids, worker_id=worker_id))).scalars().all()
    return list(claimed)


async def mark_job_completed(job_id: str, *, now: datetime, db: AsyncSession) -> None:
    """
    Mark a claimed job ``completed`` and release its lease.

    Args:
        job_id: The job to complete.
        now: Current instant.
        db: Active async database session.

    Returns:
        None.
    """
    await db.execute(statements.mark_job_completed_stmt(job_id, now=now))
    await db.commit()


async def mark_job_failed(
    job_id: str,
    error_message: str,
    *,
    base_seconds: float,
    max_seconds: float,
    now: datetime,
    db: AsyncSession,
) -> str:
    """
    Record a failed attempt: reschedule with backoff, or dead-letter if exhausted.

    Args:
        job_id: The job that failed.
        error_message: The failure reason (truncated for storage).
        base_seconds: Backoff base delay.
        max_seconds: Backoff ceiling.
        now: Current instant.
        db: Active async database session.

    Returns:
        The job's new status (``pending`` when rescheduled, ``dead_letter`` when
        the attempt ceiling was reached), or the empty string when the job was
        not found.
    """
    job = await db.get(ProcessingJob, job_id)
    if job is None:
        return ""
    attempts = job.attempts
    exhausted = attempts >= job.max_attempts
    if exhausted:
        await db.execute(statements.mark_job_dead_letter_stmt(job_id, error_message, now=now))
        await db.commit()
        return STATUS_DEAD_LETTER
    await db.execute(
        statements.reschedule_stmt(
            job_id,
            error_message,
            attempts=attempts,
            base_seconds=base_seconds,
            max_seconds=max_seconds,
            now=now,
        )
    )
    await db.commit()
    return STATUS_PENDING


async def reclaim_expired_leases(*, now: datetime, db: AsyncSession, limit: int = 100) -> int:
    """
    Return jobs whose lease expired (a crashed worker) to a runnable state.

    Exhausted rows (``attempts >= max_attempts``) are dead-lettered; the rest are
    made ``pending`` and immediately available again.

    Args:
        now: Current instant.
        db: Active async database session.
        limit: Maximum number of expired leases to reclaim in one pass.

    Returns:
        The number of jobs this call reclaimed (requeued plus dead-lettered).
    """
    id_stmt = statements.expired_lease_ids_stmt(now=now, limit=limit, skip_locked=supports_skip_locked(db.bind))
    job_ids = list((await db.execute(id_stmt)).scalars().all())
    if not job_ids:
        await db.commit()
        return 0
    dead_lettered = cast(CursorResult[Any], await db.execute(statements.reap_dead_letter_stmt(job_ids, now=now)))
    requeued = cast(CursorResult[Any], await db.execute(statements.reap_requeue_stmt(job_ids, now=now)))
    await db.commit()
    return dead_lettered.rowcount + requeued.rowcount


async def get_job(job_id: str, db: AsyncSession) -> ProcessingJob | None:
    """
    Fetch a job by id.

    Args:
        job_id: The job identifier.
        db: Active async database session.

    Returns:
        The job, or ``None`` when it does not exist.
    """
    return await db.get(ProcessingJob, job_id)


async def get_jobs_summary(
    db: AsyncSession,
    *,
    hours: int = 24,
    dead_letter_limit: int = 50,
) -> jobs_schema.JobsSummary:
    """
    Aggregate ``processing_jobs`` into the admin-dashboard payload.

    Args:
        db: Active async database session.
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
        count_rows=(await db.execute(statements.subscriber_counts_stmt(window_start))).all(),
        oldest_pending=(await db.execute(statements.oldest_pending_stmt())).scalar(),
        dead_letter_rows=(await db.execute(statements.dead_letter_list_stmt(dead_letter_limit))).scalars().all(),
    )


async def replay_dead_letter_job(job_id: str, *, now: datetime, db: AsyncSession) -> bool:
    """
    Requeue a dead-lettered job for a fresh run.

    Resets the job to ``pending`` with a full attempt budget so the worker picks
    it up again. Only a job currently in ``dead_letter`` is affected.

    Args:
        job_id: The job to replay.
        now: Current instant (the new availability time).
        db: Active async database session.

    Returns:
        True when a dead-letter job was requeued; False when none matched.
    """
    job = await db.get(ProcessingJob, job_id)
    if job is None or job.status != STATUS_DEAD_LETTER:
        return False
    await db.execute(statements.replay_stmt(job_id, now=now))
    await db.commit()
    return True


async def delete_completed_jobs_before(
    cutoff: datetime,
    *,
    db: AsyncSession,
    batch_size: int = pruning.PRUNE_BATCH_SIZE,
) -> int:
    """
    Delete ``completed`` jobs older than ``cutoff``, in bounded batches.

    Only terminal ``completed`` rows are pruned. In-flight rows (``pending`` /
    ``claimed``) are never touched, and ``dead_letter`` rows are deliberately kept
    for operator review.

    Args:
        cutoff: Delete rows whose ``completed_at`` is strictly before this instant.
        db: Active async database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    return await pruning.bounded_delete_async(
        ProcessingJob,
        *statements.jobs_prune_condition(cutoff),
        db=db,
        batch_size=batch_size,
    )


def _dialect_name(db: AsyncSession) -> str:
    """Return the async session bind's dialect name, or the empty string when unbound."""
    bind = db.bind
    return bind.dialect.name if bind is not None else ""
