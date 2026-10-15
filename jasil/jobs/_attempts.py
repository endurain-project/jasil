"""Internal reads and pruning of ``job_attempts``, the per-attempt history.

The writes live in :mod:`jasil.jobs.crud`, beside the job state change each one
must share a transaction with. Everything here reads, or prunes, and is surfaced
to hosts through :mod:`jasil.admin`.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import and_, case, delete, exists, func, or_, select
from sqlalchemy.orm import Session

from jasil._core.cursors import decode_cursor, encode_cursor
from jasil._core.pruning import PRUNE_BATCH_SIZE, PRUNE_MAX_BATCHES
from jasil.jobs.models import JobAttempt, ProcessingJob
from jasil.jobs.schema import DurationBuckets, EventJobs, JobAttemptInfo, JobDetail, WorkerAttempts

#: How one attempt to run a job ended, as recorded in ``job_attempts.outcome``.
OUTCOME_COMPLETED = "completed"  # the handler succeeded and this claim recorded it
OUTCOME_FAILED = "failed"  # the handler raised; the job was rescheduled with backoff
OUTCOME_DEAD_LETTER = "dead_letter"  # the handler raised on the last attempt
OUTCOME_LEASE_EXPIRED = "lease_expired"  # the lease ran out with no result: the worker died or went silent
OUTCOME_SUPERSEDED = "superseded"  # the handler finished after its job was taken: it ran twice
OUTCOME_RELEASED = "released"  # claimed but never run; returned to pending with the attempt refunded
OUTCOME_ABANDONED = "abandoned"  # exceeded the maximum runtime; signalled and left to the reaper
OUTCOMES = (
    OUTCOME_COMPLETED,
    OUTCOME_FAILED,
    OUTCOME_DEAD_LETTER,
    OUTCOME_LEASE_EXPIRED,
    OUTCOME_SUPERSEDED,
    OUTCOME_RELEASED,
    OUTCOME_ABANDONED,
)

DEFAULT_ATTEMPT_LIMIT = 50
MAX_ATTEMPT_LIMIT = 500
MAX_EVENT_JOBS = 200
DEFAULT_WORKER_ATTEMPT_PAGE_SIZE = 100
MAX_WORKER_ATTEMPT_PAGE_SIZE = 500

# Upper bounds, in milliseconds, of the duration buckets, in order. The fixed
# boundaries are part of ``DurationBuckets``, so they are not configurable.
_BUCKET_BOUNDS_MS = (100, 1_000, 10_000, 60_000, 300_000)


@dataclass(frozen=True)
class AttemptStats:
    """Duration and double-run figures for one group of attempts."""

    avg_duration_ms: float | None
    max_duration_ms: int | None
    buckets: DurationBuckets
    superseded: int


def attempt_stats(db: Session, *group_by: Any, window_start: datetime) -> dict[tuple[Any, ...], AttemptStats]:
    """Aggregate attempts that ended since ``window_start``, per ``group_by`` key.

    Durations describe attempts that *completed*, since they say how long
    successful work takes; ``superseded`` counts every attempt that ended by
    finishing after its job was taken. Only portable aggregates are used
    (average, maximum, conditional sums), so every dialect agrees.

    Args:
        db: Active database session.
        *group_by: Columns of :class:`JobAttempt` to group by.
        window_start: Attempts that ended before this are not counted.

    Returns:
        ``{key tuple: stats}``; a group with no attempts in the window is absent.
    """
    completed = JobAttempt.outcome == OUTCOME_COMPLETED
    duration = JobAttempt.duration_ms
    lower = (None, *_BUCKET_BOUNDS_MS)
    upper = (*_BUCKET_BOUNDS_MS, None)
    bucket_columns = []
    for low, high in zip(lower, upper, strict=True):
        conditions = [completed, duration.is_not(None)]
        if low is not None:
            conditions.append(duration >= low)
        if high is not None:
            conditions.append(duration < high)
        bucket_columns.append(func.coalesce(func.sum(case((and_(*conditions), 1), else_=0)), 0))
    rows = db.execute(
        select(
            *group_by,
            func.avg(case((completed, duration))),
            func.max(case((completed, duration))),
            *bucket_columns,
            func.coalesce(func.sum(case((JobAttempt.outcome == OUTCOME_SUPERSEDED, 1), else_=0)), 0),
        )
        .where(JobAttempt.finished_at >= window_start)
        .group_by(*group_by)
    ).all()
    width = len(group_by)
    stats: dict[tuple[Any, ...], AttemptStats] = {}
    for row in rows:
        average, slowest, *counts = row[width:]
        *buckets, superseded = counts
        stats[tuple(row[:width])] = AttemptStats(
            avg_duration_ms=float(average) if average is not None else None,
            max_duration_ms=int(slowest) if slowest is not None else None,
            buckets=DurationBuckets(
                under_100ms=int(buckets[0]),
                under_1s=int(buckets[1]),
                under_10s=int(buckets[2]),
                under_1m=int(buckets[3]),
                under_5m=int(buckets[4]),
                over_5m=int(buckets[5]),
            ),
            superseded=int(superseded),
        )
    return stats


def last_worker_ids(db: Session, job_ids: list[str]) -> dict[str, str]:
    """Return the worker of each job's most recent attempt (jobs with none are absent)."""
    if not job_ids:
        return {}
    rows = db.execute(
        select(JobAttempt.job_id, JobAttempt.worker_id)
        .where(JobAttempt.job_id.in_(job_ids))
        # A released claim never ran, and shares its attempt number (and, on
        # second-resolution MySQL, its timestamp) with the claim that did.
        .order_by(
            case((func.coalesce(JobAttempt.outcome, "") == OUTCOME_RELEASED, 1), else_=0),
            JobAttempt.claimed_at.desc(),
            JobAttempt.id.desc(),
        )
    ).all()
    latest: dict[str, str] = {}
    for job_id, worker_id in rows:
        latest.setdefault(job_id, worker_id)
    return latest


def _history(db: Session, job_id: str, limit: int) -> list[JobAttemptInfo]:
    attempts = (
        db.execute(
            select(JobAttempt)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.claimed_at.desc(), JobAttempt.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [JobAttemptInfo.model_validate(attempt) for attempt in attempts]


def check_limit(limit: int, *, maximum: int, name: str) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")


def get_job_detail(
    job_id: str,
    *,
    db: Session,
    attempt_limit: int = DEFAULT_ATTEMPT_LIMIT,
    record_attempts: bool = True,
) -> JobDetail | None:
    """Return one job with its attempts, newest first; ``None`` when it does not exist."""
    check_limit(attempt_limit, maximum=MAX_ATTEMPT_LIMIT, name="attempt_limit")
    job = db.get(ProcessingJob, job_id)
    if job is None:
        return None
    detail = JobDetail.model_validate(job)
    return detail.model_copy(
        update={"history": _history(db, job_id, attempt_limit), "attempts_recorded": record_attempts}
    )


def get_event_jobs(
    event_id: str,
    *,
    db: Session,
    attempt_limit: int = 20,
    record_attempts: bool = True,
) -> EventJobs:
    """Return every subscriber's job for ``event_id``, each with its newest attempts."""
    check_limit(attempt_limit, maximum=MAX_ATTEMPT_LIMIT, name="attempt_limit")
    jobs = (
        db.execute(
            select(ProcessingJob)
            .where(ProcessingJob.event_id == event_id)
            .order_by(ProcessingJob.created_at, ProcessingJob.subscriber_id)
            .limit(MAX_EVENT_JOBS)
        )
        .scalars()
        .all()
    )
    details = [
        JobDetail.model_validate(job).model_copy(
            update={"history": _history(db, job.id, attempt_limit), "attempts_recorded": record_attempts}
        )
        for job in jobs
    ]
    return EventJobs(event_id=event_id, jobs=details, attempts_recorded=record_attempts)


def get_worker_attempts(
    instance_id: str,
    *,
    db: Session,
    limit: int = DEFAULT_WORKER_ATTEMPT_PAGE_SIZE,
    cursor: str | None = None,
    record_attempts: bool = True,
) -> WorkerAttempts:
    """Return one page of a worker instance's attempts, newest first."""
    check_limit(limit, maximum=MAX_WORKER_ATTEMPT_PAGE_SIZE, name="limit")
    statement = select(JobAttempt).where(JobAttempt.worker_id == instance_id)
    if cursor is not None:
        claimed_at, attempt_id = decode_cursor(cursor, name="worker attempts")
        statement = statement.where(
            or_(
                JobAttempt.claimed_at < claimed_at,
                and_(JobAttempt.claimed_at == claimed_at, JobAttempt.id < attempt_id),
            )
        )
    rows = list(
        db.execute(statement.order_by(JobAttempt.claimed_at.desc(), JobAttempt.id.desc()).limit(limit + 1))
        .scalars()
        .all()
    )
    has_more = len(rows) > limit
    rows = rows[:limit]
    return WorkerAttempts(
        instance_id=instance_id,
        attempts=[JobAttemptInfo.model_validate(row) for row in rows],
        next_cursor=encode_cursor(rows[-1].claimed_at, rows[-1].id) if has_more else None,
        attempts_recorded=record_attempts,
    )


def delete_attempts_before(cutoff: datetime, *, db: Session, batch_size: int = PRUNE_BATCH_SIZE) -> int:
    """Delete attempts claimed before ``cutoff``, in bounded batches.

    A live job (``pending``, ``claimed``) and a dead-lettered one keep their whole
    history, matching the rule that dead letters are kept for operator review. A
    ``completed`` job's history goes only once the job itself is past the cutoff
    (by ``completed_at``), so a job that still exists is never left with part of
    its history. Attempts whose job is gone (pruned, or never existed) are
    orphans and go.

    Args:
        cutoff: Delete attempts whose ``claimed_at`` is strictly before this.
        db: Active database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    # A job that is still being kept, or completed too recently to be pruned.
    kept = exists().where(
        ProcessingJob.id == JobAttempt.job_id,
        or_(
            ProcessingJob.status != "completed",
            ProcessingJob.completed_at.is_(None),
            ProcessingJob.completed_at >= cutoff,
        ),
    )
    total = 0
    for _ in range(PRUNE_MAX_BATCHES):
        ids = list(
            db.execute(select(JobAttempt.id).where(JobAttempt.claimed_at < cutoff, ~kept).limit(batch_size))
            .scalars()
            .all()
        )
        if not ids:
            break
        db.execute(delete(JobAttempt).where(JobAttempt.id.in_(ids)))
        db.commit()
        total += len(ids)
        if len(ids) < batch_size:
            break
    return total
