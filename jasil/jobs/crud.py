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

Every query is portable, so the same code runs on PostgreSQL, MySQL and SQLite.
The claim takes ``SELECT ... FOR UPDATE SKIP LOCKED`` wherever the dialect
supports it (see :func:`jasil._core.dialects.supports_skip_locked`) so concurrent
workers never grab the same row; where it does not, the clause is omitted and a
single worker is still correct. Callers pass an explicit ``now`` (from the
``ClockProvider``) so lease, backoff, and reaping are deterministic under test.
"""

import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import CursorResult, and_, func, insert, or_, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

import jasil._core.pruning as pruning
import jasil.jobs.backoff as jobs_backoff
import jasil.jobs.schema as jobs_schema
from jasil._core.dialects import supports_skip_locked
from jasil._core.limits import MAX_STORED_ERROR_LENGTH, fit_length
from jasil._core.sessions import commit_or_flush
from jasil._core.timestamps import age_seconds, as_utc
from jasil.events import Event
from jasil.jobs._attempts import (
    OUTCOME_ABANDONED,
    OUTCOME_COMPLETED,
    OUTCOME_DEAD_LETTER,
    OUTCOME_DEFERRED,
    OUTCOME_FAILED,
    OUTCOME_LEASE_EXPIRED,
    OUTCOME_RELEASED,
    OUTCOME_SUPERSEDED,
    attempt_stats,
    last_worker_ids,
)
from jasil.jobs.models import JobAttempt, ProcessingJob
from jasil.jobs.registry import DEFAULT_QUEUE, normalize_queue_selector, validate_queue_name

STATUS_PENDING = "pending"
STATUS_CLAIMED = "claimed"
STATUS_COMPLETED = "completed"
STATUS_DEAD_LETTER = "dead_letter"
#: Not a stored status: what :func:`mark_job_deferred` returns for a job it rescheduled.
STATUS_DEFERRED = "deferred"
#: Not a stored status: what :func:`mark_job_deferred` returns when the job has used its deferrals.
STATUS_DEFERRALS_EXHAUSTED = "deferrals_exhausted"

_CONFLICT_KEYS = ["event_id", "subscriber_id"]


def insert_job(
    event: Event,
    subscriber_id: str,
    *,
    queue: str = DEFAULT_QUEUE,
    max_attempts: int,
    now: datetime,
    db: Session,
    available_at: datetime | None = None,
    commit: bool = True,
) -> str:
    """Insert one job like :func:`enqueue_job`, returning the id it was given.

    For a caller that does not need to know whether the row is new, such as the relay fanning a batch out:
    unlike :func:`enqueue_job` it issues no ``SELECT`` per job. When a job for ``(event, subscriber_id)``
    already existed nothing is inserted and the id names no row.
    """
    validate_queue_name(queue)
    job_id = str(uuid.uuid4())
    values = {
        "id": job_id,
        "queue": queue,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "subscriber_id": subscriber_id,
        "source": event.source,
        "event_timestamp": event.timestamp,
        "payload": event.payload,
        "schema_version": event.schema_version,
        "job_metadata": event.metadata or None,
        "status": STATUS_PENDING,
        "attempts": 0,
        "max_attempts": max_attempts,
        "available_at": available_at or now,
        "created_at": now,
        "updated_at": now,
    }
    db.execute(_insert_ignoring_duplicate(values, db))
    commit_or_flush(db, commit)
    return job_id


def enqueue_job(
    event: Event,
    subscriber_id: str,
    *,
    queue: str = DEFAULT_QUEUE,
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
        queue: Named queue eligible workers must select; defaults to ``default``.
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
    # Returns the row only when this call inserted it: a skipped conflict leaves
    # our unique job_id absent, so ``get`` yields None for a duplicate enqueue.
    job_id = insert_job(
        event,
        subscriber_id,
        queue=queue,
        max_attempts=max_attempts,
        now=now,
        db=db,
        available_at=available_at,
        commit=commit,
    )
    return db.get(ProcessingJob, job_id)


def claim_jobs(
    *,
    worker_id: str,
    limit: int,
    lease_seconds: int,
    now: datetime,
    db: Session,
    queues: Iterable[str] | None = None,
    queue_cursor: str | None = None,
    default_queue_subscribers: Iterable[str] | None = None,
    excluded_default_queue_subscribers: Iterable[str] | None = None,
    record_attempts: bool = True,
) -> list[ProcessingJob]:
    """
    Atomically claim up to ``limit`` due jobs, taking a time-bounded lease.

    Selects ``pending`` rows whose ``available_at`` has passed and marks them
    ``claimed`` (incrementing ``attempts`` and stamping the lease). Where the
    dialect supports it the rows are locked with ``FOR UPDATE SKIP LOCKED`` so
    concurrent workers select disjoint sets; where it does not, the update is a
    compare-and-set on ``status = 'pending'`` and only the worker that wins the
    race is handed the rows. The attempt is counted at claim time so a worker that
    crashes mid-run still consumes an attempt, bounding crash loops. ``queues``
    is an optional allowlist; omitting it preserves all-queue consumption. The
    cursor rotates queue ordering between batches so a backlog on one queue
    cannot permanently starve another.

    Args:
        worker_id: Identifier of the claiming worker (the lease holder).
        limit: Maximum number of jobs to claim.
        lease_seconds: Lease duration; the reaper requeues jobs past it.
        now: Current instant.
        db: Active database session.
        queues: Optional non-empty queue allowlist.
        queue_cursor: Queue handled last by this worker, used for fair rotation.
        default_queue_subscribers: Subscribers whose rows from pre-queue writers
            may still carry the database-side ``default`` queue.
        excluded_default_queue_subscribers: Registered subscribers assigned to
            unselected queues. When ``default`` is selected, their legacy rows
            must remain available to the worker for their assigned queue.
        record_attempts: Open one ``job_attempts`` row per claimed job, in the
            claim's own transaction, so the history can never disagree with the
            claim.

    Returns:
        The jobs this call actually claimed, in rotation order: queues after
        ``queue_cursor`` first, wrapping, and oldest-available first within a queue.
    """
    selected_queues = normalize_queue_selector(queues)
    if queue_cursor is not None:
        validate_queue_name(queue_cursor)
    filters = [ProcessingJob.status == STATUS_PENDING, ProcessingJob.available_at <= now]
    if selected_queues is not None:
        queue_filter = ProcessingJob.queue.in_(selected_queues)
        legacy_subscribers = tuple(dict.fromkeys(default_queue_subscribers or ()))
        excluded_legacy_subscribers = tuple(dict.fromkeys(excluded_default_queue_subscribers or ()))
        if DEFAULT_QUEUE in selected_queues and excluded_legacy_subscribers:
            queue_filter = and_(
                queue_filter,
                or_(
                    ProcessingJob.queue != DEFAULT_QUEUE,
                    ProcessingJob.subscriber_id.not_in(excluded_legacy_subscribers),
                ),
            )
        elif legacy_subscribers and DEFAULT_QUEUE not in selected_queues:
            queue_filter = or_(
                queue_filter,
                and_(
                    ProcessingJob.queue == DEFAULT_QUEUE,
                    ProcessingJob.subscriber_id.in_(legacy_subscribers),
                ),
            )
        filters.append(queue_filter)
    skip_locked = supports_skip_locked(db.bind)
    job_ids = _select_due_ids(filters, queue_cursor=queue_cursor, limit=limit, skip_locked=skip_locked, db=db)
    if not job_ids:
        db.commit()  # end the transaction so FOR UPDATE locks are released
        return []
    db.execute(
        update(ProcessingJob)
        .where(
            ProcessingJob.id.in_(job_ids),
            # Compare-and-set. Without SKIP LOCKED two workers can select the same
            # ids, and only the one that still finds them pending may take them.
            ProcessingJob.status == STATUS_PENDING,
        )
        .values(
            status=STATUS_CLAIMED,
            attempts=ProcessingJob.attempts + 1,
            locked_by=worker_id,
            locked_at=now,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
            updated_at=now,
        )
    )
    if record_attempts:
        _open_attempts(job_ids, worker_id=worker_id, now=now, db=db)
    db.commit()
    claimed = (
        db.execute(
            select(ProcessingJob).where(
                ProcessingJob.id.in_(job_ids),
                ProcessingJob.status == STATUS_CLAIMED,
                # The lease we just took. ``job_ids`` were all ``pending`` at
                # select time and the update was a compare-and-set on that, so
                # these three together name exactly the rows *this* call
                # transitioned: a row a competing worker won carries its id, and
                # a row this worker claimed in an earlier round was never
                # ``pending`` to be selected here. Deliberately not also matching
                # ``locked_at == now`` — MySQL's DATETIME keeps whole seconds and
                # rounds anything finer, so that equality silently matched
                # nothing and the worker claimed batches it then never ran.
                ProcessingJob.locked_by == worker_id,
            )
        )
        .scalars()
        .all()
    )
    position = {job_id: index for index, job_id in enumerate(job_ids)}
    return sorted(claimed, key=lambda job: position[job.id])


def backfill_legacy_queues(assignments: dict[str, str], *, db: Session) -> int:
    """Move pending ``default`` rows to the queue their subscriber is assigned to.

    Rows written before named queues existed (or by an older writer) carry the
    database default. Retagging them once, from the registry, lets claims filter
    on ``queue`` alone instead of patching those rows in the hot path. Only
    ``pending`` rows move: a claimed row already has an attempt history that names
    its queue. Commits.

    Args:
        assignments: ``{subscriber_id: queue}`` of the registered subscribers.
        db: Active database session.

    Returns:
        The number of rows retagged.
    """
    by_queue: dict[str, list[str]] = defaultdict(list)
    for subscriber_id, queue in assignments.items():
        if queue != DEFAULT_QUEUE:
            by_queue[queue].append(subscriber_id)
    moved = 0
    for queue, subscriber_ids in by_queue.items():
        result = cast(
            CursorResult[Any],
            db.execute(
                update(ProcessingJob)
                .where(
                    ProcessingJob.status == STATUS_PENDING,
                    ProcessingJob.queue == DEFAULT_QUEUE,
                    ProcessingJob.subscriber_id.in_(subscriber_ids),
                )
                .values(queue=queue)
            ),
        )
        moved += result.rowcount
    db.commit()
    return moved


def _open_attempts(job_ids: list[str], *, worker_id: str, now: datetime, db: Session) -> None:
    """Open one attempt row for each job this worker just claimed.

    Reads the claimed rows back *before* the claim commits, so the history rows
    join the claim's transaction, but selects plain columns rather than entities:
    entities loaded here would be expired by that commit and reloaded one by one.
    The predicate is the one the claim's own read-back uses.
    """
    rows = db.execute(
        select(
            ProcessingJob.id,
            ProcessingJob.event_id,
            ProcessingJob.event_type,
            ProcessingJob.subscriber_id,
            ProcessingJob.queue,
            ProcessingJob.attempts,
        ).where(
            ProcessingJob.id.in_(job_ids),
            ProcessingJob.status == STATUS_CLAIMED,
            ProcessingJob.locked_by == worker_id,
        )
    ).all()
    if not rows:
        return
    db.execute(
        insert(JobAttempt),
        [
            {
                "id": str(uuid.uuid4()),
                "job_id": job_id,
                "event_id": event_id,
                "event_type": event_type,
                "subscriber_id": subscriber_id,
                "queue": queue,
                "attempt": attempt,
                "worker_id": worker_id,
                "claimed_at": now,
            }
            for job_id, event_id, event_type, subscriber_id, queue, attempt in rows
        ],
    )


def _finish_attempt(
    db: Session,
    *,
    job_id: str,
    worker_id: str,
    attempt: int,
    now: datetime,
    outcome: str,
    duration_ms: int | None = None,
    error: str | None = None,
    owned: bool,
) -> None:
    """Close the attempt row of one claim, in the caller's transaction.

    A worker that still owned the claim closes its row with ``outcome``. One that
    did not (the lease expired and the job was taken) finished *late*: its row is
    either still open or was closed ``lease_expired`` by the reaper, and becomes
    ``superseded``, keeping the late duration and error: it is the only durable
    record that the handler ran twice.

    A row closed ``abandoned`` (the handler overran ``max_runtime_seconds``) is
    settled by whichever happens to the job next: if the worker still owns the
    claim the handler's own result replaces ``abandoned``, and if the job was
    taken meanwhile the row becomes ``superseded``, so an overrun handler that
    ran twice is counted like any other.

    ``superseded`` means the job really was claimed again. A handler that outlasted
    its lease while the job sat requeued or dead-lettered, with no later claim, ran
    only once: its row keeps the reaper's ``lease_expired`` outcome and just gains
    the late duration and error.

    A dead-letter replay restarts ``attempts``, so one worker can hold two rows
    for the same ``(job, attempt)``; the newest matching row is the one settled.
    """
    settleable = or_(JobAttempt.finished_at.is_(None), JobAttempt.outcome == OUTCOME_ABANDONED)
    finished_at: Any = now
    if not owned:
        latest_attempt = db.execute(select(ProcessingJob.attempts).where(ProcessingJob.id == job_id)).scalar()
        if latest_attempt is not None and latest_attempt > attempt:
            outcome = OUTCOME_SUPERSEDED
        else:
            outcome = OUTCOME_LEASE_EXPIRED
            finished_at = func.coalesce(JobAttempt.finished_at, now)
        settleable = or_(settleable, JobAttempt.outcome == OUTCOME_LEASE_EXPIRED)
    target = db.execute(
        select(JobAttempt.id)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.worker_id == worker_id,
            JobAttempt.attempt == attempt,
            settleable,
        )
        .order_by(JobAttempt.claimed_at.desc(), JobAttempt.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if target is None:
        return
    db.execute(
        update(JobAttempt)
        .where(JobAttempt.id == target)
        .values(outcome=outcome, finished_at=finished_at, duration_ms=duration_ms, error=error)
        # Attempt rows are never read back through this session, so skip the sync.
        .execution_options(synchronize_session=False)
    )


def close_attempts(
    job_attempts: dict[str, int],
    *,
    worker_id: str,
    outcome: str,
    now: datetime,
    db: Session,
) -> int:
    """
    Close the open attempt rows of claims this worker will not finish.

    Used for jobs that are released unstarted and for a job abandoned past its
    maximum runtime; each is matched by job, worker and the attempt it took.
    Commits.

    Args:
        job_attempts: ``{job_id: attempt number of the claim}``.
        worker_id: The worker that holds the claims.
        outcome: One of :data:`OUTCOMES`.
        now: The instant the attempts ended.
        db: Active database session.

    Returns:
        The number of attempt rows closed.
    """
    closed = _close_open(job_attempts, worker_id=worker_id, outcome=outcome, now=now, db=db)
    db.commit()
    return closed


def _close_open(job_attempts: dict[str, int], *, worker_id: str, outcome: str, now: datetime, db: Session) -> int:
    if not job_attempts:
        return 0
    result = cast(
        CursorResult[Any],
        db.execute(
            update(JobAttempt)
            .where(
                _claims_match(JobAttempt.job_id, JobAttempt.attempt, job_attempts),
                JobAttempt.worker_id == worker_id,
                JobAttempt.finished_at.is_(None),
            )
            .values(outcome=outcome, finished_at=now)
            .execution_options(synchronize_session=False)
        ),
    )
    return result.rowcount


def mark_attempt_started(job_id: str, *, worker_id: str, attempt: int, now: datetime, db: Session) -> None:
    """Stamp the moment a claim's handler began. Commits.

    Args:
        job_id: The job being run.
        worker_id: The worker running it.
        attempt: The attempt number of the claim.
        now: When the handler began.
        db: Active database session.
    """
    db.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.worker_id == worker_id,
            JobAttempt.attempt == attempt,
            JobAttempt.finished_at.is_(None),
        )
        .values(started_at=now)
        .execution_options(synchronize_session=False)
    )
    db.commit()


def _select_due_ids(
    filters: list[Any],
    *,
    queue_cursor: str | None,
    limit: int,
    skip_locked: bool,
    db: Session,
) -> list[str]:
    """Select up to ``limit`` due job ids, queues rotating after ``queue_cursor``.

    Two range scans rather than one ``ORDER BY CASE ...``: an expression cannot be
    served by an index, so a single ordered query would sort every due row on
    each poll. Each scan is ``queue, available_at`` ordered within ``status = 'pending'`` and so rides
    ``idx_processing_jobs_queue_claim``; the second wraps around to the queues at
    or before the cursor only when the first did not fill the batch.
    """
    ranges: list[list[Any]] = [[]]
    if queue_cursor is not None:
        ranges = [[ProcessingJob.queue > queue_cursor], [ProcessingJob.queue <= queue_cursor]]
    job_ids: list[str] = []
    for bounds in ranges:
        remaining = limit - len(job_ids)
        if remaining <= 0:
            break
        statement = (
            select(ProcessingJob.id)
            .where(*filters, *bounds)
            .order_by(ProcessingJob.queue, ProcessingJob.available_at, ProcessingJob.created_at, ProcessingJob.id)
            .limit(remaining)
        )
        if skip_locked:  # pragma: no cover - server-side locking, not exercised on SQLite
            statement = statement.with_for_update(skip_locked=True)
        job_ids.extend(db.execute(statement).scalars().all())
    return job_ids


def renew_leases(
    job_attempts: dict[str, int],
    *,
    worker_id: str,
    lease_seconds: int,
    now: datetime,
    db: Session,
) -> set[str]:
    """
    Extend the lease of claimed jobs this worker still holds.

    A batch is claimed under one lease but run one job after another, so a late
    job would otherwise start with most of its lease already spent, or expired and
    requeued to another worker while this one still runs it. Only rows still
    ``claimed`` by ``worker_id`` at the given attempt are touched: a job the reaper
    already requeued is never taken back, even when this worker has since claimed
    it again (a later attempt, whose lease is not this snapshot's to renew).

    Args:
        job_attempts: The jobs to renew, as ``{job_id: attempt number of the
            claim being renewed}``.
        worker_id: The lease holder.
        lease_seconds: New lease duration from ``now``.
        now: Current instant.
        db: Active database session.

    Returns:
        The ids whose lease was renewed. A job missing from the result is no
        longer held by this worker and must not be run.
    """
    if not job_attempts:
        return set()
    held = _held_claims(job_attempts, worker_id=worker_id)
    db.execute(
        update(ProcessingJob)
        .where(*held)
        .values(lease_expires_at=now + timedelta(seconds=lease_seconds), updated_at=now)
    )
    # No ``UPDATE ... RETURNING``: MySQL and MariaDB do not support it. Same
    # transaction, so the rows read back are the ones just renewed.
    ids = set(db.execute(select(ProcessingJob.id).where(*held)).scalars().all())
    db.commit()
    return ids


def release_leases(
    job_attempts: dict[str, int],
    *,
    worker_id: str,
    now: datetime,
    db: Session,
    record_attempts: bool = True,
) -> int:
    """
    Return claimed jobs this worker will not run to ``pending``, at no attempt cost.

    A batch is claimed up front, and a worker that stops early (a job lost its
    lease, or ran past ``max_runtime_seconds``) would otherwise leave the rest
    waiting out a lease it keeps renewing, or expiring into a requeue that counts
    as an attempt for work that never started. Releasing returns each row to
    ``pending``, immediately available, with the attempt this claim took refunded.
    Only rows still ``claimed`` by ``worker_id`` at the given attempt are touched,
    so a job already requeued and claimed again elsewhere is left alone.

    Args:
        job_attempts: The jobs to release, as ``{job_id: attempt number of the
            claim being released}``.
        worker_id: The lease holder.
        now: Current instant; the jobs are available again from here.
        db: Active database session.
        record_attempts: Close the released claims' attempt rows as ``released``.

    Returns:
        The number of jobs released.
    """
    if not job_attempts:
        return 0
    released = cast(
        CursorResult[Any],
        db.execute(
            update(ProcessingJob)
            .where(*_held_claims(job_attempts, worker_id=worker_id))
            .values(
                status=STATUS_PENDING,
                attempts=ProcessingJob.attempts - 1,
                available_at=now,
                updated_at=now,
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
            )
        ),
    )
    # Closes by (job, worker, attempt, still open), not by the rows just released:
    # a claim this worker no longer holds had its attempt closed by whoever took
    # it (the reaper, a late completion), so those rows are not matched again.
    if record_attempts and released.rowcount:
        _close_open(job_attempts, worker_id=worker_id, outcome=OUTCOME_RELEASED, now=now, db=db)
    db.commit()
    return released.rowcount


def _claims_match(job_id_column: Any, attempt_column: Any, job_attempts: dict[str, int]) -> Any:
    """Match each ``{job_id: attempt}`` pair, with one term per distinct attempt rather than per job.

    A term per job makes the condition as deep as the batch, which SQLite's expression-depth limit refuses at
    about a thousand jobs. Attempt numbers are few (at most ``max_attempts``), so this stays small.
    """
    by_attempt: dict[int, list[str]] = {}
    for job_id, attempt in job_attempts.items():
        by_attempt.setdefault(attempt, []).append(job_id)
    return or_(*(and_(attempt_column == attempt, job_id_column.in_(ids)) for attempt, ids in by_attempt.items()))


def _held_claims(job_attempts: dict[str, int], *, worker_id: str) -> list[Any]:
    """Match the claims ``worker_id`` still holds, each at the attempt it took."""
    return [
        _claims_match(ProcessingJob.id, ProcessingJob.attempts, job_attempts),
        ProcessingJob.status == STATUS_CLAIMED,
        ProcessingJob.locked_by == worker_id,
    ]


def mark_job_completed(
    job_id: str,
    *,
    worker_id: str,
    attempt: int,
    now: datetime,
    db: Session,
    duration_ms: int | None = None,
    record_attempt: bool = True,
) -> bool:
    """
    Mark a claimed job ``completed`` and release its lease.

    The attempt row is closed in the same transaction: ``completed`` when this
    exact claim was completed, ``superseded`` when the handler finished after its
    job had been taken from it (it ran twice).

    Args:
        job_id: The job to complete.
        worker_id: Worker that owns the claim being completed.
        attempt: Claim generation being completed.
        now: Current instant.
        db: Active database session.
        duration_ms: Handler wall time, recorded on the attempt.
        record_attempt: Close the claim's ``job_attempts`` row.

    Returns:
        True when this exact claim was completed; False when ownership was lost.
    """
    completed = cast(
        CursorResult[Any],
        db.execute(
            update(ProcessingJob)
            .where(*_owned_claim(job_id, worker_id=worker_id, attempt=attempt))
            .values(
                status=STATUS_COMPLETED,
                completed_at=now,
                updated_at=now,
                last_error=None,
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
            )
        ),
    )
    owned = completed.rowcount == 1
    if record_attempt:
        _finish_attempt(
            db,
            job_id=job_id,
            worker_id=worker_id,
            attempt=attempt,
            now=now,
            outcome=OUTCOME_COMPLETED,
            duration_ms=duration_ms,
            owned=owned,
        )
    db.commit()
    return owned


def _owned_claim(job_id: str, *, worker_id: str, attempt: int) -> tuple[Any, ...]:
    """Match one exact claim generation so a stale worker cannot finalize its replacement."""
    return (
        ProcessingJob.id == job_id,
        ProcessingJob.status == STATUS_CLAIMED,
        ProcessingJob.locked_by == worker_id,
        ProcessingJob.attempts == attempt,
    )


def mark_job_failed(
    job_id: str,
    error_message: str,
    *,
    worker_id: str,
    attempt: int,
    base_seconds: float,
    max_seconds: float,
    now: datetime,
    db: Session,
    duration_ms: int | None = None,
    record_attempt: bool = True,
) -> str:
    """
    Record a failed attempt if the caller still owns that exact claim.

    The attempt row is closed in the same transaction: ``failed`` or
    ``dead_letter`` for the job's new state, or ``superseded`` when the handler
    failed after its job had been taken from it.

    Args:
        job_id: The job that failed.
        error_message: The failure reason (truncated for storage).
        worker_id: Worker that owns the failed claim.
        attempt: Claim generation that failed.
        base_seconds: Backoff base delay.
        max_seconds: Backoff ceiling.
        now: Current instant.
        db: Active database session.
        duration_ms: Handler wall time, recorded on the attempt.
        record_attempt: Close the claim's ``job_attempts`` row.

    Returns:
        The job's new status (``pending`` when rescheduled, ``dead_letter`` when
        the attempt ceiling was reached), or the empty string when ownership was
        lost or the job was not found.
    """
    ownership = _owned_claim(job_id, worker_id=worker_id, attempt=attempt)
    truncated = fit_length(error_message, MAX_STORED_ERROR_LENGTH)
    status = ""
    attempt_state = db.execute(
        select(ProcessingJob.attempts, ProcessingJob.deferrals, ProcessingJob.max_attempts).where(*ownership)
    ).one_or_none()
    if attempt_state is not None:
        # A deferred claim is not an attempt, so it neither spends the budget nor
        # lengthens the backoff.
        claims, deferrals, max_attempts = attempt_state
        attempts = claims - deferrals
        if attempts >= max_attempts:
            new_status = STATUS_DEAD_LETTER
            values: dict[str, Any] = {"completed_at": now}
        else:
            delay = jobs_backoff.backoff_seconds(attempts, base_seconds=base_seconds, max_seconds=max_seconds)
            new_status = STATUS_PENDING
            values = {"available_at": now + timedelta(seconds=delay)}
        failed = cast(
            CursorResult[Any],
            db.execute(
                update(ProcessingJob)
                .where(*ownership)
                .values(
                    status=new_status,
                    last_error=truncated,
                    updated_at=now,
                    locked_by=None,
                    locked_at=None,
                    lease_expires_at=None,
                    **values,
                )
            ),
        )
        if failed.rowcount == 1:
            status = new_status
    if record_attempt:
        _finish_attempt(
            db,
            job_id=job_id,
            worker_id=worker_id,
            attempt=attempt,
            now=now,
            outcome=OUTCOME_DEAD_LETTER if status == STATUS_DEAD_LETTER else OUTCOME_FAILED,
            duration_ms=duration_ms,
            error=truncated,
            owned=bool(status),
        )
    db.commit()
    return status


def mark_job_deferred(
    job_id: str,
    reason: str,
    *,
    worker_id: str,
    attempt: int,
    retry_at: datetime,
    max_deferrals: int,
    now: datetime,
    db: Session,
    duration_ms: int | None = None,
    record_attempt: bool = True,
) -> str:
    """
    Return a claimed job to ``pending`` for ``retry_at`` without spending an attempt.

    Not a failure: the job's ``deferrals`` counter goes up, which is what keeps
    this claim out of the ``max_attempts`` budget (``attempts`` itself keeps
    counting, as it fences the claim), and the job never dead-letters. Written
    only while the caller still owns that exact claim. The attempt row is closed
    ``deferred`` in the same transaction, or ``superseded`` when the handler asked
    after its job had been taken from it.

    A job already deferred ``max_deferrals`` times is left untouched and
    :data:`STATUS_DEFERRALS_EXHAUSTED` is returned: the caller then fails it like
    any other exception, so a job cannot defer forever.

    ``retry_at`` is used as given: clamping and jitter are the caller's, which
    knows the configured ceiling.

    Args:
        job_id: The job being deferred.
        reason: Why, kept as ``last_error`` (truncated for storage).
        worker_id: Worker that owns the claim.
        attempt: Claim generation being deferred.
        retry_at: Earliest instant the job may be claimed again.
        max_deferrals: Deferrals a job may take.
        now: Current instant.
        db: Active database session.
        duration_ms: Handler wall time, recorded on the attempt.
        record_attempt: Close the claim's ``job_attempts`` row.

    Returns:
        :data:`STATUS_DEFERRED` when rescheduled, :data:`STATUS_DEFERRALS_EXHAUSTED`
        when the job has used its deferrals, or the empty string when ownership
        was lost or the job was not found.
    """
    ownership = _owned_claim(job_id, worker_id=worker_id, attempt=attempt)
    truncated = fit_length(reason, MAX_STORED_ERROR_LENGTH)
    status = ""
    deferrals = db.execute(select(ProcessingJob.deferrals).where(*ownership)).scalar_one_or_none()
    if deferrals is not None:
        if deferrals >= max_deferrals:
            db.rollback()  # nothing was written; end the read transaction
            return STATUS_DEFERRALS_EXHAUSTED
        deferred = cast(
            CursorResult[Any],
            db.execute(
                update(ProcessingJob)
                .where(*ownership)
                .values(
                    status=STATUS_PENDING,
                    deferrals=ProcessingJob.deferrals + 1,
                    available_at=retry_at,
                    last_error=truncated,
                    updated_at=now,
                    locked_by=None,
                    locked_at=None,
                    lease_expires_at=None,
                )
            ),
        )
        if deferred.rowcount == 1:
            status = STATUS_DEFERRED
    if record_attempt:
        _finish_attempt(
            db,
            job_id=job_id,
            worker_id=worker_id,
            attempt=attempt,
            now=now,
            outcome=OUTCOME_DEFERRED,
            duration_ms=duration_ms,
            error=truncated,
            owned=bool(status),
        )
    db.commit()
    return status


def reclaim_expired_leases(*, now: datetime, db: Session, limit: int = 100) -> int:
    """
    Return jobs whose lease expired (a crashed worker) to a runnable state.

    Exhausted rows (``attempts - deferrals >= max_attempts``) are dead-lettered; the rest are
    made ``pending`` and immediately available again. Their ``on_dead_letter`` callbacks are owed
    to the scheduled sweep.

    Args:
        now: Current instant.
        db: Active database session.
        limit: Maximum number of expired leases to reclaim in one pass.

    Returns:
        The number of jobs this call reclaimed (requeued plus dead-lettered).
    """
    id_stmt = (
        select(ProcessingJob.id)
        .where(ProcessingJob.status == STATUS_CLAIMED, ProcessingJob.lease_expires_at < now)
        .limit(limit)
    )
    if supports_skip_locked(db.bind):  # pragma: no cover - server-side locking, not exercised on SQLite
        id_stmt = id_stmt.with_for_update(skip_locked=True)
    job_ids = list(db.execute(id_stmt).scalars().all())
    if not job_ids:
        db.commit()
        return 0
    # Both updates re-assert ``status = 'claimed'`` *and* the expiry. Without SKIP
    # LOCKED two reapers can select the same rows, and the loser must not
    # overwrite the requeue the winner already performed; and a worker's heartbeat
    # can renew a lease between this select and these updates, which must keep
    # the job rather than requeue a lease that is no longer expired.
    dead_lettered = cast(
        CursorResult[Any],
        db.execute(
            update(ProcessingJob)
            .where(
                ProcessingJob.id.in_(job_ids),
                ProcessingJob.status == STATUS_CLAIMED,
                ProcessingJob.lease_expires_at < now,
                ProcessingJob.attempts - ProcessingJob.deferrals >= ProcessingJob.max_attempts,
            )
            .values(
                status=STATUS_DEAD_LETTER,
                last_error="lease expired; max attempts exhausted",
                completed_at=now,
                updated_at=now,
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
            )
            # ``fetch`` rather than the default in-Python evaluation: the criteria now
            # compare a datetime, which the evaluator would run against any job the
            # session already holds, and a loaded value may be naive (SQLite, MySQL).
            .execution_options(synchronize_session="fetch")
        ),
    )
    requeued = cast(
        CursorResult[Any],
        db.execute(
            update(ProcessingJob)
            .where(
                ProcessingJob.id.in_(job_ids),
                ProcessingJob.status == STATUS_CLAIMED,
                ProcessingJob.lease_expires_at < now,
                ProcessingJob.attempts - ProcessingJob.deferrals < ProcessingJob.max_attempts,
            )
            .values(
                status=STATUS_PENDING,
                last_error="lease expired; requeued",
                available_at=now,
                updated_at=now,
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
            )
            .execution_options(synchronize_session="fetch")
        ),
    )
    # Close the attempt rows of exactly the jobs reclaimed: one renewed between the
    # select and the updates is still ``claimed`` and keeps its open attempt. Same
    # transaction, so no worker can claim a reclaimed row (and open a new attempt)
    # before this runs. Unconditional: it is what closes rows left open by a worker
    # that has since had ``record_attempts`` switched off.
    db.execute(
        update(JobAttempt)
        .where(
            JobAttempt.finished_at.is_(None),
            JobAttempt.job_id.in_(
                select(ProcessingJob.id).where(ProcessingJob.id.in_(job_ids), ProcessingJob.status != STATUS_CLAIMED)
            ),
        )
        .values(outcome=OUTCOME_LEASE_EXPIRED, finished_at=now)
        .execution_options(synchronize_session=False)
    )
    db.commit()
    return dead_lettered.rowcount + requeued.rowcount


def expired_lease_holders(*, now: datetime, db: Session) -> dict[str, int]:
    """
    Count the jobs whose lease has expired, by the worker that held them.

    Read before :func:`reclaim_expired_leases`, which clears the holder, so a
    reaper's log can say which workers went silent. Informational: it is not the
    set the reaper will reclaim, only the picture a moment earlier.

    Args:
        now: Current instant.
        db: Active database session.

    Returns:
        ``{worker instance id: number of expired leases}``.
    """
    rows = db.execute(
        select(ProcessingJob.locked_by, func.count())
        .where(
            ProcessingJob.status == STATUS_CLAIMED,
            ProcessingJob.lease_expires_at < now,
            ProcessingJob.locked_by.is_not(None),
        )
        .group_by(ProcessingJob.locked_by)
    ).all()
    return {str(worker): int(count) for worker, count in rows}


def get_jobs_summary(
    db: Session,
    *,
    now: datetime,
    hours: int = 24,
    dead_letter_limit: int = 50,
    record_attempts: bool = True,
) -> jobs_schema.JobsSummary:
    """
    Aggregate ``processing_jobs`` into the admin-dashboard payload.

    Args:
        db: Active database session.
        now: The instant the window ends at and ages are measured from.
        hours: Look-back window for the status/subscriber counts.
        dead_letter_limit: Maximum dead-letter jobs to return for inspection.
        record_attempts: Whether attempts are being recorded. When ``False`` the
            attempt-derived fields (durations, double runs, last worker) are
            ``None`` rather than ``0``, so "not recorded" is never read as
            "nothing happened".

    Returns:
        The durable-jobs summary — window counts, per-subscriber breakdown,
        oldest pending age, and the current dead-letter queue.
    """
    window_start = now - timedelta(hours=hours)
    rows = db.execute(
        select(ProcessingJob.subscriber_id, ProcessingJob.event_type, ProcessingJob.status, func.count())
        .where(ProcessingJob.created_at >= window_start)
        .group_by(ProcessingJob.subscriber_id, ProcessingJob.event_type, ProcessingJob.status)
    ).all()
    counts: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    totals: dict[str, int] = defaultdict(int)
    for subscriber_id, event_type, status, count in rows:
        counts[(subscriber_id, event_type)][status] += count
        totals[status] += count
    subscriber_attempts = (
        attempt_stats(db, JobAttempt.subscriber_id, JobAttempt.event_type, window_start=window_start)
        if record_attempts
        else {}
    )
    queue_attempts = attempt_stats(db, JobAttempt.queue, window_start=window_start) if record_attempts else {}
    by_subscriber = [
        jobs_schema.JobSubscriberStats(
            subscriber_id=subscriber_id,
            event_type=event_type,
            total=sum(status_counts.values()),
            pending=status_counts.get(STATUS_PENDING, 0),
            claimed=status_counts.get(STATUS_CLAIMED, 0),
            completed=status_counts.get(STATUS_COMPLETED, 0),
            dead_letter=status_counts.get(STATUS_DEAD_LETTER, 0),
            **_attempt_fields(subscriber_attempts.get((subscriber_id, event_type)), record_attempts),
        )
        for (subscriber_id, event_type), status_counts in sorted(counts.items())
    ]
    queue_rows = db.execute(
        select(ProcessingJob.queue, ProcessingJob.status, func.count())
        .where(ProcessingJob.created_at >= window_start)
        .group_by(ProcessingJob.queue, ProcessingJob.status)
    ).all()
    queue_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for queue, status, count in queue_rows:
        queue_counts[queue][status] += count
    queue_oldest: dict[str, datetime] = {
        queue: oldest
        for queue, oldest in db.execute(
            select(ProcessingJob.queue, func.min(ProcessingJob.created_at))
            .where(ProcessingJob.status.in_((STATUS_PENDING, STATUS_CLAIMED)))
            .group_by(ProcessingJob.queue)
        ).all()
    }
    queue_names = queue_counts.keys() | queue_oldest.keys()
    by_queue = [
        jobs_schema.JobQueueStats(
            queue=queue,
            total=sum(status_counts.values()),
            pending=status_counts.get(STATUS_PENDING, 0),
            claimed=status_counts.get(STATUS_CLAIMED, 0),
            completed=status_counts.get(STATUS_COMPLETED, 0),
            dead_letter=status_counts.get(STATUS_DEAD_LETTER, 0),
            oldest_pending_seconds=age_seconds(queue_oldest.get(queue), now),
            **_attempt_fields(queue_attempts.get((queue,)), record_attempts),
        )
        for queue in sorted(queue_names)
        for status_counts in (queue_counts[queue],)
    ]
    oldest_pending = min(queue_oldest.values(), default=None)
    dead_letter_jobs = (
        db.execute(
            select(ProcessingJob)
            .where(ProcessingJob.status == STATUS_DEAD_LETTER)
            .order_by(ProcessingJob.updated_at.desc())
            .limit(dead_letter_limit)
        )
        .scalars()
        .all()
    )
    last_workers = last_worker_ids(db, [job.id for job in dead_letter_jobs]) if record_attempts else {}
    return jobs_schema.JobsSummary(
        window_hours=hours,
        total_jobs=sum(totals.values()),
        pending=totals.get(STATUS_PENDING, 0),
        claimed=totals.get(STATUS_CLAIMED, 0),
        completed=totals.get(STATUS_COMPLETED, 0),
        dead_letter=totals.get(STATUS_DEAD_LETTER, 0),
        oldest_pending_seconds=age_seconds(oldest_pending, now),
        by_subscriber=by_subscriber,
        by_queue=by_queue,
        recent_dead_letter=[
            jobs_schema.DeadLetterJob.model_validate(job).model_copy(
                update={"last_worker_id": last_workers.get(job.id)}
            )
            for job in dead_letter_jobs
        ],
    )


def _attempt_fields(stats: Any, record_attempts: bool) -> dict[str, Any]:
    """The attempt-derived summary fields: ``None`` when not recorded, zeros when quiet."""
    if not record_attempts:
        return {"avg_duration_ms": None, "max_duration_ms": None, "duration_buckets": None, "superseded": None}
    if stats is None:
        return {
            "avg_duration_ms": None,
            "max_duration_ms": None,
            "duration_buckets": jobs_schema.DurationBuckets(),
            "superseded": 0,
        }
    return {
        "avg_duration_ms": stats.avg_duration_ms,
        "max_duration_ms": stats.max_duration_ms,
        "duration_buckets": stats.buckets,
        "superseded": stats.superseded,
    }


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
    replayed = cast(
        CursorResult[Any],
        db.execute(
            update(ProcessingJob)
            .where(ProcessingJob.id == job_id, ProcessingJob.status == STATUS_DEAD_LETTER)
            .values(
                status=STATUS_PENDING,
                attempts=0,
                deferrals=0,
                available_at=now,
                updated_at=now,
                last_error=None,
                locked_by=None,
                locked_at=None,
                lease_expires_at=None,
                completed_at=None,
                dead_letter_notified_at=None,
                dead_letter_notify_after=None,
                dead_letter_attempts=0,
                dead_letter_callback_error=None,
            )
        ),
    )
    db.commit()
    return replayed.rowcount == 1


@dataclass(frozen=True)
class DeadLetterJob:
    """A detached snapshot of a claimed dead letter, safe to use after its session closes."""

    id: str
    subscriber_id: str
    event_type: str
    #: The envelope as the handler last saw it, with its final attempt counts.
    event: Event
    last_error: str | None
    delivery_attempt: int
    #: When the job became ``dead_letter`` (its ``completed_at``). A replay clears it, so it tells this dead
    #: letter apart from a later one of the same job, whose delivery count starts again.
    dead_at: datetime | None

    def to_event(self) -> Event:
        """The envelope handed to the callback."""
        return self.event


def envelope_timestamp(row: ProcessingJob) -> str:
    """Return the publish timestamp a job's envelope carries; a job from before ``rev0006`` has its enqueue time."""
    return row.event_timestamp or as_utc(row.created_at).isoformat()


def event_from_row(row: ProcessingJob) -> Event:
    """Rebuild the envelope a job's handler sees from its row."""
    return Event(
        event_id=row.event_id,
        event_type=row.event_type,
        source=row.source,
        timestamp=envelope_timestamp(row),
        payload=dict(row.payload),
        metadata=dict(row.job_metadata) if row.job_metadata else {},
        retry_count=row.attempts - row.deferrals,
        schema_version=row.schema_version,
        max_attempts=row.max_attempts,
        deferral_count=row.deferrals,
    )


def claim_dead_letter_callbacks(
    subscriber_ids: Iterable[str],
    *,
    now: datetime,
    lease_seconds: float,
    db: Session,
    job_ids: Iterable[str] | None = None,
    limit: int = 100,
    max_deliveries: int | None = None,
    dead_after: datetime | None = None,
) -> list[DeadLetterJob]:
    """
    Claim the dead-lettered jobs whose subscriber's callback is owed and due.

    A job is due while ``dead_letter_notified_at`` is NULL and its
    ``dead_letter_notify_after`` is unset or past. Each claim is a compare-and-set
    that pushes ``dead_letter_notify_after`` ``lease_seconds`` ahead and counts the
    delivery, so of several processes going for the same job (a worker and a
    reaper, or two reapers) exactly one wins and the others skip it. The lease also
    keeps a slow callback from being started a second time before it is given up on.

    Args:
        subscriber_ids: Subscribers that have a callback in this process.
        now: Current instant.
        lease_seconds: How long the claim holds off other deliveries.
        db: Active database session.
        job_ids: Restrict to these jobs (the ones this process just dead-lettered).
        limit: Maximum number of jobs claimed.
        max_deliveries: Give up on a job once this many deliveries were claimed.
        dead_after: Ignore jobs dead-lettered before this instant, so a callback
            added to a subscriber later does not fire for its old history.

    Returns:
        The jobs this call claimed, oldest first. ``delivery_attempt`` on each is
        the token its outcome has to be written with.
    """
    subscribers = list(subscriber_ids)
    if not subscribers:
        return []
    due = (
        ProcessingJob.status == STATUS_DEAD_LETTER,
        ProcessingJob.dead_letter_notified_at.is_(None),
        or_(ProcessingJob.dead_letter_notify_after.is_(None), ProcessingJob.dead_letter_notify_after <= now),
    )
    statement = select(ProcessingJob).where(*due, ProcessingJob.subscriber_id.in_(subscribers))
    if max_deliveries is not None:
        statement = statement.where(ProcessingJob.dead_letter_attempts < max_deliveries)
    if dead_after is not None:
        statement = statement.where(ProcessingJob.updated_at >= dead_after)
    if job_ids is not None:
        statement = statement.where(ProcessingJob.id.in_(list(job_ids)))
    rows = db.execute(statement.order_by(ProcessingJob.updated_at, ProcessingJob.id).limit(limit)).scalars().all()
    lease_until = now + timedelta(seconds=lease_seconds)
    jobs: list[DeadLetterJob] = []
    for row in rows:
        claimed = cast(
            CursorResult[Any],
            db.execute(
                update(ProcessingJob)
                .where(ProcessingJob.id == row.id, ProcessingJob.dead_letter_attempts == row.dead_letter_attempts, *due)
                .values(
                    dead_letter_notify_after=lease_until,
                    dead_letter_attempts=row.dead_letter_attempts + 1,
                )
                .execution_options(synchronize_session=False)
            ),
        )
        if claimed.rowcount != 1:
            continue
        jobs.append(
            DeadLetterJob(
                id=row.id,
                subscriber_id=row.subscriber_id,
                event_type=row.event_type,
                event=event_from_row(row),
                last_error=row.last_error,
                delivery_attempt=row.dead_letter_attempts + 1,
                dead_at=row.completed_at,
            )
        )
    db.commit()
    return jobs


def mark_dead_letter_notified(claim: DeadLetterJob, *, now: datetime, db: Session) -> bool:
    """
    Record that the dead-letter callback succeeded for the job ``claim`` delivered.

    Args:
        claim: The delivery being settled, as :func:`claim_dead_letter_callbacks` returned it.
        now: Current instant.
        db: Active database session.

    Returns:
        True when this call set the marker; False when the claim was no longer
        the live one (already recorded, re-claimed after its lease ran out, or
        replayed meanwhile, also when the job has dead-lettered again since).
    """
    return _update_dead_letter(
        claim,
        db=db,
        dead_letter_notified_at=now,
        dead_letter_notify_after=None,
        dead_letter_callback_error=None,
    )


def defer_dead_letter_callback(claim: DeadLetterJob, *, retry_at: datetime, error: str, db: Session) -> bool:
    """
    Schedule a failed callback's next delivery and keep the failure for diagnosis.

    Leaves ``updated_at`` alone, which is the instant the job was dead-lettered.

    Args:
        claim: The delivery being settled, as :func:`claim_dead_letter_callbacks` returned it.
        retry_at: Earliest instant the callback may be delivered again.
        error: What the callback raised.
        db: Active database session.

    Returns:
        True when the claim was still the live one and the retry was recorded.
    """
    return _update_dead_letter(
        claim,
        db=db,
        dead_letter_notify_after=retry_at,
        dead_letter_callback_error=fit_length(error, MAX_STORED_ERROR_LENGTH),
    )


def release_dead_letter_claim(claim: DeadLetterJob, *, db: Session) -> bool:
    """
    Undo a claim whose callback was never run, so the delivery is not counted.

    Args:
        claim: The delivery being released, as :func:`claim_dead_letter_callbacks` returned it.
        db: Active database session.

    Returns:
        True when the claim was still the live one and was released.
    """
    return _update_dead_letter(
        claim, db=db, dead_letter_notify_after=None, dead_letter_attempts=claim.delivery_attempt - 1
    )


def _update_dead_letter(claim: DeadLetterJob, *, db: Session, **values: Any) -> bool:
    """Write to a dead letter only while ``claim`` is still the live claim of the same dead letter."""
    same_dead_letter = (
        ProcessingJob.completed_at.is_(None) if claim.dead_at is None else ProcessingJob.completed_at == claim.dead_at
    )
    updated = cast(
        CursorResult[Any],
        db.execute(
            update(ProcessingJob)
            .where(
                ProcessingJob.id == claim.id,
                ProcessingJob.status == STATUS_DEAD_LETTER,
                ProcessingJob.dead_letter_notified_at.is_(None),
                ProcessingJob.dead_letter_attempts == claim.delivery_attempt,
                same_dead_letter,
            )
            .values(**values)
        ),
    )
    db.commit()
    return updated.rowcount == 1


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
        ProcessingJob.status == STATUS_COMPLETED,
        ProcessingJob.completed_at < cutoff,
        db=db,
        batch_size=batch_size,
    )


def _insert_ignoring_duplicate(values: dict, db: Session):
    """
    Build a dialect-appropriate "insert, or do nothing if it exists" statement.

    The ``(event_id, subscriber_id)`` unique constraint is what makes a durable
    subscriber an idempotent consumer, so a duplicate enqueue must be a silent
    no-op on **every** supported database — not an ``IntegrityError`` the relay
    would have to catch.

    Args:
        values: Column values for the new row.
        db: Active database session (used only for dialect detection).

    Returns:
        An executable insert statement that ignores a duplicate
        ``(event_id, subscriber_id)``.

    Raises:
        RuntimeError: On a dialect with no supported conflict clause, rather
            than emitting a plain INSERT that would raise on the second enqueue.
    """
    dialect = db.bind.dialect.name if db.bind is not None else ""
    if dialect == "postgresql":  # pragma: no cover - exercised on Postgres, not in SQLite tests
        return pg_insert(ProcessingJob).values(**values).on_conflict_do_nothing(index_elements=_CONFLICT_KEYS)
    if dialect == "sqlite":
        return sqlite_insert(ProcessingJob).values(**values).on_conflict_do_nothing(index_elements=_CONFLICT_KEYS)
    if dialect in ("mysql", "mariadb"):  # pragma: no cover - exercised on MySQL, not in SQLite tests
        # MySQL has no ON CONFLICT DO NOTHING. Assigning a column to *itself*
        # is the standard no-op form; ``INSERT IGNORE`` would also swallow
        # unrelated errors such as truncation.
        statement = mysql_insert(ProcessingJob).values(**values)
        return statement.on_duplicate_key_update(event_id=ProcessingJob.event_id)
    raise RuntimeError(
        f"JASIL's durable jobs need an insert-or-ignore clause, which dialect {dialect!r} does not provide. "
        "Supported: postgresql, mysql, mariadb, sqlite."
    )
