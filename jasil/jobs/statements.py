"""Execution-agnostic query builders for ``processing_jobs`` and ``event_outbox``.

This is where the durable-jobs layer's correctness actually lives: the claim's
``SELECT ... FOR UPDATE SKIP LOCKED`` plus compare-and-set, the reaper's
double-guarded requeue, the dialect-specific insert-or-ignore that makes a
durable subscriber an idempotent consumer, and the relay's locked batch select.
Every one of them is subtle, and a second hand-written copy for the async face
would drift from this one silently — the failure mode being duplicated or
skipped jobs under concurrency, which no unit test on a single connection would
catch.

So none of it is duplicated. A ``Select``/``Update``/``Insert`` is not bound to
an execution model, so the statements are built here, once, and both
:mod:`jasil.jobs.crud` / :mod:`jasil.jobs.outbox` and their async twins
(:mod:`jasil.jobs.crud_async` / :mod:`jasil.jobs.outbox_async`) execute what this
module returns. The two faces differ only in ``execute`` versus ``await
execute``.

Every query is portable, so the same code runs on PostgreSQL, MySQL and SQLite.
Where a statement can take ``SELECT ... FOR UPDATE SKIP LOCKED`` the builder
takes a ``skip_locked`` flag rather than deciding for itself: the caller knows
its own bind, and :func:`jasil._core.dialects.supports_skip_locked` reads the
dialect off it identically for a sync ``Engine`` and an ``AsyncEngine``.

**Internal.** Not covered by the API-stability contract, and it reaches a model
at import time, so it cannot be imported before ``jasil.orm.map_models`` has run.
"""

import uuid
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Select, Update, func, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

import jasil.jobs.backoff as jobs_backoff
import jasil.jobs.schema as jobs_schema
from jasil._core.limits import MAX_STORED_ERROR_LENGTH, fit_length
from jasil._core.timestamps import age_seconds
from jasil.events import Event
from jasil.jobs.models import EventOutbox, ProcessingJob

__all__ = [
    "STATUS_CLAIMED",
    "STATUS_COMPLETED",
    "STATUS_DEAD_LETTER",
    "STATUS_PENDING",
    "build_jobs_summary",
    "claim_update_stmt",
    "claimed_jobs_stmt",
    "dead_letter_list_stmt",
    "due_job_ids_stmt",
    "expired_lease_ids_stmt",
    "insert_ignoring_duplicate",
    "job_values",
    "jobs_prune_condition",
    "mark_job_completed_stmt",
    "mark_job_dead_letter_stmt",
    "mark_relayed_stmt",
    "new_outbox_row",
    "oldest_pending_stmt",
    "outbox_prune_condition",
    "reap_dead_letter_stmt",
    "reap_requeue_stmt",
    "replay_stmt",
    "reschedule_stmt",
    "subscriber_counts_stmt",
    "unrelayed_stmt",
]

STATUS_PENDING = "pending"
STATUS_CLAIMED = "claimed"
STATUS_COMPLETED = "completed"
STATUS_DEAD_LETTER = "dead_letter"

_CONFLICT_KEYS = ["event_id", "subscriber_id"]


def job_values(
    event: Event,
    subscriber_id: str,
    *,
    job_id: str | None = None,
    max_attempts: int,
    now: datetime,
    available_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the column values for one durable job row.

    Args:
        event: The originating event envelope.
        subscriber_id: The durable subscriber this job runs.
        job_id: The new row's primary key; a fresh uuid4 when omitted. The caller
            keeps it so it can tell an inserted row from a skipped duplicate.
        max_attempts: Attempt ceiling before the job is dead-lettered.
        now: Current instant (created/updated/available timestamps).
        available_at: Earliest claim instant; defaults to ``now``.

    Returns:
        The values dict, ready for :func:`insert_ignoring_duplicate`.
    """
    return {
        "id": job_id or str(uuid.uuid4()),
        "event_id": event.event_id,
        "event_type": event.event_type,
        "subscriber_id": subscriber_id,
        "source": event.source,
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


def insert_ignoring_duplicate(values: dict, dialect_name: str) -> Any:
    """
    Build a dialect-appropriate "insert, or do nothing if it exists" statement.

    The ``(event_id, subscriber_id)`` unique constraint is what makes a durable
    subscriber an idempotent consumer, so a duplicate enqueue must be a silent
    no-op on **every** supported database — not an ``IntegrityError`` the relay
    would have to catch.

    Args:
        values: Column values for the new row.
        dialect_name: The bind's dialect name (``postgresql`` / ``mysql`` /
            ``sqlite``). Taken as a plain string rather than a session, so the
            same builder serves a ``Session`` and an ``AsyncSession``.

    Returns:
        An executable insert statement that ignores a duplicate
        ``(event_id, subscriber_id)``.

    Raises:
        RuntimeError: On a dialect with no supported conflict clause, rather
            than emitting a plain INSERT that would raise on the second enqueue.
    """
    if dialect_name == "postgresql":  # pragma: no cover - exercised on Postgres, not in SQLite tests
        return pg_insert(ProcessingJob).values(**values).on_conflict_do_nothing(index_elements=_CONFLICT_KEYS)
    if dialect_name == "sqlite":
        return sqlite_insert(ProcessingJob).values(**values).on_conflict_do_nothing(index_elements=_CONFLICT_KEYS)
    if dialect_name == "mysql":  # pragma: no cover - exercised on MySQL, not in SQLite tests
        # MySQL has no ON CONFLICT DO NOTHING. Assigning a column to *itself*
        # is the standard no-op form; ``INSERT IGNORE`` would also swallow
        # unrelated errors such as truncation.
        statement = mysql_insert(ProcessingJob).values(**values)
        return statement.on_duplicate_key_update(event_id=ProcessingJob.event_id)
    raise RuntimeError(
        f"JASIL's durable jobs need an insert-or-ignore clause, which dialect {dialect_name!r} does not provide. "
        "Supported: postgresql, mysql, sqlite."
    )


def due_job_ids_stmt(*, now: datetime, limit: int, skip_locked: bool) -> Select:
    """Build the select naming the due jobs a worker may claim.

    Args:
        now: Current instant; only jobs whose ``available_at`` has passed match.
        limit: Maximum number of ids to return.
        skip_locked: Whether to add ``FOR UPDATE SKIP LOCKED``, so concurrent
            workers select disjoint sets. The caller decides from its own bind.

    Returns:
        The statement to execute.
    """
    stmt = (
        select(ProcessingJob.id)
        .where(ProcessingJob.status == STATUS_PENDING, ProcessingJob.available_at <= now)
        .order_by(ProcessingJob.available_at)
        .limit(limit)
    )
    if skip_locked:  # pragma: no cover - server-side locking, not exercised on SQLite
        stmt = stmt.with_for_update(skip_locked=True)
    return stmt


def claim_update_stmt(
    job_ids: Sequence[str],
    *,
    worker_id: str,
    now: datetime,
    lease_seconds: int,
) -> Update:
    """Build the compare-and-set that takes a lease on the named jobs.

    The attempt is counted at claim time so a worker that crashes mid-run still
    consumes an attempt, bounding crash loops.

    Args:
        job_ids: The ids selected by :func:`due_job_ids_stmt`.
        worker_id: Identifier of the claiming worker (the lease holder).
        now: Current instant.
        lease_seconds: Lease duration; the reaper requeues jobs past it.

    Returns:
        The statement to execute.
    """
    return (
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


def claimed_jobs_stmt(job_ids: Sequence[str], *, worker_id: str) -> Select:
    """Build the select returning the rows this worker's claim actually won.

    Args:
        job_ids: The ids the claim was attempted on.
        worker_id: The claiming worker.

    Returns:
        The statement to execute, ordered oldest-available first.
    """
    return (
        select(ProcessingJob)
        .where(
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
        .order_by(ProcessingJob.available_at)
    )


def mark_job_completed_stmt(job_id: str, *, now: datetime) -> Update:
    """Build the update marking a claimed job ``completed`` and releasing its lease.

    Args:
        job_id: The job to complete.
        now: Current instant.

    Returns:
        The statement to execute.
    """
    return (
        update(ProcessingJob)
        .where(ProcessingJob.id == job_id)
        .values(
            status=STATUS_COMPLETED,
            completed_at=now,
            updated_at=now,
            last_error=None,
            locked_by=None,
            lease_expires_at=None,
        )
    )


def mark_job_dead_letter_stmt(job_id: str, error_message: str, *, now: datetime) -> Update:
    """Build the update dead-lettering a job whose attempt ceiling was reached.

    Args:
        job_id: The job that failed for the last time.
        error_message: The failure reason (truncated for storage).
        now: Current instant.

    Returns:
        The statement to execute.
    """
    return (
        update(ProcessingJob)
        .where(ProcessingJob.id == job_id)
        .values(
            status=STATUS_DEAD_LETTER,
            last_error=fit_length(error_message, MAX_STORED_ERROR_LENGTH),
            completed_at=now,
            updated_at=now,
            locked_by=None,
            lease_expires_at=None,
        )
    )


def reschedule_stmt(
    job_id: str,
    error_message: str,
    *,
    attempts: int,
    base_seconds: float,
    max_seconds: float,
    now: datetime,
) -> Update:
    """Build the update rescheduling a failed job with exponential backoff.

    Args:
        job_id: The job that failed.
        error_message: The failure reason (truncated for storage).
        attempts: Attempts consumed so far, which sets the backoff step.
        base_seconds: Backoff base delay.
        max_seconds: Backoff ceiling.
        now: Current instant.

    Returns:
        The statement to execute.
    """
    delay = jobs_backoff.backoff_seconds(attempts, base_seconds=base_seconds, max_seconds=max_seconds)
    return (
        update(ProcessingJob)
        .where(ProcessingJob.id == job_id)
        .values(
            status=STATUS_PENDING,
            last_error=fit_length(error_message, MAX_STORED_ERROR_LENGTH),
            available_at=now + timedelta(seconds=delay),
            updated_at=now,
            locked_by=None,
            locked_at=None,
            lease_expires_at=None,
        )
    )


def expired_lease_ids_stmt(*, now: datetime, limit: int, skip_locked: bool) -> Select:
    """Build the select naming jobs whose lease has expired.

    Args:
        now: Current instant.
        limit: Maximum number of expired leases to reclaim in one pass.
        skip_locked: Whether to add ``FOR UPDATE SKIP LOCKED``, so concurrent
            reapers reclaim disjoint rows.

    Returns:
        The statement to execute.
    """
    stmt = (
        select(ProcessingJob.id)
        .where(ProcessingJob.status == STATUS_CLAIMED, ProcessingJob.lease_expires_at < now)
        .limit(limit)
    )
    if skip_locked:  # pragma: no cover - server-side locking, not exercised on SQLite
        stmt = stmt.with_for_update(skip_locked=True)
    return stmt


def reap_dead_letter_stmt(job_ids: Sequence[str], *, now: datetime) -> Update:
    """Build the update dead-lettering expired leases with no attempts left.

    Args:
        job_ids: The expired-lease ids this pass claimed.
        now: Current instant.

    Returns:
        The statement to execute.
    """
    return (
        update(ProcessingJob)
        .where(
            ProcessingJob.id.in_(job_ids),
            # Re-asserted: without SKIP LOCKED two reapers can select the same
            # rows, and the loser must not overwrite the winner's write.
            ProcessingJob.status == STATUS_CLAIMED,
            ProcessingJob.attempts >= ProcessingJob.max_attempts,
        )
        .values(
            status=STATUS_DEAD_LETTER,
            last_error="lease expired; max attempts exhausted",
            completed_at=now,
            updated_at=now,
            locked_by=None,
            lease_expires_at=None,
        )
    )


def reap_requeue_stmt(job_ids: Sequence[str], *, now: datetime) -> Update:
    """Build the update returning expired leases with attempts left to ``pending``.

    Args:
        job_ids: The expired-lease ids this pass claimed.
        now: Current instant.

    Returns:
        The statement to execute.
    """
    return (
        update(ProcessingJob)
        .where(
            ProcessingJob.id.in_(job_ids),
            # Re-asserted, for the same reason as the dead-letter update above.
            ProcessingJob.status == STATUS_CLAIMED,
            ProcessingJob.attempts < ProcessingJob.max_attempts,
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
    )


def replay_stmt(job_id: str, *, now: datetime) -> Update:
    """Build the update requeuing a dead-lettered job with a full attempt budget.

    Args:
        job_id: The job to replay.
        now: Current instant (the new availability time).

    Returns:
        The statement to execute.
    """
    return (
        update(ProcessingJob)
        .where(ProcessingJob.id == job_id, ProcessingJob.status == STATUS_DEAD_LETTER)
        .values(
            status=STATUS_PENDING,
            attempts=0,
            available_at=now,
            updated_at=now,
            last_error=None,
            locked_by=None,
            locked_at=None,
            lease_expires_at=None,
            completed_at=None,
        )
    )


def subscriber_counts_stmt(window_start: datetime) -> Select:
    """Build the per-(subscriber, event type, status) count query for the dashboard.

    Args:
        window_start: Only jobs created at/after this instant are counted.

    Returns:
        The statement to execute.
    """
    return (
        select(ProcessingJob.subscriber_id, ProcessingJob.event_type, ProcessingJob.status, func.count())
        .where(ProcessingJob.created_at >= window_start)
        .group_by(ProcessingJob.subscriber_id, ProcessingJob.event_type, ProcessingJob.status)
    )


def oldest_pending_stmt() -> Select:
    """Build the query for the creation time of the oldest unfinished job.

    Returns:
        The statement to execute.
    """
    return select(func.min(ProcessingJob.created_at)).where(ProcessingJob.status.in_((STATUS_PENDING, STATUS_CLAIMED)))


def dead_letter_list_stmt(limit: int) -> Select:
    """Build the query listing the current dead-letter queue.

    Args:
        limit: Maximum dead-letter jobs to return for inspection.

    Returns:
        The statement to execute, most recently updated first.
    """
    return (
        select(ProcessingJob)
        .where(ProcessingJob.status == STATUS_DEAD_LETTER)
        .order_by(ProcessingJob.updated_at.desc())
        .limit(limit)
    )


def jobs_prune_condition(cutoff: datetime) -> tuple[Any, ...]:
    """Build the filters selecting ``processing_jobs`` rows that are safe to delete.

    Only terminal ``completed`` rows are prunable. In-flight rows (``pending`` /
    ``claimed``) are never touched, and ``dead_letter`` rows are deliberately kept
    for operator review.

    Args:
        cutoff: Rows whose ``completed_at`` is strictly before this instant.

    Returns:
        The filter conditions.
    """
    return (ProcessingJob.status == STATUS_COMPLETED, ProcessingJob.completed_at < cutoff)


def build_jobs_summary(
    *,
    hours: int,
    now: datetime,
    count_rows: Sequence[Any],
    oldest_pending: datetime | None,
    dead_letter_rows: Sequence[Any],
) -> jobs_schema.JobsSummary:
    """Assemble the durable-jobs dashboard payload from the three result sets.

    Pure: given the same rows it returns the same summary, whichever session
    fetched them.

    Args:
        hours: The look-back window the rows were fetched for.
        now: Reference instant for the oldest-pending age computation.
        count_rows: ``(subscriber_id, event_type, status, count)`` rows.
        oldest_pending: Creation time of the oldest unfinished job, if any.
        dead_letter_rows: ``ProcessingJob`` instances in the dead-letter queue.

    Returns:
        Window counts, the per-subscriber breakdown, the age of the oldest
        unfinished job, and the current dead-letter queue.
    """
    counts: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    totals: dict[str, int] = defaultdict(int)
    for subscriber_id, event_type, status, count in count_rows:
        counts[(subscriber_id, event_type)][status] += count
        totals[status] += count
    by_subscriber = [
        jobs_schema.JobSubscriberStats(
            subscriber_id=subscriber_id,
            event_type=event_type,
            total=sum(status_counts.values()),
            pending=status_counts.get(STATUS_PENDING, 0),
            claimed=status_counts.get(STATUS_CLAIMED, 0),
            completed=status_counts.get(STATUS_COMPLETED, 0),
            dead_letter=status_counts.get(STATUS_DEAD_LETTER, 0),
        )
        for (subscriber_id, event_type), status_counts in sorted(counts.items())
    ]
    return jobs_schema.JobsSummary(
        window_hours=hours,
        total_jobs=sum(totals.values()),
        pending=totals.get(STATUS_PENDING, 0),
        claimed=totals.get(STATUS_CLAIMED, 0),
        completed=totals.get(STATUS_COMPLETED, 0),
        dead_letter=totals.get(STATUS_DEAD_LETTER, 0),
        oldest_pending_seconds=age_seconds(oldest_pending, now),
        by_subscriber=by_subscriber,
        recent_dead_letter=[jobs_schema.DeadLetterJob.model_validate(job) for job in dead_letter_rows],
    )


def new_outbox_row(event: Event, *, now: datetime, outbox_id: str | None = None) -> Any:
    """Build the ``event_outbox`` row staging one event for durable delivery.

    A mapped instance rather than an ``insert()``, because ``Session.add`` is
    synchronous on an ``AsyncSession`` too — the await happens at flush time — so
    one builder serves both faces.

    Args:
        event: The event envelope to persist.
        now: Current instant (the outbox write time).
        outbox_id: The new row's primary key; a fresh uuid4 when omitted.

    Returns:
        An unpersisted ``EventOutbox`` instance.
    """
    return EventOutbox(
        id=outbox_id or str(uuid.uuid4()),
        event_id=event.event_id,
        event_type=event.event_type,
        source=event.source,
        timestamp=event.timestamp,
        payload=event.payload,
        schema_version=event.schema_version,
        event_metadata=event.metadata or None,
        created_at=now,
    )


def unrelayed_stmt(*, limit: int, skip_locked: bool) -> Select:
    """Build the select claiming the oldest not-yet-relayed outbox rows.

    Args:
        limit: Maximum number of rows to return.
        skip_locked: Whether to add ``FOR UPDATE SKIP LOCKED``, so concurrent
            relayers take disjoint batches — but only for as long as the caller
            holds the transaction.

    Returns:
        The statement to execute, oldest-first.
    """
    stmt = select(EventOutbox).where(EventOutbox.relayed_at.is_(None)).order_by(EventOutbox.created_at).limit(limit)
    if skip_locked:  # pragma: no cover - server-side locking, not exercised on SQLite
        stmt = stmt.with_for_update(skip_locked=True)
    return stmt


def mark_relayed_stmt(outbox_id: str, *, now: datetime) -> Update:
    """Build the update stamping an outbox row as relayed.

    Args:
        outbox_id: The outbox row id.
        now: Current instant.

    Returns:
        The statement to execute.
    """
    return update(EventOutbox).where(EventOutbox.id == outbox_id).values(relayed_at=now)


def outbox_prune_condition(cutoff: datetime) -> tuple[Any, ...]:
    """Build the filters selecting ``event_outbox`` rows that are safe to delete.

    Only rows that have been relayed (``relayed_at`` set) and whose relay is older
    than ``cutoff`` are removed; unrelayed rows are pending work and are never
    touched. A relayed row's only remaining value is audit — the per-subscriber
    jobs it fanned out into are the source of truth — so it is safe to prune.

    Args:
        cutoff: Rows whose ``relayed_at`` is strictly before this instant.

    Returns:
        The filter conditions.
    """
    return (EventOutbox.relayed_at.is_not(None), EventOutbox.relayed_at < cutoff)
