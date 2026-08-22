"""Execution-agnostic query builders for the ``event_log`` table.

The one thing that must not happen when a synchronous data layer grows an async
twin is two copies of the SQL. A ``Select`` or an ``Update`` is not bound to an
execution model — only ``session.execute(...)`` versus ``await
session.execute(...)`` is — so every statement the event log needs is built
here, once, and both :mod:`jasil.event_log.crud` and
:mod:`jasil.event_log.crud_async` execute what this module returns.

The same goes for the parts of the dashboard aggregate that are *not* SQL: the
grouping, sorting and schema assembly in :func:`build_summary` are pure
functions of the rows, so the two CRUD modules differ in exactly one respect —
how they get those rows.

**Internal.** Not covered by the API-stability contract, and it reaches a model
at import time, so it cannot be imported before ``jasil.orm.map_models`` has run.
"""

from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import Select, Update, func, select, update

import jasil.event_log.schema as event_log_schema
from jasil._core.limits import (
    MAX_HANDLER_NAME_LENGTH,
    MAX_STORED_ERROR_LENGTH,
    MAX_WORKER_ID_LENGTH,
    fit_length,
)
from jasil._core.timestamps import age_seconds
from jasil.event_log.models import EventLog
from jasil.events import Event

__all__ = [
    "STATUS_COMPLETED",
    "STATUS_DEAD_LETTER",
    "STATUS_FAILED",
    "STATUS_PROCESSING",
    "STATUS_PUBLISHED",
    "STATUS_QUEUED",
    "build_summary",
    "latency_by_type_stmt",
    "mark_completed_stmt",
    "mark_failed_stmt",
    "mark_processing_stmt",
    "new_row",
    "pending_groups_stmt",
    "prune_condition",
    "recent_failures_stmt",
    "status_counts_stmt",
]

STATUS_PUBLISHED = "published"
STATUS_QUEUED = "queued"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_DEAD_LETTER = "dead_letter"


def new_row(event: Event, *, status: str) -> Any:
    """Build the ``event_log`` row for a freshly published or queued event.

    A mapped instance rather than an ``insert()``, because ``Session.add`` is
    synchronous on an ``AsyncSession`` too — the await happens at flush time — so
    one builder serves both faces.

    Args:
        event: The event envelope being recorded.
        status: The initial lifecycle status (:data:`STATUS_PUBLISHED` for a
            bus-delivered event, :data:`STATUS_QUEUED` for a durable one).

    Returns:
        An unpersisted ``EventLog`` instance.
    """
    return EventLog(
        id=event.event_id,
        event_type=event.event_type,
        event_source=event.source,
        event_payload=event.payload,
        event_metadata=event.metadata or None,
        status=status,
        retry_count=event.retry_count,
    )


def mark_processing_stmt(event_id: str, worker_id: str) -> Update:
    """Build the update transitioning an event to ``processing``.

    Args:
        event_id: The event_id (primary key).
        worker_id: The process/consumer handling the event.

    Returns:
        The statement to execute.
    """
    return (
        update(EventLog)
        .where(EventLog.id == event_id)
        .values(
            status=STATUS_PROCESSING,
            worker_id=fit_length(worker_id, MAX_WORKER_ID_LENGTH),
            processed_at=func.now(),
        )
    )


def mark_completed_stmt(event_id: str, handler_name: str | None, processing_time_ms: int) -> Update:
    """Build the update transitioning an event to ``completed``.

    Args:
        event_id: The event_id (primary key).
        handler_name: The subscriber(s) that processed the event.
        processing_time_ms: Handler execution time in milliseconds.

    Returns:
        The statement to execute.
    """
    return (
        update(EventLog)
        .where(EventLog.id == event_id)
        .values(
            status=STATUS_COMPLETED,
            handler_name=fit_length(handler_name, MAX_HANDLER_NAME_LENGTH),
            processing_time_ms=processing_time_ms,
            completed_at=func.now(),
        )
    )


def mark_failed_stmt(
    event_id: str,
    handler_name: str | None,
    error_message: str,
    processing_time_ms: int,
) -> Update:
    """Build the update transitioning an event to ``failed``.

    Args:
        event_id: The event_id (primary key).
        handler_name: The subscriber(s) that processed the event.
        error_message: The failure reason (truncated for storage).
        processing_time_ms: Handler execution time in milliseconds.

    Returns:
        The statement to execute.
    """
    return (
        update(EventLog)
        .where(EventLog.id == event_id)
        .values(
            status=STATUS_FAILED,
            handler_name=fit_length(handler_name, MAX_HANDLER_NAME_LENGTH),
            error_message=fit_length(error_message, MAX_STORED_ERROR_LENGTH),
            processing_time_ms=processing_time_ms,
            completed_at=func.now(),
        )
    )


def status_counts_stmt(window_start: datetime) -> Select:
    """Build the per-(type, status) count query for the dashboard window.

    Args:
        window_start: Only events created at/after this instant are counted.

    Returns:
        The statement to execute.
    """
    return (
        select(EventLog.event_type, EventLog.status, func.count())
        .where(EventLog.created_at >= window_start)
        .group_by(EventLog.event_type, EventLog.status)
    )


def latency_by_type_stmt(window_start: datetime) -> Select:
    """Build the per-type average/maximum processing-time query.

    Args:
        window_start: Only events created at/after this instant are measured.

    Returns:
        The statement to execute.
    """
    return (
        select(
            EventLog.event_type,
            func.avg(EventLog.processing_time_ms),
            func.max(EventLog.processing_time_ms),
        )
        .where(EventLog.created_at >= window_start, EventLog.processing_time_ms.is_not(None))
        .group_by(EventLog.event_type)
    )


def pending_groups_stmt() -> Select:
    """Build the query grouping not-yet-finished events with each group's oldest row.

    Returns:
        The statement to execute.
    """
    return (
        select(EventLog.event_type, EventLog.status, func.count(), func.min(EventLog.created_at))
        .where(EventLog.status.in_((STATUS_PUBLISHED, STATUS_PROCESSING)))
        .group_by(EventLog.event_type, EventLog.status)
    )


def recent_failures_stmt(limit: int) -> Select:
    """Build the query fetching the most recent failed/dead-lettered events.

    Args:
        limit: Maximum number of rows to return.

    Returns:
        The statement to execute.
    """
    return (
        select(EventLog)
        .where(EventLog.status.in_((STATUS_FAILED, STATUS_DEAD_LETTER)))
        .order_by(EventLog.created_at.desc())
        .limit(limit)
    )


def prune_condition(cutoff: datetime) -> Any:
    """Build the filter selecting ``event_log`` rows that are safe to delete.

    Every row is prunable regardless of status: event_log is a best-effort,
    safe-to-lose observability trail (the dashboard is a recent-window view), so
    nothing here is a source of truth worth preserving past the retention window.

    Args:
        cutoff: Rows whose ``created_at`` is strictly before this instant.

    Returns:
        The filter condition.
    """
    return EventLog.created_at < cutoff


def build_summary(
    *,
    hours: int,
    now: datetime,
    status_rows: Sequence[Any],
    latency_rows: Sequence[Any],
    pending_rows: Sequence[Any],
    failure_rows: Sequence[Any],
) -> event_log_schema.EventLogSummary:
    """Assemble the dashboard payload from the four result sets.

    Pure: given the same rows it returns the same summary, whichever session
    fetched them.

    Args:
        hours: The look-back window the rows were fetched for.
        now: Reference instant for the pending-age computation.
        status_rows: ``(event_type, status, count)`` rows.
        latency_rows: ``(event_type, avg_ms, max_ms)`` rows.
        pending_rows: ``(event_type, status, count, oldest_created_at)`` rows.
        failure_rows: ``EventLog`` instances, newest first.

    Returns:
        The aggregated dashboard summary.
    """
    by_type = _summarize_by_type(status_rows, latency_rows)
    return event_log_schema.EventLogSummary(
        window_hours=hours,
        total_events=sum(stats.total for stats in by_type),
        by_type=by_type,
        pending=_summarize_pending(pending_rows, now),
        recent_failures=[event_log_schema.EventLogFailure.model_validate(row) for row in failure_rows],
    )


def _summarize_by_type(
    status_rows: Sequence[Any],
    latency_rows: Sequence[Any],
) -> list[event_log_schema.EventTypeStats]:
    """Fold the count and latency rows into per-event-type statistics."""
    latency_by_type = {event_type: (avg, maximum) for event_type, avg, maximum in latency_rows}

    counts_by_type: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for event_type, status, count in status_rows:
        counts_by_type[event_type][status] += count

    stats: list[event_log_schema.EventTypeStats] = []
    for event_type in sorted(counts_by_type):
        statuses = counts_by_type[event_type]
        avg, maximum = latency_by_type.get(event_type, (None, None))
        stats.append(
            event_log_schema.EventTypeStats(
                event_type=event_type,
                total=sum(statuses.values()),
                published=statuses.get(STATUS_PUBLISHED, 0),
                queued=statuses.get(STATUS_QUEUED, 0),
                processing=statuses.get(STATUS_PROCESSING, 0),
                completed=statuses.get(STATUS_COMPLETED, 0),
                failed=statuses.get(STATUS_FAILED, 0),
                dead_letter=statuses.get(STATUS_DEAD_LETTER, 0),
                avg_processing_time_ms=float(avg) if avg is not None else None,
                max_processing_time_ms=int(maximum) if maximum is not None else None,
            )
        )
    return stats


def _summarize_pending(pending_rows: Sequence[Any], now: datetime) -> list[event_log_schema.EventLogPending]:
    """Turn the pending groups into schema objects, oldest group first."""
    pending = [
        event_log_schema.EventLogPending(
            event_type=event_type,
            status=status,
            count=count,
            oldest_seconds=age_seconds(oldest, now),
        )
        for event_type, status, count, oldest in pending_rows
    ]
    pending.sort(key=lambda group: group.oldest_seconds or 0.0, reverse=True)
    return pending
