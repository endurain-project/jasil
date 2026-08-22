"""Async CRUD for the event_log table — the asynchronous twin of :mod:`jasil.event_log.crud`.

**Internal.** Not covered by the API-stability contract, and it reaches a model
at import time, so it cannot be imported before ``jasil.orm.map_models`` has run.

This module is deliberately thin. Nothing here builds a query or aggregates a
result: every statement and the whole dashboard aggregation live in
:mod:`jasil.event_log.statements`, shared verbatim with the synchronous module.
What differs between the two faces is only ``execute`` versus ``await execute``,
which is exactly as much as should differ — the event lifecycle is a contract,
and two copies of it would be two things to keep in step.

**Every write here commits the session it is given**, matching the sync module:
the async recorder hands each one a short-lived session of its own, so pass a
session JASIL owns, not one carrying a caller's uncommitted work.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

import jasil._core.pruning as pruning
import jasil.event_log.schema as event_log_schema
import jasil.event_log.statements as statements
from jasil.event_log.models import EventLog
from jasil.events import Event


async def record_published(event: Event, db: AsyncSession) -> None:
    """
    Insert the initial ``published`` row for a freshly published event.

    Args:
        event: The event envelope being published.
        db: Active async database session.

    Returns:
        None.
    """
    # ``add`` is synchronous even on an AsyncSession — it only stages the
    # instance in the identity map; the I/O happens at flush/commit.
    db.add(statements.new_row(event, status=statements.STATUS_PUBLISHED))
    await db.commit()


async def record_queued(event: Event, db: AsyncSession) -> None:
    """
    Insert a terminal ``queued`` row for an event handed to the durable job queue.

    Durable events are executed by the worker (tracked per-subscriber in
    ``processing_jobs``), not by the event bus, so ``queued`` is terminal from the
    event_log's perspective: it keeps durable events visible in the dashboard
    without counting them as perpetually ``pending``.

    Args:
        event: The event envelope being staged for durable delivery.
        db: Active async database session.

    Returns:
        None.
    """
    db.add(statements.new_row(event, status=statements.STATUS_QUEUED))
    await db.commit()


async def mark_processing(event_id: str, worker_id: str, db: AsyncSession) -> None:
    """
    Transition an event to ``processing`` when a consumer picks it up.

    Args:
        event_id: The event_id (primary key).
        worker_id: The process/consumer handling the event.
        db: Active async database session.

    Returns:
        None.
    """
    await db.execute(statements.mark_processing_stmt(event_id, worker_id))
    await db.commit()


async def mark_completed(event_id: str, handler_name: str | None, processing_time_ms: int, db: AsyncSession) -> None:
    """
    Transition an event to ``completed`` after its handlers succeed.

    Args:
        event_id: The event_id (primary key).
        handler_name: The subscriber(s) that processed the event.
        processing_time_ms: Handler execution time in milliseconds.
        db: Active async database session.

    Returns:
        None.
    """
    await db.execute(statements.mark_completed_stmt(event_id, handler_name, processing_time_ms))
    await db.commit()


async def mark_failed(
    event_id: str,
    handler_name: str | None,
    error_message: str,
    processing_time_ms: int,
    db: AsyncSession,
) -> None:
    """
    Transition an event to ``failed`` when a handler raises.

    Args:
        event_id: The event_id (primary key).
        handler_name: The subscriber(s) that processed the event.
        error_message: The failure reason (truncated for storage).
        processing_time_ms: Handler execution time in milliseconds.
        db: Active async database session.

    Returns:
        None.
    """
    await db.execute(statements.mark_failed_stmt(event_id, handler_name, error_message, processing_time_ms))
    await db.commit()


async def get_event_log_summary(
    db: AsyncSession,
    *,
    hours: int = 24,
    failure_limit: int = 20,
) -> event_log_schema.EventLogSummary:
    """
    Aggregate the event_log into the admin-dashboard payload.

    Args:
        db: Active async database session.
        hours: Look-back window for throughput/outcome/latency stats.
        failure_limit: Maximum number of recent failures to return.

    Returns:
        The aggregated dashboard summary.
    """
    now = datetime.now(UTC)
    window_start = now - timedelta(hours=hours)
    return statements.build_summary(
        hours=hours,
        now=now,
        status_rows=(await db.execute(statements.status_counts_stmt(window_start))).all(),
        latency_rows=(await db.execute(statements.latency_by_type_stmt(window_start))).all(),
        pending_rows=(await db.execute(statements.pending_groups_stmt())).all(),
        failure_rows=(await db.execute(statements.recent_failures_stmt(failure_limit))).scalars().all(),
    )


async def delete_events_before(
    cutoff: datetime,
    *,
    db: AsyncSession,
    batch_size: int = pruning.PRUNE_BATCH_SIZE,
) -> int:
    """
    Delete ``event_log`` rows older than ``cutoff``, in bounded batches.

    Every row is prunable regardless of status: event_log is a best-effort,
    safe-to-lose observability trail, so nothing here is a source of truth worth
    preserving past the retention window.

    Args:
        cutoff: Delete rows whose ``created_at`` is strictly before this instant.
        db: Active async database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    return await pruning.bounded_delete_async(
        EventLog,
        statements.prune_condition(cutoff),
        db=db,
        batch_size=batch_size,
    )
