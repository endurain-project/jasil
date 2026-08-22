"""CRUD for the event_log table — lifecycle writes and dashboard aggregates.

**Internal.** Not covered by the API-stability contract, and it reaches a model
at import time, so it cannot be imported before ``jasil.orm.map_models`` has run.
Hosts wanting the dashboard aggregate should use :mod:`jasil.admin`, which is
importable from anywhere and opens its own session.

**Every write here commits the session it is given.** The recorder hands each one
a short-lived session of its own; pass a session JASIL owns, not one carrying a
caller's uncommitted work.

The recording helpers write the event lifecycle: ``record_published`` /
``mark_processing`` / ``mark_completed`` / ``mark_failed`` for bus-delivered
events (via the recorder in :mod:`jasil.event_log.recorder`), and ``record_queued``
for events handed to the durable job queue (via the publish facade). The
``get_event_log_summary`` helper powers the admin dashboard.

Nothing here builds a query. Every statement, and the whole dashboard
aggregation, lives in :mod:`jasil.event_log.statements` so this module and its
async twin (:mod:`jasil.event_log.crud_async`) cannot drift apart; what remains
here is the execution — synchronous ``execute`` and ``commit``. Every query
there is portable SQL, so the same code runs on PostgreSQL, MySQL and SQLite.
"""

from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

import jasil._core.pruning as pruning
import jasil.event_log.schema as event_log_schema
import jasil.event_log.statements as statements
from jasil.event_log.models import EventLog
from jasil.events import Event


def record_published(event: Event, db: Session) -> None:
    """
    Insert the initial ``published`` row for a freshly published event.

    Args:
        event: The event envelope being published.
        db: Active database session.

    Returns:
        None.
    """
    db.add(statements.new_row(event, status=statements.STATUS_PUBLISHED))
    db.commit()


def record_queued(event: Event, db: Session) -> None:
    """
    Insert a terminal ``queued`` row for an event handed to the durable job queue.

    Durable events are executed by the worker (tracked per-subscriber in
    ``processing_jobs``), not by the event bus, so ``queued`` is terminal from the
    event_log's perspective: it keeps durable events visible in the dashboard
    without counting them as perpetually ``pending``. Execution detail lives in
    the Jobs dashboard.

    Args:
        event: The event envelope being staged for durable delivery.
        db: Active database session.

    Returns:
        None.
    """
    db.add(statements.new_row(event, status=statements.STATUS_QUEUED))
    db.commit()


def mark_processing(event_id: str, worker_id: str, db: Session) -> None:
    """
    Transition an event to ``processing`` when a consumer picks it up.

    Args:
        event_id: The event_id (primary key).
        worker_id: The process/consumer handling the event.
        db: Active database session.

    Returns:
        None.
    """
    db.execute(statements.mark_processing_stmt(event_id, worker_id))
    db.commit()


def mark_completed(event_id: str, handler_name: str | None, processing_time_ms: int, db: Session) -> None:
    """
    Transition an event to ``completed`` after its handlers succeed.

    Args:
        event_id: The event_id (primary key).
        handler_name: The subscriber(s) that processed the event.
        processing_time_ms: Handler execution time in milliseconds.
        db: Active database session.

    Returns:
        None.
    """
    db.execute(statements.mark_completed_stmt(event_id, handler_name, processing_time_ms))
    db.commit()


def mark_failed(
    event_id: str,
    handler_name: str | None,
    error_message: str,
    processing_time_ms: int,
    db: Session,
) -> None:
    """
    Transition an event to ``failed`` when a handler raises.

    Args:
        event_id: The event_id (primary key).
        handler_name: The subscriber(s) that processed the event.
        error_message: The failure reason (truncated for storage).
        processing_time_ms: Handler execution time in milliseconds.
        db: Active database session.

    Returns:
        None.
    """
    db.execute(statements.mark_failed_stmt(event_id, handler_name, error_message, processing_time_ms))
    db.commit()


def get_event_log_summary(
    db: Session,
    *,
    hours: int = 24,
    failure_limit: int = 20,
) -> event_log_schema.EventLogSummary:
    """
    Aggregate the event_log into the admin-dashboard payload.

    Args:
        db: Active database session.
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
        status_rows=db.execute(statements.status_counts_stmt(window_start)).all(),
        latency_rows=db.execute(statements.latency_by_type_stmt(window_start)).all(),
        pending_rows=db.execute(statements.pending_groups_stmt()).all(),
        failure_rows=db.execute(statements.recent_failures_stmt(failure_limit)).scalars().all(),
    )


def delete_events_before(cutoff: datetime, *, db: Session, batch_size: int = pruning.PRUNE_BATCH_SIZE) -> int:
    """
    Delete ``event_log`` rows older than ``cutoff``, in bounded batches.

    Every row is prunable regardless of status: event_log is a best-effort,
    safe-to-lose observability trail (the dashboard is a recent-window view), so
    nothing here is a source of truth worth preserving past the retention window.

    Args:
        cutoff: Delete rows whose ``created_at`` is strictly before this instant.
        db: Active database session.
        batch_size: Maximum rows deleted per batch.

    Returns:
        The total number of rows deleted.
    """
    return pruning.bounded_delete(EventLog, statements.prune_condition(cutoff), db=db, batch_size=batch_size)
