"""The read-only query surface of the audit trail, for the observability product.

Like :mod:`jasil.admin`, nothing here accepts a session: each call opens a short one of its own, so a
host cannot hand it the transaction a request is in the middle of. Authentication and authorization are
the host's — the host application exposes no read route; only the observability product reads the trail.

Pages are keyset-paginated over ``(occurred_at, id)``, newest first, never by offset. Every listing and
summary is bounded in time, 24 hours by default and :data:`MAX_WINDOW` at most, so PostgreSQL prunes
partitions and no call scans the whole trail.

An asynchronous route must run these through its framework's thread-pool helper; they use synchronous
sessions.
"""

from datetime import datetime, timedelta

from sqlalchemy import and_, func, or_, select
from sqlalchemy.engine import ScalarResult
from sqlalchemy.sql.elements import ColumnElement, SQLColumnExpression

import jasil.orm as jasil_orm
from jasil._core.cursors import decode_cursor, encode_cursor
from jasil._core.timestamps import as_utc
from jasil.audit import service
from jasil.audit.checkpoints import verify_checkpoints
from jasil.audit.schema import (
    ActionCount,
    AuditActionInfo,
    AuditEntry,
    AuditFilters,
    AuditPage,
    AuditSummary,
)

__all__ = ["get_entry", "list_catalog", "list_entries", "summary", "verify_checkpoints"]

DEFAULT_WINDOW = timedelta(hours=24)
#: The longest time range one listing or summary may cover.
MAX_WINDOW = timedelta(days=31)
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500
TOP_ACTIONS = 10


def list_entries(
    filters: AuditFilters | None = None, cursor: str | None = None, limit: int = DEFAULT_PAGE_SIZE
) -> AuditPage:
    """
    List audit entries, newest first, one keyset page at a time.

    Args:
        filters: What to match; see :class:`~jasil.audit.schema.AuditFilters`. The time range defaults to
            the 24 hours before ``to_ts``, which defaults to now, and may cover at most 31 days.
        cursor: The previous page's ``next_cursor``, or ``None`` for the first page.
        limit: Entries per page, 1 to 500.

    Returns:
        The page and the cursor of the next one (``None`` after the last).

    Raises:
        ValueError: When ``limit`` is out of range, ``from_ts`` is after ``to_ts``, the range is longer than
            31 days, or ``cursor`` is not one this function returned.
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    from jasil.audit.models import AuditLog

    if not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}, not {limit}")
    filters = filters or AuditFilters()
    to_ts = as_utc(filters.to_ts) if filters.to_ts is not None else service.now()
    from_ts = as_utc(filters.from_ts) if filters.from_ts is not None else to_ts - DEFAULT_WINDOW
    if from_ts > to_ts:
        raise ValueError(f"from_ts ({from_ts.isoformat()}) is after to_ts ({to_ts.isoformat()})")
    if to_ts - from_ts > MAX_WINDOW:
        raise ValueError(f"the time range may cover at most {MAX_WINDOW.days} days; page through longer ones")

    conditions: list[ColumnElement[bool]] = [AuditLog.occurred_at >= from_ts, AuditLog.occurred_at <= to_ts]
    for column, values in (
        (AuditLog.action_id, filters.action_ids),
        (AuditLog.category, filters.categories),
        (AuditLog.severity, filters.severities),
        (AuditLog.outcome, filters.outcomes),
    ):
        if values is not None:
            conditions.append(column.in_(values))
    for column, value in (
        (AuditLog.actor_id, filters.actor_id),
        (AuditLog.actor_type, filters.actor_type),
        (AuditLog.target_type, filters.target_type),
        (AuditLog.target_id, filters.target_id),
        (AuditLog.target_owner_id, filters.target_owner_id),
        (AuditLog.request_id, filters.request_id),
        (AuditLog.event_id, filters.event_id),
        (AuditLog.source, filters.source),
    ):
        if value is not None:
            conditions.append(column == value)
    if cursor is not None:
        moment, identifier = decode_cursor(cursor, name="audit")
        conditions.append(
            or_(
                AuditLog.occurred_at < moment,
                and_(AuditLog.occurred_at == moment, AuditLog.id < identifier),
            )
        )

    statement = (
        select(AuditLog).where(*conditions).order_by(AuditLog.occurred_at.desc(), AuditLog.id.desc()).limit(limit + 1)
    )
    with jasil_orm.get_sessionmaker()() as db:
        rows: list[AuditLog] = list(db.scalars(statement))
    entries = [AuditEntry.model_validate(row) for row in rows[:limit]]
    next_cursor = encode_cursor(entries[-1].occurred_at, entries[-1].id) if len(rows) > limit else None
    return AuditPage(entries=entries, next_cursor=next_cursor)


def get_entry(entry_id: str, occurred_at: datetime) -> AuditEntry | None:
    """
    Return one entry by its primary key.

    The timestamp is part of the key so PostgreSQL goes straight to one partition.

    Args:
        entry_id: The entry id.
        occurred_at: The entry's ``occurred_at``, as listed.

    Returns:
        The entry, or ``None`` when there is none.

    Raises:
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    from jasil.audit.models import AuditLog

    statement = select(AuditLog).where(AuditLog.id == entry_id, AuditLog.occurred_at == as_utc(occurred_at))
    with jasil_orm.get_sessionmaker()() as db:
        row = db.scalars(statement).first()
        return AuditEntry.model_validate(row) if row is not None else None


def summary(hours: int = 24) -> AuditSummary:
    """
    Count entries over a look-back window.

    Args:
        hours: The window, ending now: 1 to 744 (31 days).

    Returns:
        Counts by category, outcome and severity, and the most frequent actions.

    Raises:
        ValueError: When ``hours`` is out of range.
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    from jasil.audit.models import AuditLog

    longest = int(MAX_WINDOW.total_seconds() // 3600)
    if not 1 <= hours <= longest:
        raise ValueError(f"hours must be between 1 and {longest}, not {hours}")
    until = service.now()
    window = [AuditLog.occurred_at >= until - timedelta(hours=hours), AuditLog.occurred_at <= until]

    def counts(column: SQLColumnExpression[str]) -> dict[str, int]:
        return {key: count for key, count in db.execute(select(column, func.count()).where(*window).group_by(column))}

    with jasil_orm.get_sessionmaker()() as db:
        by_category = counts(AuditLog.category)
        by_outcome = counts(AuditLog.outcome)
        by_severity = counts(AuditLog.severity)
        top = db.execute(
            select(AuditLog.action_id, func.count().label("n"))
            .where(*window)
            .group_by(AuditLog.action_id)
            .order_by(func.count().desc(), AuditLog.action_id)
            .limit(TOP_ACTIONS)
        ).all()
    return AuditSummary(
        window_hours=hours,
        total=sum(by_outcome.values()),
        by_category=by_category,
        by_outcome=by_outcome,
        by_severity=by_severity,
        top_actions=[ActionCount(action_id=action_id, count=count) for action_id, count in top],
    )


def list_catalog() -> list[AuditActionInfo]:
    """
    Return the catalog as the host last synchronized it, deprecated actions included.

    Returns:
        Every row of ``jasil_audit_actions``, ordered by id.

    Raises:
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    from jasil.audit.models import AuditActionRecord

    with jasil_orm.get_sessionmaker()() as db:
        records: ScalarResult[AuditActionRecord] = db.scalars(select(AuditActionRecord).order_by(AuditActionRecord.id))
        return [AuditActionInfo.model_validate(row) for row in records]
