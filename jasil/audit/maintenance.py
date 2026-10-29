"""Audit maintenance: catalog sync, partitions, checkpoints and retention.

Owned by JASIL, scheduled by the host: JASIL never starts a scheduler. Call :func:`run_audit_maintenance`
yourself, or register it with :func:`schedule_audit_maintenance`. It is separate from
:func:`jasil.retention.prune_expired_records` and takes its own lock, so neither can block the other.

Retention here is not a ``DELETE``. On PostgreSQL the log is range-partitioned by day and expiry drops
whole partitions through ``jasil_audit_drop_partitions``, which also refuses to go younger than the
retention floor and writes its own ``audit.retention.prune`` row. Other dialects (test environments)
``DELETE`` the rows instead and record the prune the same way.

The process that runs this needs the maintenance grants (see :func:`jasil.audit.grants_sql`): sealing
checkpoints means reading the log.
"""

import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil._core.pruning import bounded_delete
from jasil.audit import checkpoints, service
from jasil.audit.catalog import (
    CATALOG_SYNC,
    RETENTION_CHANGE,
    RETENTION_PRUNE,
    CatalogSyncDetails,
    RetentionChangeDetails,
    RetentionPruneDetails,
    get_catalog,
)
from jasil.audit.context import ActorType, AuditContext, Source, audit_context
from jasil.settings import get_settings

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

__all__ = ["CatalogSyncResult", "run_audit_maintenance", "schedule_audit_maintenance", "sync_catalog"]

_LOCK_NAME = "jasil_audit_maintenance"
_JOB_ID = "jasil_audit_maintenance"
#: Checkpoints are sealed once a window plus its grace has passed, so this bounds how far behind they run.
_INTERVAL_MINUTES = 5

_warned_plain = False


@dataclass(frozen=True)
class CatalogSyncResult:
    """
    What a catalog synchronization changed.

    Attributes:
        added: Actions inserted.
        updated: Existing rows whose definition changed.
        deprecated: Rows marked deprecated because the action is no longer registered.
    """

    added: int = 0
    updated: int = 0
    deprecated: int = 0

    @property
    def changed(self) -> bool:
        """Whether anything was written beyond the ``last_seen_at`` stamp."""
        return bool(self.added or self.updated or self.deprecated)


def sync_catalog() -> CatalogSyncResult:
    """
    Publish the registered catalog to ``jasil_audit_actions``.

    New actions are inserted, changed ones updated, and actions no longer registered are marked
    ``deprecated``; rows are never deleted. This is how the observability product learns the catalog
    without importing the host's code, so call it at startup, after registering every action, and **only
    from the application that owns the catalog**: a process with an empty catalog would deprecate
    everything. Safe to call from several replicas at once. A change is recorded as
    ``audit.catalog.sync``.

    Returns:
        The counts of what changed.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On a database error.
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    if not get_settings().audit.enabled:
        return CatalogSyncResult()
    try:
        return _sync_catalog()
    except IntegrityError:
        # Another replica inserted the same action first; its rows are visible now.
        return _sync_catalog()


def _sync_catalog() -> CatalogSyncResult:
    from jasil.audit.models import AuditActionRecord

    now = service.now()
    registered = {action.id: action for action in get_catalog()}
    added = updated = deprecated = 0
    with jasil_orm.get_sessionmaker()() as db:
        existing = {row.id: row for row in db.scalars(select(AuditActionRecord))}
        for action in registered.values():
            row = existing.get(action.id)
            if row is None:
                db.add(
                    AuditActionRecord(
                        id=action.id,
                        category=action.category.value,
                        severity=action.severity.value,
                        description_key=action.description_key,
                        description_default=action.description_default,
                        deprecated=action.deprecated,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                )
                added += 1
                continue
            wanted = (
                action.category.value,
                action.severity.value,
                action.description_key,
                action.description_default,
                action.deprecated,
            )
            if wanted != (row.category, row.severity, row.description_key, row.description_default, row.deprecated):
                (row.category, row.severity, row.description_key, row.description_default, row.deprecated) = wanted
                updated += 1
        for action_id, row in existing.items():
            if action_id not in registered and not row.deprecated:
                row.deprecated = True
                deprecated += 1
        if registered:
            db.execute(
                update(AuditActionRecord)
                .where(AuditActionRecord.id.in_([action_id for action_id in registered if action_id in existing]))
                .values(last_seen_at=now)
            )
        result = CatalogSyncResult(added, updated, deprecated)
        if result.changed:
            with _system_context():
                service.stage(
                    db, CATALOG_SYNC, details=CatalogSyncDetails(added=added, updated=updated, deprecated=deprecated)
                )
        db.commit()
    return result


def _system_context() -> AbstractContextManager[AuditContext]:
    return audit_context(actor_type=ActorType.SYSTEM, actor_id=None, source=Source.WORKER)


def run_audit_maintenance() -> None:
    """
    Run the audit maintenance steps once, under a lock shared by every replica.

    In order: synchronize the catalog; record a change to the retention settings; create the next
    partitions (PostgreSQL); seal the checkpoints that are due; apply retention. Each step runs on its own
    sessions and a failure in one is logged and does not skip the others. Idempotent, and a no-op while
    another replica holds the lock or ``audit.enabled`` is false.

    Retention is skipped while ``audit.legal_hold`` is set or ``audit.retention_days`` is ``<= 0``.

    Returns:
        None.
    """
    settings = get_settings().audit
    if not settings.enabled:
        return
    platform = platform_runtime.get_active_platform()
    with platform.lock.try_acquire(_LOCK_NAME) as acquired:
        if not acquired:
            logger.debug("Audit maintenance: another replica holds the lock; skipping")
            return
        now = platform.clock.now()
        steps: tuple[tuple[str, Callable[[], object]], ...] = (
            ("catalog sync", sync_catalog),
            ("retention settings", _record_retention_change),
            ("partitions", _ensure_partitions),
            ("checkpoints", lambda: checkpoints.seal_pending_windows(now)),
            ("retention", lambda: _apply_retention(now)),
        )
        for name, step in steps:
            try:
                step()
            except Exception:
                logger.exception("Audit maintenance step %r failed", name)


def _record_retention_change() -> None:
    """Stage ``audit.retention.change`` when the retention settings differ from the last run's."""
    from jasil.audit.models import Instance

    settings = get_settings().audit
    with jasil_orm.get_sessionmaker()() as db:
        service.ensure_instance_id(service.engine_of(db))
        row = db.scalars(select(Instance).where(Instance.id == 1)).one()
        if row.retention_days is not None and (
            row.retention_days != settings.retention_days or row.legal_hold != settings.legal_hold
        ):
            with _system_context():
                service.stage(
                    db,
                    RETENTION_CHANGE,
                    details=RetentionChangeDetails(
                        retention_days_before=row.retention_days,
                        retention_days_after=settings.retention_days,
                        legal_hold_before=bool(row.legal_hold),
                        legal_hold_after=settings.legal_hold,
                    ),
                )
        row.retention_days = settings.retention_days
        row.legal_hold = settings.legal_hold
        db.commit()


def _partitioned(db: Session) -> bool:
    """Whether the log was built by migration ``rev0006`` on PostgreSQL, with its partition functions.

    A schema built by ``create_all`` (or on another dialect) is a plain table: there are no partitions to
    manage, and retention is a ``DELETE``. A PostgreSQL schema that is plain is logged once, because it also
    lacks the append-only triggers and the retention floor.
    """
    global _warned_plain
    if db.get_bind().dialect.name != "postgresql":
        return False
    if db.scalar(text("SELECT to_regprocedure('jasil_audit_drop_partitions(timestamptz)')")) is not None:
        return True
    if not _warned_plain:
        logger.warning(
            "jasil_audit_log is a plain table (migration rev0006 has not run): it is not partitioned, and nothing "
            "keeps it append-only or enforces the retention floor"
        )
        _warned_plain = True
    return False


def _ensure_partitions() -> None:
    with jasil_orm.get_sessionmaker()() as db:
        if not _partitioned(db):
            return
        furthest = db.execute(
            text("SELECT jasil_audit_ensure_partitions(:days)"), {"days": get_settings().audit.partition_days_ahead}
        ).scalar_one()
        db.commit()
    service.note_maintenance(furthest_partition_date=furthest)


def _apply_retention(now: datetime) -> None:
    settings = get_settings().audit
    if settings.legal_hold:
        logger.info("Audit retention: legal hold is set; nothing is pruned")
        return
    if settings.retention_days <= 0:
        return
    cutoff = now - timedelta(days=settings.retention_days)
    with jasil_orm.get_sessionmaker()() as db:
        if _partitioned(db):
            dropped = db.execute(text("SELECT jasil_audit_drop_partitions(:cutoff)"), {"cutoff": cutoff}).scalar_one()
            db.commit()
            if dropped:
                logger.info("Audit retention: dropped %d partition(s) older than %s", dropped, cutoff.isoformat())
            return
        from jasil.audit.models import AuditLog

        # Whole checkpoint windows only, as a partition drop is, so no sealed window is left half-empty.
        span = timedelta(minutes=settings.checkpoint_window_minutes).total_seconds()
        cutoff = datetime.fromtimestamp(cutoff.timestamp() // span * span, UTC)
        deleted = bounded_delete(AuditLog, AuditLog.occurred_at < cutoff, db=db)
        if deleted:
            with _system_context():
                service.stage(db, RETENTION_PRUNE, details=RetentionPruneDetails(cutoff=cutoff, partitions_dropped=0))
            db.commit()
            logger.info("Audit retention: deleted %d row(s) older than %s", deleted, cutoff.isoformat())


def schedule_audit_maintenance(scheduler: "AsyncIOScheduler", *, run_at_startup: bool = True) -> None:
    """
    Register the recurring audit maintenance on the host's scheduler.

    Register it on every replica. The run takes a lock, so only one of them does the work per pass.

    Args:
        scheduler: The application scheduler to register the job on.
        run_at_startup: Run one pass as soon as the scheduler starts, so a process redeployed more often
            than the interval still seals checkpoints and creates partitions.

    Returns:
        None.
    """
    starts_now = {"next_run_time": datetime.now(UTC)} if run_at_startup else {}
    scheduler.add_job(
        run_audit_maintenance,
        "interval",
        minutes=_INTERVAL_MINUTES,
        id=_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        **starts_now,
    )
    logger.info("Scheduled JASIL audit maintenance")
