"""Scheduled retention pruning for the substrate's append-only bookkeeping tables.

The event-log and durable-job tables are append-only: every event writes an
``event_log`` row, the relay stamps ``event_outbox`` rows, and each
``(event, subscriber)`` pair produces a ``processing_jobs`` row. Left alone they
grow without bound, fastest in whichever subsystem publishes most. This module
prunes rows past their retention window on a schedule (one window for the
event_log trail, another for the durable-job tables — each configured and
disabled independently), deleting only what is safe to lose:

* every ``event_log`` row (best-effort, safe-to-lose observability trail),
* relayed ``event_outbox`` rows (already fanned out into jobs), and
* ``completed`` ``processing_jobs`` rows.

In-flight and human-actionable rows are never touched: unrelayed outbox rows
(pending relay), ``pending`` / ``claimed`` jobs (in-flight work), and
``dead_letter`` jobs (kept for operator review). It runs single-runner across
replicas via the platform ``LockProvider`` and is inert when both windows are
``<= 0`` (retention disabled — keep every row forever).
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil.settings import get_settings

# Only for the annotation: apscheduler is the optional ``jobs`` extra, and a
# deployment that prunes without durable jobs must still be able to import this.
if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger(__name__)

# Single-runner lock name: the deletes are idempotent, but the lock keeps the
# work from being duplicated across replicas.
_PRUNE_LOCK_NAME = "jasil_retention_prune"

# How often the scheduled prune runs, and the id it is registered under.
_PRUNE_INTERVAL_HOURS = 24
_PRUNE_JOB_ID = "jasil_retention_prune"


def prune_expired_records() -> None:
    """Prune substrate bookkeeping rows older than their retention windows.

    Call it yourself, or register it with
    :func:`schedule_retention_maintenance`. Each window is applied
    independently: ``JasilSettings.event_log.retention_days`` gates the event_log
    trail and ``JasilSettings.jobs.retention_days`` gates the durable-job tables.
    No-ops when both are disabled (``<= 0``) or when another replica already
    holds the prune lock.

    Returns:
        None.
    """
    settings = get_settings()
    event_log_days = settings.event_log.retention_days
    jobs_days = settings.jobs.retention_days
    if event_log_days <= 0 and jobs_days <= 0:
        return

    platform = platform_runtime.get_active_platform()
    with platform.lock.try_acquire(_PRUNE_LOCK_NAME) as acquired:
        if not acquired:
            logger.debug("Retention prune: another replica holds the lock; skipping")
            return
        _run_prune(platform.clock.now(), event_log_days, jobs_days)


def _run_prune(now: datetime, event_log_days: int, jobs_days: int) -> None:
    """Delete prunable rows older than each table's window.

    Each table is pruned on its own short-lived session so one table's failure
    cannot roll back another's progress. A window of ``<= 0`` skips that group.

    Args:
        now: The current instant; each cutoff is ``now`` minus that table's window.
        event_log_days: Retention window for the event_log trail (days).
        jobs_days: Retention window for the durable-job tables (days).

    Returns:
        None.
    """
    # Imported here rather than at module scope: these bind to the host's
    # declarative base, so a top-level import would make ``import jasil.retention``
    # fail until ``jasil.orm.map_models`` had run.
    import jasil.event_log.crud as event_log_crud
    import jasil.jobs.crud as jobs_crud
    import jasil.jobs.outbox as jobs_outbox

    events = outbox = jobs = 0

    if event_log_days > 0:
        cutoff = now - timedelta(days=event_log_days)
        with jasil_orm.get_sessionmaker()() as db:
            events = event_log_crud.delete_events_before(cutoff, db=db)

    if jobs_days > 0:
        cutoff = now - timedelta(days=jobs_days)
        with jasil_orm.get_sessionmaker()() as db:
            outbox = jobs_outbox.delete_relayed_before(cutoff, db=db)
        with jasil_orm.get_sessionmaker()() as db:
            jobs = jobs_crud.delete_completed_jobs_before(cutoff, db=db)

    if events or outbox or jobs:
        logger.info(
            f"Retention prune: deleted {events} event_log, {outbox} relayed outbox, and {jobs} completed job row(s)"
        )
    else:
        logger.debug("Retention prune: nothing to delete")


def schedule_retention_maintenance(scheduler: "AsyncIOScheduler", *, run_at_startup: bool = True) -> None:
    """
    Register the recurring retention prune on the host's scheduler.

    The counterpart of :func:`jasil.jobs.service.schedule_job_maintenance`, kept
    separate because retention also prunes the event_log — so it applies to a
    deployment that never enabled durable jobs.

    Register it on every replica. The prune takes the platform's coordination
    lock, so only one of them does the work per pass.

    Args:
        scheduler: The application scheduler to register the job on.
        run_at_startup: Run one pass as soon as the scheduler starts. On by
            default because a daily interval otherwise means a process that is
            redeployed daily never prunes at all.

    Returns:
        None.
    """
    starts_now = {"next_run_time": datetime.now(UTC)} if run_at_startup else {}
    scheduler.add_job(
        prune_expired_records,
        "interval",
        hours=_PRUNE_INTERVAL_HOURS,
        id=_PRUNE_JOB_ID,
        replace_existing=True,
        **starts_now,
    )
    logger.info("Scheduled JASIL retention pruning")
