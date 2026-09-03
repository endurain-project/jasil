"""The operator-facing surface: dashboard aggregates and dead-letter replay.

The event-log and durable-job tables are what an operator needs to see, and the
queries that aggregate them live in :mod:`jasil.event_log.crud` and
:mod:`jasil.jobs.crud`. Neither is a good thing for a host to wire a route to:
both reach a model at import time, so importing either before
:func:`jasil.orm.map_models` has run raises, and both take a session they then
**commit** — which, handed a session that already carries an admin request's own
uncommitted work, commits that too.

This module is the stable seam in front of them. It is safe to import from
anywhere in the host's import graph (the model imports are deferred into the
functions), and nothing here accepts a session: each call opens a short-lived one
of its own, so a host cannot hand JASIL the transaction its request is in the
middle of::

    import jasil.admin as jasil_admin

    @router.get("/admin/jobs")
    def jobs_dashboard() -> jasil_admin.JobsSummary:
        return jasil_admin.get_jobs_summary()

    @router.post("/admin/jobs/{job_id}/replay")
    def replay(job_id: str) -> jasil_admin.JobReplayResult:
        return jasil_admin.replay_dead_letter_job(job_id)

The response schemas are re-exported here so a host can type its routes without
importing from an internal module. Authentication and authorization are the
host's: these functions expose operational data and one state-changing action,
and JASIL has no notion of who is calling.
"""

import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil.event_log.schema import (
    EventLogFailure,
    EventLogPending,
    EventLogSummary,
    EventTypeStats,
)
from jasil.jobs.schema import (
    DeadLetterJob,
    JobQueueStats,
    JobReplayResult,
    JobsSummary,
    JobSubscriberStats,
    WorkerInfo,
    WorkersSummary,
)
from jasil.settings import get_settings

__all__ = [
    "DeadLetterJob",
    "EventLogFailure",
    "EventLogPending",
    "EventLogSummary",
    "EventTypeStats",
    "JobQueueStats",
    "JobReplayResult",
    "JobSubscriberStats",
    "JobsSummary",
    "WorkerInfo",
    "WorkersSummary",
    "get_event_log_summary",
    "get_jobs_summary",
    "get_workers_summary",
    "replay_dead_letter_job",
]


def get_jobs_summary(*, hours: int = 24, dead_letter_limit: int = 50) -> JobsSummary:
    """Aggregate ``processing_jobs`` into the durable-jobs dashboard payload.

    Args:
        hours: Look-back window for the status and per-subscriber counts.
        dead_letter_limit: Maximum dead-letter jobs to return for inspection.

    Returns:
        Window counts, the per-subscriber breakdown, the age of the oldest
        unfinished job, and the current dead-letter queue.

    Raises:
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    import jasil.jobs.crud as jobs_crud

    with jasil_orm.get_sessionmaker()() as db:
        return jobs_crud.get_jobs_summary(db, hours=hours, dead_letter_limit=dead_letter_limit)


def get_event_log_summary(*, hours: int = 24, failure_limit: int = 20) -> EventLogSummary:
    """Aggregate ``event_log`` into the observability dashboard payload.

    Args:
        hours: Look-back window for the throughput, outcome, and latency stats.
        failure_limit: Maximum recent failures to return.

    Returns:
        Per-event-type statistics, the not-yet-finished groups and their oldest
        age, and the most recent failures.

    Raises:
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    import jasil.event_log.crud as event_log_crud

    with jasil_orm.get_sessionmaker()() as db:
        return event_log_crud.get_event_log_summary(db, hours=hours, failure_limit=failure_limit)


def get_workers_summary(
    *,
    stale_after_seconds: float | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> WorkersSummary:
    """Return global worker totals and one bounded telemetry page.

    Status is operator telemetry only: JASIL does not map it onto a host health
    endpoint or choose an alert policy.

    Args:
        stale_after_seconds: Maximum heartbeat age still considered running.
            Defaults to three configured heartbeat intervals.
        limit: Page size from 1 through 500.
        cursor: Opaque cursor returned as ``next_cursor`` by the previous page.

    Returns:
        Worker counts and retained worker-instance details.
    """
    from datetime import UTC, datetime

    import jasil.jobs._worker_registry as worker_registry

    effective_stale_after = (
        stale_after_seconds if stale_after_seconds is not None else get_settings().jobs.heartbeat_interval_seconds * 3
    )
    with jasil_orm.get_sessionmaker()() as db:
        return worker_registry.get_workers_summary(
            now=datetime.now(UTC),
            stale_after_seconds=effective_stale_after,
            db=db,
            limit=limit,
            cursor=cursor,
        )


def replay_dead_letter_job(job_id: str) -> JobReplayResult:
    """Requeue a dead-lettered job for a fresh run with a full attempt budget.

    Only a job currently in ``dead_letter`` is affected, so replaying one twice
    is harmless: the second call finds it ``pending`` and reports ``False``.

    Args:
        job_id: The job to replay.

    Returns:
        Whether a dead-letter job was requeued.

    Raises:
        RuntimeError: When no platform has been published, or when
            ``jasil.orm.configure_sessionmaker`` has not run.
    """
    import jasil.jobs.crud as jobs_crud

    now = platform_runtime.get_active_platform().clock.now()
    with jasil_orm.get_sessionmaker()() as db:
        replayed = jobs_crud.replay_dead_letter_job(job_id, now=now, db=db)
    return JobReplayResult(replayed=replayed)
