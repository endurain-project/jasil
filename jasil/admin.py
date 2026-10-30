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
from jasil._core.limits import MAX_DASHBOARD_LIST_LIMIT, MAX_DASHBOARD_WINDOW_HOURS, check_limit
from jasil.event_log.schema import (
    EventLogFailure,
    EventLogPending,
    EventLogSummary,
    EventTypeStats,
)
from jasil.jobs.schema import (
    DeadLetterJob,
    DurationBuckets,
    EventJobs,
    JobAttemptInfo,
    JobDetail,
    JobQueueStats,
    JobReplayResult,
    JobsSummary,
    JobSubscriberStats,
    WorkerAttempts,
    WorkerInfo,
    WorkersSummary,
)
from jasil.settings import get_settings

__all__ = [
    "DeadLetterJob",
    "DurationBuckets",
    "EventJobs",
    "EventLogFailure",
    "EventLogPending",
    "EventLogSummary",
    "EventTypeStats",
    "JobAttemptInfo",
    "JobDetail",
    "JobQueueStats",
    "JobReplayResult",
    "JobSubscriberStats",
    "JobsSummary",
    "WorkerAttempts",
    "WorkerInfo",
    "WorkersSummary",
    "get_event_jobs",
    "get_event_log_summary",
    "get_job",
    "get_jobs_summary",
    "get_worker_attempts",
    "get_workers_summary",
    "replay_dead_letter_job",
]


def _check_window(hours: int) -> None:
    check_limit(hours, maximum=MAX_DASHBOARD_WINDOW_HOURS, name="hours")


def get_jobs_summary(*, hours: int = 24, dead_letter_limit: int = 50) -> JobsSummary:
    """Aggregate ``processing_jobs`` into the durable-jobs dashboard payload.

    Args:
        hours: Look-back window for the status and per-subscriber counts, from 1 to 8784 (366 days).
        dead_letter_limit: Maximum dead-letter jobs to return for inspection, from 0 to 500.

    Returns:
        Window counts, the per-subscriber breakdown, the age of the oldest
        unfinished job, and the current dead-letter queue. The window ends at
        the platform clock's instant.

    Raises:
        ValueError: When ``hours`` or ``dead_letter_limit`` is out of range.
        RuntimeError: When no platform has been published, or when
            ``jasil.orm.configure_sessionmaker`` has not run.
    """
    import jasil.jobs.crud as jobs_crud

    _check_window(hours)
    check_limit(dead_letter_limit, minimum=0, maximum=MAX_DASHBOARD_LIST_LIMIT, name="dead_letter_limit")
    now = platform_runtime.get_active_platform().clock.now()
    with jasil_orm.get_sessionmaker()() as db:
        return jobs_crud.get_jobs_summary(
            db,
            now=now,
            hours=hours,
            dead_letter_limit=dead_letter_limit,
            record_attempts=get_settings().jobs.record_attempts,
        )


def get_event_log_summary(*, hours: int = 24, failure_limit: int = 20) -> EventLogSummary:
    """Aggregate ``event_log`` into the observability dashboard payload.

    Args:
        hours: Look-back window for the throughput, outcome, and latency stats, from 1 to 8784 (366 days).
        failure_limit: Maximum recent failures to return, from 0 to 500.

    Returns:
        Per-event-type statistics, the not-yet-finished groups and their oldest
        age, and the most recent failures. The window ends at the platform
        clock's instant.

    Raises:
        ValueError: When ``hours`` or ``failure_limit`` is out of range.
        RuntimeError: When no platform has been published, or when
            ``jasil.orm.configure_sessionmaker`` has not run.
    """
    import jasil.event_log.crud as event_log_crud

    _check_window(hours)
    check_limit(failure_limit, minimum=0, maximum=MAX_DASHBOARD_LIST_LIMIT, name="failure_limit")
    now = platform_runtime.get_active_platform().clock.now()
    with jasil_orm.get_sessionmaker()() as db:
        return event_log_crud.get_event_log_summary(db, now=now, hours=hours, failure_limit=failure_limit)


def get_workers_summary(
    *,
    stale_after_seconds: float | None = None,
    limit: int = 100,
    cursor: str | None = None,
    hours: int = 24,
) -> WorkersSummary:
    """Return global worker totals and one bounded telemetry page.

    Status is operator telemetry only: JASIL does not map it onto a host health
    endpoint or choose an alert policy.

    Args:
        stale_after_seconds: Maximum heartbeat age still considered running.
            Defaults to three configured heartbeat intervals.
        limit: Page size from 1 through 500.
        cursor: Opaque cursor returned as ``next_cursor`` by the previous page.
        hours: Look-back window for each worker's attempt counts (attempts that
            ended within it) and ``last_finished_at``, from 1 to 8784 (366 days).

    Returns:
        Worker counts and retained worker-instance details. The attempt counts
        are ``None`` when this process does not record attempts, and
        ``claimed_by_stale_workers`` counts jobs waiting for the reaper behind a
        worker that stopped beating.

    Raises:
        ValueError: When ``hours`` or ``limit`` is out of range, or ``cursor`` is not one this function returned.
        RuntimeError: When no platform has been published, or when
            ``jasil.orm.configure_sessionmaker`` has not run.
    """
    import jasil.jobs._worker_registry as worker_registry

    _check_window(hours)
    effective_stale_after = (
        stale_after_seconds if stale_after_seconds is not None else get_settings().jobs.heartbeat_interval_seconds * 3
    )
    now = platform_runtime.get_active_platform().clock.now()
    with jasil_orm.get_sessionmaker()() as db:
        return worker_registry.get_workers_summary(
            now=now,
            stale_after_seconds=effective_stale_after,
            db=db,
            limit=limit,
            cursor=cursor,
            hours=hours,
            record_attempts=get_settings().jobs.record_attempts,
        )


def get_job(job_id: str, *, attempt_limit: int = 50) -> JobDetail | None:
    """Return one job and its attempt history, newest attempt first.

    Answers "what happened to this job?": which worker ran each attempt, how long
    it took, how it ended, and why it failed.

    Args:
        job_id: The job id (as shown on a dead letter).
        attempt_limit: Most attempts to return, from 1 through 500.

    Returns:
        The job with its history, or ``None`` when no such job exists. With
        ``jobs.record_attempts`` off ``history`` holds only what was recorded
        earlier and ``attempts_recorded`` is ``False``.

    Raises:
        ValueError: When ``attempt_limit`` is out of range.
    """
    import jasil.jobs._attempts as attempts

    with jasil_orm.get_sessionmaker()() as db:
        return attempts.get_job_detail(
            job_id,
            db=db,
            attempt_limit=attempt_limit,
            record_attempts=get_settings().jobs.record_attempts,
        )


def get_event_jobs(event_id: str, *, attempt_limit: int = 20) -> EventJobs:
    """Return every subscriber's job for one event, each with its attempts.

    Answers "which worker processed this event, for each subscriber?". An event's
    ``event_log`` row records its publication only; a durable event's outcome is
    the state of these jobs.

    Args:
        event_id: The event id.
        attempt_limit: Most attempts to return per job, from 1 through 500.

    Returns:
        The event's jobs (at most 200), oldest first; empty when the event has
        none.

    Raises:
        ValueError: When ``attempt_limit`` is out of range.
    """
    import jasil.jobs._attempts as attempts

    with jasil_orm.get_sessionmaker()() as db:
        return attempts.get_event_jobs(
            event_id,
            db=db,
            attempt_limit=attempt_limit,
            record_attempts=get_settings().jobs.record_attempts,
        )


def get_worker_attempts(
    instance_id: str,
    *,
    limit: int = 100,
    cursor: str | None = None,
) -> WorkerAttempts:
    """Return one page of a worker instance's attempts, newest first.

    Answers "what has this worker been doing, and how did it go?".

    Args:
        instance_id: The worker instance id (``WorkerInfo.instance_id``).
        limit: Page size from 1 through 500.
        cursor: Opaque cursor returned as ``next_cursor`` by the previous page.

    Returns:
        The page of attempts; empty for an unknown worker.

    Raises:
        ValueError: When ``limit`` is out of range or ``cursor`` is not one
            this function returned.
    """
    import jasil.jobs._attempts as attempts

    with jasil_orm.get_sessionmaker()() as db:
        return attempts.get_worker_attempts(
            instance_id,
            db=db,
            limit=limit,
            cursor=cursor,
            record_attempts=get_settings().jobs.record_attempts,
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
