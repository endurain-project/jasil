"""Wiring for durable job processing — worker lifecycle and scheduled maintenance.

Consumed by application startup (start/stop the in-process worker) and the
scheduler (relay the outbox into jobs; reap expired leases). The relay, reaper,
and worker all run on every process and coordinate through ``SELECT ... FOR
UPDATE SKIP LOCKED`` plus compare-and-set claims and the idempotent job fan-out
(dedup on ``event_id + subscriber_id``), so replicas scale horizontally without a
single-runner lock — duplicate work is skipped or deduplicated rather than
serialized. All of this is inert unless durable jobs are enabled.
"""

import logging
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import jasil._core.identity as identity
import jasil.jobs.registry as jobs_registry
import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil.settings import get_settings

# The runner, the worker, the relay and the job CRUD all reach a model bound to
# the host's declarative base, so importing them at module scope would make
# ``import jasil.jobs.service`` fail until ``jasil.orm.map_models`` had run. Each
# function imports what it needs instead.
if TYPE_CHECKING:
    from jasil.jobs.runner import JobRunner
    from jasil.jobs.runner_async import AsyncJobRunner
    from jasil.jobs.worker import BackgroundWorker
    from jasil.jobs.worker_async import AsyncBackgroundWorker

logger = logging.getLogger(__name__)

# How often the scheduler relays the outbox and reaps expired leases (seconds).
_RELAY_INTERVAL_SECONDS = 5
_REAP_INTERVAL_SECONDS = 60
# Bound how many outbox batches one relay pass drains before yielding.
_MAX_RELAY_BATCHES = 100

_RELAY_JOB_ID = "jasil_outbox_relay"
_REAP_JOB_ID = "jasil_job_reaper"
_ASYNC_RELAY_JOB_ID = "jasil_outbox_relay_async"
_ASYNC_REAP_JOB_ID = "jasil_job_reaper_async"

_worker: "BackgroundWorker | None" = None
_async_worker: "AsyncBackgroundWorker | None" = None


def _worker_id() -> str:
    """Return a per-process worker identifier (host + pid)."""
    return identity.process_identity()


def build_runner() -> "JobRunner":
    """
    Build a :class:`JobRunner` from settings and the active platform.

    Returns:
        A runner wired to the durable-subscriber registry, the platform clock,
        and the main-database session factory.
    """
    from jasil.jobs.runner import JobRunner

    jobs = get_settings().jobs
    platform = platform_runtime.get_active_platform()
    return JobRunner(
        registry=jobs_registry.registry,
        clock=platform.clock,
        session_factory=jasil_orm.get_sessionmaker(),
        worker_id=_worker_id(),
        lease_seconds=jobs.lease_seconds,
        batch_size=jobs.batch_size,
        backoff_base_seconds=jobs.backoff_base_seconds,
        backoff_max_seconds=jobs.backoff_max_seconds,
    )


def start_job_worker() -> None:
    """Start the in-process job worker (idempotent)."""
    global _worker
    if _worker is not None:
        return
    from jasil.jobs.worker import BackgroundWorker

    # The loop logs its own start and stop, from the thread that actually runs it.
    _worker = BackgroundWorker(build_runner(), poll_interval_seconds=get_settings().jobs.poll_interval_seconds)
    _worker.start()


def stop_job_worker() -> None:
    """Stop the in-process job worker if it is running."""
    global _worker
    if _worker is None:
        return
    _worker.stop()
    _worker = None


def relay_outbox_scheduled() -> None:
    """Relay the outbox into per-subscriber jobs.

    Runs on every replica; each pass claims its batch with ``FOR UPDATE SKIP
    LOCKED`` held across the fan-out, and the idempotent fan-out dedups any
    overlap where the dialect cannot lock, so no single-runner lock is needed.
    """
    import jasil.jobs.relay as jobs_relay

    jobs = get_settings().jobs
    platform = platform_runtime.get_active_platform()
    total = 0
    for _ in range(_MAX_RELAY_BATCHES):
        relayed = jobs_relay.relay_outbox_once(
            registry=jobs_registry.registry,
            clock=platform.clock,
            session_factory=jasil_orm.get_sessionmaker(),
            max_attempts=jobs.max_attempts,
            batch_size=jobs.batch_size,
        )
        total += relayed
        if relayed == 0:
            break
    if total:
        # Debug, not info: this runs every few seconds, and "an event was
        # published but never processed" is the question it exists to answer.
        logger.debug("Relayed %d outbox row(s) into durable jobs", total)


def reap_expired_jobs_scheduled() -> None:
    """Requeue or dead-letter jobs with expired leases.

    Runs on every replica; ``reclaim_expired_leases`` combines ``FOR UPDATE SKIP
    LOCKED`` with a compare-and-set on ``status``, so concurrent reapers reclaim
    disjoint rows and a loser never overwrites the winner's requeue.
    """
    import jasil.jobs.crud as jobs_crud

    platform = platform_runtime.get_active_platform()
    with jasil_orm.get_sessionmaker()() as db:
        reclaimed = jobs_crud.reclaim_expired_leases(now=platform.clock.now(), db=db)
    if reclaimed:
        # A warning, not an info: a lease only expires because the worker holding
        # it died or overran, so this is the symptom of something else going wrong.
        logger.warning("Reaped %d expired job lease(s); a worker died mid-job or overran its lease", reclaimed)


def schedule_job_maintenance(scheduler: AsyncIOScheduler) -> None:
    """
    Register the recurring relay and reaper jobs on the scheduler.

    Args:
        scheduler: The application scheduler to register the jobs on.

    Returns:
        None.
    """
    scheduler.add_job(
        relay_outbox_scheduled,
        "interval",
        seconds=_RELAY_INTERVAL_SECONDS,
        id=_RELAY_JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        reap_expired_jobs_scheduled,
        "interval",
        seconds=_REAP_INTERVAL_SECONDS,
        id=_REAP_JOB_ID,
        replace_existing=True,
    )
    logger.info("Scheduled durable-job outbox relay and lease reaper")


# ---------------------------------------------------------------------------
# Async face — mirrors the sync API above but uses the async runner/relay/worker
# ---------------------------------------------------------------------------


def build_async_runner() -> "AsyncJobRunner":
    """
    Build an :class:`~jasil.jobs.runner_async.AsyncJobRunner` from settings and the active async platform.

    Returns:
        An async runner wired to the async durable-subscriber registry, the
        platform clock, and the async-database session factory.
    """
    from jasil.jobs.runner_async import AsyncJobRunner, async_registry

    jobs = get_settings().jobs
    platform = platform_runtime.get_active_async_platform()
    return AsyncJobRunner(
        registry=async_registry,
        clock=platform.clock,
        session_factory=jasil_orm.get_async_sessionmaker(),
        worker_id=_worker_id(),
        lease_seconds=jobs.lease_seconds,
        batch_size=jobs.batch_size,
        backoff_base_seconds=jobs.backoff_base_seconds,
        backoff_max_seconds=jobs.backoff_max_seconds,
    )


async def start_async_job_worker() -> None:
    """Start the in-process async job worker (idempotent)."""
    global _async_worker
    if _async_worker is not None:
        return
    from jasil.jobs.worker_async import AsyncBackgroundWorker

    _async_worker = AsyncBackgroundWorker(
        build_async_runner(), poll_interval_seconds=get_settings().jobs.poll_interval_seconds
    )
    await _async_worker.start()


async def stop_async_job_worker() -> None:
    """Stop the in-process async job worker if it is running."""
    global _async_worker
    if _async_worker is None:
        return
    worker, _async_worker = _async_worker, None
    await worker.stop()


async def relay_outbox_async_scheduled() -> None:
    """Relay the outbox into per-subscriber jobs (async edition).

    Async twin of :func:`relay_outbox_scheduled`.  Runs on every replica; each
    pass claims its batch with ``FOR UPDATE SKIP LOCKED`` held across the
    fan-out, and the idempotent fan-out dedups any overlap where the dialect
    cannot lock, so no single-runner lock is needed.
    """
    import jasil.jobs.relay_async as jobs_relay_async
    from jasil.jobs.runner_async import async_registry

    jobs = get_settings().jobs
    platform = platform_runtime.get_active_async_platform()
    total = 0
    for _ in range(_MAX_RELAY_BATCHES):
        relayed = await jobs_relay_async.relay_outbox_once(
            registry=async_registry,
            clock=platform.clock,
            session_factory=jasil_orm.get_async_sessionmaker(),
            max_attempts=jobs.max_attempts,
            batch_size=jobs.batch_size,
        )
        total += relayed
        if relayed == 0:
            break
    if total:
        # Debug, not info: this runs every few seconds, and "an event was
        # published but never processed" is the question it exists to answer.
        logger.debug("Async relay: relayed %d outbox row(s) into durable jobs", total)


async def reap_expired_jobs_async_scheduled() -> None:
    """Requeue or dead-letter jobs with expired leases (async edition).

    Async twin of :func:`reap_expired_jobs_scheduled`.  Runs on every replica;
    ``reclaim_expired_leases`` combines ``FOR UPDATE SKIP LOCKED`` with a
    compare-and-set on ``status``, so concurrent reapers reclaim disjoint rows
    and a loser never overwrites the winner's requeue.
    """
    import jasil.jobs.crud_async as jobs_crud

    platform = platform_runtime.get_active_async_platform()
    async with jasil_orm.get_async_sessionmaker()() as db:
        reclaimed = await jobs_crud.reclaim_expired_leases(now=platform.clock.now(), db=db)
    if reclaimed:
        # A warning, not an info: a lease only expires because the worker holding
        # it died or overran, so this is the symptom of something else going wrong.
        logger.warning(
            "Async reaper: reaped %d expired job lease(s); a worker died mid-job or overran its lease", reclaimed
        )


def schedule_async_job_maintenance(scheduler: AsyncIOScheduler) -> None:
    """
    Register the recurring async relay and reaper coroutine jobs on the scheduler.

    The scheduler is an :class:`~apscheduler.schedulers.asyncio.AsyncIOScheduler`,
    which natively awaits coroutine jobs, so the coroutines
    :func:`relay_outbox_async_scheduled` and
    :func:`reap_expired_jobs_async_scheduled` are registered directly.  A
    separate function (rather than an optional flag on
    :func:`schedule_job_maintenance`) keeps the two registrations independent:
    a process running only the async face calls only this function, and one
    running both faces calls both.

    Args:
        scheduler: The application scheduler to register the async jobs on.

    Returns:
        None.
    """
    scheduler.add_job(
        relay_outbox_async_scheduled,
        "interval",
        seconds=_RELAY_INTERVAL_SECONDS,
        id=_ASYNC_RELAY_JOB_ID,
        replace_existing=True,
    )
    scheduler.add_job(
        reap_expired_jobs_async_scheduled,
        "interval",
        seconds=_REAP_INTERVAL_SECONDS,
        id=_ASYNC_REAP_JOB_ID,
        replace_existing=True,
    )
    logger.info("Scheduled async durable-job outbox relay and lease reaper")
