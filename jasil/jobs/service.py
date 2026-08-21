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
    from jasil.jobs.worker import BackgroundWorker

logger = logging.getLogger(__name__)

# How often the scheduler relays the outbox and reaps expired leases (seconds).
_RELAY_INTERVAL_SECONDS = 5
_REAP_INTERVAL_SECONDS = 60
# Bound how many outbox batches one relay pass drains before yielding.
_MAX_RELAY_BATCHES = 100

_RELAY_JOB_ID = "jasil_outbox_relay"
_REAP_JOB_ID = "jasil_job_reaper"

_worker: "BackgroundWorker | None" = None


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
