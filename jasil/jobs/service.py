"""Wiring for durable job processing — worker lifecycle and scheduled maintenance.

Consumed by application startup (start/stop the in-process worker) and the
scheduler (relay the outbox into jobs; reap expired leases). The relay, reaper,
and worker all run on every process and coordinate through ``SELECT ... FOR
UPDATE SKIP LOCKED`` plus compare-and-set claims and the idempotent job fan-out
(dedup on ``event_id + subscriber_id``), so replicas scale horizontally without a
single-runner lock — duplicate work is skipped or deduplicated rather than
serialized. All of this is inert unless durable jobs are enabled.
"""

import asyncio
import logging
import threading
import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler

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
    """Return a restart-unique worker instance identifier."""
    return str(uuid.uuid4())


def build_runner(*, queues: Iterable[str] | None = None) -> "JobRunner":
    """
    Build a :class:`JobRunner` from settings and the active platform.

    Returns:
        A runner wired to the durable-subscriber registry, the platform clock,
        and the main-database session factory.

    Args:
        queues: Optional non-empty queue allowlist. Omit it to consume all queues.
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
        queues=queues,
    )


def start_job_worker(
    *,
    role: str | None = None,
    label: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Start the in-process job worker (idempotent)."""
    global _worker
    if _worker is not None:
        if _worker.is_alive:
            return
        _worker = None
    _ensure_in_process_topology()
    from jasil.jobs.worker import BackgroundWorker

    # The loop logs its own start and stop, from the thread that actually runs it.
    runner = build_runner()
    jobs = get_settings().jobs
    telemetry = _build_worker_telemetry(runner, role=role, label=label, metadata=metadata)
    _worker = BackgroundWorker(runner, poll_interval_seconds=jobs.poll_interval_seconds, telemetry=telemetry)
    _worker.start()


def run_job_worker(
    *,
    queues: Iterable[str] | None = None,
    role: str | None = None,
    label: str | None = None,
    metadata: dict[str, Any] | None = None,
    stop: threading.Event | None = None,
) -> None:
    """Run a blocking standalone durable-job worker.

    This is the supported process entrypoint for distributed deployments. The
    host configures JASIL, registers its durable subscribers, and calls this
    function without importing runner or CRUD internals. Signal handling remains
    host-owned: pass an event that the host sets during graceful shutdown. This
    function blocks until that event is set; run it only as a dedicated worker
    process entrypoint, never on an API request or event-loop thread.

    Args:
        queues: Optional non-empty queue allowlist. Omit it to consume all queues.
        role: Optional host-supplied worker role.
        label: Optional host-supplied operator label.
        metadata: Optional host-supplied neutral metadata.
        stop: Event ending the loop; a new unset event is used when omitted.

    Raises:
        RuntimeError: When the configured database is SQLite, which supports
            only JASIL's one in-process consumer topology.
        ValueError: When an explicit queue allowlist is empty or invalid.
    """
    runner = build_runner(queues=queues)
    _ensure_standalone_topology()
    from jasil.jobs.worker import run_worker

    jobs = get_settings().jobs
    telemetry = _build_worker_telemetry(runner, role=role, label=label, metadata=metadata)
    run_worker(
        runner,
        poll_interval_seconds=jobs.poll_interval_seconds,
        stop=stop or threading.Event(),
        telemetry=telemetry,
    )


def _build_worker_telemetry(
    runner: "JobRunner",
    *,
    role: str | None,
    label: str | None,
    metadata: dict[str, Any] | None,
):
    from jasil.jobs.worker import WorkerTelemetry

    return WorkerTelemetry(
        instance_id=runner.worker_id,
        clock=runner.clock,
        session_factory=jasil_orm.get_sessionmaker(),
        heartbeat_interval_seconds=get_settings().jobs.heartbeat_interval_seconds,
        queues=runner.queues,
        role=role,
        label=label,
        metadata=metadata,
    )


def stop_job_worker() -> None:
    """Stop the in-process worker from synchronous shutdown code."""
    global _worker
    if _worker is None:
        return
    worker = _worker
    if worker.stop() and _worker is worker:
        _worker = None


async def stop_job_worker_async() -> None:
    """Stop the in-process worker without blocking the caller's event loop."""
    await asyncio.to_thread(stop_job_worker)


def _ensure_in_process_topology() -> None:
    if jasil_orm.get_engine().dialect.name == "sqlite" and get_settings().web_workers != 1:
        raise RuntimeError(
            "SQLite durable jobs support exactly one API process with one in-process consumer; "
            "configure web_workers=1 or use PostgreSQL workers"
        )


def _ensure_standalone_topology() -> None:
    if jasil_orm.get_engine().dialect.name == "sqlite":
        raise RuntimeError(
            "SQLite durable jobs support only one in-process consumer; "
            "use start_job_worker() or configure PostgreSQL for standalone workers"
        )


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
