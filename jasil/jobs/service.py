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
import threading
import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import jasil.jobs.registry as jobs_registry
import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil.capabilities import topology_of
from jasil.profile import JobWorkerMode
from jasil.settings import get_settings

# The runner, the worker, the relay and the job CRUD all reach a model bound to
# the host's declarative base, so importing them at module scope would make
# ``import jasil.jobs.service`` fail until ``jasil.orm.map_models`` had run. Each
# function imports what it needs instead.
if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from jasil.jobs.runner import JobRunner
    from jasil.jobs.worker import BackgroundWorker, WorkerHeartbeat

logger = logging.getLogger(__name__)

# How often the scheduler relays the outbox and reaps expired leases (seconds).
_RELAY_INTERVAL_SECONDS = 5
_REAP_INTERVAL_SECONDS = 60
# Bound how many outbox batches one relay pass drains before yielding.
_MAX_RELAY_BATCHES = 100

_RELAY_JOB_ID = "jasil_outbox_relay"
_REAP_JOB_ID = "jasil_job_reaper"
_DEAD_LETTER_JOB_ID = "jasil_dead_letter_delivery"

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
        max_runtime_seconds=jobs.max_runtime_seconds,
        heartbeat_interval_seconds=jobs.heartbeat_interval_seconds,
        batch_size=jobs.batch_size,
        backoff_base_seconds=jobs.backoff_base_seconds,
        backoff_max_seconds=jobs.backoff_max_seconds,
        defer_max_seconds=jobs.defer_max_seconds,
        max_deferrals=jobs.max_deferrals,
        queues=queues,
        record_attempts=jobs.record_attempts,
    )


def start_job_worker(
    *,
    role: str | None = None,
    label: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Start the in-process job worker (idempotent).

    Belongs to ``job_worker_mode=in_process``, the default: each API process runs
    its own consumer. A deployment with dedicated worker processes declares
    ``standalone`` and uses :func:`run_job_worker` instead.

    Args:
        role: Optional host-supplied worker role.
        label: Optional host-supplied operator label.
        metadata: Optional host-supplied neutral metadata.

    Raises:
        RuntimeError: When ``job_worker_mode`` is not ``in_process``, or when the
            configured database is SQLite and the topology is multi-process.
    """
    global _worker
    if _worker is not None:
        if _worker.is_alive:
            return
        _worker = None
    _ensure_job_topology(JobWorkerMode.IN_PROCESS, entry_point="start_job_worker", other="run_job_worker")
    from jasil.jobs.worker import BackgroundWorker

    # The loop logs its own start and stop, from the thread that actually runs it.
    runner = build_runner()
    jobs = get_settings().jobs
    heartbeat = _build_heartbeat(runner, role=role, label=label, metadata=metadata)
    _worker = BackgroundWorker(runner, poll_interval_seconds=jobs.poll_interval_seconds, heartbeat=heartbeat)
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
    function without importing runner or CRUD internals. Register every durable
    subscriber first, including those on queues this worker does not select: the
    worker re-reads the registry on every claim, but retags legacy ``default`` rows
    from it once, on its first batch. Signal handling remains
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
        RuntimeError: When ``job_worker_mode`` is not ``standalone``, or when the
            configured database is SQLite, which supports only a single-process
            topology.
        ValueError: When an explicit queue allowlist is empty or invalid.
    """
    _ensure_job_topology(JobWorkerMode.STANDALONE, entry_point="run_job_worker", other="start_job_worker")
    runner = build_runner(queues=queues)
    from jasil.jobs.worker import run_worker

    jobs = get_settings().jobs
    heartbeat = _build_heartbeat(runner, role=role, label=label, metadata=metadata)
    run_worker(
        runner,
        poll_interval_seconds=jobs.poll_interval_seconds,
        stop=stop or threading.Event(),
        heartbeat=heartbeat,
    )


def _build_heartbeat(
    runner: "JobRunner",
    *,
    role: str | None,
    label: str | None,
    metadata: dict[str, Any] | None,
) -> "WorkerHeartbeat":
    from jasil.jobs.worker import WorkerHeartbeat, WorkerTelemetry

    telemetry = WorkerTelemetry(
        instance_id=runner.worker_id,
        clock=runner.clock,
        session_factory=jasil_orm.get_sessionmaker(),
        queues=runner.queues,
        role=role,
        label=label,
        metadata=metadata,
    )
    return WorkerHeartbeat(
        leases=runner.leases,
        interval_seconds=runner.heartbeat_interval_seconds,
        telemetry=telemetry,
    )


def stop_job_worker() -> None:
    """Stop the in-process worker from synchronous shutdown code."""
    global _worker
    if _worker is None:
        return
    worker = _worker
    if worker.stop() and _worker is worker:
        _worker = None


def _ensure_job_topology(expected: JobWorkerMode, *, entry_point: str, other: str) -> None:
    """Refuse to start a consumer the deployment's declared shape does not have.

    Two rules, both about whether this process is what the settings say it is:

    * **The role must match ``job_worker_mode``.** ``start_job_worker`` belongs to
      ``in_process`` and ``run_job_worker`` to ``standalone``. Every process of a
      deployment loads the same settings, and the consistency checks reason from
      that mode, so a consumer started the other way would be a process they
      never counted. This is the entry point's contract rather than capability
      wiring, so ``enforce_deployment_consistency=False`` does not relax it.
    * **SQLite needs a single-process topology.** One file cannot be shared by
      competing consumers, and a multi-process topology (several API processes,
      standalone workers, or replicas on separate nodes) has more than one.
    """
    settings = get_settings()
    if settings.job_worker_mode is not expected:
        raise RuntimeError(
            f"{entry_point}() requires job_worker_mode={expected.value!r}, but the configured mode is "
            f"{settings.job_worker_mode.value!r}; set job_worker_mode in every process of the deployment, "
            f"or use {other}() for this mode"
        )
    topology = topology_of(settings)
    if topology.multi_process and jasil_orm.get_engine().dialect.name == "sqlite":
        raise RuntimeError(
            "SQLite durable jobs support exactly one process with one in-process consumer; "
            f"the configured topology runs several ({topology.multi_process_reason}). "
            "Use a database that supports concurrent consumers, such as PostgreSQL"
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
    """Requeue or dead-letter jobs with expired leases; the dead-letter sweep runs their callbacks.

    Runs on every replica; ``reclaim_expired_leases`` combines ``FOR UPDATE SKIP
    LOCKED`` with a compare-and-set on ``status``, so concurrent reapers reclaim
    disjoint rows and a loser never overwrites the winner's requeue.
    """
    import jasil.jobs.crud as jobs_crud

    platform = platform_runtime.get_active_platform()
    now = platform.clock.now()
    session_factory = jasil_orm.get_sessionmaker()
    with session_factory() as db:
        # Read before reclaiming, which clears the holder: a crash should name the
        # worker, not just say that one happened.
        holders = jobs_crud.expired_lease_holders(now=now, db=db)
        reclaimed = jobs_crud.reclaim_expired_leases(now=now, db=db)
    if reclaimed:
        # A warning, not an info: a lease only expires because the worker holding
        # it died or went silent, so this is the symptom of something else going wrong.
        logger.warning(
            "Reaped %d expired job lease(s); a worker died mid-job or went silent",
            reclaimed,
            extra={"workers": holders},
        )


def deliver_dead_letters_scheduled() -> None:
    """
    Run the owed ``on_dead_letter`` callbacks of subscribers registered here.

    A job of its own, not part of the reaper, so slow callbacks never hold up the
    requeueing of expired leases. Never raises: whatever fails is retried by the
    next pass.
    """
    from jasil.jobs.dead_letter import deliver_dead_letters

    platform = platform_runtime.get_active_platform()
    try:
        deliver_dead_letters(
            registry=jobs_registry.registry,
            session_factory=jasil_orm.get_sessionmaker(),
            clock=platform.clock,
        )
    except Exception as error:
        logger.error("Dead-letter callbacks could not be delivered", exc_info=error)


def schedule_job_maintenance(scheduler: "AsyncIOScheduler") -> None:
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
    scheduler.add_job(
        deliver_dead_letters_scheduled,
        "interval",
        seconds=_REAP_INTERVAL_SECONDS,
        id=_DEAD_LETTER_JOB_ID,
        replace_existing=True,
    )
    logger.info("Scheduled durable-job outbox relay, lease reaper and dead-letter delivery")
