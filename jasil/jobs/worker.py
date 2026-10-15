"""The worker loop that drains the durable job queue.

``run_worker`` is the blocking claim/run loop used both by the standalone worker
entrypoint and by :class:`BackgroundWorker`, which runs it on a daemon thread so
a single-process (``local``) deployment can process durable jobs without a
separate container. Reaping expired leases is a scheduled job, not part of this
loop.

A worker has one heartbeat, :class:`WorkerHeartbeat`. On every beat it extends the
leases of the jobs the worker holds and refreshes the worker's registry row, so
"this worker is alive" and "this worker still holds these jobs" are a single
signal on a single interval.
"""

import logging
import threading
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from jasil._core.threads import STOP_JOIN_TIMEOUT_SECONDS, signal_and_join
from jasil.jobs._worker_metadata import normalize_worker_metadata
from jasil.jobs.heartbeat import LeaseKeeper
from jasil.jobs.registry import normalize_queue_selector
from jasil.jobs.runner import JobRunner
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)


class WorkerTelemetry:
    """Best-effort durable lifecycle telemetry for one worker instance.

    Writes the worker's registry row at start, on every heartbeat and at stop.
    It owns no thread: :class:`WorkerHeartbeat` calls it, so telemetry can never
    pace or delay lease renewal, and a failed write is logged and dropped.
    """

    def __init__(
        self,
        *,
        instance_id: str,
        clock: ClockProvider,
        session_factory: Callable,
        queues: Iterable[str] | None,
        role: str | None = None,
        label: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        selected_queues = normalize_queue_selector(queues)
        normalized_metadata = normalize_worker_metadata(role=role, label=label, metadata=metadata)
        self._instance_id = instance_id
        self._clock = clock
        self._session_factory = session_factory
        self._queues = selected_queues
        self._role = role
        self._label = label
        self._metadata = normalized_metadata
        self._started_at: datetime | None = None

    def record_start(self) -> None:
        """Record that this worker instance started."""
        import jasil.jobs._worker_registry as worker_registry

        self._started_at = self._clock.now()
        try:
            with self._session_factory() as db:
                worker_registry.record_worker_start(
                    self._instance_id,
                    started_at=self._started_at,
                    queues=self._queues,
                    role=self._role,
                    label=self._label,
                    metadata=self._metadata,
                    db=db,
                )
        except Exception as error:
            logger.warning("Durable worker start telemetry failed", exc_info=error)

    def record_heartbeat(self) -> None:
        """Refresh this worker's last-heartbeat time."""
        import jasil.jobs._worker_registry as worker_registry

        started_at = self._started_at
        if started_at is None:
            return
        try:
            with self._session_factory() as db:
                worker_registry.record_worker_heartbeat(
                    self._instance_id,
                    started_at=started_at,
                    now=self._clock.now(),
                    queues=self._queues,
                    role=self._role,
                    label=self._label,
                    metadata=self._metadata,
                    db=db,
                )
        except Exception as error:
            logger.warning("Durable worker heartbeat failed", exc_info=error)

    def record_stop(self) -> None:
        """Record a graceful shutdown."""
        import jasil.jobs._worker_registry as worker_registry

        try:
            with self._session_factory() as db:
                worker_registry.record_worker_stop(self._instance_id, now=self._clock.now(), db=db)
        except Exception as error:
            logger.warning("Durable worker stop telemetry failed", exc_info=error)


class WorkerHeartbeat:
    """The one periodic beat of a worker: renew held leases, then record liveness.

    Each beat runs two independent steps. Lease renewal is correctness: a missed
    renewal can have a healthy handler requeued and run twice. The registry write
    is telemetry. Neither is allowed to skip or delay the other, so each has its
    own session and failure handling.
    """

    def __init__(
        self,
        *,
        leases: LeaseKeeper,
        interval_seconds: float,
        telemetry: WorkerTelemetry | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be greater than zero")
        self._leases = leases
        self._interval_seconds = interval_seconds
        self._telemetry = telemetry
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def beat(self) -> None:
        """Run one tick. Never raises."""
        try:
            self._leases.renew()
        except Exception as error:
            logger.warning("Durable job lease renewal failed", exc_info=error)
        if self._telemetry is not None:
            self._telemetry.record_heartbeat()

    def start(self) -> None:
        """Record startup and begin the periodic beat (idempotent)."""
        if self._thread is not None:
            if self._thread.is_alive():
                return
            self._thread = None
        if self._telemetry is not None:
            self._telemetry.record_start()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="durable-job-worker-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> bool:
        """Stop beating and, if the thread really stopped, record a graceful shutdown.

        A thread that outlived the join is still beating, so no stop is recorded:
        the registry keeps reporting what is true of it.

        Returns:
            True when the thread stopped; False when it had to be abandoned.
        """
        thread = self._thread
        stopped = signal_and_join(thread, self._stop)
        if stopped and self._thread is thread:
            self._thread = None
        if stopped and self._telemetry is not None:
            self._telemetry.record_stop()
        return stopped

    def _loop(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self.beat()


def run_worker(
    runner: JobRunner,
    *,
    poll_interval_seconds: float,
    stop: threading.Event,
    heartbeat: WorkerHeartbeat | None = None,
) -> None:
    """
    Claim and process jobs until ``stop`` is set.

    Loops immediately while a batch found work (to drain a backlog) and waits
    ``poll_interval_seconds`` when the queue was empty. A failed iteration is
    logged and the loop continues.

    Args:
        runner: The job runner to drive.
        poll_interval_seconds: Idle wait between empty polls.
        stop: Event that ends the loop when set.
        heartbeat: The worker's heartbeat, started before the loop and
            stopped after it. Omit it only for a runner that holds no leases.

    Returns:
        None.
    """
    logger.info("Durable job worker started")
    if heartbeat is not None:
        heartbeat.start()
    try:
        while not stop.is_set():
            try:
                processed = runner.run_once()
            except Exception as error:
                logger.error("Durable job worker iteration failed", exc_info=error)
                processed = 0
            if processed == 0:
                stop.wait(poll_interval_seconds)
    finally:
        if heartbeat is not None:
            heartbeat.stop()
        logger.info("Durable job worker stopped")


class BackgroundWorker:
    """Runs :func:`run_worker` on a daemon thread for in-process deployments."""

    def __init__(
        self,
        runner: JobRunner,
        *,
        poll_interval_seconds: float,
        heartbeat: WorkerHeartbeat | None = None,
    ) -> None:
        self._runner = runner
        self._poll_interval_seconds = poll_interval_seconds
        self._heartbeat = heartbeat
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_alive(self) -> bool:
        """Whether this worker still owns a running thread."""
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """
        Start the worker thread (idempotent).

        Args:
            None.

        Returns:
            None.
        """
        if self._thread is not None:
            if self._thread.is_alive():
                return
            self._thread = None
        self._stop.clear()
        self._thread = threading.Thread(
            target=run_worker,
            args=(self._runner,),
            kwargs={
                "poll_interval_seconds": self._poll_interval_seconds,
                "stop": self._stop,
                "heartbeat": self._heartbeat,
            },
            name="durable-job-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self, *, timeout: float = STOP_JOIN_TIMEOUT_SECONDS) -> bool:
        """
        Signal the worker thread and wait briefly for it to finish.

        Args:
            None.

        Returns:
            True when the worker stopped, otherwise False. A timed-out live
            thread remains attached so another worker cannot overlap it.
        """
        thread = self._thread
        stopped = signal_and_join(thread, self._stop, timeout=timeout)
        if stopped and self._thread is thread:
            self._thread = None
        return stopped
