"""The worker loop that drains the durable job queue.

``run_worker`` is the blocking claim/run loop used both by the standalone worker
entrypoint and by :class:`BackgroundWorker`, which runs it on a daemon thread so
a single-process (``local``) deployment can process durable jobs without a
separate container. Reaping expired leases is a scheduled job, not part of this
loop.
"""

import logging
import threading
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from jasil._core.threads import STOP_JOIN_TIMEOUT_SECONDS, signal_and_join
from jasil.jobs._worker_metadata import normalize_worker_metadata
from jasil.jobs.registry import normalize_queue_selector
from jasil.jobs.runner import JobRunner
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)


class WorkerTelemetry:
    """Best-effort durable lifecycle telemetry for one worker instance."""

    def __init__(
        self,
        *,
        instance_id: str,
        clock: ClockProvider,
        session_factory: Callable,
        heartbeat_interval_seconds: float,
        queues: Iterable[str] | None,
        role: str | None = None,
        label: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be greater than zero")
        selected_queues = normalize_queue_selector(queues)
        normalized_metadata = normalize_worker_metadata(role=role, label=label, metadata=metadata)
        self._instance_id = instance_id
        self._clock = clock
        self._session_factory = session_factory
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._queues = selected_queues
        self._role = role
        self._label = label
        self._metadata = normalized_metadata
        self._started_at: datetime | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Record startup and begin bounded periodic heartbeats."""
        if self._thread is not None:
            if self._thread.is_alive():
                return
            self._thread = None
        self._started_at = self._clock.now()
        self._write_start()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name="durable-job-worker-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> bool:
        """Stop periodic heartbeats and record a graceful shutdown."""
        thread = self._thread
        stopped = signal_and_join(thread, self._stop)
        if stopped and self._thread is thread:
            self._thread = None
        self._write_stop()
        return stopped

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self._heartbeat_interval_seconds):
            self._write_heartbeat()

    def _write_start(self) -> None:
        import jasil.jobs._worker_registry as worker_registry

        started_at = self._started_at
        if started_at is None:
            return
        try:
            with self._session_factory() as db:
                worker_registry.record_worker_start(
                    self._instance_id,
                    started_at=started_at,
                    queues=self._queues,
                    role=self._role,
                    label=self._label,
                    metadata=self._metadata,
                    db=db,
                )
        except Exception as error:
            logger.warning("Durable worker start telemetry failed", exc_info=error)

    def _write_heartbeat(self) -> None:
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

    def _write_stop(self) -> None:
        import jasil.jobs._worker_registry as worker_registry

        try:
            with self._session_factory() as db:
                worker_registry.record_worker_stop(self._instance_id, now=self._clock.now(), db=db)
        except Exception as error:
            logger.warning("Durable worker stop telemetry failed", exc_info=error)


def run_worker(
    runner: JobRunner,
    *,
    poll_interval_seconds: float,
    stop: threading.Event,
    telemetry: WorkerTelemetry | None = None,
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
        telemetry: Optional durable worker-lifecycle recorder.

    Returns:
        None.
    """
    logger.info("Durable job worker started")
    if telemetry is not None:
        telemetry.start()
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
        if telemetry is not None:
            telemetry.stop()
        logger.info("Durable job worker stopped")


class BackgroundWorker:
    """Runs :func:`run_worker` on a daemon thread for in-process deployments."""

    def __init__(
        self,
        runner: JobRunner,
        *,
        poll_interval_seconds: float,
        telemetry: WorkerTelemetry | None = None,
    ) -> None:
        self._runner = runner
        self._poll_interval_seconds = poll_interval_seconds
        self._telemetry = telemetry
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
                "telemetry": self._telemetry,
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
