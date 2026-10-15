"""What a worker holds, and keeping it alive while it runs.

A batch is claimed under one lease but run one job after another. Without
renewal a handler longer than ``lease_seconds`` looks like a crashed worker: the
reaper requeues the job, another worker runs it, and a side effect (an email)
happens twice. :class:`LeaseKeeper` is the runner's record of the jobs this
worker holds, and :meth:`LeaseKeeper.renew` is the single routine that extends
them. It is called on the worker heartbeat (see :class:`jasil.jobs.worker.
WorkerHeartbeat`) and, synchronously, before each job's turn, so a runner driven
without a heartbeat thread still renews at the same points it always did.

``lease_seconds`` therefore means *how long a worker may go silent* before its
jobs are reclaimed, and ``max_runtime_seconds`` is the cap on any one handler.

Everything here is measured with the injected clock: ``now()`` for the
timestamps written to the database, ``monotonic()`` for elapsed time.
"""

import logging
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from sqlalchemy.orm import Session

import jasil.jobs.crud as jobs_crud
from jasil.jobs._attempts import OUTCOME_ABANDONED
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)


@dataclass
class _Running:
    """The job whose handler is executing right now."""

    job_id: str
    started: float
    lost: threading.Event
    abandoned: bool = False


class LeaseKeeper:
    """Thread-safe record of the leases one worker holds, and their renewal.

    Held jobs are the running job plus the jobs of the batch still waiting for
    their turn. All state changes and every renewal are serialised by one lock,
    held across the database round-trip, so the heartbeat thread and the runner's
    own before-turn renewal can never interleave; the runner's calls wait at most
    one round-trip.
    """

    def __init__(
        self,
        *,
        session_factory: Callable[[], Session],
        clock: ClockProvider,
        worker_id: str,
        lease_seconds: int,
        max_runtime_seconds: float,
        interval_seconds: float,
        record_attempts: bool = True,
    ) -> None:
        self._record_attempts = record_attempts
        self._session_factory = session_factory
        self._clock = clock
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._max_runtime_seconds = max_runtime_seconds
        self._interval_seconds = interval_seconds
        self._lock = threading.Lock()
        self._held: dict[str, int] = {}
        self._running: _Running | None = None
        self._last_renewed = clock.monotonic()

    def hold(self, job_attempts: Mapping[str, int]) -> None:
        """Start tracking a freshly claimed batch, as ``{job_id: attempt}``."""
        with self._lock:
            self._held = dict(job_attempts)
            self._running = None
            # The claim just took these leases, so this is the last proof of life.
            self._last_renewed = self._clock.monotonic()

    def begin(self, job_id: str) -> threading.Event:
        """Mark ``job_id`` as running; returns the event set if its lease is lost."""
        lost = threading.Event()
        with self._lock:
            self._running = _Running(job_id, self._clock.monotonic(), lost)
        return lost

    def finish(self, job_id: str) -> None:
        """Stop tracking ``job_id``: it completed, failed, or was abandoned."""
        with self._lock:
            if self._running is not None and self._running.job_id == job_id:
                self._running = None
            self._held.pop(job_id, None)

    def drop(self, job_id: str) -> None:
        """Stop renewing a job this worker will not run."""
        with self._lock:
            self._held.pop(job_id, None)

    def clear(self) -> None:
        """Forget everything; the batch is over."""
        with self._lock:
            self._held.clear()
            self._running = None

    def renew(self) -> set[str]:
        """Extend the lease of every held job this worker still owns.

        Also applies the two protections a lease needs now that handlers may run
        longer than one: the running job is *abandoned* once it exceeds
        ``max_runtime_seconds`` (no longer renewed, signalled, and the waiting
        jobs released so they run elsewhere), and it is *self-fenced* when
        renewals have failed for so long that another worker may already have
        reclaimed it.

        Returns:
            The ids whose lease was renewed. A held job missing from the result
            is no longer owned by this worker and must not be run.

        Raises:
            Exception: Whatever the database raised. The caller decides what an
                unknown lease means; the failure has already been counted toward
                self-fencing.
        """
        with self._lock:
            running = self._running
            if (
                running is not None
                and not running.abandoned
                and self._clock.monotonic() - running.started >= self._max_runtime_seconds
            ):
                self._abandon(running)
            if not self._held:
                return set()
            attempts = dict(self._held)
            try:
                with self._session_factory() as db:
                    renewed = jobs_crud.renew_leases(
                        attempts,
                        worker_id=self._worker_id,
                        lease_seconds=self._lease_seconds,
                        now=self._clock.now(),
                        db=db,
                    )
            except Exception:
                self._fence_if_silent(running)
                raise
            self._last_renewed = self._clock.monotonic()
            for job_id in attempts.keys() - renewed:
                self._held.pop(job_id, None)
                if running is not None and job_id == running.job_id:
                    running.lost.set()
                    logger.warning(
                        "Durable job lease was lost while it ran; its result will be discarded",
                        extra={"job_id": job_id, "worker_id": self._worker_id},
                    )
            return renewed

    def release_waiting(self) -> None:
        """Return the jobs still waiting for their turn to ``pending``."""
        with self._lock:
            self._release_waiting()

    def _abandon(self, running: _Running) -> None:
        running.abandoned = True
        running.lost.set()
        attempt = self._held.pop(running.job_id, None)
        logger.error(
            "Durable job exceeded its maximum runtime; abandoned to the reaper",
            extra={
                "job_id": running.job_id,
                "worker_id": self._worker_id,
                "max_runtime_seconds": self._max_runtime_seconds,
            },
        )
        if self._record_attempts and attempt is not None:
            self._close_attempt(running.job_id, attempt)
        # Python cannot kill the handler's thread, and a worker stuck in one job
        # must not hold the rest of its batch hostage behind leases it keeps renewing.
        self._release_waiting()

    def _close_attempt(self, job_id: str, attempt: int) -> None:
        """Record the abandoned attempt; best-effort, the log line already says it."""
        try:
            with self._session_factory() as db:
                jobs_crud.close_attempts(
                    {job_id: attempt},
                    worker_id=self._worker_id,
                    outcome=OUTCOME_ABANDONED,
                    now=self._clock.now(),
                    db=db,
                )
        except Exception as error:
            logger.warning("Abandoned durable job attempt could not be recorded", exc_info=error)

    def _fence_if_silent(self, running: _Running | None) -> None:
        silent = self._clock.monotonic() - self._last_renewed
        if running is None or running.lost.is_set() or silent < self._lease_seconds - self._interval_seconds:
            return
        # Another worker may have reclaimed the job already, or will before the
        # next beat: stop the handler's side effects now rather than after.
        running.lost.set()
        logger.error(
            "Durable job lease renewal has failed for %.0f s; assuming the lease is lost",
            silent,
            extra={"job_id": running.job_id, "worker_id": self._worker_id},
        )

    def _release_waiting(self) -> None:
        running_id = self._running.job_id if self._running is not None else None
        waiting = {job_id: attempt for job_id, attempt in self._held.items() if job_id != running_id}
        if not waiting:
            return
        try:
            with self._session_factory() as db:
                released = jobs_crud.release_leases(waiting, worker_id=self._worker_id, now=self._clock.now(), db=db)
            logger.info(
                "Released %d unstarted durable job(s) back to the queue",
                released,
                extra={"worker_id": self._worker_id},
            )
        except Exception as error:
            # They stop being renewed either way; the reaper requeues them once
            # their leases expire, which costs an attempt but loses no work.
            logger.warning("Durable jobs could not be released; leaving them to the reaper", exc_info=error)
        for job_id in waiting:
            self._held.pop(job_id, None)
