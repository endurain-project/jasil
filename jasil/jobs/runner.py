"""The job runner — claims durable jobs and executes their subscribers.

Records outcomes to ``processing_jobs`` only (the per-subscriber execution
state), never to ``event_log`` (which stays a one-row-per-event publication
log). A successful run marks the job ``completed``; a failure reschedules it
with backoff, or dead-letters it once the attempt ceiling is reached.
"""

import logging
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from sqlalchemy.orm import Session

import jasil.jobs.crud as jobs_crud
from jasil._core.job_context import job_lease
from jasil._core.timestamps import as_utc
from jasil.events import Event
from jasil.jobs.heartbeat import LeaseKeeper
from jasil.jobs.models import ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry, normalize_queue_selector
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimedJob:
    """A detached snapshot of a claimed job, safe to use after its session closes.

    Attributes:
        id: The job id.
        event_id: The originating envelope event_id.
        event_type: The domain-event channel.
        subscriber_id: The durable subscriber to run.
        source: Where the originating event came from.
        payload: The domain payload.
        metadata: Correlation context, if any.
        attempts: The attempt number this run represents.
        timestamp: ISO-8601 enqueue time, used to rebuild the event envelope.
        schema_version: The payload-shape version carried from the envelope, so
            the handler can upgrade or refuse a payload written by another build.
        queue: Named queue from which the job was claimed.
    """

    id: str
    event_id: str
    event_type: str
    subscriber_id: str
    source: str
    payload: dict
    metadata: dict | None
    attempts: int
    timestamp: str
    schema_version: int
    queue: str


class JobRunner:
    """Claims a batch of due jobs and runs each one's registered subscriber."""

    def __init__(
        self,
        *,
        registry: JobHandlerRegistry,
        clock: ClockProvider,
        session_factory: Callable[[], Session],
        worker_id: str,
        lease_seconds: int,
        max_runtime_seconds: float,
        heartbeat_interval_seconds: float,
        batch_size: int,
        backoff_base_seconds: float,
        backoff_max_seconds: float,
        queues: Iterable[str] | None = None,
        record_attempts: bool = True,
    ) -> None:
        self._record_attempts = record_attempts
        self._registry = registry
        self._clock = clock
        self._session_factory = session_factory
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._batch_size = batch_size
        self._backoff_base_seconds = backoff_base_seconds
        self._backoff_max_seconds = backoff_max_seconds
        self._queues = normalize_queue_selector(queues)
        if self._queues is None:
            self._default_queue_subscribers = None
            self._excluded_default_queue_subscribers = None
        else:
            selected_subscribers = registry.subscriber_ids_for_queues(self._queues)
            self._default_queue_subscribers = selected_subscribers
            self._excluded_default_queue_subscribers = tuple(
                sorted(registry.subscriber_ids().difference(selected_subscribers))
            )
        self._queue_cursor: str | None = None
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._leases = LeaseKeeper(
            session_factory=session_factory,
            clock=clock,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            max_runtime_seconds=max_runtime_seconds,
            interval_seconds=heartbeat_interval_seconds,
            record_attempts=record_attempts,
        )

    @property
    def leases(self) -> LeaseKeeper:
        """The record of what this worker holds, renewed by the worker heartbeat."""
        return self._leases

    @property
    def heartbeat_interval_seconds(self) -> float:
        """How often the worker heartbeat renews leases and refreshes telemetry."""
        return self._heartbeat_interval_seconds

    @property
    def worker_id(self) -> str:
        """Restart-unique worker and lease-holder identity."""
        return self._worker_id

    @property
    def clock(self) -> ClockProvider:
        """Clock shared by claims and worker telemetry."""
        return self._clock

    @property
    def queues(self) -> tuple[str, ...] | None:
        """Normalized queue allowlist, or None when all queues are selected."""
        return self._queues

    def run_once(self, stop: threading.Event | None = None) -> int:
        """
        Claim and process one batch of due jobs.

        Args:
            stop: When set between jobs, the jobs not yet started are released
                back to the queue instead of being run, so a graceful shutdown
                does not hold them for the rest of the batch.

        Returns:
            The number of jobs processed in this batch (0 when none were due).
        """
        now = self._clock.now()
        with self._session_factory() as db:
            claimed = jobs_crud.claim_jobs(
                worker_id=self._worker_id,
                limit=self._batch_size,
                lease_seconds=self._lease_seconds,
                now=now,
                db=db,
                queues=self._queues,
                queue_cursor=self._queue_cursor,
                default_queue_subscribers=self._default_queue_subscribers,
                excluded_default_queue_subscribers=self._excluded_default_queue_subscribers,
                record_attempts=self._record_attempts,
            )
            snapshots = [self._snapshot(job) for job in claimed]
        if not snapshots:
            return 0
        self._queue_cursor = snapshots[-1].queue
        logger.debug("Claimed %d durable job(s) as %s", len(snapshots), self._worker_id)
        self._leases.hold({snapshot.id: snapshot.attempts for snapshot in snapshots})
        try:
            for index, snapshot in enumerate(snapshots):
                # A DB error finishing one job must not abort the rest of the batch;
                # its lease simply expires and the reaper requeues it.
                if stop is not None and stop.is_set():
                    self._leases.release_waiting()
                    break
                try:
                    # The first job's lease was taken a moment ago; every later one
                    # has been waiting, so it is renewed (and confirmed held) first.
                    if index and not self._still_held(snapshot):
                        continue
                    if self._run_job(snapshot):
                        # This worker lost its claim or overran its cap: it is in
                        # trouble, so the rest of the batch goes back to the queue
                        # for a worker that is not.
                        self._leases.release_waiting()
                        break
                except Exception as error:
                    logger.error(
                        "Durable job could not be finalized",
                        exc_info=error,
                        extra={"job_id": snapshot.id, "subscriber": snapshot.subscriber_id},
                    )
        finally:
            self._leases.clear()
        return len(snapshots)

    def _still_held(self, job: ClaimedJob) -> bool:
        """Renew the held leases; False when ``job`` is no longer this worker's.

        The batch was claimed under one lease, but jobs run in turn. A job the
        reaper already requeued belongs to another worker now and must not be run
        here as well. The same renewal routine runs on the worker heartbeat, so a
        waiting job normally has a fresh lease already; this is the confirmation
        that it is still held.
        """
        try:
            renewed = self._leases.renew()
        except Exception as error:
            # Not run: the lease is unknown, and the reaper requeues it.
            logger.error(
                "Durable job lease could not be renewed; skipping",
                exc_info=error,
                extra=self._context(job),
            )
            self._leases.drop(job.id)
            return False
        if job.id in renewed:
            return True
        logger.warning(
            "Durable job lease was lost before its turn; skipping",
            extra=self._context(job),
        )
        return False

    def reap_once(self) -> int:
        """
        Requeue (or dead-letter) jobs whose lease expired.

        Returns:
            The number of jobs reclaimed.
        """
        now = self._clock.now()
        with self._session_factory() as db:
            return jobs_crud.reclaim_expired_leases(now=now, db=db)

    def _run_job(self, job: ClaimedJob) -> bool:
        """Run one job and record its outcome.

        Returns:
            True when the job's lease was lost while it ran, abandoned past
            ``max_runtime_seconds`` or fenced after failed renewals.
        """
        lost = self._leases.begin(job.id)
        try:
            self._mark_started(job)
            lost_claim = self._execute(job, lost)
        finally:
            # After the outcome is written, so the heartbeat keeps renewing the
            # lease through the completion write.
            self._leases.finish(job.id)
        return lost_claim or lost.is_set()

    def _mark_started(self, job: ClaimedJob) -> None:
        """Stamp when the handler began. Best-effort: history must not stop the job."""
        if not self._record_attempts:
            return
        try:
            with self._session_factory() as db:
                jobs_crud.mark_attempt_started(
                    job.id, worker_id=self._worker_id, attempt=job.attempts, now=self._clock.now(), db=db
                )
        except Exception as error:
            logger.warning("Durable job start could not be recorded", exc_info=error, extra=self._context(job))

    def _execute(self, job: ClaimedJob, lost: threading.Event) -> bool:
        """Run the handler and write its outcome.

        Returns:
            True when the outcome write found the claim no longer owned. This, not
            a renewal seeing the row finished, is what says the claim was lost once
            the handler has returned.
        """
        handler = self._registry.get(job.subscriber_id)
        event = self._event_from(job)
        began = self._clock.monotonic()
        try:
            if handler is None:
                raise LookupError(f"no durable handler registered for subscriber_id {job.subscriber_id!r}")
            with job_lease(lost):
                handler(event)
        except Exception as error:
            self._leases.finalize(job.id)
            return self._fail(job, error, duration_ms=self._elapsed_ms(began))
        self._leases.finalize(job.id)
        duration_ms = self._elapsed_ms(began)
        now = self._clock.now()
        with self._session_factory() as db:
            completed = jobs_crud.mark_job_completed(
                job.id,
                worker_id=self._worker_id,
                attempt=job.attempts,
                now=now,
                db=db,
                duration_ms=duration_ms,
                record_attempt=self._record_attempts,
            )
        if not completed:
            logger.warning(
                "Durable job completion skipped because claim ownership was lost",
                extra={**self._context(job), "duration_ms": duration_ms},
            )
        return not completed

    def _elapsed_ms(self, began: float) -> int:
        return max(0, round((self._clock.monotonic() - began) * 1000))

    def _context(self, job: ClaimedJob) -> dict[str, object]:
        """The structured fields every job-outcome log record carries."""
        return {
            "job_id": job.id,
            "subscriber": job.subscriber_id,
            "queue": job.queue,
            "attempts": job.attempts,
            "worker_id": self._worker_id,
        }

    def _fail(self, job: ClaimedJob, error: Exception, *, duration_ms: int | None = None) -> bool:
        """Record a failure; True when the claim was no longer owned."""
        now = self._clock.now()
        with self._session_factory() as db:
            status = jobs_crud.mark_job_failed(
                job.id,
                str(error),
                worker_id=self._worker_id,
                attempt=job.attempts,
                base_seconds=self._backoff_base_seconds,
                max_seconds=self._backoff_max_seconds,
                now=now,
                db=db,
                duration_ms=duration_ms,
                record_attempt=self._record_attempts,
            )
        if not status:
            logger.warning(
                "Durable job failure skipped because claim ownership was lost",
                exc_info=error,
                extra={**self._context(job), "duration_ms": duration_ms},
            )
            return True
        level = logging.ERROR if status == jobs_crud.STATUS_DEAD_LETTER else logging.WARNING
        logger.log(
            level,
            "Durable job failed",
            exc_info=error,
            extra={
                **self._context(job),
                "event_type": job.event_type,
                "job_status": status,
                "duration_ms": duration_ms,
            },
        )
        return False

    def _event_from(self, job: ClaimedJob) -> Event:
        return Event(
            event_id=job.event_id,
            event_type=job.event_type,
            source=job.source,
            timestamp=job.timestamp,
            payload=job.payload,
            metadata=job.metadata or {},
            retry_count=job.attempts,
            schema_version=job.schema_version,
        )

    def _snapshot(self, job: ProcessingJob) -> ClaimedJob:
        return ClaimedJob(
            id=job.id,
            event_id=job.event_id,
            event_type=job.event_type,
            subscriber_id=job.subscriber_id,
            source=job.source,
            payload=dict(job.payload),
            metadata=dict(job.job_metadata) if job.job_metadata else None,
            attempts=job.attempts,
            timestamp=as_utc(job.created_at).isoformat(),
            schema_version=job.schema_version,
            queue=job.queue,
        )
