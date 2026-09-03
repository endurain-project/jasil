"""The job runner — claims durable jobs and executes their subscribers.

Records outcomes to ``processing_jobs`` only (the per-subscriber execution
state), never to ``event_log`` (which stays a one-row-per-event publication
log). A successful run marks the job ``completed``; a failure reschedules it
with backoff, or dead-letters it once the attempt ceiling is reached.
"""

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from sqlalchemy.orm import Session

import jasil.jobs.crud as jobs_crud
from jasil._core.timestamps import as_utc
from jasil.events import Event
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
        batch_size: int,
        backoff_base_seconds: float,
        backoff_max_seconds: float,
        queues: Iterable[str] | None = None,
    ) -> None:
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
            selected_subscribers = registry._subscriber_ids_for_queues(self._queues)
            self._default_queue_subscribers = selected_subscribers
            self._excluded_default_queue_subscribers = tuple(
                sorted(registry.subscriber_ids().difference(selected_subscribers))
            )
        self._queue_cursor: str | None = None

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

    def run_once(self) -> int:
        """
        Claim and process one batch of due jobs.

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
            )
            snapshots = [self._snapshot(job) for job in claimed]
        if snapshots:
            self._queue_cursor = snapshots[-1].queue
            logger.debug("Claimed %d durable job(s) as %s", len(snapshots), self._worker_id)
        for snapshot in snapshots:
            # A DB error finishing one job must not abort the rest of the batch;
            # its lease simply expires and the reaper requeues it.
            try:
                self._run_job(snapshot)
            except Exception as error:
                logger.error(
                    "Durable job could not be finalized",
                    exc_info=error,
                    extra={"job_id": snapshot.id, "subscriber": snapshot.subscriber_id},
                )
        return len(snapshots)

    def reap_once(self) -> int:
        """
        Requeue (or dead-letter) jobs whose lease expired.

        Returns:
            The number of jobs reclaimed.
        """
        now = self._clock.now()
        with self._session_factory() as db:
            return jobs_crud.reclaim_expired_leases(now=now, db=db)

    def _run_job(self, job: ClaimedJob) -> None:
        handler = self._registry.get(job.subscriber_id)
        event = self._event_from(job)
        try:
            if handler is None:
                raise LookupError(f"no durable handler registered for subscriber_id {job.subscriber_id!r}")
            handler(event)
        except Exception as error:
            self._fail(job, error)
            return
        now = self._clock.now()
        with self._session_factory() as db:
            completed = jobs_crud.mark_job_completed(
                job.id,
                worker_id=self._worker_id,
                attempt=job.attempts,
                now=now,
                db=db,
            )
        if not completed:
            logger.warning(
                "Durable job completion skipped because claim ownership was lost",
                extra={"job_id": job.id, "subscriber": job.subscriber_id, "attempts": job.attempts},
            )

    def _fail(self, job: ClaimedJob, error: Exception) -> None:
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
            )
        if not status:
            logger.warning(
                "Durable job failure skipped because claim ownership was lost",
                exc_info=error,
                extra={"job_id": job.id, "subscriber": job.subscriber_id, "attempts": job.attempts},
            )
            return
        level = logging.ERROR if status == jobs_crud.STATUS_DEAD_LETTER else logging.WARNING
        logger.log(
            level,
            "Durable job failed",
            exc_info=error,
            extra={
                "job_id": job.id,
                "subscriber": job.subscriber_id,
                "event_type": job.event_type,
                "attempts": job.attempts,
                "job_status": status or "unknown",
            },
        )

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
