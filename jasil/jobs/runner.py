"""The job runner — claims durable jobs and executes their subscribers.

Records outcomes to ``processing_jobs`` only (the per-subscriber execution
state), never to ``event_log`` (which stays a one-row-per-event publication
log). A successful run marks the job ``completed``; a failure reschedules it
with backoff, or dead-letters it once the attempt ceiling is reached. A handler
that raises :class:`~jasil.jobs.DeferJob` is rescheduled for the time it names
without failing or using up an attempt.
"""

import logging
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

import jasil.jobs.backoff as jobs_backoff
import jasil.jobs.crud as jobs_crud
from jasil._core.job_context import job_lease
from jasil._core.timestamps import as_utc
from jasil.events import Event
from jasil.jobs.defer import DeferJob
from jasil.jobs.heartbeat import LeaseKeeper
from jasil.jobs.models import ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry, normalize_queue_selector, validate_queue_name
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
        attempts: The claim number of this run; it fences the claim, and counts
            deferred claims too.
        deferrals: Earlier claims of this job the handler deferred. ``attempts -
            deferrals`` is the attempt this run is against ``max_attempts``.
        timestamp: The envelope's ISO-8601 publish timestamp (the enqueue time for a job from before ``rev0006``).
        schema_version: The payload-shape version carried from the envelope, so
            the handler can upgrade or refuse a payload written by another build.
        queue: Named queue from which the job was claimed.
        max_attempts: The attempt ceiling stamped on the job row.
    """

    id: str
    event_id: str
    event_type: str
    subscriber_id: str
    source: str
    payload: dict
    metadata: dict | None
    attempts: int
    deferrals: int
    timestamp: str
    schema_version: int
    queue: str
    max_attempts: int


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
        defer_max_seconds: float = 86400,
        max_deferrals: int = 100,
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
        self._defer_max_seconds = defer_max_seconds
        self._max_deferrals = max_deferrals
        self._queues = normalize_queue_selector(queues)
        self._legacy_queues_backfilled = False
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
        self._backfill_legacy_queues()
        default_subscribers, excluded_subscribers = self._legacy_routing()
        with self._session_factory() as db:
            claimed = jobs_crud.claim_jobs(
                worker_id=self._worker_id,
                limit=self._batch_size,
                lease_seconds=self._lease_seconds,
                now=now,
                db=db,
                queues=self._queues,
                queue_cursor=self._queue_cursor,
                default_queue_subscribers=default_subscribers,
                excluded_default_queue_subscribers=excluded_subscribers,
                record_attempts=self._record_attempts,
            )
            snapshots = [self._snapshot(job) for job in claimed]
        if not snapshots:
            return 0
        self._queue_cursor = self._cursor_for(snapshots[-1].queue)
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
        except BaseException:
            # The worker is stopping: the jobs still waiting go back to the queue for another worker.
            self._leases.release_waiting()
            raise
        finally:
            self._leases.clear()
        return len(snapshots)

    @staticmethod
    def _cursor_for(queue: str) -> str | None:
        """The rotation cursor for a claimed row's queue, or None when unusable.

        The name comes from the database, which may hold a row written by a host
        or an older tool. A name that is not a valid queue must restart the
        rotation rather than fail every later claim.
        """
        try:
            return validate_queue_name(queue)
        except ValueError:
            logger.warning("Durable job has a non-conforming queue name; restarting queue rotation")
            return None

    def _legacy_routing(self) -> tuple[tuple[str, ...] | None, tuple[str, ...] | None]:
        """Subscribers whose ``default``-queue rows this worker adopts or leaves.

        Read from the registry on every claim, so a subscriber registered after
        the runner was built is routed like the rest.
        """
        if self._queues is None:
            return None, None
        selected = self._registry.subscriber_ids_for_queues(self._queues)
        return selected, tuple(sorted(self._registry.subscriber_ids().difference(selected)))

    def _backfill_legacy_queues(self) -> None:
        """Retag pending ``default`` rows to their subscribers' queues, once.

        Best-effort: a failure is retried on the next batch and never stops a claim.
        """
        if self._legacy_queues_backfilled:
            return
        try:
            with self._session_factory() as db:
                moved = jobs_crud.backfill_legacy_queues(self._registry.queue_assignments(), db=db)
        except Exception as error:
            logger.warning("Durable job queue backfill failed; will retry", exc_info=error)
            return
        self._legacy_queues_backfilled = True
        if moved:
            logger.info("Moved %d pending durable job(s) to their subscribers' queues", moved)

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
        # A job this dead-lettered owes its ``on_dead_letter`` callback to the scheduled sweep, never to this
        # worker: run here, a slow callback would hold the worker and the leases of the jobs behind it.
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
        except DeferJob as deferral:
            self._leases.finalize(job.id)
            return self._defer(job, deferral, duration_ms=self._elapsed_ms(began))
        except Exception as error:
            self._leases.finalize(job.id)
            return self._fail(job, error, duration_ms=self._elapsed_ms(began))
        except BaseException as error:
            # SystemExit, KeyboardInterrupt, a cancellation: the worker stops, but the attempt is recorded first,
            # rather than left claimed and unexplained until its lease expires.
            self._leases.finalize(job.id)
            try:
                self._fail(job, error, duration_ms=self._elapsed_ms(began))
            except Exception as record_error:
                logger.error(
                    "Durable job failure could not be recorded", exc_info=record_error, extra=self._context(job)
                )
            raise
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

    def _defer(self, job: ClaimedJob, deferral: DeferJob, *, duration_ms: int | None = None) -> bool:
        """Reschedule a job its handler deferred; True when the claim was no longer owned.

        Past ``max_deferrals`` the request becomes an ordinary failure.
        """
        now = self._clock.now()
        requested = (as_utc(deferral.retry_at) - now).total_seconds()
        delay = jobs_backoff.defer_seconds(requested, max_seconds=self._defer_max_seconds)
        retry_at = now + timedelta(seconds=delay)
        reason = deferral.reason or str(deferral)
        with self._session_factory() as db:
            status = jobs_crud.mark_job_deferred(
                job.id,
                reason,
                worker_id=self._worker_id,
                attempt=job.attempts,
                retry_at=retry_at,
                max_deferrals=self._max_deferrals,
                now=now,
                db=db,
                duration_ms=duration_ms,
                record_attempt=self._record_attempts,
            )
        if status == jobs_crud.STATUS_DEFERRALS_EXHAUSTED:
            return self._fail(
                job,
                RuntimeError(f"deferred more than {self._max_deferrals} times: {reason}"),
                duration_ms=duration_ms,
            )
        if not status:
            logger.warning(
                "Durable job deferral skipped because claim ownership was lost",
                extra={**self._context(job), "duration_ms": duration_ms, "retry_at": retry_at.isoformat()},
            )
            return True
        logger.info(
            "Durable job deferred",
            extra={
                **self._context(job),
                "event_type": job.event_type,
                "job_status": jobs_crud.STATUS_PENDING,
                "retry_at": retry_at.isoformat(),
                "requested_retry_at": as_utc(deferral.retry_at).isoformat(),
                "reason": reason,
                "deferrals": job.deferrals + 1,
                "duration_ms": duration_ms,
            },
        )
        return False

    def _fail(self, job: ClaimedJob, error: BaseException, *, duration_ms: int | None = None) -> bool:
        """Record a failure; True when the claim was no longer owned."""
        now = self._clock.now()
        with self._session_factory() as db:
            status = jobs_crud.mark_job_failed(
                job.id,
                f"{type(error).__name__}: {error}",
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
            retry_count=job.attempts - job.deferrals,
            schema_version=job.schema_version,
            max_attempts=job.max_attempts,
            deferral_count=job.deferrals,
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
            deferrals=job.deferrals,
            timestamp=jobs_crud.envelope_timestamp(job),
            schema_version=job.schema_version,
            queue=job.queue,
            max_attempts=job.max_attempts,
        )
