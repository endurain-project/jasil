"""Async job runner — claims durable jobs and executes their coroutine subscribers.

The async twin of :mod:`jasil.jobs.runner`. The structural layout is identical:
a frozen ``ClaimedJob`` snapshot detaches the job data from the session so the
claim session can close before the handler runs; ``AsyncJobRunner.run_once``
claims a batch, snapshots every row, and then processes each one independently
so a single failure cannot abort its siblings; ``reap_once`` delegates expired-
lease reclaim to the CRUD layer.

**Why coroutine handlers need a separate registry:**

``jasil.jobs.registry.JobHandlerRegistry`` types its stored handlers as
``Callable[[Event], None]`` — a synchronous callable. An ``async def`` handler
returns ``Coroutine[Any, Any, None]``, not ``None``, so registering an async
handler against the sync type would require a silent cast to satisfy mypy, which
would push the type error from startup (``register``) to deep inside a worker
loop (dispatch). Instead this module defines ``AsyncJobHandler`` and
``AsyncJobHandlerRegistry``: a structurally identical pair whose handler type
is ``Callable[[Event], Awaitable[None]]``. The two registries are independent
singletons (``registry`` in :mod:`jasil.jobs.registry` for sync,
``async_registry`` for async), and each runner receives its own at
construction. Both live in :mod:`jasil.jobs.registry`, which imports no model and
no session: the publisher has to ask "is anything durably subscribed to this
event type?" on the async path too, and it must be able to do that without
dragging the ORM into ``import jasil.publisher``. They are re-exported here so a
host wiring the async worker imports one module.

**Session management:**

Each claim/reap call opens its own ``async with session_factory() as db:`` block,
which is *short*: claim the rows, snapshot them, close, then run the handlers
outside any session. Opening a long-lived session that spans handler execution
would hold a database connection for the full duration of every handler, which
is expensive and makes a slow handler a connection-pool pressure point. The
handler-per-session pattern — one brief session for the claim, one brief session
per outcome — mirrors the sync runner exactly.

**CancelledError:**

No ``except`` clause here catches :class:`asyncio.CancelledError`. In Python 3.8+
``CancelledError`` is a subclass of ``BaseException``, not ``Exception``, so
``except Exception`` never reaches it. This is intentional: a cancelled task must
propagate cancellation so the event loop can tear it down cleanly.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

import jasil.jobs.crud_async as jobs_crud
from jasil._core.timestamps import as_utc
from jasil.events import Event
from jasil.jobs.models import ProcessingJob
from jasil.jobs.registry import AsyncJobHandler, AsyncJobHandlerRegistry, async_registry

__all__ = ["AsyncJobHandler", "AsyncJobHandlerRegistry", "AsyncJobRunner", "ClaimedJob", "async_registry"]
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)

# The async handler type: a coroutine function that accepts one Event.


@dataclass(frozen=True)
class ClaimedJob:
    """A detached snapshot of a claimed job, safe to use after its session closes.

    Snapshotting before the claim session closes is necessary under asyncio:
    SQLAlchemy expires ORM instances on commit, and a later attribute read on
    an expired instance triggers a lazy refresh, which raises
    ``MissingGreenlet`` rather than quietly issuing a query.  The snapshot is a
    plain dataclass, independent of the session, so the session can close before
    the handler runs.

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


class AsyncJobRunner:
    """Claims a batch of due jobs and runs each one's registered async subscriber."""

    def __init__(
        self,
        *,
        registry: AsyncJobHandlerRegistry,
        clock: ClockProvider,
        session_factory: Callable[[], AsyncSession],
        worker_id: str,
        lease_seconds: int,
        batch_size: int,
        backoff_base_seconds: float,
        backoff_max_seconds: float,
    ) -> None:
        self._registry = registry
        self._clock = clock
        self._session_factory = session_factory
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._batch_size = batch_size
        self._backoff_base_seconds = backoff_base_seconds
        self._backoff_max_seconds = backoff_max_seconds

    async def run_once(self) -> int:
        """
        Claim and process one batch of due jobs.

        Returns:
            The number of jobs processed in this batch (0 when none were due).
        """
        now = self._clock.now()
        async with self._session_factory() as db:
            claimed = await jobs_crud.claim_jobs(
                worker_id=self._worker_id,
                limit=self._batch_size,
                lease_seconds=self._lease_seconds,
                now=now,
                db=db,
            )
            snapshots = [self._snapshot(job) for job in claimed]
        if snapshots:
            logger.debug("Claimed %d durable job(s) as %s", len(snapshots), self._worker_id)
        for snapshot in snapshots:
            # A DB error finishing one job must not abort the rest of the batch;
            # its lease simply expires and the reaper requeues it.
            try:
                await self._run_job(snapshot)
            except Exception as error:
                logger.error(
                    "Durable job could not be finalized",
                    exc_info=error,
                    extra={"job_id": snapshot.id, "subscriber": snapshot.subscriber_id},
                )
        return len(snapshots)

    async def reap_once(self) -> int:
        """
        Requeue (or dead-letter) jobs whose lease expired.

        Returns:
            The number of jobs reclaimed.
        """
        now = self._clock.now()
        async with self._session_factory() as db:
            return await jobs_crud.reclaim_expired_leases(now=now, db=db)

    async def _run_job(self, job: ClaimedJob) -> None:
        handler = self._registry.get(job.subscriber_id)
        event = self._event_from(job)
        try:
            if handler is None:
                raise LookupError(f"no durable handler registered for subscriber_id {job.subscriber_id!r}")
            await handler(event)
        except Exception as error:
            await self._fail(job, error)
            return
        now = self._clock.now()
        async with self._session_factory() as db:
            await jobs_crud.mark_job_completed(job.id, now=now, db=db)

    async def _fail(self, job: ClaimedJob, error: Exception) -> None:
        now = self._clock.now()
        async with self._session_factory() as db:
            status = await jobs_crud.mark_job_failed(
                job.id,
                str(error),
                base_seconds=self._backoff_base_seconds,
                max_seconds=self._backoff_max_seconds,
                now=now,
                db=db,
            )
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
        )
