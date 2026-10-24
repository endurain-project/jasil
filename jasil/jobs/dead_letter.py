"""Delivery of the ``on_dead_letter`` callbacks of durable subscribers.

A callback is owed for every job that is ``dead_letter`` while
``processing_jobs.dead_letter_notified_at`` is NULL. It is delivered twice over: at
once by the process that dead-lettered the job (the worker after a failed last
attempt, the reaper after a lease expiry), and by a sweep that picks up whatever a
crash left behind. A delivery first claims the job with a compare-and-set lease, so
concurrent processes never run the same callback together, and the marker is
written only after the callback returns, so delivery is at least once and a
callback has to be idempotent.
"""

import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from functools import partial

from sqlalchemy.orm import Session

import jasil.jobs.crud as jobs_crud
from jasil.jobs.registry import JobHandlerRegistry
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)

#: How long a claimed delivery holds off every other process. The callback runs
#: inline, so this is the longest one is expected to take: past it a second process
#: may start the callback again while the first is still running.
CLAIM_LEASE_SECONDS = 300

#: A failed callback is retried after this long, doubling per failure up to the cap.
RETRY_BASE_SECONDS = 60
RETRY_MAX_SECONDS = 3600


def deliver_dead_letters(
    *,
    registry: JobHandlerRegistry,
    session_factory: Callable[[], Session],
    clock: ClockProvider,
    job_ids: Iterable[str] | None = None,
    limit: int = 100,
) -> int:
    """
    Run the owed ``on_dead_letter`` callbacks of subscribers registered here.

    Args:
        registry: Where the callbacks are registered. Subscribers without one are
            left alone, so a process that did not register them cannot consume
            the notification.
        session_factory: Opens short-lived sessions; none is open while a
            callback runs.
        clock: Source of the current instant.
        job_ids: The jobs this process just dead-lettered. Omit it to sweep every
            owed callback that is due.
        limit: Maximum number of jobs handled in one call.

    Returns:
        The number of callbacks that returned normally. A callback that raises is logged and swallowed; its job stays ``dead_letter``, the
        failure is stored on it, and a later sweep retries it after a backoff.
    """
    subscribers = registry.dead_letter_subscriber_ids()
    ids = None if job_ids is None else list(job_ids)
    if not subscribers or ids == []:
        return 0
    with session_factory() as db:
        jobs = jobs_crud.claim_dead_letter_callbacks(
            subscribers,
            now=clock.now(),
            lease_seconds=CLAIM_LEASE_SECONDS,
            db=db,
            job_ids=ids,
            limit=limit,
        )
    delivered = 0
    for job in jobs:
        callback = registry.dead_letter_callback(job.subscriber_id)
        if callback is None:
            continue
        context = {
            "job_id": job.id,
            "subscriber": job.subscriber_id,
            "event_type": job.event_type,
            "delivery_attempt": job.delivery_attempt,
        }
        try:
            callback(job.to_event(), job.last_error)
        except Exception as error:
            logger.error("Dead-letter callback failed; will retry", exc_info=error, extra=context)
            retry_at = clock.now() + timedelta(seconds=_backoff(job.delivery_attempt))
            _record(
                partial(
                    jobs_crud.defer_dead_letter_callback,
                    job.id,
                    retry_at=retry_at,
                    error=f"{type(error).__name__}: {error}",
                ),
                session_factory,
                context,
            )
            continue
        delivered += 1
        _record(partial(jobs_crud.mark_dead_letter_notified, job.id, now=clock.now()), session_factory, context)
    return delivered


def deliver_after_reap(
    *,
    registry: JobHandlerRegistry,
    session_factory: Callable[[], Session],
    clock: ClockProvider,
    dead_lettered: list[str],
) -> None:
    """
    Deliver the callbacks a lease reap owes: the jobs it just killed, then the sweep.

    Never raises: the reap is already committed, and a failure here is retried by
    the next one.

    Args:
        registry: Where the callbacks are registered.
        session_factory: Opens short-lived sessions.
        clock: Source of the current instant.
        dead_lettered: Ids the reap dead-lettered.
    """
    for job_ids in (dead_lettered, None):
        try:
            deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock, job_ids=job_ids)
        except Exception as error:
            logger.error("Dead-letter callbacks could not be delivered", exc_info=error)


def _backoff(delivery_attempt: int) -> int:
    """Seconds before a delivery that has failed ``delivery_attempt`` times is retried."""
    return min(RETRY_BASE_SECONDS * 2 ** max(0, delivery_attempt - 1), RETRY_MAX_SECONDS)


def _record(
    write: Callable[..., bool],
    session_factory: Callable[[], Session],
    context: Mapping[str, object],
) -> None:
    """Write the delivery outcome; a failure is logged, and the callback then repeats."""
    try:
        with session_factory() as db:
            write(db=db)
    except Exception as error:
        logger.error("Dead-letter delivery could not be recorded; it will repeat", exc_info=error, extra=context)
