"""Delivery of the ``on_dead_letter`` callbacks of durable subscribers.

A callback is owed for every job that is ``dead_letter`` while
``processing_jobs.dead_letter_notified_at`` is NULL. It is delivered twice over: at
once by the process that dead-lettered the job (the worker after a failed last
attempt, the reaper after a lease expiry), and by a sweep that picks up whatever a
crash left behind. The marker is written only after the callback returns, so
delivery is at least once and a callback has to be idempotent.
"""

import logging
from collections.abc import Callable, Iterable
from datetime import timedelta

from sqlalchemy.orm import Session

import jasil.jobs.crud as jobs_crud
from jasil.jobs.registry import JobHandlerRegistry
from jasil.providers import ClockProvider

logger = logging.getLogger(__name__)

#: A sweep leaves a dead letter alone this long after it last changed, so it does
#: not race the process that is delivering it, and a failing callback is retried
#: at this pace rather than on every pass.
SWEEP_GRACE_SECONDS = 60


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
            owed callback that has been idle for :data:`SWEEP_GRACE_SECONDS`.
        limit: Maximum number of jobs handled in one call.

    Returns:
        The number of callbacks that returned normally. A callback that raises is
        logged and swallowed; its job stays ``dead_letter`` and is retried by a
        later sweep.
    """
    subscribers = registry.dead_letter_subscriber_ids()
    ids = None if job_ids is None else list(job_ids)
    if not subscribers or ids == []:
        return 0
    now = clock.now()
    with session_factory() as db:
        jobs = jobs_crud.dead_letters_awaiting_callback(
            subscribers,
            db=db,
            job_ids=ids,
            idle_before=None if ids is not None else now - timedelta(seconds=SWEEP_GRACE_SECONDS),
            limit=limit,
        )
    delivered = 0
    for job in jobs:
        callback = registry.dead_letter_callback(job.subscriber_id)
        if callback is None:
            continue
        context = {"job_id": job.id, "subscriber": job.subscriber_id, "event_type": job.event_type}
        try:
            callback(job.to_event(), job.last_error)
        except Exception as error:
            logger.error("Dead-letter callback failed; will retry", exc_info=error, extra=context)
            _record(jobs_crud.defer_dead_letter_callback, job.id, session_factory, clock, context)
            continue
        delivered += 1
        _record(jobs_crud.mark_dead_letter_notified, job.id, session_factory, clock, context)
    return delivered


def _record(
    write: Callable[..., bool],
    job_id: str,
    session_factory: Callable[[], Session],
    clock: ClockProvider,
    context: dict[str, str],
) -> None:
    """Write the delivery outcome; a failure is logged, and the callback then repeats."""
    try:
        with session_factory() as db:
            write(job_id, now=clock.now(), db=db)
    except Exception as error:
        logger.error("Dead-letter delivery could not be recorded; it will repeat", exc_info=error, extra=context)
