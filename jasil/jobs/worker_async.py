"""Async worker loop that drains the durable job queue.

The async twin of :mod:`jasil.jobs.worker`. ``run_worker`` (the sync version)
blocks a daemon thread on ``threading.Event.wait``; ``run_worker_async`` (this
module) drives the same claim/run cycle as an ``asyncio`` coroutine that parks
on ``asyncio.Event.wait``.

**Stop event vs. task cancellation:**

The loop watches a dedicated :class:`asyncio.Event` (``stop``) that is set by
:meth:`AsyncBackgroundWorker.stop` before it cancels the task.  This two-phase
shutdown — signal the event, give the loop its budget to finish an in-flight
iteration, cancel only if it overruns — mirrors the sync twin's
:func:`~jasil._core.threads.signal_and_join` policy and is the reason the loop
is never ``task.cancel()``-only: an in-flight handler that is cancelled halfway
through has side effects (the DB claim is taken but the completion mark never
lands), so we allow the current iteration to complete before tearing the task
down.  :func:`~jasil._core.tasks.signal_and_cancel` enforces both phases.

**Backoff while idle:**

When the queue is empty the loop must not spin-poll the database.  The sync
worker calls ``stop.wait(poll_interval_seconds)``, which either wakes when the
event is set (fast shutdown) or after the timeout (next poll).  The async
version does the same with::

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=poll_interval_seconds)

``asyncio.wait_for`` raises ``TimeoutError`` on expiry, so the ``suppress``
turns that into a normal continuation.  Crucially, :class:`asyncio.CancelledError`
is *not* suppressed: a cancelled task must propagate cancellation or it becomes
unstoppable and hangs shutdown.  This is the established pattern in
:mod:`jasil.backends.events_redis_async`.

**CancelledError propagation:**

No ``except`` clause in this module catches :class:`asyncio.CancelledError`.  In
Python 3.8+, ``CancelledError`` is a subclass of ``BaseException``, not
``Exception``, so ``except Exception`` never intercepts it.  Any future changes
to the error-handling here must preserve that invariant.
"""

import asyncio
import contextlib
import logging

from jasil._core.tasks import signal_and_cancel
from jasil.jobs.runner_async import AsyncJobRunner

logger = logging.getLogger(__name__)


async def run_worker_async(
    runner: AsyncJobRunner,
    *,
    poll_interval_seconds: float,
    stop: asyncio.Event,
) -> None:
    """
    Claim and process jobs until ``stop`` is set.

    Loops immediately while a batch found work (to drain a backlog) and parks
    on ``stop`` for ``poll_interval_seconds`` when the queue was empty.  A
    failed iteration is logged and the loop continues.

    The idle wait is implemented as::

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=poll_interval_seconds)

    so that a ``stop`` signal received mid-wait wakes the loop immediately, and
    a ``CancelledError`` from task teardown is never swallowed (only
    ``TimeoutError`` is suppressed).

    Args:
        runner: The async job runner to drive.
        poll_interval_seconds: Idle wait between empty polls.
        stop: Event that ends the loop when set.

    Returns:
        None.
    """
    logger.info("Async durable job worker started")
    while not stop.is_set():
        try:
            processed = await runner.run_once()
        except Exception as error:
            logger.error("Async durable job worker iteration failed", exc_info=error)
            processed = 0
        if processed == 0:
            # The queue is empty; back off before the next poll.  Using
            # wait_for on the stop event means a shutdown request wakes us
            # immediately rather than waiting out the full interval.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval_seconds)
    logger.info("Async durable job worker stopped")


class AsyncBackgroundWorker:
    """Runs :func:`run_worker_async` as an :class:`asyncio.Task` for in-process deployments.

    The async twin of :class:`jasil.jobs.worker.BackgroundWorker`.  Lifecycle is
    the same — ``start`` / ``stop`` — but both are coroutines because
    ``asyncio.create_task`` requires a running event loop (a check that start
    naturally satisfies) and ``stop`` must ``await`` the task wind-down.
    """

    def __init__(self, runner: AsyncJobRunner, *, poll_interval_seconds: float) -> None:
        self._runner = runner
        self._poll_interval_seconds = poll_interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """
        Start the worker task (idempotent).

        Args:
            None.

        Returns:
            None.
        """
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(
            run_worker_async(self._runner, poll_interval_seconds=self._poll_interval_seconds, stop=self._stop),
            name="async-durable-job-worker",
        )

    async def stop(self) -> None:
        """
        Signal the worker task and wait for it to finish, cancelling if it takes too long.

        Delegates to :func:`jasil._core.tasks.signal_and_cancel`, which sets
        ``stop``, gives the task its cooperative-stop budget, and only cancels
        if the budget runs out — preserving any in-flight handler iteration.

        Args:
            None.

        Returns:
            None.
        """
        task, self._task = self._task, None
        await signal_and_cancel(task, self._stop)
