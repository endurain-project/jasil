"""Shutdown handling for the library's background asyncio tasks.

The async Redis Streams consumer and the async durable-job worker both run a
poll loop as an :class:`asyncio.Task` that watches a stop event, exactly as
their threaded counterparts in :mod:`jasil._core.threads` watch a
:class:`threading.Event`. Winding one down has to follow the same policy for the
same reason: the loops only observe the event *between* iterations, so the wait
has to be bounded or a slow poll would block shutdown indefinitely.

The two modules are deliberately separate rather than one polymorphic helper.
A single function taking "a thread or a task" would have to branch on its
argument's type at every step, and the one thing worth sharing — the timeout
policy — is shared already: this module imports
:data:`~jasil._core.threads.STOP_JOIN_TIMEOUT_SECONDS` rather than restating it,
so the sync and async loops cannot drift onto different shutdown budgets.

Cancellation is the second half of the policy and the part with no threaded
equivalent. ``signal_and_join`` can only ask; a task can be *told*. So the wait
here is two-phase: set the event and give the loop its budget to notice, and
only cancel if that budget runs out. A loop parked in ``await`` on a Redis read
that ignores its stop event still gets torn down, but a loop that is merely mid
iteration is allowed to finish it, which is what keeps an in-flight job from
being cancelled halfway through by a routine shutdown.
"""

import asyncio
import contextlib
import logging

from jasil._core.threads import STOP_JOIN_TIMEOUT_SECONDS

__all__ = ["STOP_JOIN_TIMEOUT_SECONDS", "signal_and_cancel"]

logger = logging.getLogger(__name__)


async def signal_and_cancel(
    task: "asyncio.Task[None] | None",
    stop: asyncio.Event,
    *,
    timeout: float = STOP_JOIN_TIMEOUT_SECONDS,
) -> None:
    """Set ``stop`` and wait up to ``timeout`` for ``task`` to finish, cancelling if it does not.

    Args:
        task: The task to wind down, or ``None`` when none is running.
        stop: The event its loop watches.
        timeout: Seconds to wait for a cooperative stop before cancelling.
    """
    stop.set()
    if task is None:
        return

    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except TimeoutError:
        # The loop did not notice its stop event in time. Unlike a daemon
        # thread, a task left running would keep the event loop alive and keep
        # doing work after shutdown, so cancel it — but say so, because work
        # that appears to stop mid-flight is otherwise impossible to explain.
        logger.warning("Task %r did not stop within %ss; cancelling it", task.get_name(), timeout)
        task.cancel()
        # A cancelled task still has to be awaited for its cancellation to be
        # retrieved; skipping this is what produces "Task exception was never
        # retrieved" noise at interpreter exit.
        with contextlib.suppress(asyncio.CancelledError):
            await task
    except Exception as error:
        # The loop stopped by raising. Shutdown must not raise on its behalf —
        # the caller is tearing the platform down and has nothing to do with
        # this — but a loop that died is worth a line in the log.
        logger.warning("Task %r stopped with an error during shutdown: %r", task.get_name(), error)
