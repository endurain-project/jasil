"""Shutdown handling and retry pacing for the library's daemon threads.

The Redis Streams consumer, the realtime subscriber, the worker heartbeat and the
durable-job worker each run a loop on a daemon thread that watches a stop event.
Winding one down is the same three steps for all of them, with a bounded wait:
the loops only observe the event between iterations, so a join has to be given a
timeout or a slow iteration would block shutdown indefinitely.
"""

import logging
import random
import threading

__all__ = ["STOP_JOIN_TIMEOUT_SECONDS", "backoff_delay", "signal_and_join"]

logger = logging.getLogger(__name__)

#: How long to wait for a signalled thread to finish before giving up on it.
#: The Redis consumer and subscriber, and the heartbeat, observe their stop event
#: within about a second, so for them exceeding it means the thread is wedged. The
#: job worker observes it only between jobs: a handler still running when the wait
#: ends is left to finish on its daemon thread. Shutdown proceeds either way.
STOP_JOIN_TIMEOUT_SECONDS = 5.0


def signal_and_join(
    thread: threading.Thread | None,
    stop: threading.Event,
    *,
    timeout: float = STOP_JOIN_TIMEOUT_SECONDS,
) -> bool:
    """Set ``stop`` and wait up to ``timeout`` for ``thread`` to finish.

    Args:
        thread: The thread to wind down, or ``None`` when none is running.
        stop: The event its loop watches.
        timeout: Seconds to wait before returning regardless.

    Returns:
        True when no live thread remains, otherwise False. Stopping the thread from itself (a handler running
        on it) only signals: the loop ends once the handler returns, so there is nothing to wait for and the
        result is True.
    """
    stop.set()
    if thread is None or thread is threading.current_thread():
        return True
    thread.join(timeout=timeout)
    if thread.is_alive():
        logger.warning("Thread %r did not stop within %ss; continuing shutdown without it", thread.name, timeout)
        return False
    return True


def backoff_delay(failures: int, *, base: float, maximum: float) -> float:
    """Return how long to wait before retry number ``failures``: capped exponential backoff with equal jitter.

    Args:
        failures: Consecutive failures so far (0 for the first retry).
        base: The delay after the first failure, before jitter.
        maximum: The cap on the delay.

    Returns:
        A delay between half of the capped exponential and all of it.
    """
    delay = min(maximum, base * 2 ** min(failures, 32))
    return delay * 0.5 + random.uniform(0.0, delay * 0.5)  # noqa: S311 - jitter, not security
