"""Shutdown handling for the library's daemon threads.

The Redis Streams consumer and the durable-job worker both run a poll loop on a
daemon thread that watches a stop event. Winding one down is the same three
steps in both, with the same bounded wait: the loops only observe the event
between iterations, so a join has to be given a timeout or a slow poll would
block shutdown indefinitely.
"""

import logging
import threading

__all__ = ["STOP_JOIN_TIMEOUT_SECONDS", "signal_and_join"]

logger = logging.getLogger(__name__)

#: How long to wait for a signalled thread to finish before giving up on it.
#: Both loops check their stop event at least once a second, so this is generous;
#: exceeding it means the thread is wedged and shutdown should proceed regardless.
STOP_JOIN_TIMEOUT_SECONDS = 5.0


def signal_and_join(
    thread: threading.Thread | None,
    stop: threading.Event,
    *,
    timeout: float = STOP_JOIN_TIMEOUT_SECONDS,
) -> None:
    """Set ``stop`` and wait up to ``timeout`` for ``thread`` to finish.

    Args:
        thread: The thread to wind down, or ``None`` when none is running.
        stop: The event its loop watches.
        timeout: Seconds to wait before returning regardless.
    """
    stop.set()
    if thread is None:
        return
    thread.join(timeout=timeout)
    if thread.is_alive():
        # Abandoning it is the right call — it is a daemon thread and shutdown
        # must not block — but doing so silently makes the next symptom
        # (work that appears to run after shutdown) impossible to explain.
        logger.warning("Thread %r did not stop within %ss; continuing shutdown without it", thread.name, timeout)
