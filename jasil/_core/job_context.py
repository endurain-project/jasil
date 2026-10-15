"""The signal a running durable job can read: has its lease been lost?

A worker cannot stop a handler, but it can tell it to stop. While a job runs, the
runner binds an event to the handler's context; the worker's heartbeat sets it
when this worker no longer holds the job (the row was requeued and may be running
elsewhere), when the job outran ``jobs.max_runtime_seconds``, or when renewal has
failed for so long that the lease must be assumed gone.

Lives in ``_core`` so a backend (mail, say) can read it without importing the
jobs package, which touches the ORM at import. Pure: no I/O, no infrastructure.

A ``ContextVar`` does not follow work a handler hands to its own threads unless
the handler copies the context (``contextvars.copy_context()``).
"""

import contextvars
import threading
from collections.abc import Iterator
from contextlib import contextmanager

__all__ = ["job_lease", "lease_lost"]

_lease_lost: contextvars.ContextVar[threading.Event | None] = contextvars.ContextVar(
    "jasil_job_lease_lost", default=None
)


def lease_lost() -> bool:
    """Whether the durable job running in this context has lost its lease.

    A handler that loops, or sends several messages, should check this between
    units of work and return early: the result of a run that lost its lease is
    discarded, but its side effects are not.

    Returns:
        True once the lease is gone. Always False outside a durable job.
    """
    event = _lease_lost.get()
    return event is not None and event.is_set()


@contextmanager
def job_lease(lost: threading.Event) -> Iterator[None]:
    """Bind ``lost`` as the lease-lost signal of the enclosed handler call.

    Args:
        lost: The event the worker's heartbeat sets when the lease is lost.
    """
    token = _lease_lost.set(lost)
    try:
        yield
    finally:
        _lease_lost.reset(token)
