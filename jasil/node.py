"""Process identity for competing-consumer coordination.

A single helper so the durable-job worker and the Redis Streams consumer derive
their identifier the same way. The value distinguishes competing consumers
across replicas in logs, lease holders (``processing_jobs.locked_by``), and
Redis pending-entry lists.
"""

import hashlib
import os
import socket

from jasil._core.limits import MAX_WORKER_ID_LENGTH


def process_identity() -> str:
    """Return a stable per-process identifier that fits the lease column.

    Uniqueness is the point: this value is what ``claim_jobs`` compares against
    to decide whether *this* worker won a row, so two live processes sharing one
    identity would run the same job twice. A long hostname therefore cannot
    simply be clipped — the truncated form carries a digest of the full identity
    instead, which stays distinct where a shared prefix would not.

    Returns:
        ``"{hostname}-{pid}"`` — unique per process, stable for its lifetime — or
        a digest-suffixed form of it when that would exceed
        :data:`~jasil._core.limits.MAX_WORKER_ID_LENGTH`.
    """
    identity = f"{socket.gethostname()}-{os.getpid()}"
    if len(identity) <= MAX_WORKER_ID_LENGTH:
        return identity
    digest = hashlib.blake2s(identity.encode(), digest_size=8).hexdigest()
    return f"{identity[: MAX_WORKER_ID_LENGTH - len(digest) - 1]}-{digest}"
