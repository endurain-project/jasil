"""Process identity for competing-consumer coordination.

The Redis Streams backend needs one stable consumer name per process. The value
distinguishes replicas in logs and Redis pending-entry lists and fits the event
log's fixed-width worker column.
"""

import hashlib
import os
import socket

from jasil._core.limits import MAX_WORKER_ID_LENGTH


def process_identity() -> str:
    """Return a stable per-process identifier that fits persisted worker fields.

    Uniqueness is the point: two Redis consumers sharing one identity would
    collide in the consumer group's pending-entry state. A long hostname
    therefore cannot simply be clipped — the truncated form carries a digest of
    the full identity instead, which stays distinct where a shared prefix would
    not.

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
