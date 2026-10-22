"""No-op ``LockProvider`` backend (single process; always acquires)."""

from collections.abc import Generator
from contextlib import contextmanager


class NoopLock:
    """``LockProvider`` that always acquires — correct for a single process.

    With one process there is nothing to coordinate with, so ``try_acquire``
    always yields ``True``. The Postgres-advisory backend provides real
    cross-replica coordination for the distributed profile.
    """

    @contextmanager
    def try_acquire(self, name: str, ttl_seconds: int | None = None) -> Generator[bool]:
        yield True
