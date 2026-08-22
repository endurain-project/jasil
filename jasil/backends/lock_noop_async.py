"""Async no-op ``AsyncLockProvider`` backend (single process; always acquires).

This is the awaitable twin of :mod:`jasil.backends.lock_noop`. The semantics
are identical: with a single process there is nothing to coordinate with, so
:meth:`AsyncNoopLock.try_acquire` always yields ``True``. The
:class:`~jasil.backends.lock_pg_async.AsyncPgAdvisoryLock` backend provides
real cross-replica coordination for the distributed profile.

There is no I/O in this module and no async setup required, so no module-level
factory is needed; :class:`AsyncNoopLock` can be instantiated with a plain
``AsyncNoopLock()``.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class AsyncNoopLock:
    """``AsyncLockProvider`` that always acquires — correct for a single process.

    With one process there is nothing to coordinate with, so ``try_acquire``
    always yields ``True``. The Postgres-advisory async backend provides real
    cross-replica coordination for the distributed profile.
    """

    @asynccontextmanager
    async def try_acquire(self, name: str, ttl_seconds: int | None = None) -> AsyncIterator[bool]:
        """Yield ``True`` unconditionally.

        Args:
            name: Logical lock name (ignored).
            ttl_seconds: Lock lifetime hint (ignored — single-process needs no
                TTL).

        Yields:
            ``True``.
        """
        yield True
