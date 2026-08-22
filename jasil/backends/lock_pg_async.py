"""Async PostgreSQL advisory-lock ``AsyncLockProvider`` backend.

This is the awaitable twin of :mod:`jasil.backends.lock_pg`. The lock
semantics, key derivation, and connection-lifetime model are identical — read
that module for the full contract.

Only imported by the async composition root when ``lock_uri`` selects
``postgres-advisory://`` (the distributed-profile default), so single-process
deployments never load it.

**Design decision — one dedicated ``AsyncConnection`` per lock acquisition.**
PostgreSQL session-level advisory locks (``pg_try_advisory_lock`` /
``pg_advisory_unlock``) are bound to the *connection*, not the transaction: they
survive ``COMMIT`` and ``ROLLBACK`` and are released only when the connection is
closed or ``pg_advisory_unlock`` is called explicitly. A session-scoped API
(``async with AsyncSession(...)``) returns the connection to the pool after each
operation, so it cannot hold an advisory lock reliably between the acquire and
the release. This backend therefore calls ``AsyncEngine.connect()`` — which
checks out a *raw* connection for the duration of the ``async with`` block —
rather than going through the session factory.

**Key derivation is not reimplemented.** The :func:`advisory_key` function is
imported from the sync module. Both backends produce the same signed 64-bit key
for the same lock name, so a running sync worker and a running async worker
contend for the same Postgres lock rather than silently using different ones.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import text

import jasil.orm as jasil_orm
from jasil.backends.lock_pg import advisory_key

logger = logging.getLogger(__name__)


class AsyncPgAdvisoryLock:
    """``AsyncLockProvider`` using PostgreSQL session-level advisory locks on the main DB.

    ``try_acquire`` is non-blocking (``pg_try_advisory_lock``); it holds one
    dedicated ``AsyncConnection`` open while the lock is held and releases it on
    exit. ``ttl_seconds`` does not apply — a session advisory lock lives until it
    is explicitly unlocked or the connection is dropped.

    Build via :meth:`from_main_database`.
    """

    def __init__(self, engine: Any) -> None:
        self._engine = engine

    @classmethod
    def from_main_database(cls) -> "AsyncPgAdvisoryLock":
        """Build against the engine the host's async session factory is bound to.

        No I/O is performed; the engine is only used when ``try_acquire`` is
        called. Therefore, no ``async def create_*`` factory is necessary — a
        plain class-method is sufficient.

        Returns:
            A configured :class:`AsyncPgAdvisoryLock`.

        Raises:
            RuntimeError: If :func:`jasil.orm.configure_async_sessionmaker` has
                not been called, or its factory is not bound to an engine.
        """
        return cls(jasil_orm.get_async_engine())

    @asynccontextmanager
    async def try_acquire(self, name: str, ttl_seconds: int | None = None) -> AsyncIterator[bool]:
        """Attempt a non-blocking advisory lock and yield whether it was taken.

        Holds a single ``AsyncConnection`` for the entire duration so the
        session-level lock stays on the right backend connection. The connection
        is released (and the lock is dropped with it) when the context exits,
        even if the body raises.

        Args:
            name: Logical lock name (e.g. ``"nightly_backfill"``). Hashed to a
                signed 64-bit key via :func:`~jasil.backends.lock_pg.advisory_key`.
            ttl_seconds: Ignored — session advisory locks are not TTL-based.

        Yields:
            ``True`` if the lock was acquired, ``False`` if another holder
            already owns it.
        """
        del ttl_seconds  # session advisory locks are not TTL-based
        key = advisory_key(name)
        # ``AsyncEngine.connect()`` checks out a raw connection from the pool
        # that stays checked out until ``aconn.close()`` is awaited. Using it as
        # an async context manager gives us that guarantee even on exceptions.
        async with self._engine.connect() as connection:
            result = await connection.execute(text("SELECT pg_try_advisory_lock(CAST(:key AS bigint))"), {"key": key})
            acquired = bool(result.scalar())
            try:
                yield acquired
            finally:
                if acquired:
                    try:
                        await connection.execute(text("SELECT pg_advisory_unlock(CAST(:key AS bigint))"), {"key": key})
                    except Exception as error:
                        # Never let an unlock failure mask a body exception; drop
                        # the still-locked connection so its backend session (and
                        # the lock) is discarded instead of returned to the pool.
                        logger.error("Failed to release advisory lock %r", name, exc_info=error)
                        await connection.invalidate()
