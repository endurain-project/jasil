"""Database-dialect capability checks.

JASIL's tables are portable across SQLite, PostgreSQL and MySQL, but the
concurrency primitives are not uniformly available. Rather than testing
``dialect.name == "postgresql"`` at each call site — which silently downgrades
every other database — the checks live here.
"""

from typing import Any

__all__ = ["supports_skip_locked"]


def supports_skip_locked(bind: Any) -> bool:
    """Whether ``bind`` supports ``SELECT ... FOR UPDATE SKIP LOCKED``.

    Used by the claim, relay and prune queries so concurrent workers take
    disjoint batches instead of contending. Where it is unavailable the caller
    falls back to a plain select, which stays *correct* for a single worker but
    offers no protection against two workers claiming the same row — so this
    must not report a false positive.

    Args:
        bind: The session's bind (an ``Engine`` or ``Connection``), or ``None``.

    Returns:
        True when the clause is supported. SQLite always returns False: it has
        no row-level locking, and a single writer makes the clause unnecessary.
    """
    if bind is None:
        return False
    dialect = bind.dialect
    if dialect.name == "postgresql":
        return True
    if dialect.name == "mysql":
        # Populated on first connect; treat "unknown" as unsupported.
        version = getattr(dialect, "server_version_info", None)
        if not version:
            return False
        if getattr(dialect, "is_mariadb", False):
            return version >= (10, 6)
        return version >= (8, 0, 1)
    return False
