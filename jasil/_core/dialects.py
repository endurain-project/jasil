"""Database-dialect capability checks.

JASIL's tables are portable across SQLite, PostgreSQL and MySQL, but the
concurrency primitives are not uniformly available. Rather than testing
``dialect.name == "postgresql"`` at each call site — which silently downgrades
every other database — the checks live here.
"""

from collections.abc import Sequence
from typing import Any

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

__all__ = ["insert_ignoring_conflict", "needs_locking_read", "supports_skip_locked"]


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
    if dialect.name in ("mysql", "mariadb"):
        # Populated on first connect; treat "unknown" as unsupported.
        version = getattr(dialect, "server_version_info", None)
        if not version:
            return False
        if getattr(dialect, "is_mariadb", False):
            return version >= (10, 6)
        return version >= (8, 0, 1)
    return False


def needs_locking_read(bind: Any) -> bool:
    """Whether a read that must see the latest committed row has to lock it.

    After an insert-or-ignore that hit a duplicate, the caller reads back the row that won. On MySQL and
    MariaDB a transaction reads from a ``REPEATABLE READ`` snapshot taken at its first read, which would hide a
    row another transaction committed first; ``SELECT ... FOR UPDATE`` reads the latest committed row instead.
    On PostgreSQL (``READ COMMITTED``) ``ON CONFLICT DO NOTHING`` already waits for a conflicting insert still
    in flight and the next statement sees the winner, and on SQLite a single writer makes the question moot, so
    the lock would only serialise duplicate writers for no gain.

    Args:
        bind: The session's bind (an ``Engine`` or ``Connection``), or ``None``.

    Returns:
        True on MySQL and MariaDB, False on every other database and when there is no bind.
    """
    return bind is not None and bind.dialect.name in ("mysql", "mariadb")


def insert_ignoring_conflict(model: Any, values: dict, *, conflict_columns: Sequence[str], db: Any):
    """Build a dialect-appropriate "insert, or do nothing if it exists" statement.

    A unique constraint is what makes a write idempotent, so a duplicate must be a silent no-op on **every**
    supported database, not an ``IntegrityError`` the caller would have to catch.

    Args:
        model: The mapped class to insert into.
        values: Column values for the new row, keyed by attribute name.
        conflict_columns: The columns of the unique constraint whose violation is ignored.
        db: Active database session (used only for dialect detection).

    Returns:
        An executable insert statement that ignores a duplicate of ``conflict_columns``.

    Raises:
        RuntimeError: On a dialect with no supported conflict clause, rather than emitting a plain INSERT that
            would raise on the second write.
    """
    # The engine ``model`` is routed to: a session built with ``binds=`` has no single ``db.bind``.
    dialect = db.get_bind(model).dialect.name
    if dialect == "postgresql":  # pragma: no cover - exercised on Postgres, not in SQLite tests
        return pg_insert(model).values(**values).on_conflict_do_nothing(index_elements=list(conflict_columns))
    if dialect == "sqlite":
        return sqlite_insert(model).values(**values).on_conflict_do_nothing(index_elements=list(conflict_columns))
    if dialect in ("mysql", "mariadb"):  # pragma: no cover - exercised on MySQL, not in SQLite tests
        # MySQL has no ON CONFLICT DO NOTHING. Assigning a column to *itself*
        # is the standard no-op form; ``INSERT IGNORE`` would also swallow
        # unrelated errors such as truncation.
        statement = mysql_insert(model).values(**values)
        column = conflict_columns[0]
        return statement.on_duplicate_key_update(**{column: getattr(model, column)})
    raise RuntimeError(
        f"An idempotent insert needs an insert-or-ignore clause, which dialect {dialect!r} does not provide. "
        "Supported: postgresql, mysql, mariadb, sqlite."
    )
