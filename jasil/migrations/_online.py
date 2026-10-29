"""Index builds on ``processing_jobs`` that do not stop the workers writing to it.

A plain ``CREATE INDEX`` blocks every write to its table until it finishes, and one on ``processing_jobs`` takes as
long as the retained jobs make it, during which no worker of the running release can claim or finish a job. On
PostgreSQL :func:`create_index` builds ``CONCURRENTLY`` instead. That cannot run inside a transaction, so it commits
the revision's work so far: a revision calls it after every other change, and asks :func:`resumed` first, because a
run that failed during the build leaves that work committed and the revision unrecorded. On other databases both
are the plain operation inside the revision's transaction.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

__all__ = ["create_index", "resumed"]


def _builds_concurrently() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def resumed(table: str, column: str) -> bool:
    """Whether an earlier run of this revision committed its changes, adding ``column`` to ``table``, then failed.

    Always False off PostgreSQL, where nothing is committed before the revision is, and when rendering SQL.
    """
    if not _builds_concurrently() or op.get_context().as_sql:
        return False
    columns = sa.inspect(op.get_bind()).get_columns(table)  # pragma: no cover - PostgreSQL job only
    return column in {column_info["name"] for column_info in columns}  # pragma: no cover - PostgreSQL job only


def create_index(name: str, table: str, columns: Sequence[str]) -> None:
    """Create the index ``name`` on ``table``; on PostgreSQL without blocking writes to it.

    On PostgreSQL the revision's changes so far are committed first. An index a failed build left invalid is
    dropped and built again, and a valid one an earlier run built is kept.
    """
    if not _builds_concurrently():
        op.create_index(name, table, list(columns))
        return
    context = op.get_context()
    with context.autocommit_block():
        _create_concurrently(name, table, columns, rendering=context.as_sql)


def _create_concurrently(name: str, table: str, columns: Sequence[str], *, rendering: bool) -> None:
    if not rendering:  # pragma: no cover - PostgreSQL job only (tests/test_migrations.py::TestRevisionTransactions)
        valid = (
            op.get_bind()
            .execute(sa.text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"), {"name": name})
            .scalar()
        )
        if valid:
            return
        if valid is not None:
            op.drop_index(name, table_name=table, postgresql_concurrently=True)
    op.create_index(name, table, list(columns), postgresql_concurrently=True)
