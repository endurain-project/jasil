"""Index builds on tables that the running release keeps writing to, without stopping those writes.

A plain ``CREATE INDEX`` blocks every write to its table until it finishes, and one on ``processing_jobs`` or
``event_outbox`` takes as long as the retained rows make it, during which no process of the running release can
write them. On PostgreSQL :func:`create_index` builds ``CONCURRENTLY`` instead. That cannot run inside a
transaction, so it commits the revision's work so far: a revision calls it after every other change, and asks
:func:`resumed` first when it has changes to commit, because a run that failed during the build leaves that work
committed and the revision unrecorded. A revision that only builds an index has no such work and needs no
:func:`resumed`: the build itself recognizes the index an earlier run left valid or invalid. On other databases
both are the plain operation inside the revision's transaction.

PostgreSQL cannot build an index on a partitioned table concurrently, so :func:`create_partitioned_index` builds
the parent's index ``ON ONLY`` and each partition's concurrently, attaching each as it is built.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

__all__ = ["create_index", "create_partitioned_index", "resumed"]


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


def create_index(name: str, table: str, columns: Sequence[str], *, unique: bool = False) -> None:
    """Create the index ``name`` on ``table``, ``unique`` or not; on PostgreSQL without blocking writes to it.

    On PostgreSQL the revision's changes so far are committed first. An index a failed build left invalid is
    dropped and built again, and a valid one an earlier run built is kept.
    """
    if not _builds_concurrently():
        op.create_index(name, table, list(columns), unique=unique)
        return
    context = op.get_context()
    with context.autocommit_block():
        _create_concurrently(name, table, columns, unique=unique, rendering=context.as_sql)


def _create_concurrently(name: str, table: str, columns: Sequence[str], *, unique: bool, rendering: bool) -> None:
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
    op.create_index(name, table, list(columns), unique=unique, postgresql_concurrently=True)


def create_partitioned_index(name: str, table: str, columns: Sequence[str]) -> None:
    """Create the index ``name`` on the partitioned ``table``; on PostgreSQL without blocking writes to it.

    On PostgreSQL the revision's changes so far are committed first. The parent's index is created ``ON ONLY``,
    which builds nothing and leaves it invalid; a partition created from then on gets its own index at creation.
    Each existing partition's index is then built concurrently and attached, and attaching the last makes the
    parent's valid. A partition already attached, by an earlier run or at its creation, is skipped, so a run that
    failed part-way resumes where it stopped. Rendered as SQL there are no partitions to list, so the script holds
    the plain build, which blocks writes to every partition while it runs.
    """
    context = op.get_context()
    if not _builds_concurrently() or context.as_sql:
        op.create_index(name, table, list(columns))
        return
    with context.autocommit_block():
        _create_partitions_concurrently(name, table, columns)


def _create_partitions_concurrently(  # pragma: no cover - PostgreSQL job only
    name: str, table: str, columns: Sequence[str]
) -> None:
    bind = op.get_bind()
    quote = bind.dialect.identifier_preparer.quote
    column_list = ", ".join(quote(column) for column in columns)
    op.execute(f"CREATE INDEX IF NOT EXISTS {quote(name)} ON ONLY {quote(table)} ({column_list})")
    unattached = bind.execute(
        sa.text(
            "SELECT member.relname FROM pg_inherits AS link JOIN pg_class AS member ON member.oid = link.inhrelid"
            " WHERE link.inhparent = to_regclass(:table) AND NOT EXISTS ("
            "  SELECT 1 FROM pg_inherits AS attached JOIN pg_index AS built ON built.indexrelid = attached.inhrelid"
            "  WHERE attached.inhparent = to_regclass(:name) AND built.indrelid = link.inhrelid)"
            " ORDER BY member.relname"
        ),
        {"table": table, "name": name},
    ).scalars()
    for partition in list(unattached):
        child = f"{partition}_{'_'.join(columns)}_idx"
        _create_concurrently(child, partition, columns, unique=False, rendering=False)
        op.execute(f"ALTER INDEX {quote(name)} ATTACH PARTITION {quote(child)}")
