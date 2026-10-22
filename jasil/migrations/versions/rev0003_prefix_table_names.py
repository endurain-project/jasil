"""Prefix every JASIL table, index and constraint name with ``jasil_``.

JASIL's tables live in the host's database and the host's declarative registry,
so a generic name such as ``event_log`` or ``job_attempts`` can collide with a
table the host already has. This revision renames each table, its indexes and
its unique constraint (and, on PostgreSQL, its primary-key index); no column or row changes.

A database created by this release already has the prefixed names (the earlier
revisions create them directly when a fresh install is heading here), and this
revision then does nothing. Only tables still carrying the old names are renamed.

* PostgreSQL renames in place (``ALTER TABLE/INDEX ... RENAME``), which is
  instant and keeps the data pages.
* SQLite and MySQL rename the table and recreate its indexes under the new names;
  the unique constraint is swapped through a batch operation, which on SQLite
  rebuilds the one table that carries it.

Every code path of the previous release looks the old names up, so **stop every
process running 0.8 or earlier before this migration**; a rolling deployment
across it is not possible.

Revision ID: rev0003
Revises: rev0002
Create Date: 2026-10-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil.migrations._names import PREFIX

revision: str = "rev0003"
down_revision: str | None = "rev0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: ``(unprefixed table, plain indexes as (unprefixed name, columns))`` for every
#: table, in the order they are renamed.
_TABLES: tuple[tuple[str, tuple[tuple[str, tuple[str, ...]], ...]], ...] = (
    (
        "event_log",
        (
            ("idx_event_log_type_status", ("event_type", "status")),
            ("idx_event_log_created", ("created_at",)),
        ),
    ),
    ("event_outbox", (("idx_event_outbox_relayed", ("relayed_at", "created_at")),)),
    (
        "processing_jobs",
        (
            ("idx_processing_jobs_claim", ("status", "available_at")),
            ("idx_processing_jobs_lease", ("status", "lease_expires_at")),
            ("idx_processing_jobs_queue_claim", ("status", "queue", "available_at")),
            ("idx_processing_jobs_worker_claim", ("status", "locked_by")),
        ),
    ),
    (
        "job_workers",
        (
            ("idx_job_workers_heartbeat", ("last_heartbeat_at",)),
            ("idx_job_workers_stopped", ("stopped_at",)),
        ),
    ),
    (
        "job_attempts",
        (
            ("idx_job_attempts_job", ("job_id", "claimed_at")),
            ("idx_job_attempts_worker", ("worker_id", "claimed_at")),
            ("idx_job_attempts_event", ("event_id",)),
            ("idx_job_attempts_claimed", ("claimed_at",)),
            ("idx_job_attempts_finished", ("finished_at",)),
        ),
    ),
)

#: The one unique constraint: ``(table, unprefixed name, columns)``.
_UNIQUE = ("processing_jobs", "uq_processing_jobs_event_subscriber", ("event_id", "subscriber_id"))

#: The PostgreSQL-only GIN index on ``event_log.event_metadata`` (rev0001).
_GIN_INDEX = "idx_event_log_metadata_gin"


def _prefixed(name: str) -> str:
    """Return ``name`` as it is called from this revision on.

    Deliberately not :func:`jasil.migrations._names.index`: that one follows the
    runner's context variable, while this revision always means the final names.

    Indexes and constraints read ``idx_<table>_...`` / ``uq_<table>_...``; the
    prefix goes after the ``idx_`` / ``uq_`` marker so the convention is kept.
    """
    for marker in ("idx_", "uq_"):
        if name.startswith(marker):
            return f"{marker}{PREFIX}{name[len(marker) :]}"
    return f"{PREFIX}{name}"


def _rename(tables: Sequence[str], *, to_prefixed: bool) -> None:
    """Rename the given tables, with their indexes and constraints, one way or the other."""

    def source(name: str) -> str:
        return name if to_prefixed else _prefixed(name)

    def target(name: str) -> str:
        return _prefixed(name) if to_prefixed else name

    on_postgres = op.get_bind().dialect.name == "postgresql"
    for table, indexes in _TABLES:
        if table not in tables:
            continue
        op.rename_table(source(table), target(table))
        if on_postgres:
            # The primary-key index keeps the old table's name through a rename.
            op.execute(f'ALTER INDEX IF EXISTS "{source(table)}_pkey" RENAME TO "{target(table)}_pkey"')
            for index, _ in indexes:
                op.execute(f'ALTER INDEX IF EXISTS "{source(index)}" RENAME TO "{target(index)}"')
            if table == "event_log":
                op.execute(f'ALTER INDEX IF EXISTS "{source(_GIN_INDEX)}" RENAME TO "{target(_GIN_INDEX)}"')
        else:
            for index, columns in indexes:
                op.drop_index(source(index), table_name=target(table))
                op.create_index(target(index), target(table), list(columns))

    table, name, columns = _UNIQUE
    if table not in tables:
        return
    if on_postgres:
        op.execute(f'ALTER TABLE "{target(table)}" RENAME CONSTRAINT "{source(name)}" TO "{target(name)}"')
    else:
        with op.batch_alter_table(target(table)) as batch:
            batch.drop_constraint(source(name), type_="unique")
            batch.create_unique_constraint(target(name), list(columns))


def upgrade() -> None:
    # A fresh install already created the prefixed tables in rev0001/rev0002 (see
    # ``jasil.migrations._names``), so only a table that is still unprefixed is
    # renamed - and never when a prefixed one exists, which would mean the
    # unprefixed one is the host's own.
    if op.get_context().as_sql:
        # Offline (``--sql``): there is no database to inspect. The earlier
        # revisions emit the unprefixed names in this mode, so rename them all.
        _rename([table for table, _ in _TABLES], to_prefixed=True)
        return
    inspector = sa.inspect(op.get_bind())
    legacy = [
        table for table, _ in _TABLES if inspector.has_table(table) and not inspector.has_table(f"{PREFIX}{table}")
    ]
    _rename(legacy, to_prefixed=True)


def downgrade() -> None:
    _rename([table for table, _ in _TABLES], to_prefixed=False)
