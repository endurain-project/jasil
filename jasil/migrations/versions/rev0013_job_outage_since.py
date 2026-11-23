"""Add ``processing_jobs.outage_since``, the start of a job's run of storage or database outages.

A handler that fails because storage or the database is unreachable is deferred rather than failed, for as long
as ``jobs.outage_defer_max_seconds`` from its first such deferral; this column holds that instant, and is NULL
while the job is not in an outage. Existing rows start NULL, which is what they mean. Previous-release processes
neither read nor write the column: they fail an outage error as before, and a job they complete or fail keeps
the instant a new-release process stored, so a later outage of that job starts from the earlier one's window.
The column is ``DATETIME(6)`` on MySQL and MariaDB, as every JASIL timestamp column has been since rev0012.

Outage deferrals count in ``deferrals``, so its comment is reworded where the database keeps comments (not on
SQLite). On MySQL and MariaDB that is a ``MODIFY COLUMN`` restating the column's type, nullability and default.

Revision ID: rev0013
Revises: rev0012
Create Date: 2026-11-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

from jasil.migrations._names import PREFIX

revision: str = "rev0013"
down_revision: str | None = "rev0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JOBS = f"{PREFIX}processing_jobs"

_DEFERRALS_COMMENT = "Claims deferred by the handler (DeferJob) or by an outage; they do not count against max_attempts"
_PREVIOUS_DEFERRALS_COMMENT = "Claims the handler deferred with DeferJob; they do not count against max_attempts"


def upgrade() -> None:
    op.add_column(
        _JOBS,
        sa.Column(
            "outage_since",
            sa.DateTime(timezone=True).with_variant(mysql.DATETIME(timezone=True, fsp=6), "mysql", "mariadb"),
            nullable=True,
            comment="First deferral in the current run of storage or database outages; NULL when not in one",
        ),
    )
    _comment_deferrals(_DEFERRALS_COMMENT, previous=_PREVIOUS_DEFERRALS_COMMENT)


def downgrade() -> None:
    _comment_deferrals(_PREVIOUS_DEFERRALS_COMMENT, previous=_DEFERRALS_COMMENT)
    op.drop_column(_JOBS, "outage_since")


def _comment_deferrals(comment: str, *, previous: str) -> None:
    """Set the comment of ``deferrals``; SQLite keeps no column comments, so nothing is written there."""
    if op.get_bind().dialect.name == "sqlite":
        return
    op.alter_column(
        _JOBS,
        "deferrals",
        comment=comment,
        existing_comment=previous,
        existing_type=sa.Integer(),
        existing_nullable=False,
        existing_server_default="0",
    )
