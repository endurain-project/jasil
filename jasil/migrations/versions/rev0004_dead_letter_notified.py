"""Add ``processing_jobs.dead_letter_notified_at`` for the dead-letter hook.

The column records that a subscriber's ``on_dead_letter`` callback has run for a
dead-lettered job, which is what lets the callback be delivered at least once
across a worker or reaper crash. It is NULL while a callback is owed.

Rows already in ``dead_letter`` are stamped with ``updated_at`` so the first
sweep after the upgrade does not call back for failures that predate the hook.
Previous-release processes neither read nor write the column.

Revision ID: rev0004
Revises: rev0003
Create Date: 2026-10-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil.migrations._names import PREFIX

revision: str = "rev0004"
down_revision: str | None = "rev0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Every table has carried the ``jasil_`` prefix since rev0003, whatever the runner's
# prefix is for the earlier revisions.
_JOBS = f"{PREFIX}processing_jobs"


def upgrade() -> None:
    op.add_column(
        _JOBS,
        sa.Column(
            "dead_letter_notified_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the on_dead_letter callback last succeeded; NULL while one is owed",
        ),
    )
    jobs = sa.table(
        _JOBS,
        sa.column("status", sa.String),
        sa.column("updated_at", sa.DateTime(timezone=True)),
        sa.column("dead_letter_notified_at", sa.DateTime(timezone=True)),
    )
    op.execute(jobs.update().where(jobs.c.status == "dead_letter").values(dead_letter_notified_at=jobs.c.updated_at))


def downgrade() -> None:
    op.drop_column(_JOBS, "dead_letter_notified_at")
