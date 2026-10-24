"""Add the ``processing_jobs`` columns of the dead-letter hook.

``dead_letter_notified_at`` records that a subscriber's ``on_dead_letter`` callback
has run for a dead-lettered job, which is what lets the callback be delivered at
least once across a worker or reaper crash. It is NULL while a callback is owed.
``dead_letter_notify_after`` is the lease or backoff gating the next delivery,
``dead_letter_attempts`` counts deliveries and ``dead_letter_callback_error`` keeps
the last callback failure, so a stuck notification can be diagnosed.

Rows already in ``dead_letter`` are stamped with the instant they were
dead-lettered so the first sweep after the upgrade does not call back for failures
that predate the hook. A job that a previous-release process dead-letters while the
upgrade rolls out keeps NULL and is called back, as any other owed one.
Previous-release processes neither read nor write the columns.

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
    op.add_column(
        _JOBS,
        sa.Column(
            "dead_letter_notify_after",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="Earliest instant an on_dead_letter delivery may be claimed (lease or backoff)",
        ),
    )
    op.add_column(
        _JOBS,
        sa.Column(
            "dead_letter_attempts",
            sa.Integer,
            nullable=False,
            server_default="0",
            comment="on_dead_letter deliveries claimed so far",
        ),
    )
    op.add_column(
        _JOBS,
        sa.Column(
            "dead_letter_callback_error",
            sa.Text,
            nullable=True,
            comment="Most recent on_dead_letter callback failure (truncated for storage)",
        ),
    )
    jobs = sa.table(
        _JOBS,
        sa.column("status", sa.String),
        sa.column("updated_at", sa.DateTime(timezone=True)),
        sa.column("completed_at", sa.DateTime(timezone=True)),
        sa.column("dead_letter_notified_at", sa.DateTime(timezone=True)),
    )
    op.execute(
        jobs.update()
        .where(jobs.c.status == "dead_letter")
        .values(dead_letter_notified_at=sa.func.coalesce(jobs.c.completed_at, jobs.c.updated_at))
    )


def downgrade() -> None:
    for column in (
        "dead_letter_callback_error",
        "dead_letter_attempts",
        "dead_letter_notify_after",
        "dead_letter_notified_at",
    ):
        op.drop_column(_JOBS, column)
