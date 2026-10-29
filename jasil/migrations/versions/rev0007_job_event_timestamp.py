"""Add ``processing_jobs.event_timestamp``, the envelope's publish timestamp.

A subscriber's envelope used to be rebuilt with the job's ``created_at`` (when the
relay enqueued it) as its ``timestamp``, which is documented as the time of the
first publish. Nullable, so a previous-release process keeps writing rows without
it; a job with no value falls back to its ``created_at``, as before.

Revision ID: rev0007
Revises: rev0006
Create Date: 2026-10-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil.migrations._names import PREFIX

revision: str = "rev0007"
down_revision: str | None = "rev0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JOBS = f"{PREFIX}processing_jobs"


def upgrade() -> None:
    op.add_column(
        _JOBS,
        sa.Column(
            "event_timestamp",
            sa.String(40),
            nullable=True,
            comment="Envelope ISO-8601 publish timestamp; NULL on jobs enqueued before rev0007",
        ),
    )


def downgrade() -> None:
    op.drop_column(_JOBS, "event_timestamp")
