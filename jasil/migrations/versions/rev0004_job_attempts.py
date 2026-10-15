"""Add the durable job attempt history.

One row per attempt to run a job, written in the transaction that claims or
finishes it. Purely additive: previous-release workers neither read nor write the
table, so a rolling deployment is safe in either order for them; the new release
requires it, so run this migration before deploying the new code, as with the
queue migration.

Revision ID: rev0004
Revises: rev0003
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "rev0004"
down_revision: str | None = "rev0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "job_attempts",
        sa.Column("id", sa.String(36), primary_key=True, comment="Attempt identifier (UUIDv4)"),
        sa.Column(
            "job_id",
            sa.String(36),
            nullable=False,
            comment="The processing_jobs.id this attempt ran (no foreign key; pruned independently)",
        ),
        sa.Column("event_id", sa.String(36), nullable=False, comment="The originating envelope event_id"),
        sa.Column("event_type", sa.String(100), nullable=False, comment="The domain-event channel"),
        sa.Column("subscriber_id", sa.String(200), nullable=False, comment="The durable subscriber that ran"),
        sa.Column("queue", sa.String(100), nullable=False, comment="The named queue the job was claimed from"),
        sa.Column(
            "attempt",
            sa.Integer,
            nullable=False,
            comment="Attempt number at claim; restarts at 1 after a dead-letter replay",
        ),
        sa.Column("worker_id", sa.String(100), nullable=False, comment="The worker instance that claimed the job"),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False, comment="When the worker took the lease"),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the handler began; NULL if it never ran",
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the attempt ended; NULL while open",
        ),
        sa.Column("outcome", sa.String(16), nullable=True, comment="How the attempt ended; NULL while open"),
        sa.Column("duration_ms", sa.Integer, nullable=True, comment="Handler wall time in milliseconds"),
        sa.Column("error", sa.Text, nullable=True, comment="Failure reason (truncated for storage)"),
    )
    op.create_index("idx_job_attempts_job", "job_attempts", ["job_id", "claimed_at"])
    op.create_index("idx_job_attempts_worker", "job_attempts", ["worker_id", "claimed_at"])
    op.create_index("idx_job_attempts_event", "job_attempts", ["event_id"])
    op.create_index("idx_job_attempts_claimed", "job_attempts", ["claimed_at"])


def downgrade() -> None:
    op.drop_index("idx_job_attempts_claimed", table_name="job_attempts")
    op.drop_index("idx_job_attempts_event", table_name="job_attempts")
    op.drop_index("idx_job_attempts_worker", table_name="job_attempts")
    op.drop_index("idx_job_attempts_job", table_name="job_attempts")
    op.drop_table("job_attempts")
