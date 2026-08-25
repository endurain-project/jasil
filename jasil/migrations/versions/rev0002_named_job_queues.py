"""Add named durable-job queues.

Revision ID: rev0002
Revises: rev0001
Create Date: 2026-08-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "rev0002"
down_revision: str | None = "rev0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "processing_jobs",
        sa.Column(
            "queue",
            sa.String(100),
            nullable=False,
            server_default="default",
            comment="Named queue used to select which workers may claim the job",
        ),
    )
    op.create_index(
        "idx_processing_jobs_queue_claim",
        "processing_jobs",
        ["queue", "status", "available_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_processing_jobs_queue_claim", table_name="processing_jobs")
    op.drop_column("processing_jobs", "queue")
