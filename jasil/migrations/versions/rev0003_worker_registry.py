"""Add the durable worker registry.

Revision ID: rev0003
Revises: rev0002
Create Date: 2026-08-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "rev0003"
down_revision: str | None = "rev0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JSON = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "job_workers",
        sa.Column("instance_id", sa.String(36), primary_key=True, comment="Restart-unique worker instance UUID"),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), nullable=False, comment="When this worker instance started"
        ),
        sa.Column(
            "last_heartbeat_at",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Most recent successful worker heartbeat",
        ),
        sa.Column(
            "stopped_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When this worker stopped gracefully; NULL after a crash",
        ),
        sa.Column("queues", _JSON, nullable=True, comment="Selected queue allowlist; NULL means all queues"),
        sa.Column("role", sa.String(100), nullable=True, comment="Optional host-supplied worker role"),
        sa.Column("label", sa.String(200), nullable=True, comment="Optional host-supplied operator label"),
        sa.Column(
            "worker_metadata",
            _JSON,
            nullable=True,
            comment="Optional host-supplied neutral metadata",
        ),
    )
    op.create_index("idx_job_workers_heartbeat", "job_workers", ["last_heartbeat_at"])
    op.create_index("idx_job_workers_stopped", "job_workers", ["stopped_at"])
    op.create_index("idx_processing_jobs_worker_claim", "processing_jobs", ["status", "locked_by"])


def downgrade() -> None:
    op.drop_index("idx_processing_jobs_worker_claim", table_name="processing_jobs")
    op.drop_index("idx_job_workers_stopped", table_name="job_workers")
    op.drop_index("idx_job_workers_heartbeat", table_name="job_workers")
    op.drop_table("job_workers")
