"""Add named job queues, the worker registry and the job attempt history.

Three additions to the durable-job tables, shipped as one revision:

* ``processing_jobs.queue``, with a database-side default so writers from the
  previous release keep working during a rolling deployment, and the index claims
  select by;
* ``job_workers``, the registry of worker instances, and its indexes;
* ``job_attempts``, one row per attempt to run a job, written in the transaction
  that claims or finishes it. Previous-release workers neither read nor write it.

The new release requires all of it, so run this migration before deploying the
new code.

Revision ID: rev0002
Revises: rev0001
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

from jasil.migrations._names import index as ix
from jasil.migrations._names import table as tb

revision: str = "rev0002"
down_revision: str | None = "rev0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JSON = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade() -> None:
    op.add_column(
        tb("processing_jobs"),
        sa.Column(
            "queue",
            sa.String(100),
            nullable=False,
            server_default="default",
            comment="Named queue used to select which workers may claim the job",
        ),
    )
    # ``status`` leads so a claim's range scan over queues never walks retained
    # completed or dead-letter rows.
    op.create_index(
        ix("idx_processing_jobs_queue_claim"),
        tb("processing_jobs"),
        ["status", "queue", "available_at"],
    )
    op.create_table(
        tb("job_workers"),
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
    op.create_index(ix("idx_job_workers_heartbeat"), tb("job_workers"), ["last_heartbeat_at"])
    op.create_index(ix("idx_job_workers_stopped"), tb("job_workers"), ["stopped_at"])
    op.create_index(ix("idx_processing_jobs_worker_claim"), tb("processing_jobs"), ["status", "locked_by"])
    op.create_table(
        tb("job_attempts"),
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
    op.create_index(ix("idx_job_attempts_job"), tb("job_attempts"), ["job_id", "claimed_at"])
    op.create_index(ix("idx_job_attempts_worker"), tb("job_attempts"), ["worker_id", "claimed_at"])
    op.create_index(ix("idx_job_attempts_event"), tb("job_attempts"), ["event_id"])
    op.create_index(ix("idx_job_attempts_claimed"), tb("job_attempts"), ["claimed_at"])
    op.create_index(ix("idx_job_attempts_finished"), tb("job_attempts"), ["finished_at"])


def downgrade() -> None:
    op.drop_index(ix("idx_job_attempts_finished"), table_name=tb("job_attempts"))
    op.drop_index(ix("idx_job_attempts_claimed"), table_name=tb("job_attempts"))
    op.drop_index(ix("idx_job_attempts_event"), table_name=tb("job_attempts"))
    op.drop_index(ix("idx_job_attempts_worker"), table_name=tb("job_attempts"))
    op.drop_index(ix("idx_job_attempts_job"), table_name=tb("job_attempts"))
    op.drop_table(tb("job_attempts"))
    op.drop_index(ix("idx_processing_jobs_worker_claim"), table_name=tb("processing_jobs"))
    op.drop_index(ix("idx_job_workers_stopped"), table_name=tb("job_workers"))
    op.drop_index(ix("idx_job_workers_heartbeat"), table_name=tb("job_workers"))
    op.drop_table(tb("job_workers"))
    op.drop_index(ix("idx_processing_jobs_queue_claim"), table_name=tb("processing_jobs"))
    op.drop_column(tb("processing_jobs"), "queue")
