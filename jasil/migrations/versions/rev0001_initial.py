"""Baseline: event_log, event_outbox and processing_jobs.

Revision ID: rev0001
Revises:
Create Date: 2026-08-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "rev0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# JSONB on PostgreSQL, plain JSON elsewhere — mirrors the model definitions so a
# SQLite test database and a production Postgres get the same logical schema.
_JSON = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "event_log",
        sa.Column("id", sa.String(36), primary_key=True, comment="Envelope event_id (UUIDv4); stable across retries"),
        sa.Column("event_type", sa.String(100), nullable=False, comment="Domain-event channel, e.g. order.created"),
        sa.Column(
            "event_source", sa.String(50), nullable=False, comment="Where the event originated, e.g. api:create_order"
        ),
        sa.Column("event_payload", _JSON, nullable=False, comment="Domain payload, passed through untouched"),
        sa.Column(
            "event_metadata",
            _JSON,
            nullable=True,
            comment="Correlation context (request_id, plus any host-defined keys)",
        ),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="published",
            comment="published | queued | processing | completed | failed | dead_letter",
        ),
        sa.Column(
            "handler_name",
            sa.String(500),
            nullable=True,
            comment="Subscriber(s) that processed the event, comma-separated",
        ),
        sa.Column("worker_id", sa.String(100), nullable=True, comment="Process/consumer that handled the event"),
        sa.Column(
            "error_message", sa.Text(), nullable=True, comment="Failure reason when status is failed/dead_letter"
        ),
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="Processing attempts so far; 0 on first publish",
        ),
        sa.Column("processing_time_ms", sa.Integer(), nullable=True, comment="Handler execution time in milliseconds"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="When the event was published",
        ),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), nullable=True, comment="When a consumer picked the event up"
        ),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When processing finished (success or failure)",
        ),
    )
    op.create_index("idx_event_log_type_status", "event_log", ["event_type", "status"])
    op.create_index("idx_event_log_created", "event_log", ["created_at"])

    op.create_table(
        "event_outbox",
        sa.Column("id", sa.String(36), primary_key=True, comment="Outbox row identifier (UUIDv4)"),
        sa.Column(
            "event_id", sa.String(36), nullable=False, comment="Envelope event_id carried onto the fanned-out jobs"
        ),
        sa.Column("event_type", sa.String(100), nullable=False, comment="Domain-event channel, e.g. order.created"),
        sa.Column("source", sa.String(50), nullable=False, comment="Where the event originated, e.g. api:create_order"),
        sa.Column("timestamp", sa.String(40), nullable=False, comment="Envelope ISO-8601 publish timestamp"),
        sa.Column("payload", _JSON, nullable=False, comment="Domain payload"),
        sa.Column(
            "schema_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
            comment="Version of the payload shape, so a consumer on a different build can upgrade or refuse it",
        ),
        sa.Column(
            "event_metadata",
            _JSON,
            nullable=True,
            comment="Correlation context (request_id, plus any host-defined keys)",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="When the event was written to the outbox",
        ),
        sa.Column(
            "relayed_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the relay fanned the event out; NULL while pending",
        ),
    )
    op.create_index("idx_event_outbox_relayed", "event_outbox", ["relayed_at", "created_at"])

    op.create_table(
        "processing_jobs",
        sa.Column("id", sa.String(36), primary_key=True, comment="Job identifier (UUIDv4)"),
        sa.Column(
            "event_id",
            sa.String(36),
            nullable=False,
            comment="Originating envelope event_id (correlation + dedup with subscriber_id)",
        ),
        sa.Column("event_type", sa.String(100), nullable=False, comment="Domain-event channel, e.g. order.created"),
        sa.Column(
            "subscriber_id",
            sa.String(200),
            nullable=False,
            comment="Durable subscriber this job runs, e.g. invoice.render",
        ),
        sa.Column(
            "source",
            sa.String(50),
            nullable=False,
            comment="Where the originating event came from, e.g. api:create_order",
        ),
        sa.Column("payload", _JSON, nullable=False, comment="Domain payload the subscriber consumes"),
        sa.Column(
            "schema_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
            comment="Version of the payload shape, carried from the originating envelope",
        ),
        sa.Column(
            "job_metadata",
            _JSON,
            nullable=True,
            comment="Correlation context (request_id, plus any host-defined keys)",
        ),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="pending",
            comment="pending | claimed | completed | dead_letter",
        ),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="Processing attempts so far; incremented when claimed",
        ),
        sa.Column(
            "max_attempts", sa.Integer(), nullable=False, comment="Attempt ceiling before the job is dead-lettered"
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Earliest instant the job may be claimed (backoff gate)",
        ),
        sa.Column("locked_by", sa.String(100), nullable=True, comment="Worker holding the current lease, when claimed"),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True, comment="When the current lease was taken"),
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the current lease expires (drives reaping)",
        ),
        sa.Column("last_error", sa.Text(), nullable=True, comment="Most recent failure reason (truncated for storage)"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="When the job was enqueued",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="When the job last changed state",
        ),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the job reached a terminal state (completed/dead_letter)",
        ),
        # The idempotent-consumer guarantee: a subscriber runs at most once per
        # event, enforced by the database rather than by the relay's logic.
        sa.UniqueConstraint("event_id", "subscriber_id", name="uq_processing_jobs_event_subscriber"),
    )
    op.create_index("idx_processing_jobs_claim", "processing_jobs", ["status", "available_at"])
    op.create_index("idx_processing_jobs_lease", "processing_jobs", ["status", "lease_expires_at"])

    # Postgres-only: a GIN index makes ``event_metadata @> '{...}'`` correlation
    # lookups usable on a large trail. Not declared on the model because SQLite
    # cannot build it.
    if op.get_bind().dialect.name == "postgresql":
        op.create_index(
            "idx_event_log_metadata_gin",
            "event_log",
            ["event_metadata"],
            postgresql_using="gin",
            postgresql_ops={"event_metadata": "jsonb_path_ops"},
        )


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.drop_index("idx_event_log_metadata_gin", table_name="event_log")
    op.drop_index("idx_processing_jobs_lease", table_name="processing_jobs")
    op.drop_index("idx_processing_jobs_claim", table_name="processing_jobs")
    op.drop_table("processing_jobs")
    op.drop_index("idx_event_outbox_relayed", table_name="event_outbox")
    op.drop_table("event_outbox")
    op.drop_index("idx_event_log_created", table_name="event_log")
    op.drop_index("idx_event_log_type_status", table_name="event_log")
    op.drop_table("event_log")
