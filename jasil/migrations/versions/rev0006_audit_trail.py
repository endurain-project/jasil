"""Add the audit trail: ``jasil_audit_log``, its catalog, its checkpoints and ``jasil_instance``.

``jasil_audit_log`` is append-only. On PostgreSQL it is range-partitioned by ``occurred_at`` (one
partition per UTC day), triggers reject ``UPDATE``, ``DELETE`` and ``TRUNCATE``, and two ``SECURITY
DEFINER`` functions create the next partitions and drop expired ones without any row-by-row delete.
``jasil_audit_drop_partitions`` refuses a cutoff younger than the retention floor, which is baked into
the function from ``AuditSettings.retention_floor_days`` as it is when this revision runs; change it
afterwards with ``jasil.audit.retention_floor_sql``. On other databases the log is a plain table with the
same columns.

Needs PostgreSQL 13 or newer. Run it as the role that should own the table and the functions. Nothing
else reads or writes the new tables, so processes of the previous release are unaffected.

Revision ID: rev0006
Revises: rev0005
Create Date: 2026-10-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

from jasil.audit import _postgres as pg
from jasil.migrations._names import PREFIX
from jasil.settings import get_settings

revision: str = "rev0006"
down_revision: str | None = "rev0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOG = f"{PREFIX}audit_log"
_ACTIONS = f"{PREFIX}audit_actions"
_CHECKPOINTS = f"{PREFIX}audit_checkpoints"
_INSTANCE = f"{PREFIX}instance"

_JSON = sa.JSON().with_variant(JSONB(), "postgresql")


def upgrade() -> None:
    bind = op.get_bind()
    postgresql = bind.dialect.name == "postgresql"

    op.create_table(
        _LOG,
        sa.Column("id", sa.String(36), primary_key=True, comment="UUIDv7 (time-ordered); part of the primary key"),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            primary_key=True,
            autoincrement=False,
            server_default=sa.func.now(),
            comment="When the action was staged; partition key",
        ),
        sa.Column("action_id", sa.String(100), nullable=False, comment="Catalog action id, <module>.<resource>.<verb>"),
        sa.Column("category", sa.String(50), nullable=False, comment="Copied from the catalog"),
        sa.Column("severity", sa.String(50), nullable=False, comment="Copied from the catalog"),
        sa.Column("outcome", sa.String(10), nullable=False, comment="success | failure | denied"),
        sa.Column("actor_type", sa.String(20), nullable=False, comment="user | api_key | system | anonymous"),
        sa.Column("actor_id", sa.String(64), nullable=True, comment="Who acted"),
        sa.Column("credential_type", sa.String(50), nullable=True, comment="How the actor authenticated"),
        sa.Column("credential_id", sa.String(64), nullable=True, comment="The credential's id, never its value"),
        sa.Column("on_behalf_of_id", sa.String(64), nullable=True, comment="Who the actor acted for, when different"),
        sa.Column("target_type", sa.String(50), nullable=True, comment="Kind of thing acted on"),
        sa.Column("target_id", sa.String(64), nullable=True, comment="The thing acted on"),
        sa.Column("target_owner_id", sa.String(64), nullable=True, comment="Who owns the thing acted on"),
        sa.Column("request_id", sa.String(100), nullable=True, comment="Correlation id of the request"),
        sa.Column("event_id", sa.String(36), nullable=True, comment="Domain event being handled, if any"),
        sa.Column("source", sa.String(20), nullable=False, comment="api | worker | cli | migration"),
        sa.Column("ip_address", sa.String(45), nullable=True, comment="Client address"),
        sa.Column("user_agent", sa.String(512), nullable=True, comment="Client User-Agent, clipped"),
        sa.Column(
            "details", _JSON, nullable=True, comment="Validated details payload: ids and changed field names only"
        ),
        sa.Column(
            "details_version",
            sa.Integer,
            nullable=False,
            server_default="1",
            comment="SCHEMA_VERSION of the details model",
        ),
        sa.Column("instance_id", sa.String(36), nullable=True, comment="Deployment id from jasil_instance"),
        sa.Column("app_name", sa.String(100), nullable=True, comment="Host application name"),
        sa.Column("app_version", sa.String(50), nullable=True, comment="Host application version"),
        sa.CheckConstraint("outcome IN ('success', 'failure', 'denied')", name=f"ck_{_LOG}_outcome"),
        postgresql_partition_by="RANGE (occurred_at)",
    )
    op.create_index(f"idx_{_LOG}_occurred", _LOG, ["occurred_at"])
    op.create_index(f"idx_{_LOG}_action", _LOG, ["action_id", "occurred_at"])
    op.create_index(f"idx_{_LOG}_actor", _LOG, ["actor_id", "occurred_at"])
    op.create_index(f"idx_{_LOG}_target", _LOG, ["target_type", "target_id"])
    op.create_index(f"idx_{_LOG}_request", _LOG, ["request_id"])

    op.create_table(
        _ACTIONS,
        sa.Column("id", sa.String(100), primary_key=True, comment="Action id"),
        sa.Column("category", sa.String(50), nullable=False, comment="Category"),
        sa.Column("severity", sa.String(50), nullable=False, comment="Severity"),
        sa.Column("description_key", sa.String(200), nullable=False, comment="Translation key"),
        sa.Column("description_default", sa.String(500), nullable=False, comment="Untranslated description"),
        sa.Column("deprecated", sa.Boolean, nullable=False, server_default=sa.false(), comment="No longer emitted"),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="First synchronization",
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="Last synchronization that saw it",
        ),
    )
    op.create_table(
        _CHECKPOINTS,
        sa.Column(
            "window_start",
            sa.DateTime(timezone=True),
            primary_key=True,
            autoincrement=False,
            comment="Window start, inclusive",
        ),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False, comment="Window end, exclusive"),
        sa.Column("row_count", sa.Integer, nullable=False, comment="Rows in the window when sealed"),
        sa.Column("hash", sa.String(64), nullable=False, comment="SHA-256 hex of the rows plus prev_hash"),
        sa.Column("prev_hash", sa.String(64), nullable=True, comment="Previous checkpoint's hash"),
        sa.Column("signature", sa.String(128), nullable=True, comment="Base64 Ed25519 signature"),
        sa.Column("key_id", sa.String(32), nullable=True, comment="Signing-key fingerprint"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="When the checkpoint was written",
        ),
    )
    op.create_table(
        _INSTANCE,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False, comment="Always 1"),
        sa.Column("instance_id", sa.String(36), nullable=False, comment="Random deployment id"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="When the id was generated",
        ),
        sa.Column("retention_days", sa.Integer, nullable=True, comment="Retention last seen"),
        sa.Column("legal_hold", sa.Boolean, nullable=True, comment="Legal hold last seen"),
        sa.CheckConstraint("id = 1", name=f"ck_{_INSTANCE}_single"),
    )

    if postgresql:
        audit = get_settings().audit
        schema = bind.execute(sa.text("SELECT current_schema()")).scalar_one()
        for statement in pg.install_statements(schema, audit.retention_floor_days, audit.partition_days_ahead):
            op.execute(statement)


def downgrade() -> None:
    bind = op.get_bind()
    postgresql = bind.dialect.name == "postgresql"
    schema = bind.execute(sa.text("SELECT current_schema()")).scalar_one() if postgresql else None

    # Dropping the log drops its partitions and triggers with it.
    op.drop_table(_INSTANCE)
    op.drop_table(_CHECKPOINTS)
    op.drop_table(_ACTIONS)
    op.drop_table(_LOG)

    if schema is not None:
        for statement in pg.uninstall_statements(schema):
            op.execute(statement)
