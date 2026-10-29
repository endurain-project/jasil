"""Audit-trail models — the append-only log, its catalog, its checkpoints and the instance id."""

from datetime import date, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, CheckConstraint, Date, DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import false, func

from jasil._core.limits import (
    MAX_AUDIT_ACTION_ID_LENGTH,
    MAX_AUDIT_ID_LENGTH,
    MAX_AUDIT_NAME_LENGTH,
    MAX_IP_ADDRESS_LENGTH,
    MAX_REQUEST_ID_LENGTH,
    MAX_USER_AGENT_LENGTH,
)
from jasil.events import MAX_EVENT_ID_LENGTH
from jasil.orm import get_active_base

# Binds to the host-owned declarative base at map_models() time.
Base = get_active_base()


class AuditLog(Base):
    """
    One row per audited action. Append-only.

    On PostgreSQL the migration range-partitions the table by ``occurred_at`` (one partition per day)
    and installs triggers that reject ``UPDATE``, ``DELETE`` and ``TRUNCATE``; retention drops whole
    partitions. Elsewhere it is a plain table. ``create_all`` builds the plain form on every dialect,
    so a PostgreSQL deployment must run the migrations to get the protections.

    Attributes:
        id: UUIDv7, time-ordered; part of the primary key.
        occurred_at: When the action was staged; part of the primary key and the partition key.
        action_id: The catalog action id.
        category: Copied from the catalog so the row stands alone.
        severity: Copied from the catalog so the row stands alone.
        outcome: ``success``, ``failure`` or ``denied``.
        actor_type: ``user``, ``api_key``, ``system`` or ``anonymous``.
        actor_id: Who acted.
        credential_type: How the actor authenticated.
        credential_id: The credential's id, never its value.
        on_behalf_of_id: Who the actor acted for, when different.
        target_type: The kind of thing acted on.
        target_id: The thing acted on.
        target_owner_id: Who owns the thing acted on.
        request_id: The correlation id of the request.
        event_id: The domain event being handled, if any.
        source: ``api``, ``worker``, ``cli`` or ``migration``.
        ip_address: The client address.
        user_agent: The client's ``User-Agent``, clipped.
        details: The action's validated details payload: ids and changed field names only.
        details_version: The ``SCHEMA_VERSION`` of the model ``details`` was validated against.
        instance_id: The deployment's id from ``jasil_instance``.
        app_name: The host application's name.
        app_version: The host application's version.
    """

    __tablename__ = "jasil_audit_log"
    __table_args__ = (
        CheckConstraint("outcome IN ('success', 'failure', 'denied')", name="ck_jasil_audit_log_outcome"),
        Index("idx_jasil_audit_log_occurred", "occurred_at"),
        Index("idx_jasil_audit_log_action", "action_id", "occurred_at"),
        Index("idx_jasil_audit_log_actor", "actor_id", "occurred_at"),
        Index("idx_jasil_audit_log_target", "target_type", "target_id"),
        Index("idx_jasil_audit_log_request", "request_id"),
    )

    id: Mapped[str] = mapped_column(
        String(MAX_EVENT_ID_LENGTH), primary_key=True, comment="UUIDv7 (time-ordered); part of the primary key"
    )
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        autoincrement=False,
        server_default=func.now(),
        comment="When the action was staged; partition key",
    )
    action_id: Mapped[str] = mapped_column(
        String(MAX_AUDIT_ACTION_ID_LENGTH), nullable=False, comment="Catalog action id, <module>.<resource>.<verb>"
    )
    category: Mapped[str] = mapped_column(
        String(MAX_AUDIT_NAME_LENGTH), nullable=False, comment="Copied from the catalog"
    )
    severity: Mapped[str] = mapped_column(
        String(MAX_AUDIT_NAME_LENGTH), nullable=False, comment="Copied from the catalog"
    )
    outcome: Mapped[str] = mapped_column(String(10), nullable=False, comment="success | failure | denied")
    actor_type: Mapped[str] = mapped_column(String(20), nullable=False, comment="user | api_key | system | anonymous")
    actor_id: Mapped[str | None] = mapped_column(String(MAX_AUDIT_ID_LENGTH), nullable=True, comment="Who acted")
    credential_type: Mapped[str | None] = mapped_column(
        String(MAX_AUDIT_NAME_LENGTH), nullable=True, comment="How the actor authenticated"
    )
    credential_id: Mapped[str | None] = mapped_column(
        String(MAX_AUDIT_ID_LENGTH), nullable=True, comment="The credential's id, never its value"
    )
    on_behalf_of_id: Mapped[str | None] = mapped_column(
        String(MAX_AUDIT_ID_LENGTH), nullable=True, comment="Who the actor acted for, when different"
    )
    target_type: Mapped[str | None] = mapped_column(
        String(MAX_AUDIT_NAME_LENGTH), nullable=True, comment="Kind of thing acted on"
    )
    target_id: Mapped[str | None] = mapped_column(
        String(MAX_AUDIT_ID_LENGTH), nullable=True, comment="The thing acted on"
    )
    target_owner_id: Mapped[str | None] = mapped_column(
        String(MAX_AUDIT_ID_LENGTH), nullable=True, comment="Who owns the thing acted on"
    )
    request_id: Mapped[str | None] = mapped_column(
        String(MAX_REQUEST_ID_LENGTH), nullable=True, comment="Correlation id of the request"
    )
    event_id: Mapped[str | None] = mapped_column(
        String(MAX_EVENT_ID_LENGTH), nullable=True, comment="Domain event being handled, if any"
    )
    source: Mapped[str] = mapped_column(String(20), nullable=False, comment="api | worker | cli | migration")
    ip_address: Mapped[str | None] = mapped_column(
        String(MAX_IP_ADDRESS_LENGTH), nullable=True, comment="Client address"
    )
    user_agent: Mapped[str | None] = mapped_column(
        String(MAX_USER_AGENT_LENGTH), nullable=True, comment="Client User-Agent, clipped"
    )
    details: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=True,
        comment="Validated details payload: ids and changed field names only",
    )
    details_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1", comment="SCHEMA_VERSION of the details model"
    )
    instance_id: Mapped[str | None] = mapped_column(
        String(MAX_EVENT_ID_LENGTH), nullable=True, comment="Deployment id from jasil_instance"
    )
    app_name: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Host application name")
    app_version: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Host application version")


class AuditActionRecord(Base):
    """
    One row per catalog action, so a reader can describe rows without importing the host's code.

    Synchronized by the host at startup: new actions are inserted, changed ones updated, and actions
    no longer registered are marked ``deprecated``. Rows are never deleted.

    Attributes:
        id: The action id.
        category: The action's category.
        severity: The action's severity.
        description_key: Translation key of the description.
        description_default: Description used when the key has no translation.
        deprecated: The action is no longer emitted.
        first_seen_at: When the action first appeared in a synchronization.
        last_seen_at: The last synchronization that saw it registered.
    """

    __tablename__ = "jasil_audit_actions"

    id: Mapped[str] = mapped_column(String(MAX_AUDIT_ACTION_ID_LENGTH), primary_key=True, comment="Action id")
    category: Mapped[str] = mapped_column(String(MAX_AUDIT_NAME_LENGTH), nullable=False, comment="Category")
    severity: Mapped[str] = mapped_column(String(MAX_AUDIT_NAME_LENGTH), nullable=False, comment="Severity")
    description_key: Mapped[str] = mapped_column(String(200), nullable=False, comment="Translation key")
    description_default: Mapped[str] = mapped_column(String(500), nullable=False, comment="Untranslated description")
    deprecated: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false(), comment="No longer emitted"
    )
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), comment="First synchronization"
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), comment="Last synchronization that saw it"
    )


class AuditCheckpoint(Base):
    """
    A sealed tamper-evidence window: the hash of its rows, chained to the previous window's.

    Attributes:
        window_start: Start of the window, inclusive; the primary key.
        window_end: End of the window, exclusive.
        row_count: How many audit rows the window held when it was sealed.
        hash: SHA-256 (hex) over the window's canonical rows followed by ``prev_hash``.
        prev_hash: The previous checkpoint's hash; ``None`` for the first.
        signature: Base64 Ed25519 signature over the window and hash; ``None`` when unsigned.
        key_id: Fingerprint of the signing key; ``None`` when unsigned.
        retention_days: ``audit.retention_days`` when the window was sealed; signed, it is what verification
            accepts as the age past which a window may have been pruned.
        legal_hold: ``audit.legal_hold`` when the window was sealed; a checkpoint sealed under a hold allows
            no prune.
        created_at: When the checkpoint was written.
    """

    __tablename__ = "jasil_audit_checkpoints"

    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, autoincrement=False, comment="Window start, inclusive"
    )
    window_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="Window end, exclusive"
    )
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, comment="Rows in the window when sealed")
    hash: Mapped[str] = mapped_column(String(64), nullable=False, comment="SHA-256 hex of the rows plus prev_hash")
    prev_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="Previous checkpoint's hash")
    signature: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="Base64 Ed25519 signature")
    key_id: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="Signing-key fingerprint")
    retention_days: Mapped[int] = mapped_column(Integer, nullable=False, comment="Retention when sealed")
    legal_hold: Mapped[bool] = mapped_column(Boolean, nullable=False, comment="Legal hold when sealed")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), comment="When the checkpoint was written"
    )


class Instance(Base):
    """
    The single row identifying this deployment, generated once on first use.

    Attributes:
        id: Always 1; the check constraint keeps the table to one row.
        instance_id: A random UUID, stamped on every audit row.
        created_at: When the id was generated.
        retention_days: The ``audit.retention_days`` last seen by maintenance, to detect a change.
        legal_hold: The ``audit.legal_hold`` last seen by maintenance, to detect a change.
        furthest_partition_date: The last day with a partition, as the latest maintenance run found it
            (PostgreSQL), so every replica's :func:`jasil.audit.get_audit_health` reports it.
        last_checkpoint_at: The end of the latest sealed window, as the latest maintenance run left it.
    """

    __tablename__ = "jasil_instance"
    __table_args__ = (CheckConstraint("id = 1", name="ck_jasil_instance_single"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False, comment="Always 1")
    instance_id: Mapped[str] = mapped_column(
        String(MAX_EVENT_ID_LENGTH), nullable=False, comment="Random deployment id"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), comment="When the id was generated"
    )
    retention_days: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Retention last seen")
    legal_hold: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="Legal hold last seen")
    furthest_partition_date: Mapped[date | None] = mapped_column(
        Date, nullable=True, comment="Last day with a partition"
    )
    last_checkpoint_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="End of the latest sealed window"
    )
