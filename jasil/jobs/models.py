"""Durable job model — the ``processing_jobs`` source-of-truth table."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from jasil._core.limits import MAX_WORKER_ID_LENGTH
from jasil.events import MAX_EVENT_ID_LENGTH, MAX_EVENT_TYPE_LENGTH, MAX_SOURCE_LENGTH
from jasil.jobs.registry import MAX_SUBSCRIBER_ID_LENGTH
from jasil.orm import get_active_base

# Binds to the host-owned declarative base at map_models() time.
Base = get_active_base()


class ProcessingJob(Base):
    """
    One row per unit of durable derived work: a subscriber reacting to an event.

    The database is the source of truth. A worker claims a ``pending`` row (taking
    a time-bounded lease), runs the subscriber, and either marks it ``completed``
    or — on failure — reschedules it back to ``pending`` with a later
    ``available_at`` (exponential backoff) until ``attempts`` reaches
    ``max_attempts``, at which point it becomes ``dead_letter``. A reaper returns
    rows whose lease expired (a crashed worker) to ``pending``.

    Attributes:
        id: Job identifier (UUIDv4 string).
        event_id: The originating envelope ``event_id`` — correlation and, with
            ``subscriber_id``, the idempotent-consumer dedup key.
        event_type: The domain-event channel, e.g. ``order.created``.
        subscriber_id: The durable subscriber this job runs, e.g.
            ``invoice.render``.
        source: Where the originating event came from, e.g. ``api:create_order``.
        payload: The domain payload the subscriber consumes.
        schema_version: The payload-shape version, carried from the envelope so a
            worker on a different build can upgrade or refuse it.
        job_metadata: Correlation context (request_id, plus any host-defined keys).
        status: Lifecycle state: pending, claimed, completed, or dead_letter.
        attempts: Processing attempts so far; incremented when the job is claimed.
        max_attempts: Attempt ceiling before the job is dead-lettered.
        available_at: Earliest instant the job may be claimed (backoff gate).
        locked_by: The worker holding the current lease, when claimed.
        locked_at: When the current lease was taken.
        lease_expires_at: When the current lease expires (drives reaping).
        last_error: The most recent failure reason (truncated for storage).
        created_at: When the job was enqueued.
        updated_at: When the job last changed state.
        completed_at: When the job reached a terminal state (completed/dead_letter).
    """

    __tablename__ = "processing_jobs"
    __table_args__ = (
        UniqueConstraint("event_id", "subscriber_id", name="uq_processing_jobs_event_subscriber"),
        # The claim query filters ``status = 'pending' AND available_at <= now``
        # and orders by ``available_at``; this composite index serves both.
        Index("idx_processing_jobs_claim", "status", "available_at"),
        # The reaper scans claimed rows whose lease has expired.
        Index("idx_processing_jobs_lease", "status", "lease_expires_at"),
    )

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        comment="Job identifier (UUIDv4)",
    )
    event_id: Mapped[str] = mapped_column(
        String(MAX_EVENT_ID_LENGTH),
        nullable=False,
        comment="Originating envelope event_id (correlation + dedup with subscriber_id)",
    )
    event_type: Mapped[str] = mapped_column(
        String(MAX_EVENT_TYPE_LENGTH),
        nullable=False,
        comment="Domain-event channel, e.g. order.created",
    )
    subscriber_id: Mapped[str] = mapped_column(
        String(MAX_SUBSCRIBER_ID_LENGTH),
        nullable=False,
        comment="Durable subscriber this job runs, e.g. invoice.render",
    )
    source: Mapped[str] = mapped_column(
        String(MAX_SOURCE_LENGTH),
        nullable=False,
        comment="Where the originating event came from, e.g. api:create_order",
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=False,
        comment="Domain payload the subscriber consumes",
    )
    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="1",
        default=1,
        comment="Version of the payload shape, carried from the originating envelope",
    )
    job_metadata: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=True,
        comment="Correlation context (request_id, plus any host-defined keys)",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="pending",
        server_default="pending",
        comment="pending | claimed | completed | dead_letter",
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
        comment="Processing attempts so far; incremented when claimed",
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Attempt ceiling before the job is dead-lettered",
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Earliest instant the job may be claimed (backoff gate)",
    )
    locked_by: Mapped[str | None] = mapped_column(
        String(MAX_WORKER_ID_LENGTH),
        nullable=True,
        comment="Worker holding the current lease, when claimed",
    )
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the current lease was taken",
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the current lease expires (drives reaping)",
    )
    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Most recent failure reason (truncated for storage)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When the job was enqueued",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When the job last changed state",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the job reached a terminal state (completed/dead_letter)",
    )


class EventOutbox(Base):
    """
    One row per published event, staged for durable per-subscriber delivery.

    A producer writes the event here as it persists its domain change so the
    event can be delivered durably. Delivery is best-effort at the seam (the
    ingestion path commits per-CRUD, so the outbox write is not atomic with the
    domain change); the domain row is the source of truth and each subscriber's
    reconciliation net recovers anything a crash drops before this row is written.
    The relay reads unrelayed rows and fans each out into one ``processing_jobs``
    row per durable subscriber, then stamps ``relayed_at``. Because the fan-out is
    idempotent (jobs dedup on ``event_id + subscriber_id``), re-relaying a row is
    harmless.

    Attributes:
        id: Outbox row identifier (UUIDv4).
        event_id: The envelope event_id carried onto the fanned-out jobs.
        event_type: The domain-event channel, e.g. ``order.created``.
        source: Where the event originated, e.g. ``api:create_order``.
        timestamp: The envelope's ISO-8601 publish timestamp.
        payload: The domain payload.
        schema_version: The payload-shape version, so a relay/worker on a
            different build can upgrade or refuse the payload rather than
            silently misreading it.
        event_metadata: Correlation context (request_id, plus any host-defined keys).
        created_at: When the event was written to the outbox.
        relayed_at: When the relay fanned the event out; ``NULL`` while pending.
    """

    __tablename__ = "event_outbox"
    __table_args__ = (
        # The relay scans unrelayed rows oldest-first.
        Index("idx_event_outbox_relayed", "relayed_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        comment="Outbox row identifier (UUIDv4)",
    )
    event_id: Mapped[str] = mapped_column(
        String(MAX_EVENT_ID_LENGTH),
        nullable=False,
        comment="Envelope event_id carried onto the fanned-out jobs",
    )
    event_type: Mapped[str] = mapped_column(
        String(MAX_EVENT_TYPE_LENGTH),
        nullable=False,
        comment="Domain-event channel, e.g. order.created",
    )
    source: Mapped[str] = mapped_column(
        String(MAX_SOURCE_LENGTH),
        nullable=False,
        comment="Where the event originated, e.g. api:create_order",
    )
    timestamp: Mapped[str] = mapped_column(
        String(40),
        nullable=False,
        comment="Envelope ISO-8601 publish timestamp",
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=False,
        comment="Domain payload",
    )
    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="1",
        default=1,
        comment="Version of the payload shape, so a consumer on a different build can upgrade or refuse it",
    )
    event_metadata: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"),
        nullable=True,
        comment="Correlation context (request_id, plus any host-defined keys)",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="When the event was written to the outbox",
    )
    relayed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the relay fanned the event out; NULL while pending",
    )
