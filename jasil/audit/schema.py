"""Pydantic schemas for the audit surface: the row as written and read, the query filters and results."""

from datetime import date, datetime
from enum import StrEnum
from typing import Any, TypedDict

from pydantic import BaseModel, ConfigDict, field_validator

from jasil._core.timestamps import as_utc

__all__ = [
    "ActionCount",
    "AuditActionInfo",
    "AuditEntry",
    "AuditFilters",
    "AuditHealth",
    "AuditPage",
    "AuditSummary",
    "VerificationReport",
    "VerificationStatus",
    "WindowVerification",
]


class AuditEntry(BaseModel):
    """
    One audit row, as staged and as read back.

    Enumerated columns are plain strings, so a reader newer or older than the writer still parses a
    value it does not know.

    Attributes:
        id: UUIDv7 of the row.
        occurred_at: When the action was staged, timezone-aware UTC.
        action_id: The catalog action id.
        category: The action's category.
        severity: The action's severity.
        outcome: ``success``, ``failure`` or ``denied``.
        actor_type: ``user``, ``api_key``, ``system`` or ``anonymous``.
        actor_id: Who acted.
        credential_type: How the actor authenticated.
        credential_id: The credential's id.
        on_behalf_of_id: Who the actor acted for.
        target_type: The kind of thing acted on.
        target_id: The thing acted on.
        target_owner_id: Who owns it.
        request_id: The correlation id of the request.
        event_id: The domain event being handled.
        source: ``api``, ``worker``, ``cli`` or ``migration``.
        ip_address: The client address.
        user_agent: The client's ``User-Agent``.
        details: The validated details payload.
        details_version: The details model's schema version.
        instance_id: The deployment id.
        app_name: The host application's name.
        app_version: The host application's version.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    occurred_at: datetime
    action_id: str
    category: str
    severity: str
    outcome: str
    actor_type: str
    actor_id: str | None = None
    credential_type: str | None = None
    credential_id: str | None = None
    on_behalf_of_id: str | None = None
    target_type: str | None = None
    target_id: str | None = None
    target_owner_id: str | None = None
    request_id: str | None = None
    event_id: str | None = None
    source: str
    ip_address: str | None = None
    user_agent: str | None = None
    details: dict[str, Any] | None = None
    details_version: int = 1
    instance_id: str | None = None
    app_name: str | None = None
    app_version: str | None = None

    @field_validator("occurred_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return as_utc(value)


class AuditFilters(BaseModel):
    """
    The filters :func:`jasil.audit.list_entries` accepts. Every one narrows; none is required.

    The time range is always bounded: ``to_ts`` defaults to now and ``from_ts`` to 24 hours before
    ``to_ts``, and :func:`jasil.audit.list_entries` refuses a range longer than 31 days, so the database
    can prune partitions.

    Attributes:
        action_ids: Match any of these action ids.
        categories: Match any of these categories.
        severities: Match any of these severities.
        outcomes: Match any of these outcomes.
        actor_id: Match this actor.
        actor_type: Match this kind of actor.
        target_type: Match this kind of target.
        target_id: Match this target.
        target_owner_id: Match targets owned by this principal.
        request_id: Match this request.
        event_id: Match this domain event.
        source: Match this source.
        from_ts: Earliest ``occurred_at``, inclusive.
        to_ts: Latest ``occurred_at``, inclusive.
    """

    action_ids: list[str] | None = None
    categories: list[str] | None = None
    severities: list[str] | None = None
    outcomes: list[str] | None = None
    actor_id: str | None = None
    actor_type: str | None = None
    target_type: str | None = None
    target_id: str | None = None
    target_owner_id: str | None = None
    request_id: str | None = None
    event_id: str | None = None
    source: str | None = None
    from_ts: datetime | None = None
    to_ts: datetime | None = None


class AuditPage(BaseModel):
    """
    One page of entries, newest first.

    Attributes:
        entries: The rows of this page.
        next_cursor: Pass it to the next call for the following page; ``None`` on the last page.
    """

    entries: list[AuditEntry]
    next_cursor: str | None = None


class ActionCount(BaseModel):
    """
    How often one action occurred.

    Attributes:
        action_id: The action id.
        count: Rows with that id in the window.
    """

    action_id: str
    count: int


class AuditSummary(BaseModel):
    """
    Counts over a look-back window.

    Attributes:
        window_hours: The look-back window.
        total: Rows in the window.
        by_category: Rows per category.
        by_outcome: Rows per outcome.
        by_severity: Rows per severity.
        top_actions: The most frequent actions, most frequent first.
    """

    window_hours: int
    total: int
    by_category: dict[str, int]
    by_outcome: dict[str, int]
    by_severity: dict[str, int]
    top_actions: list[ActionCount]


class AuditActionInfo(BaseModel):
    """
    One catalog row, as the ``jasil_audit_actions`` table holds it.

    Attributes:
        id: The action id.
        category: The action's category.
        severity: The action's severity.
        description_key: Translation key of the description.
        description_default: Untranslated description.
        deprecated: The action is no longer emitted.
        first_seen_at: When it first appeared in a synchronization.
        last_seen_at: The last synchronization that saw it registered.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    category: str
    severity: str
    description_key: str
    description_default: str
    deprecated: bool
    first_seen_at: datetime
    last_seen_at: datetime

    @field_validator("first_seen_at", "last_seen_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return as_utc(value)


class VerificationStatus(StrEnum):
    """The verdict on one checkpoint window."""

    OK = "ok"
    ROW_MISMATCH = "row_mismatch"
    MISSING_WINDOW = "missing_window"
    BROKEN_CHAIN = "broken_chain"
    BAD_SIGNATURE = "bad_signature"


class WindowVerification(BaseModel):
    """
    The result of verifying one checkpoint window.

    Attributes:
        window_start: Start of the window.
        window_end: End of the window.
        status: The verdict. When several things are wrong with one window the most serious is
            reported: ``row_mismatch``, then ``bad_signature``, then ``broken_chain``.
        pruned: The window's rows are gone where a checkpoint's signed retention allowed them to be pruned;
            only its chain link and signature could be checked.
        expected_rows: The row count recorded when the window was sealed (``None`` for a window with
            no checkpoint).
        actual_rows: The rows found now (``None`` when the window was pruned).
    """

    window_start: datetime
    window_end: datetime
    status: VerificationStatus
    pruned: bool = False
    expected_rows: int | None = None
    actual_rows: int | None = None


class VerificationReport(BaseModel):
    """
    The result of :func:`jasil.audit.verify_checkpoints`.

    Attributes:
        ok: Every window verified.
        signed: Signatures were checked (a public key was given).
        windows: One entry per window in the range, oldest first.
    """

    ok: bool
    signed: bool
    windows: list[WindowVerification]


class AuditHealth(TypedDict):
    """
    Health of the audit path, from :func:`jasil.audit.get_audit_health`.

    Attributes:
        write_failures: Detached writes that failed since this process started.
        last_failure_at: When the latest of them failed.
        furthest_partition_date: The last day with a partition, as the latest maintenance run on any
            replica recorded it (PostgreSQL only; ``None`` before the first run or elsewhere).
        last_checkpoint_at: The end of the latest sealed window, as the latest maintenance run on any
            replica recorded it.
    """

    write_failures: int
    last_failure_at: datetime | None
    furthest_partition_date: date | None
    last_checkpoint_at: datetime | None
