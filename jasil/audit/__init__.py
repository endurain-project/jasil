"""The audit trail: a complete, append-only record of who did what, kept in the caller's own transaction.

Separate from the event log and from domain events, which are asynchronous, at-least-once, miss denials
and may be pruned: an audit row is written synchronously on the caller's SQLAlchemy session, so the
change and its row commit or roll back together, identically in-process and with the outbox, Redis and
workers. No bus, outbox or Redis is involved.

* Declare actions in the catalog (:func:`get_catalog`, :class:`AuditAction`).
* Set who is acting with :func:`set_context` (or :func:`audit_context` in a worker or CLI command).
* Write with :func:`stage` inside a transaction, or :func:`record_detached` where there is none.
* Run :func:`run_audit_maintenance` on a schedule; read with :func:`list_entries` and friends.

Importing this package is safe at any point: the models are imported on first use, so it works before
:func:`jasil.orm.map_models`. Everything exported here is public API; the underscore modules and
``jasil._core`` are not.
"""

import jasil.audit.catalog as _catalog
import jasil.audit.context as _context
import jasil.audit.service as _service
from jasil.audit.catalog import (
    CATALOG_SYNC,
    INTEGRITY_VIOLATION,
    RETENTION_CHANGE,
    RETENTION_PRUNE,
    AuditAction,
    AuditCatalog,
    CatalogSyncDetails,
    Category,
    IntegrityViolationDetails,
    NoDetails,
    Outcome,
    RetentionChangeDetails,
    RetentionPruneDetails,
    Severity,
    UnknownAuditActionError,
    get_catalog,
)
from jasil.audit.context import (
    ActorType,
    AuditContext,
    Source,
    audit_context,
    configure_provider,
    get_context,
    set_context,
)
from jasil.audit.ddl import grants_sql, retention_floor_sql
from jasil.audit.maintenance import (
    CatalogSyncResult,
    run_audit_maintenance,
    schedule_audit_maintenance,
    sync_catalog,
)
from jasil.audit.query import get_entry, list_catalog, list_entries, summary, verify_checkpoints
from jasil.audit.schema import (
    ActionCount,
    AuditActionInfo,
    AuditEntry,
    AuditFilters,
    AuditHealth,
    AuditPage,
    AuditSummary,
    VerificationReport,
    VerificationStatus,
    WindowVerification,
)
from jasil.audit.service import (
    AuditDetailsError,
    AuditRecord,
    get_audit_health,
    record_detached,
    stage,
    stage_many,
)

__all__ = [
    "CATALOG_SYNC",
    "INTEGRITY_VIOLATION",
    "RETENTION_CHANGE",
    "RETENTION_PRUNE",
    "ActionCount",
    "ActorType",
    "AuditAction",
    "AuditActionInfo",
    "AuditCatalog",
    "AuditContext",
    "AuditDetailsError",
    "AuditEntry",
    "AuditFilters",
    "AuditHealth",
    "AuditPage",
    "AuditRecord",
    "AuditSummary",
    "CatalogSyncDetails",
    "CatalogSyncResult",
    "Category",
    "IntegrityViolationDetails",
    "NoDetails",
    "Outcome",
    "RetentionChangeDetails",
    "RetentionPruneDetails",
    "Severity",
    "Source",
    "UnknownAuditActionError",
    "VerificationReport",
    "VerificationStatus",
    "WindowVerification",
    "audit_context",
    "configure_provider",
    "get_audit_health",
    "get_catalog",
    "get_context",
    "get_entry",
    "grants_sql",
    "list_catalog",
    "list_entries",
    "record_detached",
    "reset",
    "reset_context",
    "retention_floor_sql",
    "run_audit_maintenance",
    "schedule_audit_maintenance",
    "set_context",
    "stage",
    "stage_many",
    "summary",
    "sync_catalog",
    "verify_checkpoints",
]

#: Clear the ambient context and its provider. See :func:`jasil.audit.context.reset`.
reset_context = _context.reset


def reset() -> None:
    """Restore the audit module to its start state: built-in catalog only, no context, no observers.

    Also forgets the cached instance id and the health counters. For tests; production configures once
    at startup and never resets.
    """
    _catalog.reset()
    _context.reset()
    _service.reset()
