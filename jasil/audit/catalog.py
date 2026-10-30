"""The audit catalog: every action the trail can record, declared once.

An audit row names *what happened* with an action id (``<module>.<resource>.<verb>``, e.g.
``session.token.refresh``). The catalog is the registry of those ids. Declaring them up front is what
lets :func:`jasil.audit.stage` refuse a misspelt id, copy the category and severity onto the row so it
stands alone, validate ``details`` against the action's own model, and publish the list to the
``jasil_audit_actions`` table for a reader that cannot import the host's code.

Ids are stable forever: they are stored in rows that outlive the code. An action the host stops using
is marked ``deprecated``, never deleted or renamed.

The catalog is pure — it imports no model and no backend — so it is safe to import from anywhere.
"""

import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from jasil._core.limits import (
    MAX_AUDIT_ACTION_ID_LENGTH,
    MAX_AUDIT_DESCRIPTION_DEFAULT_LENGTH,
    MAX_AUDIT_DESCRIPTION_KEY_LENGTH,
    check_length,
)
from jasil._core.registry import ConfigSlot
from jasil.event_versioning import VersionedPayload

__all__ = [
    "ACTION_ID_PATTERN",
    "CATALOG_SYNC",
    "INTEGRITY_VIOLATION",
    "RETENTION_CHANGE",
    "RETENTION_PRUNE",
    "AuditAction",
    "AuditCatalog",
    "CatalogSyncDetails",
    "Category",
    "IntegrityViolationDetails",
    "NoDetails",
    "Outcome",
    "RetentionChangeDetails",
    "RetentionPruneDetails",
    "Severity",
    "UnknownAuditActionError",
    "get_catalog",
    "reset",
]

#: ``<module>.<resource>.<verb>``: three lower-case snake_case segments.
ACTION_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$")


class Category(StrEnum):
    """What kind of thing an action is, for filtering and reporting."""

    AUTHENTICATION = "authentication"
    CREDENTIAL = "credential"
    AUTHORIZATION = "authorization"
    IDENTITY = "identity"
    ADMIN = "admin"
    DATA_CHANGE = "data_change"
    DATA_TRANSFER = "data_transfer"
    INTEGRATION = "integration"
    SYSTEM = "system"


class Severity(StrEnum):
    """How much an investigator should care about an action."""

    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Outcome(StrEnum):
    """How the audited operation ended.

    ``denied`` is a request refused by authorization; ``failure`` is one that was allowed but did not
    succeed.
    """

    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"


class UnknownAuditActionError(LookupError):
    """An action id, or action definition, that the catalog does not hold."""


class NoDetails(VersionedPayload):
    """The ``details`` model of an action that records nothing beyond the row's own columns."""


@dataclass(frozen=True)
class AuditAction:
    """One declared auditable action.

    Attributes:
        id: ``<module>.<resource>.<verb>``; stable forever.
        category: What kind of action it is.
        severity: How much it matters.
        description_key: Translation key for the human description, owned by the host's i18n.
        description_default: The description used when the key has no translation.
        details_model: Versioned model the row's ``details`` are validated against. It must carry ids
            and changed field names only, never a secret or a value that would be one. Keys it does not
            declare are dropped; ``extra="allow"`` is refused.
        deprecated: The action is no longer emitted. It stays in the catalog because rows with its id
            still exist.
    """

    id: str
    category: Category
    severity: Severity
    description_key: str
    description_default: str
    details_model: type[VersionedPayload] = NoDetails
    deprecated: bool = False

    def __post_init__(self) -> None:
        """Reject an id or model that could not be stored or validated against.

        Raises:
            ValueError: When ``id`` does not match :data:`ACTION_ID_PATTERN` or is too long, a
                description is empty or longer than its column, or ``details_model`` is not a ``VersionedPayload`` or allows extra keys.
        """
        if not ACTION_ID_PATTERN.fullmatch(self.id) or len(self.id) > MAX_AUDIT_ACTION_ID_LENGTH:
            raise ValueError(
                f"audit action id {self.id!r} must be <module>.<resource>.<verb> in lower-case snake_case "
                f"(pattern {ACTION_ID_PATTERN.pattern}, at most {MAX_AUDIT_ACTION_ID_LENGTH} characters)"
            )
        if not self.description_key or not self.description_default:
            raise ValueError(f"audit action {self.id!r} needs a description_key and a description_default")
        # The columns are narrower than the strings a host can pass; PostgreSQL would refuse the row at sync_catalog().
        check_length(self.description_key, field="description_key", limit=MAX_AUDIT_DESCRIPTION_KEY_LENGTH)
        check_length(self.description_default, field="description_default", limit=MAX_AUDIT_DESCRIPTION_DEFAULT_LENGTH)
        if not (isinstance(self.details_model, type) and issubclass(self.details_model, VersionedPayload)):
            raise ValueError(f"audit action {self.id!r}: details_model must subclass VersionedPayload")
        if self.details_model.model_config.get("extra") == "allow":
            raise ValueError(
                f"audit action {self.id!r}: details_model must not set extra='allow', "
                "which would store keys the model does not declare"
            )


class AuditCatalog:
    """The registry of audit actions, keyed by id."""

    def __init__(self) -> None:
        self._actions: dict[str, AuditAction] = {}

    def register(self, action: AuditAction) -> AuditAction:
        """Add ``action`` to the catalog.

        Args:
            action: The action to declare.

        Returns:
            ``action``, so a module can declare and bind it in one statement.

        Raises:
            ValueError: When an action with the same id is already registered.
        """
        if action.id in self._actions:
            raise ValueError(f"audit action {action.id!r} is already registered")
        self._actions[action.id] = action
        return action

    def get(self, action_id: str) -> AuditAction:
        """Return the action registered under ``action_id``.

        Args:
            action_id: The action id.

        Returns:
            The registered action.

        Raises:
            UnknownAuditActionError: When no such action is registered.
        """
        try:
            return self._actions[action_id]
        except KeyError:
            raise UnknownAuditActionError(f"audit action {action_id!r} is not registered") from None

    def resolve(self, action: AuditAction | str) -> AuditAction:
        """Return the registered definition of an action given by id or by value.

        Args:
            action: An id, or an :class:`AuditAction` the caller holds.

        Returns:
            The registered action.

        Raises:
            UnknownAuditActionError: When the id is not registered, or the value differs from the
                registered definition with that id.
        """
        if isinstance(action, str):
            return self.get(action)
        registered = self.get(action.id)
        if registered != action:
            raise UnknownAuditActionError(f"audit action {action.id!r} differs from the registered definition")
        return registered

    def __contains__(self, action_id: object) -> bool:
        return action_id in self._actions

    def __iter__(self) -> Iterator[AuditAction]:
        """Iterate the actions in id order."""
        return iter(sorted(self._actions.values(), key=lambda action: action.id))

    def __len__(self) -> int:
        return len(self._actions)


class RetentionPruneDetails(VersionedPayload):
    """Details of ``audit.retention.prune``, written by ``jasil_audit_drop_partitions``.

    Attributes:
        cutoff: Partitions wholly older than this instant were dropped.
        partitions_dropped: How many daily partitions were dropped.
        first_day: The oldest day dropped.
        last_day: The newest day dropped.
    """

    cutoff: datetime
    partitions_dropped: int
    first_day: date | None = None
    last_day: date | None = None


class RetentionChangeDetails(VersionedPayload):
    """Details of ``audit.retention.change``: the retention settings before and after."""

    retention_days_before: int
    retention_days_after: int
    legal_hold_before: bool
    legal_hold_after: bool


class IntegrityViolationDetails(VersionedPayload):
    """Details of ``audit.integrity.violation``: which checkpoint window failed verification and how.

    Attributes:
        window_start: Start of the failing window.
        window_end: End of the failing window.
        status: The verification status reported for it.
    """

    window_start: datetime
    window_end: datetime
    status: str


class CatalogSyncDetails(VersionedPayload):
    """Details of ``audit.catalog.sync``: how many catalog rows changed."""

    added: int
    updated: int
    deprecated: int


RETENTION_PRUNE = AuditAction(
    id="audit.retention.prune",
    category=Category.SYSTEM,
    severity=Severity.MEDIUM,
    description_key="audit.retention.prune",
    description_default="Audit partitions past retention were dropped",
    details_model=RetentionPruneDetails,
)
RETENTION_CHANGE = AuditAction(
    id="audit.retention.change",
    category=Category.ADMIN,
    severity=Severity.HIGH,
    description_key="audit.retention.change",
    description_default="Audit retention settings changed",
    details_model=RetentionChangeDetails,
)
INTEGRITY_VIOLATION = AuditAction(
    id="audit.integrity.violation",
    category=Category.SYSTEM,
    severity=Severity.HIGH,
    description_key="audit.integrity.violation",
    description_default="Audit trail integrity check failed",
    details_model=IntegrityViolationDetails,
)
CATALOG_SYNC = AuditAction(
    id="audit.catalog.sync",
    category=Category.SYSTEM,
    severity=Severity.INFO,
    description_key="audit.catalog.sync",
    description_default="Audit action catalog was synchronized",
    details_model=CatalogSyncDetails,
)


def _with_builtins() -> AuditCatalog:
    catalog = AuditCatalog()
    for action in (RETENTION_PRUNE, RETENTION_CHANGE, INTEGRITY_VIOLATION, CATALOG_SYNC):
        catalog.register(action)
    return catalog


_catalog: ConfigSlot[AuditCatalog] = ConfigSlot(default_factory=_with_builtins)


def get_catalog() -> AuditCatalog:
    """Return the process-wide catalog, which starts with the actions the substrate itself emits.

    The host registers its own actions on it once at startup, before the first
    :func:`~jasil.audit.stage`.
    """
    return _catalog.get()


def reset() -> None:
    """Restore the catalog to the built-in actions only. For tests."""
    _catalog.reset()
