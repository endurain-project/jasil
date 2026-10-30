"""The audit write paths: ``stage`` joins the caller's transaction, ``record_detached`` stands alone.

Both read the ambient :mod:`~jasil.audit.context` and :mod:`jasil.correlation`, copy the category and
severity from the catalog, validate ``details`` against the action's model and cap its size. They differ
in what happens when the row cannot be written:

* :func:`stage` **fails closed.** Validation and storage errors propagate, so the caller's transaction
  rolls back: a change that cannot be audited does not happen.
* :func:`record_detached` **fails open.** It exists for the cases where there is no transaction to join —
  a failed login, a denied request — and there an audit hiccup must not turn into an outage. It logs at
  error level, counts the failure (see :func:`get_audit_health`) and returns.

``details`` must carry ids and changed field names only: never a secret, a token, a password or a value
that would be one. The size cap is a guard against a payload that grew into a data dump, not a
substitute for that rule.

Importing this module is safe before :func:`jasil.orm.map_models`; the model is imported on first write.
"""

import json
import logging
import math
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from sqlalchemy import Connection
from sqlalchemy.orm import Session

import jasil.correlation as correlation
import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil._core.limits import (
    MAX_AUDIT_ID_LENGTH,
    MAX_AUDIT_NAME_LENGTH,
    MAX_REQUEST_ID_LENGTH,
    check_length,
    fit_length,
)
from jasil._core.timestamps import as_utc
from jasil._core.uuid7 import uuid7
from jasil.audit.catalog import AuditAction, Outcome, get_catalog
from jasil.audit.context import get_context
from jasil.audit.schema import AuditEntry, AuditHealth
from jasil.settings import AuditSettings, get_settings

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

__all__ = [
    "AuditDetailsError",
    "AuditRecord",
    "get_audit_health",
    "record_detached",
    "reset",
    "stage",
    "stage_many",
]


class AuditDetailsError(ValueError):
    """The ``details`` of a row are over the size cap, do not serialize, or hold a NUL or a non-finite number."""


@dataclass(frozen=True)
class AuditRecord:
    """
    One row to stage with :func:`stage_many`.

    Attributes:
        action: The action, or its id.
        outcome: How the operation ended.
        target_type: The kind of thing acted on.
        target_id: The thing acted on.
        target_owner_id: Who owns it.
        details: A dict, or an instance of the action's ``details_model``.
    """

    action: AuditAction | str
    outcome: Outcome = Outcome.SUCCESS
    target_type: str | None = None
    target_id: str | None = None
    target_owner_id: str | None = None
    details: Any = None


# Callbacks that see every entry the moment it is validated, whether or not it is persisted. This is how
# jasil.testing captures rows when ``audit.enabled`` is false or there is no database.
_observers: list[Callable[[AuditEntry], None]] = []

_instance_id: str | None = None

_health_lock = threading.Lock()
_write_failures = 0
_last_failure_at: datetime | None = None


def stage(
    db: Session,
    action: AuditAction | str,
    *,
    outcome: Outcome = Outcome.SUCCESS,
    target_type: str | None = None,
    target_id: str | None = None,
    target_owner_id: str | None = None,
    details: Any = None,
) -> None:
    """
    Add an audit row to the caller's session, in the caller's transaction.

    The row is added with ``Session.add`` and nothing else: it is flushed whenever the session next
    flushes (autoflush, an explicit flush, or commit) and becomes durable when the caller commits, so
    the audited change and its row commit or roll back together. This never commits and never flushes
    on its own. The first call in a process reads the deployment's instance id on ``db`` (without
    flushing it); only a deployment's very first write, before ``jasil_instance`` holds its row, creates
    it on a short session of its own on the same engine (call :func:`jasil.audit.sync_catalog` at startup
    to do that outside any request).

    Args:
        db: The caller's session.
        action: The action, or its id; it must be registered in the catalog.
        outcome: How the operation ended.
        target_type: The kind of thing acted on.
        target_id: The thing acted on.
        target_owner_id: Who owns it.
        details: A dict, or an instance of the action's ``details_model``. Ids and changed field names
            only.

    Raises:
        UnknownAuditActionError: When the action is not registered.
        AuditDetailsError: When ``details`` is over ``audit.max_details_bytes``, or holds a NUL character or a
            number that is not finite.
        ValueError: When an id is too long or holds a NUL character.
        pydantic.ValidationError: When ``details`` does not match the action's model.
        sqlalchemy.exc.SQLAlchemyError: When the row cannot be stored. Not caught: the caller's
            transaction must roll back.
    """
    _stage(db, AuditRecord(action, outcome, target_type, target_id, target_owner_id, details))


def stage_many(db: Session, records: Iterable[AuditRecord]) -> None:
    """
    Add several audit rows to the caller's session; see :func:`stage`.

    Every record is validated before any is added, so a bad one stages nothing.

    Args:
        db: The caller's session.
        records: The rows to stage.

    Raises:
        UnknownAuditActionError: When an action is not registered.
        AuditDetailsError: When a ``details`` payload is over the cap, or holds a NUL character or a number
            that is not finite.
        ValueError: When an id is too long or holds a NUL character.
        pydantic.ValidationError: When a ``details`` payload does not match its model.
        sqlalchemy.exc.SQLAlchemyError: When a row cannot be stored.
    """
    settings = get_settings().audit
    entries = [_prepare(record, settings) for record in records]
    for entry in entries:
        _notify(entry)
    for entry in entries:
        _add(db, entry, settings)


def record_detached(
    action: AuditAction | str,
    *,
    outcome: Outcome,
    target_type: str | None = None,
    target_id: str | None = None,
    target_owner_id: str | None = None,
    details: Any = None,
) -> None:
    """
    Write an audit row in its own short transaction, for when the caller has none to join.

    Use it where the operation's own transaction rolls back or does not exist: a failed login, a denied
    request, a refresh-token reuse, a CLI command. It never raises: any failure, including an invalid
    action or ``details``, is logged at error level and counted in :func:`get_audit_health`.

    Args:
        action: The action, or its id.
        outcome: How the operation ended.
        target_type: The kind of thing acted on.
        target_id: The thing acted on.
        target_owner_id: Who owns it.
        details: A dict, or an instance of the action's ``details_model``. Ids and changed field names
            only.
    """
    record = AuditRecord(action, outcome, target_type, target_id, target_owner_id, details)
    try:
        settings = get_settings().audit
        entry = _prepare(record, settings)
        _notify(entry)
        if not settings.enabled:
            return
        with jasil_orm.get_sessionmaker()() as db:
            _add(db, entry, settings)
            db.commit()
    except ValidationError as error:
        # Never the input: ``details`` is exactly what must not reach a log.
        _note_failure()
        logger.error(
            "Audit record %r was not written: invalid details %s",
            getattr(action, "id", action),
            error.errors(include_input=False, include_context=False, include_url=False),
        )
    except Exception as error:
        _note_failure()
        # Never the error text: a SQLAlchemy error carries the statement parameters, which are the row
        # (client address, user agent, details), and the engine's hide_parameters is the host's choice.
        cause = getattr(error, "orig", None)
        logger.error(
            "Audit record %r was not written: %s%s",
            getattr(action, "id", action),
            type(error).__name__,
            f" ({type(cause).__name__})" if cause is not None else "",
        )


def _note_failure() -> None:
    global _write_failures, _last_failure_at
    with _health_lock:
        _write_failures += 1
        _last_failure_at = now()


def get_audit_health() -> AuditHealth:
    """
    Return the health of the audit path.

    ``write_failures`` and ``last_failure_at`` count this process's detached writes. The partition and
    checkpoint figures are what the latest :func:`jasil.audit.run_audit_maintenance` recorded in
    ``jasil_instance``, whichever replica ran it, so every replica reports the same ones. Reads one row.

    Returns:
        ``write_failures``, ``last_failure_at``, ``furthest_partition_date`` and ``last_checkpoint_at``.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: When ``jasil_instance`` cannot be read.
        RuntimeError: When ``jasil.orm.configure_sessionmaker`` has not run.
    """
    from sqlalchemy import select

    from jasil.audit.models import Instance

    with jasil_orm.get_sessionmaker()() as db:
        recorded = db.execute(
            select(Instance.furthest_partition_date, Instance.last_checkpoint_at).where(Instance.id == 1)
        ).first()
    furthest_partition_date, last_checkpoint_at = recorded if recorded is not None else (None, None)
    with _health_lock:
        return {
            "write_failures": _write_failures,
            "last_failure_at": _last_failure_at,
            "furthest_partition_date": furthest_partition_date,
            "last_checkpoint_at": as_utc(last_checkpoint_at) if last_checkpoint_at is not None else None,
        }


def reset() -> None:
    """Forget the cached instance id, the health counters and every observer. For tests."""
    global _instance_id, _write_failures, _last_failure_at
    _instance_id = None
    _observers.clear()
    with _health_lock:
        _write_failures = 0
        _last_failure_at = None


def note_maintenance(
    db: Session, *, furthest_partition_date: date | None = None, last_checkpoint_at: datetime | None = None
) -> None:
    """Record on ``jasil_instance`` what a maintenance run observed, for :func:`get_audit_health`.

    The caller commits, so the figure lands with the work it describes. The row must exist
    (:func:`ensure_instance_id`).

    Args:
        db: The maintenance step's session.
        furthest_partition_date: The last day with a partition, when the run learned it.
        last_checkpoint_at: The end of the latest sealed window, when the run learned it.
    """
    from sqlalchemy import update

    from jasil.audit.models import Instance

    values = {
        name: value
        for name, value in (
            ("furthest_partition_date", furthest_partition_date),
            ("last_checkpoint_at", last_checkpoint_at),
        )
        if value is not None
    }
    if values:
        db.execute(update(Instance).where(Instance.id == 1).values(**values))


def add_observer(observer: Callable[[AuditEntry], None]) -> Callable[[], None]:
    """Call ``observer`` with every entry staged or recorded from now on, persisted or not.

    Args:
        observer: Receives the validated entry.

    Returns:
        A callable that removes the observer.
    """
    _observers.append(observer)

    def remove() -> None:
        if observer in _observers:
            _observers.remove(observer)

    return remove


def engine_of(db: Session) -> "Engine":
    """Return the engine ``db`` is bound to, also when it is bound to a connection."""
    bind = db.get_bind()
    return bind.engine if isinstance(bind, Connection) else bind


def ensure_instance_id(engine: "Engine") -> str:
    """
    Return the deployment's instance id, generating and storing it on first use.

    Runs on a short session of its own, so the id is durable whatever the caller's transaction does. The
    id is cached afterwards: later calls do no I/O.

    Args:
        engine: The engine of the database holding ``jasil_instance``.

    Returns:
        The instance id.
    """
    global _instance_id
    if _instance_id is not None:
        return _instance_id
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError

    from jasil.audit.models import Instance

    with Session(bind=engine) as session:
        found = session.scalar(select(Instance.instance_id).where(Instance.id == 1))
        if found is None:
            try:
                session.add(Instance(id=1, instance_id=str(uuid7())))
                session.commit()
            except IntegrityError:
                session.rollback()  # another replica won the race; read its id
            found = session.scalar(select(Instance.instance_id).where(Instance.id == 1))
    if found is None:  # pragma: no cover - the row was just inserted or read
        raise RuntimeError("jasil_instance holds no row")
    _instance_id = found
    return found


def _stage(db: Session, record: AuditRecord) -> None:
    settings = get_settings().audit
    entry = _prepare(record, settings)
    _notify(entry)
    _add(db, entry, settings)


def now() -> datetime:
    """Return the platform clock's instant when a platform is active, else the system's."""
    if platform_runtime.is_platform_active():
        return platform_runtime.get_active_platform().clock.now()
    return datetime.now(UTC)


def _unstorable(value: Any) -> str | None:
    """Say why ``value`` cannot be stored as JSON on every dialect, or return ``None``.

    SQLite accepts a NUL character and a NaN or Infinity, but PostgreSQL rejects both in ``jsonb``, and a
    rejected insert fails the request or silently drops a detached row. Refusing them here makes the
    outcome the same wherever the host runs.
    """
    if isinstance(value, str):
        return "contain a NUL character" if "\x00" in value else None
    if isinstance(value, float):
        return None if math.isfinite(value) else "contain a number that is not finite"
    if isinstance(value, dict):
        return next((reason for item in (*value.keys(), *value.values()) if (reason := _unstorable(item))), None)
    if isinstance(value, list | tuple):
        return next((reason for item in value if (reason := _unstorable(item))), None)
    return None


def _prepare(record: AuditRecord, settings: AuditSettings) -> AuditEntry:
    """Validate ``record`` and merge the ambient context."""
    action = get_catalog().resolve(record.action)
    for name, value, limit in (
        ("target_type", record.target_type, MAX_AUDIT_NAME_LENGTH),
        ("target_id", record.target_id, MAX_AUDIT_ID_LENGTH),
        ("target_owner_id", record.target_owner_id, MAX_AUDIT_ID_LENGTH),
    ):
        if value is not None:
            check_length(value, field=name, limit=limit)
            if "\x00" in value:
                raise ValueError(f"{name} must not contain a NUL character")
    payload = action.details_model.model_validate({} if record.details is None else record.details)
    details = payload.model_dump(mode="json") or None
    if details is not None:
        unstorable = _unstorable(details)
        if unstorable is not None:
            raise AuditDetailsError(f"details of {action.id!r} {unstorable}")
        size = len(json.dumps(details, separators=(",", ":"), ensure_ascii=False).encode())
        if size > settings.max_details_bytes:
            raise AuditDetailsError(
                f"details of {action.id!r} are {size} bytes; audit.max_details_bytes is {settings.max_details_bytes}"
            )
    context = get_context()
    entry = AuditEntry(
        id=str(uuid7()),
        occurred_at=now(),
        action_id=action.id,
        category=action.category.value,
        severity=action.severity.value,
        outcome=Outcome(record.outcome).value,
        actor_type=context.actor_type.value,
        actor_id=context.actor_id,
        credential_type=context.credential_type,
        credential_id=context.credential_id,
        on_behalf_of_id=context.on_behalf_of_id,
        target_type=record.target_type,
        target_id=record.target_id,
        target_owner_id=record.target_owner_id,
        request_id=fit_length(correlation.get_correlation_id(), MAX_REQUEST_ID_LENGTH),
        event_id=context.event_id,
        source=context.source.value,
        ip_address=context.ip_address,
        user_agent=context.user_agent,
        details=details,
        details_version=action.details_model.SCHEMA_VERSION,
        app_name=settings.app_name,
        app_version=settings.app_version,
    )
    return entry


def _notify(entry: AuditEntry) -> None:
    """Tell the observers about ``entry``, once it and every entry staged with it have validated."""
    for observer in tuple(_observers):
        observer(entry)


def _instance_id_of(db: Session) -> str:
    """Return the instance id, read on the caller's own session when the row already exists.

    Reading it there (without flushing what the caller has pending) keeps a staged write on the caller's
    connection; only a deployment's very first write, before the row exists, creates it on a session of
    its own (:func:`ensure_instance_id`), so the id is durable whatever the caller's transaction does.
    """
    global _instance_id
    if _instance_id is None:
        from sqlalchemy import select

        from jasil.audit.models import Instance

        with db.no_autoflush:
            found = db.scalar(select(Instance.instance_id).where(Instance.id == 1))
        if found is not None:
            _instance_id = found
            return found
    return ensure_instance_id(engine_of(db))


def _add(db: Session, entry: AuditEntry, settings: AuditSettings) -> None:
    """Add ``entry`` to ``db`` unless auditing is disabled."""
    if not settings.enabled:
        return
    from jasil.audit.models import AuditLog

    instance_id = _instance_id_of(db)
    db.add(AuditLog(**entry.model_copy(update={"instance_id": instance_id}).model_dump()))
