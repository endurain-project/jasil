"""The single publish seam every producer goes through.

One tiny facade so no producer ever assembles an :class:`~jasil.events.Event`
or touches the active platform directly. Centralising publishing here means the
transactional outbox is a change to *this* function alone,
not to every call site.

It resolves the active platform, stamps the ambient request id for correlation,
and mints the envelope. Delivery then takes one of two routes:

* **Durable (outbox):** when durable jobs are enabled, the caller supplies its DB
  session, and the event type has registered durable subscribers, the event is
  written to the ``event_outbox`` — the relay later fans it out into retryable
  per-subscriber jobs. The event is also recorded ``queued`` in ``event_log`` so
  the observability dashboard reflects durable events too (execution detail then
  lives in the Jobs dashboard).
* **Best-effort (bus):** otherwise the event is dispatched through the event bus
  (inline in ``local``, via Redis Streams in ``distributed``), which records the
  full lifecycle itself.

**Delivery guarantee.** The producer's domain row is the source of truth; this
publish is *best-effort* from the producer's perspective — failures are logged and
swallowed so publishing never breaks the producer's own work. Plain :func:`publish`
commits the outbox row on its own, so a crash between the domain commit and the
outbox write can drop an event; :func:`publish_committing` and
:func:`publish_on_commit` stage it in the caller's transaction instead. Every
subscriber should still have a reconciliation net — a backfill or sweeper that
re-derives missed work. "Durable" means *retryable once written*, not *never
lost*. Channel names and payload shape stay owned by the publishing domain; this
layer only knows the generic envelope.
"""

import copy
import logging
import re
from collections.abc import Callable, Sequence
from datetime import datetime
from functools import partial
from typing import Any

from sqlalchemy.orm import Session

import jasil.correlation as correlation
import jasil.jobs.registry as jobs_registry
import jasil.runtime as platform_runtime
from jasil._core.after_commit import run_after_commit
from jasil._core.limits import check_length
from jasil._core.sessions import commit_or_flush
from jasil.events import (
    INITIAL_SCHEMA_VERSION,
    MAX_EVENT_ID_LENGTH,
    MAX_EVENT_TYPE_LENGTH,
    MAX_SOURCE_LENGTH,
    META_REQUEST_ID,
    Event,
    new_event,
)
from jasil.providers import EventRecorder
from jasil.settings import get_settings

logger = logging.getLogger(__name__)

# An explicit event id is lowercase ASCII, like a queue name (``jasil.jobs.registry``), so equality means the same
# on SQLite, PostgreSQL and MySQL: MySQL's default ``utf8mb4_0900_ai_ci`` collation folds case and accents and
# MariaDB's ``_general_ci`` ignores trailing spaces, either of which would make two different ids one row. The id
# also reaches log lines and may be returned in URLs, so it carries no control characters.
_EVENT_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*\Z")


def _mint(
    event_type: str,
    payload: dict,
    source: str,
    metadata: dict | None,
    schema_version: int = INITIAL_SCHEMA_VERSION,
    event_id: str | None = None,
) -> Event:
    """Build the event envelope on the platform clock, stamping the ambient request id for correlation.

    Raises:
        ValueError: When ``event_id`` is not a valid explicit id (see :func:`_check_event_id`).
    """
    _check_event_id(event_id)
    merged: dict = {}
    request_id = correlation.get_correlation_id()
    if request_id:
        merged[META_REQUEST_ID] = request_id
    if metadata:
        merged.update(metadata)
    return new_event(
        event_type,
        payload,
        source=source,
        metadata=merged,
        schema_version=schema_version,
        event_id=event_id,
        now=platform_runtime.get_active_platform().clock.now(),
    )


def _stage_in_outbox(
    recorder: EventRecorder | None,
    event: Event,
    *,
    db: Any,
    now: datetime,
    commit: bool,
) -> None:
    """Write one event to the outbox, recording it ``queued`` in the event_log when the row is new.

    The event_log row is terminal for a durable event: the bus records its own
    lifecycle, the outbox path does not go through the bus, and per-subscriber
    execution is tracked in ``processing_jobs`` instead. Without it the
    observability dashboard would go dark the moment durable jobs were enabled.
    """
    # Imported here rather than at module scope: the outbox model binds to the
    # host's declarative base, so a top-level import would make ``import
    # jasil.publisher`` fail until ``jasil.orm.map_models`` had run — and this is
    # the module every producer imports.
    import jasil.jobs.outbox as jobs_outbox

    # The row goes in first: a reused id writes nothing, and then records nothing either. Both are flushed before
    # the commit so they land together, or stay together in the caller's transaction.
    if jobs_outbox.add_to_outbox(event, now=now, db=db, commit=False) is not None and recorder is not None:
        # Prefer the session-aware method so the queued row rides the caller's
        # transaction; third-party recorders that lack it keep the legacy
        # own-session write.
        record_in = getattr(recorder, "record_queued_in", None)
        if record_in is not None:
            record_in(event, db)
        else:
            recorder.record_queued(event)
    commit_or_flush(db, commit)


def _publish_on_bus(
    event_type: str,
    payload: dict,
    source: str,
    metadata_of: Callable[[dict], dict | None],
    schema_version: int,
    event_id_of: Callable[[dict], str | None],
) -> str | None:
    """Build, mint and dispatch one event on the bus, logging and swallowing any failure.

    Returns:
        The event's id, or ``None`` when minting or dispatching it failed.
    """
    try:
        event = _mint(event_type, payload, source, metadata_of(payload), schema_version, event_id_of(payload))
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event_type, _describe(err))
        return None
    return event.event_id if _dispatch_event(event) else None


def _describe(error: Exception) -> str:
    """Name ``error``, and the driver error it wraps, by type only.

    Never the text or the traceback: a SQLAlchemy error carries the statement's parameters, which are the
    event's payload and metadata, and the engine's ``hide_parameters`` is the host's choice.
    """
    cause = getattr(error, "orig", None)
    return type(error).__name__ + (f" ({type(cause).__name__})" if cause is not None else "")


def _no_metadata(_payload: dict) -> dict | None:
    return None


def _no_event_id(_payload: dict) -> str | None:
    return None


def _check_event_id(event_id: str | None) -> None:
    """Refuse an explicit event id that is not portable, before any route is taken.

    ``None`` is fine: one is generated. The empty string is refused rather than replaced by a generated id, as
    ``new_event`` would.

    Raises:
        ValueError: When ``event_id`` is not a string, is empty, is longer than its column, or does not start
            with a lowercase letter or digit and contain only a-z, 0-9, '.', '_' or '-'.
    """
    if event_id is None:
        return
    if not isinstance(event_id, str):
        raise ValueError("event_id must be a string")
    if event_id == "":
        raise ValueError("event_id must not be empty; pass None to have one generated")
    check_length(event_id, field="event_id", limit=MAX_EVENT_ID_LENGTH)
    if _EVENT_ID_PATTERN.fullmatch(event_id) is None:
        raise ValueError(
            "event_id must start with a lowercase letter or digit and contain only a-z, 0-9, '.', '_', or '-'"
        )


def _check_identifiers(event_type: str, source: str) -> None:
    """Refuse an ``event_type`` or ``source`` too long for its column, before any route is taken.

    The durable route stages before committing and so raises; checked here, the best-effort route raises the
    same way instead of logging the failure after the commit.

    Raises:
        ValueError: When either is longer than its column.
    """
    check_length(event_type, field="event_type", limit=MAX_EVENT_TYPE_LENGTH)
    check_length(source, field="source", limit=MAX_SOURCE_LENGTH)


def _dispatch_event(event: Event) -> bool:
    """Dispatch an already-minted event on the bus, logging and swallowing failure.

    Returns:
        True when the bus accepted the event, False when dispatching failed.
    """
    try:
        platform_runtime.get_active_platform().events.publish(event)
        logger.debug("Dispatched %s (%s) on the event bus", event.event_type, event.event_id)
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event.event_type, _describe(err))
        return False
    return True


def _dispatch_after_commit(event: Event) -> None:
    """Dispatch an event once its session commits, where there is no caller left to report the outcome to."""
    _dispatch_event(event)


def _stage_batch(
    event_type: str,
    payloads: Sequence[dict],
    source: str,
    metadata_of: Callable[[dict], dict | None],
    schema_version: int,
    db: Any,
    event_id_of: Callable[[dict], str | None],
) -> list[str]:
    """Stage one outbox row per payload on ``db`` without committing.

    A failure is logged and re-raised so the caller's unit of work rolls back as
    a whole — no partial domain change, no orphaned event.

    Returns:
        The event id of each payload, in order; a reused id is returned like a new one.
    """
    try:
        platform = platform_runtime.get_active_platform()
        now = platform.clock.now()
        event_ids = []
        for payload in payloads:
            event = _mint(event_type, payload, source, metadata_of(payload), schema_version, event_id_of(payload))
            _stage_in_outbox(platform.recorder, event, db=db, now=now, commit=False)
            event_ids.append(event.event_id)
    except Exception as err:
        logger.error(
            "Failed to stage %d %s event(s) in the domain transaction: %s",
            len(payloads),
            event_type,
            _describe(err),
        )
        raise
    return event_ids


def publish(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    db: Any = None,
    schema_version: int = INITIAL_SCHEMA_VERSION,
    event_id: str | None = None,
) -> str | None:
    """Publish a domain event through the active platform, best-effort.

    A reused id: on the durable route the outbox keeps one row per ``event_id``, so publishing an id that is
    already there writes nothing, records nothing and returns the id, as long as the ``event_type`` and payload
    are the same. A different ``event_type`` or payload under the same id is an error, which this function logs
    and swallows like any other failure; so is a payload that is not JSON-native (dict, list, str, int, float,
    bool or ``None``), which cannot be compared with the stored one.

    Args:
        event_type: The domain-owned channel, e.g. ``order.created``.
        payload: Domain data for the event (homogeneous per ``event_type``).
        source: Origin label, e.g. ``api:create_order``.
        metadata: Optional correlation context; merged with the ambient request
            id when one is set on the current request.
        db: The producer's SQLAlchemy session. When provided and durable jobs are
            enabled for this event type, the event is written to the outbox using
            this session (durable delivery); otherwise it is ignored.
        schema_version: Payload schema version.
        event_id: Id for the event, so a producer can answer with it or publish the same event idempotently
            (see "A reused id" above); ``None`` generates a UUID. An explicit id starts with a lowercase letter or
            digit, contains only a-z, 0-9, '.', '_' or '-', and is at most ``MAX_EVENT_ID_LENGTH`` characters,
            which a UUID string satisfies; anything else is refused.

    Returns:
        The event's id, or ``None`` when publishing failed. A failure is logged and swallowed so a publish never
        breaks the producer: an invalid ``event_id``, a reused one with a different ``event_type`` or payload,
        or a bus that could not take the event. A handler that raises is logged by the bus and does not make
        this ``None``, because the event was dispatched. The domain row remains the source of truth and each subscriber's
        reconciliation net recovers anything missed.
    """
    try:
        platform = platform_runtime.get_active_platform()
        event = _mint(event_type, payload, source, metadata, schema_version, event_id)
        if db is not None and _durable_delivery_enabled(event_type):
            _stage_in_outbox(platform.recorder, event, db=db, now=platform.clock.now(), commit=True)
            # Which route an event took is the first thing anyone asks when it
            # appears not to have been processed, and nothing else records it.
            logger.debug("Staged %s (%s) in the outbox for durable delivery", event_type, event.event_id)
            return event.event_id
        return event.event_id if _dispatch_event(event) else None
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event_type, _describe(err))
        return None


def publish_committing(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    db: Any,
    commit: Callable[[], None],
    schema_version: int = INITIAL_SCHEMA_VERSION,
    event_id: str | None = None,
) -> str | None:
    """Publish a domain event atomically around the caller's domain commit.

    Unlike :func:`publish` (which the caller invokes *after* it has already
    committed its own change), this variant owns the commit ordering so durable
    delivery can be made atomic with the domain write. ``commit`` is a zero-arg
    callable that commits the caller's unit of work.

    * **Durable delivery enabled** (durable jobs on + a durable subscriber for
      ``event_type``): the outbox row is staged on ``db`` **without** committing,
      then ``commit()`` flushes the domain rows and the outbox row in one
      transaction — so the event can never be lost relative to the change that
      produced it. A staging failure propagates (the caller's transaction is left
      uncommitted for rollback), so the whole unit of work is all-or-nothing.
    * **Otherwise** (best-effort bus path): ``commit()`` runs first so the domain
      row — the source of truth — is durable regardless, then the event is
      dispatched on the bus and any dispatch failure is logged and swallowed.

    Args:
        event_type: The domain-owned channel, e.g. ``order.created``.
        payload: Domain data for the event.
        source: Origin label, e.g. ``api:create_order``.
        metadata: Optional correlation context; merged with the ambient request id.
        db: The producer's SQLAlchemy session (holds the uncommitted domain change).
        commit: Zero-arg callable that commits the caller's unit of work.
        schema_version: Payload schema version.
        event_id: Id for the event, so a producer can answer with it or publish the same event idempotently
            (a reused id is described under Returns); ``None`` generates a UUID. An explicit id starts with a
            lowercase letter or digit, contains only a-z, 0-9, '.', '_' or '-', and is at most
            ``MAX_EVENT_ID_LENGTH`` characters, which a UUID string satisfies; anything else is refused.

    Returns:
        The event's id, or ``None`` when the event could not be published on the best-effort route. On the
        durable route a failure raises instead, so the id is always returned. A reused id (the outbox keeps one
        row per ``event_id``) writes nothing, records nothing and returns the id, provided the ``event_type`` and
        payload are the same.

    Raises:
        ValueError: When ``event_type`` or ``source`` is longer than its column, or ``event_id`` is invalid (see
            ``event_id`` above), on either route and before anything is committed or staged; or, on the durable
            route, when ``event_id`` is already in the outbox with a different ``event_type`` or payload.
        TypeError: On the durable route, when ``event_id`` is already in the outbox and the payload is not
            JSON-native (dict, list, str, int, float, bool or ``None``), so it cannot be compared with the
            stored one.
    """
    _check_event_id(event_id)
    return publish_many_committing(
        event_type,
        [payload],
        source=source,
        metadata_for=lambda _payload: metadata,
        event_id_for=lambda _payload: event_id,
        db=db,
        commit=commit,
        schema_version=schema_version,
    )[0]


def _durable_delivery_enabled(event_type: str) -> bool:
    """Whether an event type should be delivered durably (outbox -> jobs).

    True only when durable jobs are switched on and at least one durable
    subscriber is registered for the event type; otherwise the best-effort bus
    path is used.
    """
    return get_settings().jobs.enabled and bool(jobs_registry.registry.subscribers_for(event_type))


def publish_many_committing(
    event_type: str,
    payloads: Sequence[dict],
    *,
    source: str,
    metadata_for: Callable[[dict], dict | None] | None = None,
    event_id_for: Callable[[dict], str | None] | None = None,
    db: Any,
    commit: Callable[[], None],
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> list[str | None]:
    """Publish many same-type events atomically around one domain commit.

    The batch counterpart of :func:`publish_committing`, for producers that
    remove or create *many* rows in a single unit of work (bulk deletes) and must
    emit one event per affected row without committing once per event. All outbox
    rows are staged on ``db`` uncommitted and land in the caller's transaction, so
    the domain change and every event commit together or not at all.

    Args:
        event_type: The domain-owned channel shared by every event in the batch.
        payloads: One payload per event. An empty sequence still runs ``commit``
            so the caller's unit of work is committed exactly once either way.
        source: Origin label, e.g. ``api:bulk_delete``.
        metadata_for: Optional per-payload correlation metadata builder.
        event_id_for: Optional per-payload event id builder, returning ``None`` for a generated id. An id that
            is not a valid ``event_id`` (see :func:`publish_committing`) is refused: it raises on the durable
            route and skips only that event, logged, on the best-effort route. A builder that raises behaves
            like a raising ``metadata_for``.
        db: The producer's SQLAlchemy session (holds the uncommitted change).
        commit: Zero-arg callable that commits the caller's unit of work.
        schema_version: Payload schema version.

    Returns:
        One entry per payload, in payload order: the event's id, or ``None`` for an event that could not be
        published on the best-effort route (its failure is logged). On the durable route a failure raises
        instead, so every entry is an id; a reused id (see :func:`publish_committing`) is returned like a new
        one. An empty batch returns ``[]``.

    Raises:
        ValueError: When ``event_type`` or ``source`` is longer than its column, on either route and before
            anything is committed or staged; or, on the durable route, when an id is invalid or already in the
            outbox with a different ``event_type`` or payload.
        TypeError: On the durable route, when an id is already in the outbox and its payload is not JSON-native
            (dict, list, str, int, float, bool or ``None``), so it cannot be compared with the stored one.
    """
    _check_identifiers(event_type, source)
    metadata_of = metadata_for if metadata_for is not None else _no_metadata
    event_id_of = event_id_for if event_id_for is not None else _no_event_id
    if db is not None and _durable_delivery_enabled(event_type):
        # Atomic path: stage every outbox row inside the caller's transaction.
        event_ids: list[str | None] = list(
            _stage_batch(event_type, payloads, source, metadata_of, schema_version, db, event_id_of)
        )
        commit()
        return event_ids
    # Best-effort path: the domain change is the source of truth, so commit it
    # first, then dispatch each event on the bus (swallowing failures — the
    # subscriber's reconciliation net recovers anything dropped).
    commit()
    return [
        _publish_on_bus(event_type, payload, source, metadata_of, schema_version, event_id_of) for payload in payloads
    ]


def publish_on_commit(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    db: Any,
    schema_version: int = INITIAL_SCHEMA_VERSION,
    event_id: str | None = None,
) -> str | None:
    """Publish a domain event tied to ``db``'s commit, without owning the commit.

    The caller stages the event and whoever owns the transaction commits whenever
    it likes, so the publishing function composes with other writes in one atomic
    unit of work.

    * **Durable delivery enabled** (durable jobs on + a durable subscriber for
      ``event_type``): the outbox row is staged on ``db`` immediately, uncommitted,
      and commits (or rolls back) atomically with the caller's domain rows. A
      staging failure propagates to the caller.
    * **Otherwise** (best-effort bus path): the event is minted now (so the ambient
      request id is captured) and held on the session. It is dispatched on the bus
      only after ``db``'s **outermost** transaction commits, in publish order.
      An event published inside a SAVEPOINT that is rolled back, or in a
      transaction that is rolled back or closed, is discarded and never
      dispatched. Dispatch failures are logged and swallowed.

    "Outermost" is the session's outermost transaction. If the session joined an
    external transaction (bound to a connection that already has one open, in any
    ``join_transaction_mode``), its ``commit()`` is that session's outermost commit
    and the events are dispatched then, even though the enclosing transaction may
    still roll back.

    Publishing starts the session's transaction if none is active. Sessions are
    synchronous ``sqlalchemy.orm.Session`` instances; each carries its own pending
    events, so concurrent sessions never see each other's.

    Args:
        event_type: The domain-owned channel, e.g. ``order.created``.
        payload: Domain data for the event.
        source: Origin label, e.g. ``api:create_order``.
        metadata: Optional correlation context; merged with the ambient request id.
        db: The producer's SQLAlchemy session (holds the uncommitted change).
        schema_version: Payload schema version.
        event_id: Id for the event, so a producer can answer with it or publish the same event idempotently
            (a reused id is described under Returns); ``None`` generates a UUID. An explicit id starts with a
            lowercase letter or digit, contains only a-z, 0-9, '.', '_' or '-', and is at most
            ``MAX_EVENT_ID_LENGTH`` characters, which a UUID string satisfies; anything else is refused.

    Returns:
        The event's id, or ``None`` when the best-effort route could not mint the event (the failure is
        logged). On that route the id is returned when the event is minted, before the commit that dispatches
        it. On the durable route a failure raises instead, so the id is always returned; a reused id (the outbox
        keeps one row per ``event_id``) writes nothing, records nothing and returns the id, provided the
        ``event_type`` and payload are the same.

    Raises:
        ValueError: When ``event_type`` or ``source`` is longer than its column, or ``event_id`` is invalid (see
            ``event_id`` above), on either route and before anything is staged; or, on the durable route, when
            ``event_id`` is already in the outbox with a different ``event_type`` or payload.
        TypeError: On the durable route, when ``event_id`` is already in the outbox and the payload is not
            JSON-native (dict, list, str, int, float, bool or ``None``), so it cannot be compared with the
            stored one.
    """
    _check_event_id(event_id)
    return publish_many_on_commit(
        event_type,
        [payload],
        source=source,
        metadata_for=lambda _payload: metadata,
        event_id_for=lambda _payload: event_id,
        db=db,
        schema_version=schema_version,
    )[0]


def publish_many_on_commit(
    event_type: str,
    payloads: Sequence[dict],
    *,
    source: str,
    metadata_for: Callable[[dict], dict | None] | None = None,
    event_id_for: Callable[[dict], str | None] | None = None,
    db: Any,
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> list[str | None]:
    """Publish many same-type events tied to ``db``'s commit.

    The batch counterpart of :func:`publish_on_commit`, with the same delivery
    routing and the same guarantee: best-effort events dispatch, in order, only
    after the session's outermost transaction commits. An empty sequence is a
    no-op; it neither stages anything nor starts a transaction.

    Args:
        event_type: The domain-owned channel shared by every event in the batch.
        payloads: One payload per event.
        source: Origin label, e.g. ``api:bulk_delete``.
        metadata_for: Optional per-payload correlation metadata builder.
        event_id_for: Optional per-payload event id builder, returning ``None`` for a generated id. An id that
            is not a valid ``event_id`` (see :func:`publish_committing`) is refused: it raises on the durable
            route and skips only that event, logged, on the best-effort route. A builder that raises behaves
            like a raising ``metadata_for``.
        db: The producer's SQLAlchemy session (holds the uncommitted change).
        schema_version: Payload schema version.

    Returns:
        One entry per payload, in payload order: the event's id, or ``None`` for an event the best-effort route
        could not mint (its failure is logged). On that route an id is returned when the event is minted, before
        the commit that dispatches it. On the durable route a failure raises instead, so every entry is an id; a
        reused id (see :func:`publish_on_commit`) is returned like a new one. An empty batch returns ``[]``.

    Raises:
        ValueError: When ``db`` is ``None``, or ``event_type`` or ``source`` is longer than its column, on
            either route and before anything is staged; or, on the durable route, when an id is invalid or
            already in the outbox with a different ``event_type`` or payload.
        TypeError: When ``db`` is not a synchronous ``Session``; or, on the durable route, when an id is already
            in the outbox and its payload is not JSON-native (dict, list, str, int, float, bool or ``None``), so
            it cannot be compared with the stored one.
    """
    if db is None:
        raise ValueError("publish_on_commit/publish_many_on_commit require the producer's session as `db`")
    if not isinstance(db, Session):
        raise TypeError(
            f"publish_on_commit/publish_many_on_commit need a synchronous sqlalchemy.orm.Session, got {type(db).__name__}"
        )
    if not payloads:
        return []
    _check_identifiers(event_type, source)
    metadata_of = metadata_for if metadata_for is not None else _no_metadata
    event_id_of = event_id_for if event_id_for is not None else _no_event_id
    if _durable_delivery_enabled(event_type):
        return list(_stage_batch(event_type, payloads, source, metadata_of, schema_version, db, event_id_of))
    event_ids: list[str | None] = []
    for payload in payloads:
        try:
            # A copy: the event is the one published now, whatever the caller does to the dict before committing.
            event = _mint(
                event_type, copy.deepcopy(payload), source, metadata_of(payload), schema_version, event_id_of(payload)
            )
            run_after_commit(db, partial(_dispatch_after_commit, event))
        except Exception as err:
            logger.error("Failed to publish event %s: %s", event_type, _describe(err))
            event_ids.append(None)
        else:
            event_ids.append(event.event_id)
    return event_ids
