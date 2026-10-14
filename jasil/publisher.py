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

import logging
from collections.abc import Callable, Sequence
from datetime import datetime
from functools import partial
from typing import Any

from sqlalchemy.orm import Session

import jasil.correlation as correlation
import jasil.jobs.registry as jobs_registry
import jasil.runtime as platform_runtime
from jasil._core.after_commit import run_after_commit
from jasil.events import INITIAL_SCHEMA_VERSION, META_REQUEST_ID, Event, new_event
from jasil.providers import EventRecorder
from jasil.settings import get_settings

logger = logging.getLogger(__name__)


def _mint(
    event_type: str,
    payload: dict,
    source: str,
    metadata: dict | None,
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> Event:
    """Build the event envelope, stamping the ambient request id for correlation."""
    merged: dict = {}
    request_id = correlation.get_correlation_id()
    if request_id:
        merged[META_REQUEST_ID] = request_id
    if metadata:
        merged.update(metadata)
    return new_event(event_type, payload, source=source, metadata=merged, schema_version=schema_version)


def _stage_in_outbox(
    recorder: EventRecorder | None,
    event: Event,
    *,
    db: Any,
    now: datetime,
    commit: bool,
) -> None:
    """Write one event to the outbox, recording it ``queued`` in the event_log.

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

    if recorder is not None:
        recorder.record_queued(event)
    jobs_outbox.add_to_outbox(event, now=now, db=db, commit=commit)


def _publish_on_bus(
    event_type: str,
    payload: dict,
    source: str,
    metadata: dict | None,
    schema_version: int,
) -> None:
    """Mint and dispatch one event on the bus, logging and swallowing any failure."""
    try:
        event = _mint(event_type, payload, source, metadata, schema_version)
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event_type, err, exc_info=err)
        return
    _dispatch_event(event)


def _no_metadata(_payload: dict) -> dict | None:
    return None


def _dispatch_event(event: Event) -> None:
    """Dispatch an already-minted event on the bus, logging and swallowing failure."""
    try:
        platform_runtime.get_active_platform().events.publish(event)
        logger.debug("Dispatched %s (%s) on the event bus", event.event_type, event.event_id)
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event.event_type, err, exc_info=err)


def _stage_batch(
    event_type: str,
    payloads: Sequence[dict],
    source: str,
    metadata_of: Callable[[dict], dict | None],
    schema_version: int,
    db: Any,
) -> None:
    """Stage one outbox row per payload on ``db`` without committing.

    A failure is logged and re-raised so the caller's unit of work rolls back as
    a whole — no partial domain change, no orphaned event.
    """
    try:
        platform = platform_runtime.get_active_platform()
        now = platform.clock.now()
        for payload in payloads:
            event = _mint(event_type, payload, source, metadata_of(payload), schema_version)
            _stage_in_outbox(platform.recorder, event, db=db, now=now, commit=False)
    except Exception as err:
        logger.error(
            "Failed to stage %d %s event(s) in the domain transaction: %s",
            len(payloads),
            event_type,
            err,
            exc_info=err,
        )
        raise


def publish(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    db: Any = None,
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> None:
    """Publish a domain event through the active platform, best-effort.

    Args:
        event_type: The domain-owned channel, e.g. ``order.created``.
        payload: Domain data for the event (homogeneous per ``event_type``).
        source: Origin label, e.g. ``api:create_order``.
        metadata: Optional correlation context; merged with the ambient request
            id when one is set on the current request.
        db: The producer's SQLAlchemy session. When provided and durable jobs are
            enabled for this event type, the event is written to the outbox using
            this session (durable delivery); otherwise it is ignored.

    Returns:
        None. Delivery failures are logged and swallowed so a publish never
        breaks the producer. The domain row remains the source of truth and each
        subscriber's reconciliation net recovers anything missed.
    """
    try:
        platform = platform_runtime.get_active_platform()
        event = _mint(event_type, payload, source, metadata, schema_version)
        if db is not None and _durable_delivery_enabled(event_type):
            _stage_in_outbox(platform.recorder, event, db=db, now=platform.clock.now(), commit=True)
            # Which route an event took is the first thing anyone asks when it
            # appears not to have been processed, and nothing else records it.
            logger.debug("Staged %s (%s) in the outbox for durable delivery", event_type, event.event_id)
        else:
            _dispatch_event(event)
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event_type, err, exc_info=err)


def publish_committing(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    db: Any,
    commit: Callable[[], None],
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> None:
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

    Returns:
        None.
    """
    publish_many_committing(
        event_type,
        [payload],
        source=source,
        metadata_for=lambda _payload: metadata,
        db=db,
        commit=commit,
        schema_version=schema_version,
    )


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
    db: Any,
    commit: Callable[[], None],
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> None:
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
        db: The producer's SQLAlchemy session (holds the uncommitted change).
        commit: Zero-arg callable that commits the caller's unit of work.

    Returns:
        None.
    """
    metadata_of = metadata_for if metadata_for is not None else _no_metadata
    if db is not None and _durable_delivery_enabled(event_type):
        # Atomic path: stage every outbox row inside the caller's transaction.
        _stage_batch(event_type, payloads, source, metadata_of, schema_version, db)
        commit()
    else:
        # Best-effort path: the domain change is the source of truth, so commit it
        # first, then dispatch each event on the bus (swallowing failures — the
        # subscriber's reconciliation net recovers anything dropped).
        commit()
        for payload in payloads:
            _publish_on_bus(event_type, payload, source, metadata_of(payload), schema_version)


def publish_on_commit(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    db: Any,
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> None:
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
    external transaction (``join_transaction_mode="create_savepoint"``), its
    ``commit()`` is that session's outermost commit even though the enclosing
    transaction may still roll back.

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

    Returns:
        None.
    """
    publish_many_on_commit(
        event_type,
        [payload],
        source=source,
        metadata_for=lambda _payload: metadata,
        db=db,
        schema_version=schema_version,
    )


def publish_many_on_commit(
    event_type: str,
    payloads: Sequence[dict],
    *,
    source: str,
    metadata_for: Callable[[dict], dict | None] | None = None,
    db: Any,
    schema_version: int = INITIAL_SCHEMA_VERSION,
) -> None:
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
        db: The producer's SQLAlchemy session (holds the uncommitted change).
        schema_version: Payload schema version.

    Returns:
        None.
    """
    if db is None:
        raise ValueError("publish_on_commit/publish_many_on_commit require the producer's session as `db`")
    if not isinstance(db, Session):
        raise TypeError(
            f"publish_on_commit/publish_many_on_commit need a synchronous sqlalchemy.orm.Session, got {type(db).__name__}"
        )
    if not payloads:
        return
    metadata_of = metadata_for if metadata_for is not None else _no_metadata
    if _durable_delivery_enabled(event_type):
        _stage_batch(event_type, payloads, source, metadata_of, schema_version, db)
        return
    for payload in payloads:
        try:
            event = _mint(event_type, payload, source, metadata_of(payload), schema_version)
            run_after_commit(db, partial(_dispatch_event, event))
        except Exception as err:
            logger.error("Failed to publish event %s: %s", event_type, err, exc_info=err)
