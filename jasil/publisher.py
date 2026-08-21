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
swallowed so publishing never breaks the producer's own work. The outbox is not
committed in the same transaction as the domain change (the ingestion path commits
per-CRUD), so a crash between the domain commit and the outbox write can drop an
event. Every subscriber must therefore have a reconciliation net — a backfill or
sweeper that re-derives missed work). A future unit-of-work refactor can upgrade this to a genuinely
atomic outbox; until then, "durable" means *retryable once written*, not *never
lost*. Channel names and payload shape stay owned by the publishing domain; this
layer only knows the generic envelope.
"""

import logging
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any

import jasil.correlation as correlation
import jasil.jobs.registry as jobs_registry
import jasil.runtime as platform_runtime
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
    """Dispatch one event on the bus, logging and swallowing any failure."""
    try:
        platform = platform_runtime.get_active_platform()
        platform.events.publish(_mint(event_type, payload, source, metadata, schema_version))
    except Exception as err:
        logger.error("Failed to publish event %s: %s", event_type, err, exc_info=err)


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
            platform.events.publish(event)
            logger.debug("Dispatched %s (%s) on the event bus", event_type, event.event_id)
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
    metadata_of = metadata_for if metadata_for is not None else (lambda _payload: None)
    if db is not None and _durable_delivery_enabled(event_type):
        # Atomic path: stage every outbox row inside the caller's transaction, so
        # a failure here leaves it uncommitted and the caller rolls back as a
        # whole — no partial domain change, no orphaned event.
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
        commit()
    else:
        # Best-effort path: the domain change is the source of truth, so commit it
        # first, then dispatch each event on the bus (swallowing failures — the
        # subscriber's reconciliation net recovers anything dropped).
        commit()
        for payload in payloads:
            _publish_on_bus(event_type, payload, source, metadata_of(payload), schema_version)
