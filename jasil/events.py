"""The event envelope, ``new_event`` helper, and standard metadata keys.

Pure module — the single structured shape every event travels in, so the
pipeline can route, trace, correlate, dedup, and retry without knowing the
domain.

Channel names (``event_type`` values) are **owned by the domain that publishes
them**, not defined here — e.g. the orders module owns ``order.created``.
Keeping them out of the substrate stops this generic layer from accumulating
domain knowledge; a producer and its subscribers import the same domain-side
constant so they cannot drift on the string. Convention: ``<domain>.<fact>`` in
past tense. ``event_type`` stays a plain ``str`` on the envelope so the bus is
open to new events with no edits here.

The envelope is defined ahead of the bus so the wire format never
has to change once the first producer ships.

Payload versioning
------------------
``schema_version`` describes the shape of ``payload`` for its ``event_type``. It
lives on the envelope rather than inside each payload dict so the substrate can
carry it without parsing domain data, and so no producer can forget it.

It exists because the code that *writes* a payload and the code that *reads* it
are not guaranteed to be the same version: a durable event is staged in the
outbox, relayed on a schedule, retried with backoff, and may sit dead-lettered
indefinitely — and during a rolling deploy old and new replicas run at once. A
consumer that silently ignores unknown keys (every payload model sets
``extra="ignore"``) would read a renamed or repurposed field as its default and
do the wrong thing quietly. The version turns that into something a consumer can
detect: see :mod:`jasil.event_versioning`.

The number is owned by the publishing domain, like the channel name. Bump it in
the domain's publisher when the payload's shape or a field's meaning changes;
purely additive optional fields do not need a bump.
"""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from jasil._core.limits import check_length
from jasil._core.timestamps import as_utc

# --- Standard metadata key (correlation context, not domain data) ---
#
# Only the correlation id is defined here. Domain keys (user id, tenant id, the
# id of whatever the event is about) are the *host's* to name, and travel in the
# same ``metadata`` dict without JASIL needing to know them.
META_REQUEST_ID = "request_id"

#: Version assumed for an event that carries none — every event written before
#: ``schema_version`` existed. Persisted rows default to this on read.
INITIAL_SCHEMA_VERSION = 1

# --- Envelope field limits ---
#
# The width of the columns these fields are stored in (``event_log``,
# ``event_outbox``, ``processing_jobs`` all carry them), declared here because
# :func:`new_event` is where a value has to be rejected to be rejected usefully:
# by the time it reaches a write, the publish seam has already swallowed the
# failure. The models below import these, so the check can never drift from the
# column it is protecting.

#: ``event_id`` is a UUIDv4 string; only an explicitly supplied id can exceed it.
MAX_EVENT_ID_LENGTH = 36
#: ``event_type`` — the routing key, e.g. ``order.created``.
MAX_EVENT_TYPE_LENGTH = 100
#: ``source`` — the origin label, e.g. ``api:create_order``.
MAX_SOURCE_LENGTH = 50


@dataclass(frozen=True)
class Event:
    """Envelope wrapping every event in the system.

    ``payload`` and ``metadata`` are stored and transmitted verbatim: both are
    written to ``event_outbox`` and ``processing_jobs``, to ``event_log`` when the
    trail is enabled, and serialized onto the Redis stream on the ``redis://``
    bus — and ``metadata`` is additionally logged in full when a best-effort
    subscriber fails. Neither is size-capped. So carry identifiers, not secrets
    and not blobs; see the events documentation for the full contract.

    Attributes:
        event_id: UUIDv4 identifying this event instance; stable across retries.
        event_type: Dot-notation channel, e.g. ``order.created``.
        source: Where the event originated, e.g. ``api:create_order``.
        timestamp: ISO-8601 UTC timestamp of the first publish (not the retry).
        payload: Domain data, homogeneous per ``event_type``.
        metadata: Correlation context (request_id, plus any host-defined keys).
        retry_count: Processing attempts so far; 0 on first publish. In a durable
            handler it is the attempt being run, counted from 1 (see
            :attr:`attempt`), so it means different things in-process and
            durable: read :attr:`attempt` and :attr:`is_last_attempt` rather than
            comparing it yourself.
        max_attempts: The attempt ceiling of the durable job this event was
            delivered for; ``None`` for an event that is not a durable delivery.
            Set by the job runner from the job row, the same value it uses to
            decide dead-lettering.
        deferral_count: How many earlier runs of this durable job the handler
            deferred with ``DeferJob``; 0 otherwise. Deferred runs are not
            counted in ``retry_count``.
        schema_version: Which version of ``payload``'s shape this event carries,
            owned by the publishing domain. Defaults to
            :data:`INITIAL_SCHEMA_VERSION` so existing producers are unchanged.
    """

    event_id: str
    event_type: str
    source: str
    timestamp: str
    payload: dict
    metadata: dict = field(default_factory=dict)
    retry_count: int = 0
    schema_version: int = INITIAL_SCHEMA_VERSION
    max_attempts: int | None = None
    deferral_count: int = 0

    def __post_init__(self) -> None:
        """Refuse a payload or metadata that is not a dict.

        Both are written to JSON columns that accept any JSON value, and read back with ``dict(...)``, so a
        list stored today would fail the outbox relay's read on every pass and stall everything queued behind it.

        Raises:
            TypeError: When ``payload`` or ``metadata`` is not a ``dict``.
        """
        for name in ("payload", "metadata"):
            if not isinstance(getattr(self, name), dict):
                raise TypeError(f"{name} must be a dict, not {type(getattr(self, name)).__name__}")

    @property
    def attempt(self) -> int | None:
        """The 1-based attempt a durable handler is running; ``None`` when ``max_attempts`` is unset (not a durable delivery)."""
        return self.retry_count if self.max_attempts is not None else None

    @property
    def is_last_attempt(self) -> bool:
        """True when a failure now dead-letters the job; False outside a durable handler."""
        return self.max_attempts is not None and self.retry_count >= self.max_attempts


def new_event(
    event_type: str,
    payload: dict,
    *,
    source: str,
    metadata: dict | None = None,
    event_id: str | None = None,
    retry_count: int = 0,
    schema_version: int = INITIAL_SCHEMA_VERSION,
    now: datetime | None = None,
) -> Event:
    """Mint an :class:`Event`, generating ``event_id`` and ``timestamp``.

    Args:
        event_type: The channel/type, e.g. ``order.created``.
        payload: Domain data for the event.
        source: Origin label, e.g. ``api:create_order``.
        metadata: Optional correlation context.
        event_id: Optional explicit id (defaults to a fresh UUIDv4); reuse the
            original id when re-publishing a retry so tracing stays stable.
        retry_count: Attempt counter (incremented on re-publish).
        schema_version: The payload-shape version this producer writes.
        now: The instant to stamp, normally the platform clock's; a naive value is
            taken as UTC. Defaults to the system clock.

    Returns:
        A frozen :class:`Event` with a fresh id and UTC timestamp.

    Raises:
        ValueError: When ``event_id``, ``event_type`` or ``source`` is longer
            than the column it is persisted in.
        TypeError: When ``payload`` or ``metadata`` is not a ``dict``.
    """
    resolved_event_id = event_id or str(uuid.uuid4())
    check_length(resolved_event_id, field="event_id", limit=MAX_EVENT_ID_LENGTH)
    check_length(event_type, field="event_type", limit=MAX_EVENT_TYPE_LENGTH)
    check_length(source, field="source", limit=MAX_SOURCE_LENGTH)
    return Event(
        event_id=resolved_event_id,
        event_type=event_type,
        source=source,
        timestamp=as_utc(now if now is not None else datetime.now(UTC)).isoformat(),
        payload=payload,
        metadata=metadata if metadata is not None else {},
        retry_count=retry_count,
        schema_version=schema_version,
    )
