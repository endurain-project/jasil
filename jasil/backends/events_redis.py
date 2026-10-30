"""Redis Streams ``EventBusProvider`` backend.

Only imported by the composition root when ``events_uri`` selects Redis, so
``local`` deployments never load it. Publishing
is an ``XADD`` onto one stream; a background consumer thread reads through a
consumer group (``XREADGROUP``) and dispatches each event to the subscribers
registered for its ``event_type``, acking (``XACK``) after the handlers run.

A deployment normally ships one image, so every replica registers the same
subscribers. A single consumer group therefore gives "each derived computation
runs once per event across the cluster" (competing consumers), while in-process
fan-out to all handlers of an ``event_type`` still happens on whichever replica
claims the entry.

Delivery is best-effort, at most once per event: every handler runs even when one
raises, the failure is logged (and recorded in the event log), and the entry is
acked either way. Leaving it pending would not bring it back, as the consumer
only reads entries no one has claimed. An entry claimed by a consumer that then
crashed stays pending. For at-least-once delivery with per-subscriber retry,
backoff, dead-letter, and replay, enable durable jobs: publishing then routes
through the transactional outbox and ``processing_jobs`` instead of this bus,
and each subscriber's own reconciliation sweep is the safety net.
"""

import json
import logging
import threading
from collections import defaultdict
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

import jasil._core.identity as identity
import jasil._core.redis_clients as redis_clients
from jasil._core.threads import backoff_delay, signal_and_join
from jasil.events import INITIAL_SCHEMA_VERSION, Event

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from jasil.providers import EventRecorder

_DEFAULT_STREAM = "jasil:events"
_DEFAULT_GROUP = "jasil"
# Approximate cap on the stream's length. Redis trims the oldest entries whether or not a consumer has read
# them, so a consumer down for long enough misses the oldest ones.
_STREAM_MAXLEN = 10_000
_READ_BATCH = 10
_BLOCK_MS = 1_000  # XREADGROUP block window; bounds how quickly stop() is observed
# Pacing of the retries while Redis is unreachable: capped exponential backoff, so a long outage neither floods the
# log nor has every replica reconnect in lockstep.
_RETRY_BASE_SECONDS = 1.0
_RETRY_MAX_SECONDS = 30.0


def serialize_event(event: Event) -> dict[str, str]:
    """Flatten an :class:`Event` into Redis stream fields (all strings)."""
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "source": event.source,
        "timestamp": event.timestamp,
        "payload": json.dumps(event.payload),
        "metadata": json.dumps(event.metadata),
        "retry_count": str(event.retry_count),
        "schema_version": str(event.schema_version),
    }


def deserialize_event(fields: Mapping[str, str]) -> Event:
    """Rebuild an :class:`Event` from Redis stream fields (the inverse of :func:`serialize_event`)."""
    return Event(
        event_id=fields["event_id"],
        event_type=fields["event_type"],
        source=fields["source"],
        timestamp=fields["timestamp"],
        payload=json.loads(fields["payload"]),
        metadata=json.loads(fields["metadata"]),
        retry_count=int(fields["retry_count"]),
        # Unlike the outbox/jobs tables, a Redis stream has no migration: entries
        # written before this field shipped are still sitting in the stream with
        # no ``schema_version``. They were produced at the initial version, so
        # default rather than failing the consumer on them.
        schema_version=int(fields.get("schema_version", INITIAL_SCHEMA_VERSION)),
    )


class RedisStreamEventBus:
    """``EventBusProvider`` backed by one Redis stream and a consumer group.

    The Redis client is typed ``Any`` to stay clear of redis-py's sync/async
    ``ResponseT`` typing; :meth:`from_uri` builds a ``decode_responses=True``
    client so stream fields arrive as ``str``.
    """

    def __init__(
        self,
        client: Any,
        *,
        stream: str = _DEFAULT_STREAM,
        group: str = _DEFAULT_GROUP,
        consumer: str | None = None,
        recorder: "EventRecorder | None" = None,
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer or identity.process_identity()
        self._handlers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._recorder = recorder

    @classmethod
    def from_uri(
        cls,
        uri: str,
        *,
        stream: str = _DEFAULT_STREAM,
        group: str = _DEFAULT_GROUP,
        recorder: "EventRecorder | None" = None,
    ) -> "RedisStreamEventBus":
        """Build from a ``redis://…`` URI, verifying connectivity eagerly."""
        client = redis_clients.get_shared_client(uri, purpose="event bus")
        return cls(client, stream=stream, group=group, recorder=recorder)

    def publish(self, event: Event) -> None:
        """Record the event when a recorder is set, then append it to the Redis stream."""
        # Serialized first, so an envelope that cannot be sent leaves no 'published' row behind. The row is
        # then written before the entry is queued, because the consumer records processing/completed/failed
        # as updates to it; a failed XADD therefore leaves it 'published', and the publisher logs the failure.
        fields = serialize_event(event)
        if self._recorder is not None:
            self._recorder.record_published(event)
        self._client.xadd(self._stream, fields, maxlen=_STREAM_MAXLEN, approximate=True)

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """Register ``handler`` for ``event_type`` on this process's consumer."""
        self._handlers[event_type].append(handler)

    def start(self) -> None:
        """Create the consumer group (idempotent) and start the consumer thread."""
        with self._lock:
            if self._thread is not None:
                return
            self._ensure_group()
            # A fresh event per thread: a thread a timed-out stop left running keeps the one already set, so a
            # restart cannot revive it beside the new consumer.
            stop = self._stop = threading.Event()
            self._thread = threading.Thread(target=self._run, args=(stop,), name="event-bus-consumer", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Signal the consumer thread and wait briefly for it to finish.

        Safe to call from a handler: the loop then ends once the handler returns.
        """
        with self._lock:
            thread, self._thread = self._thread, None
            stop = self._stop
        signal_and_join(thread, stop)

    def _ensure_group(self) -> None:
        try:
            self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except redis_clients.ResponseError as error:
            if "BUSYGROUP" not in str(error):  # any error other than "group already exists" is real
                raise

    def _run(self, stop: threading.Event) -> None:
        group_missing = False
        failures = 0
        while not stop.is_set():
            try:
                if group_missing:
                    self._ensure_group()
                    group_missing = False
                self._poll_once()
                if failures:
                    logger.info("Event bus consumer recovered after %d failed poll(s)", failures)
                    failures = 0
            except redis_clients.RedisError as error:
                # A Redis restarted without persistence, or flushed, loses the group; XADD recreates only the
                # stream, so every read fails until the group is created again.
                group_missing = isinstance(error, redis_clients.ResponseError) and str(error).startswith("NOGROUP")
                # The first failure of a streak is the one worth a traceback; the repeats only say it goes on.
                if failures == 0:
                    logger.error("Event bus consumer poll failed", exc_info=error)
                else:
                    logger.debug("Event bus consumer poll failed again (%d in a row): %r", failures + 1, error)
                stop.wait(timeout=backoff_delay(failures, base=_RETRY_BASE_SECONDS, maximum=_RETRY_MAX_SECONDS))
                failures += 1

    def _poll_once(self) -> None:
        response = self._client.xreadgroup(
            self._group, self._consumer, {self._stream: ">"}, count=_READ_BATCH, block=_BLOCK_MS
        )
        for _stream, entries in response or []:
            for entry_id, fields in entries:
                self._dispatch(entry_id, fields)

    def _dispatch(self, entry_id: str, fields: Mapping[str, str]) -> None:
        recorder = self._recorder
        try:
            event = deserialize_event(fields)
            handlers = self._handlers.get(event.event_type, [])
            if recorder is None:
                _call_each(handlers, event)
            else:
                handler_name = ",".join(getattr(handler, "__name__", repr(handler)) for handler in handlers) or None
                with recorder.track(event, worker_id=self._consumer, handler_name=handler_name):
                    _call_each(handlers, event)
        except Exception as error:  # a poisoned entry or handler must not kill the consumer thread
            logger.error("Event handling failed for stream entry %s", entry_id, exc_info=error)
        # Acked whatever the handlers did: nothing reads a claimed entry again, so a pending one would only grow
        # the pending-entries list. Durable jobs are the path that retries. A failed ack only leaves this entry
        # pending; it must not end the poll, or the rest of the batch, already read, would never be handled.
        try:
            self._client.xack(self._stream, self._group, entry_id)
        except redis_clients.RedisError as error:
            logger.error("Event bus could not ack stream entry %s; it stays pending", entry_id, exc_info=error)


def _call_each(handlers: list[Callable[[Event], None]], event: Event) -> None:
    """Call every handler, then raise what failed: the error itself, or an ``ExceptionGroup`` of several."""
    errors = []
    for handler in handlers:
        try:
            handler(event)
        except Exception as error:  # one subscriber must not keep the event from the others
            errors.append(error)
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup(f"{len(errors)} of {len(handlers)} handlers failed for {event.event_type}", errors)
