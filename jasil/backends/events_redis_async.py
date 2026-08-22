"""Async Redis Streams ``AsyncEventBusProvider`` backend.

This is the async twin of :mod:`jasil.backends.events_redis`, mirroring it
class-for-class and method-for-method. The sync backend runs a daemon thread
watching a :class:`threading.Event`; this one runs an :class:`asyncio.Task`
watching an :class:`asyncio.Event`, wound down via
:func:`jasil._core.tasks.signal_and_cancel`, which mirrors the threaded
:func:`~jasil._core.threads.signal_and_join` policy exactly (including the
bounded-wait-then-cancel two-phase shutdown that keeps a loop parked inside
``await`` from blocking shutdown indefinitely).

Only imported by the async composition root when ``events_uri`` selects Redis.
Publishing is an ``XADD`` onto one stream; a background task reads through a
consumer group (``XREADGROUP``) and dispatches each event to the async
subscribers registered for its ``event_type``, acking (``XACK``) after the
handlers succeed.

**Why a module-level factory instead of a classmethod:**

The async Redis client requires a connectivity-verified ``await`` before it is
ready. That ``await`` cannot happen inside ``__init__``, so construction is split:
``__init__`` takes an already-connected client (enabling injection in tests), and
the :func:`create_async_redis_event_bus` module-level factory performs the
``await get_shared_async_client(...)`` call before handing the client to
``__init__``. The factory mirrors how :meth:`RedisStreamEventBus.from_uri <jasil.backends.events_redis.RedisStreamEventBus.from_uri>`
works on the sync side, where the classmethod calls the synchronous
:func:`~jasil._core.redis_clients.get_shared_client`, which is itself blocking
and therefore fine inside a regular method body.

**Delivery guarantees — unchanged from the sync backend:**

Delivery is at-least-once. An entry is acked only after its handlers succeed, so
a failed handler (or a malformed envelope) leaves the entry pending rather than
dropping it. This bus is the best-effort delivery path and has no in-bus retry
or reclaim of its own: an entry orphaned by a crashed consumer stays pending.
For at-least-once delivery with per-subscriber retry, backoff, dead-letter, and
replay, enable durable jobs: publishing then routes through the transactional
outbox and ``processing_jobs`` instead of this bus, and each subscriber's own
reconciliation sweep is the safety net.

**Handler error swallowing in ``_dispatch``:**

The sync backend's ``_dispatch`` catches :class:`Exception` to keep a poisoned
entry from killing the consumer thread. The async version does the same for the
consumer task. Where the sync backend calls ``jasil.subscribers.best_effort``,
an async equivalent does not yet exist in ``jasil.subscribers`` (it will be
added there later). In the meantime, the try/except-and-log logic is inlined
here with an explanatory comment; this will move to ``jasil.subscribers`` in a
follow-up once the async subscribers module is established.
"""

import asyncio
import contextlib
import json
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

import jasil._core.identity as identity
import jasil._core.redis_clients as redis_clients
from jasil._core.tasks import signal_and_cancel
from jasil.events import INITIAL_SCHEMA_VERSION, Event

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from jasil.providers_async import AsyncEventRecorder

_DEFAULT_STREAM = "jasil:events"
_DEFAULT_GROUP = "jasil"
_STREAM_MAXLEN = 10_000  # approximate cap so acked entries don't grow unbounded
_READ_BATCH = 10
_BLOCK_MS = 1_000  # XREADGROUP block window; bounds how quickly stop() is observed


def serialize_event(event: Event) -> dict[str, str]:
    """Flatten an :class:`~jasil.events.Event` into Redis stream fields (all strings).

    Args:
        event: The event to serialize.

    Returns:
        A ``dict[str, str]`` suitable for ``XADD``.
    """
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
    """Rebuild an :class:`~jasil.events.Event` from Redis stream fields (the inverse of :func:`serialize_event`).

    Args:
        fields: The string-valued field mapping returned by ``XREADGROUP``.

    Returns:
        A reconstructed :class:`~jasil.events.Event`.

    Raises:
        KeyError: When a required field is absent from the stream entry.
        ValueError: When a numeric field cannot be parsed.
    """
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


class AsyncRedisStreamEventBus:
    """``AsyncEventBusProvider`` backed by one Redis stream and a consumer group.

    The Redis client is typed ``Any`` to stay clear of redis-py's sync/async
    ``ResponseT`` typing; :func:`create_async_redis_event_bus` builds a
    ``decode_responses=True`` client so stream fields arrive as ``str``.

    Construction requires an already-connected async Redis client. Use
    :func:`create_async_redis_event_bus` to build one from a URI with the
    necessary ``await`` for the connectivity check.
    """

    def __init__(
        self,
        client: Any,
        *,
        stream: str = _DEFAULT_STREAM,
        group: str = _DEFAULT_GROUP,
        consumer: str | None = None,
        recorder: "AsyncEventRecorder | None" = None,
    ) -> None:
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer or identity.process_identity()
        self._handlers: dict[str, list[Callable[[Event], Awaitable[None]]]] = defaultdict(list)
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._recorder = recorder

    async def publish(self, event: Event) -> None:
        """Publish ``event`` to the Redis stream.

        Writes an ``XADD`` to the stream (with an approximate length cap), then
        returns immediately. The consumer task on any live replica will pick it
        up on its next poll. Recording the "published" lifecycle event happens
        here, in the producer process, so the recorder captures when the event
        entered the bus rather than when a consumer happened to claim it.

        Args:
            event: The event to publish. Its ``event_type`` determines which
                handlers the consumer will invoke.

        Raises:
            redis.asyncio.RedisError: When the ``XADD`` command fails.
        """
        # Record 'published' from the producer process before the entry is queued;
        # the consumer records processing/completed/failed when it claims the entry.
        if self._recorder is not None:
            await self._recorder.record_published(event)
        await self._client.xadd(self._stream, serialize_event(event), maxlen=_STREAM_MAXLEN, approximate=True)

    def subscribe(self, event_type: str, handler: Callable[[Event], Awaitable[None]]) -> None:
        """Register ``handler`` to be called whenever a consumer claims an event of ``event_type``.

        Args:
            event_type: The ``Event.event_type`` string that triggers ``handler``.
            handler: An async callable accepting an :class:`~jasil.events.Event`.

        Returns:
            None.
        """
        self._handlers[event_type].append(handler)

    async def start(self) -> None:
        """Create the consumer group (idempotent) and start the consumer task.

        Calling ``start`` a second time while the task is already running is a
        no-op: the existing task is left untouched.

        Raises:
            redis.asyncio.RedisError: When the ``XGROUP CREATE`` command fails for a
                reason other than the group already existing.
            RuntimeError: When there is no running event loop (should not occur in
                normal async usage).
        """
        if self._task is not None:
            return
        await self._ensure_group()
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="async-event-bus-consumer")

    async def stop(self) -> None:
        """Signal the consumer task and wait for it to finish, cancelling if it takes too long.

        Delegates to :func:`jasil._core.tasks.signal_and_cancel`, which mirrors the
        bounded-wait-then-cancel policy of the threaded backend's
        :func:`~jasil._core.threads.signal_and_join`.

        Returns:
            None.
        """
        task, self._task = self._task, None
        await signal_and_cancel(task, self._stop)

    async def _ensure_group(self) -> None:
        try:
            await self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except redis_clients.ResponseError as error:
            if "BUSYGROUP" not in str(error):  # any error other than "group already exists" is real
                raise

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_once()
            except redis_clients.RedisError as error:
                logger.error("Async event bus consumer poll failed", exc_info=error)
                # Back off before retrying, mirroring the sync backend's
                # ``self._stop.wait(timeout=1.0)``: wait on the stop event with a
                # timeout, so a shutdown signalled during the backoff is noticed
                # immediately instead of after the full second.
                #
                # CancelledError must NOT be suppressed here. The loop only exits
                # on its own stop event, so swallowing a cancellation that came
                # from anywhere else — loop teardown, an enclosing TaskGroup, a
                # host cancelling the task directly — would leave a task that can
                # never be stopped, and shutdown would hang waiting for it.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=1.0)

    async def _poll_once(self) -> None:
        response = await self._client.xreadgroup(
            self._group, self._consumer, {self._stream: ">"}, count=_READ_BATCH, block=_BLOCK_MS
        )
        for _stream, entries in response or []:
            for entry_id, fields in entries:
                await self._dispatch(entry_id, fields)

    async def _dispatch(self, entry_id: str, fields: Mapping[str, str]) -> None:
        recorder = self._recorder
        try:
            event = deserialize_event(fields)
            handlers = self._handlers.get(event.event_type, [])
            if recorder is None:
                for handler in handlers:
                    # NOTE: best-effort error handling for individual handlers will move
                    # to ``jasil.subscribers`` once an async equivalent of
                    # ``jasil.subscribers.best_effort`` is established there.
                    await handler(event)
            else:
                handler_name = ",".join(handler.__name__ for handler in handlers) or None
                async with recorder.track(event, worker_id=self._consumer, handler_name=handler_name):
                    for handler in handlers:
                        await handler(event)
        except Exception as error:  # a poisoned entry or handler must not kill the consumer task
            logger.error("Async event handler failed for stream entry %s; leaving it pending", entry_id, exc_info=error)
            return
        # Ack only after success so a failure stays pending (at-least-once) for
        # reprocessing; durable jobs provide the retry/dead-letter path.
        await self._client.xack(self._stream, self._group, entry_id)


async def create_async_redis_event_bus(
    uri: str,
    *,
    stream: str = _DEFAULT_STREAM,
    group: str = _DEFAULT_GROUP,
    recorder: "AsyncEventRecorder | None" = None,
) -> AsyncRedisStreamEventBus:
    """Build an :class:`AsyncRedisStreamEventBus` from a ``redis://…`` URI.

    This factory exists because constructing the bus requires an ``await`` to
    verify connectivity — something ``__init__`` cannot perform. It mirrors how
    :meth:`~jasil.backends.events_redis.RedisStreamEventBus.from_uri` works on
    the sync side, where calling :func:`~jasil._core.redis_clients.get_shared_client`
    (a blocking operation) from a classmethod is fine.

    Args:
        uri: Redis URI (e.g. ``"redis://localhost:6379"``).
        stream: Redis stream key. Defaults to ``"jasil:events"``.
        group: Consumer-group name. Defaults to ``"jasil"``.
        recorder: Optional async event recorder for lifecycle tracking.

    Returns:
        A constructed (but not yet started) :class:`AsyncRedisStreamEventBus`.

    Raises:
        RuntimeError: When Redis cannot be reached at ``uri``.
    """
    client = await redis_clients.get_shared_async_client(uri, purpose="async event bus")
    return AsyncRedisStreamEventBus(client, stream=stream, group=group, recorder=recorder)
