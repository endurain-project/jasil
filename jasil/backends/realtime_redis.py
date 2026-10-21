"""Redis Pub/Sub ``RealtimeProvider`` backend.

Only imported by the composition root when ``realtime_uri`` selects Redis, so
``local`` deployments never load it. Publishing is a ``PUBLISH`` onto one
channel; every process that called :meth:`RedisRealtime.start` holds a
``SUBSCRIBE`` on that channel on a daemon thread and hands each hint to its
registered handlers.

Pub/Sub, not Streams, on purpose: the event bus's consumer group gives each
entry to *one* replica (competing consumers), whereas a hint must reach *every*
replica, because any of them may hold the connection a hint is for. Pub/Sub is
fire-and-forget fan-out with no backlog, which is exactly the contract: a
subscriber that is reconnecting misses what was published meanwhile, and that
is acceptable because a hint is never the record.

Connections. Publishing borrows the process-wide shared client. The subscriber
holds its connection for as long as it runs, so it opens a **dedicated** client
instead of checking one out of the shared pool: a pool bounded with
``max_connections`` would otherwise lose a slot for the life of the process,
and :func:`~jasil._core.redis_clients.close_shared_clients` would cut the
subscription from under the thread. The dedicated client is closed on every
reconnect and on :meth:`RedisRealtime.stop`.

Note that Pub/Sub channels are server-wide: the database number in the URI does
not separate them. Two deployments sharing one Redis server must use different
``channel`` names.
"""

import logging
import random
import threading
import time
from collections.abc import Callable
from typing import Any

import jasil._core.realtime_envelope as realtime_envelope
import jasil._core.redis_clients as redis_clients
from jasil._core.threads import signal_and_join

logger = logging.getLogger(__name__)

_DEFAULT_CHANNEL = "jasil:realtime"
_POLL_TIMEOUT_SECONDS = 1.0  # bounds how quickly stop() is observed
_START_WAIT_SECONDS = 2.0  # how long start() waits for the first SUBSCRIBE
_RECONNECT_BASE_SECONDS = 0.5
_RECONNECT_MAX_SECONDS = 30.0
# An idle subscription is PINGed this often, and torn down when nothing at all
# (not even the PONG) has arrived for twice as long: a half-open TCP connection
# (a NAT or load-balancer idle timeout, a failover without a RST) otherwise
# leaves the replica subscribed to nothing, silently, for good.
_KEEPALIVE_SECONDS = 30.0


class _StaleSubscriptionError(ConnectionError):
    """Nothing, not even a PONG, arrived on the subscription for too long."""


class RedisRealtime:
    """``RealtimeProvider`` over one Redis Pub/Sub channel.

    The Redis clients are typed ``Any`` to stay clear of redis-py's sync/async
    ``ResponseT`` typing.

    Args:
        client: Client used for ``PUBLISH``.
        subscriber_factory: Returns a fresh client for the subscriber thread,
            called on every (re)connect. Defaults to reusing ``client``, which
            suits a test fake; :meth:`from_uri` supplies a dedicated one.
        channel: The Pub/Sub channel. Server-wide, so unique per deployment.
        max_bytes: Cap on the serialised envelope.
        reconnect_base_seconds: First reconnect delay; doubled per failure.
        reconnect_max_seconds: Ceiling on the reconnect delay.
        keepalive_seconds: Idle interval after which the subscription is
            PINGed; it is reconnected when silent for twice this long.
    """

    def __init__(
        self,
        client: Any,
        *,
        subscriber_factory: Callable[[], Any] | None = None,
        channel: str = _DEFAULT_CHANNEL,
        max_bytes: int = realtime_envelope.DEFAULT_MAX_ENVELOPE_BYTES,
        reconnect_base_seconds: float = _RECONNECT_BASE_SECONDS,
        reconnect_max_seconds: float = _RECONNECT_MAX_SECONDS,
        keepalive_seconds: float = _KEEPALIVE_SECONDS,
    ) -> None:
        self._client = client
        self._subscriber_factory = subscriber_factory or (lambda: client)
        self._channel = channel
        self._max_bytes = max_bytes
        self._reconnect_base = reconnect_base_seconds
        self._reconnect_max = reconnect_max_seconds
        self._keepalive = keepalive_seconds
        self._lock = threading.Lock()
        self._handlers = realtime_envelope.HintHandlers()
        self._thread: threading.Thread | None = None
        # Replaced on every start(), so a thread stop() gave up on keeps its own
        # signalled event instead of being revived by the next start().
        self._stop = threading.Event()
        # Set while a SUBSCRIBE is confirmed; start() waits on it so a hint
        # published right after start() returns is not lost to the subscribe race.
        self._subscribed = threading.Event()

    @classmethod
    def from_uri(
        cls,
        uri: str,
        *,
        channel: str = _DEFAULT_CHANNEL,
        max_bytes: int = realtime_envelope.DEFAULT_MAX_ENVELOPE_BYTES,
    ) -> "RedisRealtime":
        """Build from a ``redis://…`` URI, verifying connectivity eagerly."""
        client = redis_clients.get_shared_client(uri, purpose="realtime hints")
        return cls(
            client,
            # Raw bytes: another publisher on the server-wide channel may send
            # non-UTF-8 data, which ``decode`` must reject, not the Redis client.
            subscriber_factory=lambda: redis_clients.create_redis_client(
                uri, "realtime hint subscriber", decode_responses=False
            ),
            channel=channel,
            max_bytes=max_bytes,
        )

    def notify(self, key: str, hint: dict) -> None:
        """Publish a hint to every started process; a transport failure is logged and dropped.

        Raises:
            ValueError: When the key or hint is invalid, or the encoded envelope
                exceeds the configured byte cap (see
                :class:`~jasil.providers.RealtimeProvider`).
        """
        payload = realtime_envelope.encode(key, hint, max_bytes=self._max_bytes)
        try:
            self._client.publish(self._channel, payload)
        except Exception as error:  # best-effort: never fail the caller over a hint
            logger.warning("Dropped a realtime hint for key %r: publish failed: %r", key, error)

    def subscribe(self, handler: Callable[[str, dict], None]) -> None:
        """Register ``handler(key, hint)`` for every delivered hint, regardless of key.

        Callbacks run on the subscriber thread after :meth:`start`.
        Handler exceptions are caught so later handlers still run.
        """
        self._handlers.add(handler)

    def start(self) -> None:
        """Start the subscriber thread (idempotent).

        Waits briefly for the first ``SUBSCRIBE`` so hints published once this
        returns are received. Never raises: if Redis is unreachable the thread
        keeps retrying in the background.
        """
        with self._lock:
            if self._thread is not None:
                return
            stop, subscribed = self._stop, self._subscribed = threading.Event(), threading.Event()
            self._thread = threading.Thread(
                target=self._run, args=(stop, subscribed), name="realtime-subscriber", daemon=True
            )
            self._thread.start()
        if not subscribed.wait(timeout=_START_WAIT_SECONDS):
            logger.warning("Realtime subscriber is not connected yet; it keeps retrying in the background")

    def stop(self) -> None:
        """Signal the subscriber thread and wait briefly for it to finish (idempotent, never raises)."""
        with self._lock:
            thread, self._thread = self._thread, None
            stop = self._stop
        try:
            if thread is threading.current_thread():
                # Called from a handler: joining oneself raises. The loop sees
                # the event after the handler returns.
                stop.set()
            else:
                signal_and_join(thread, stop)
        except Exception as error:  # pragma: no cover - shutdown must not raise
            logger.warning("Failed to stop the realtime subscriber: %r", error)

    def _reconnect_delay(self, failures: int) -> float:
        """Capped exponential backoff with equal jitter."""
        delay = min(self._reconnect_max, self._reconnect_base * 2 ** min(failures, 32))
        return delay * 0.5 + random.uniform(0.0, delay * 0.5)  # noqa: S311 - jitter, not security

    def _run(self, stop: threading.Event, subscribed: threading.Event) -> None:
        failures = 0
        while not stop.is_set():
            subscriber = pubsub = None
            failure: Exception | None = None
            try:
                subscriber = self._subscriber_factory()
                pubsub = subscriber.pubsub(ignore_subscribe_messages=False)
                pubsub.subscribe(self._channel)
                self._await_subscribed(pubsub, stop)
                subscribed.set()
                if failures:
                    logger.info("Realtime subscriber reconnected after %d failed attempt(s)", failures)
                failures = 0
                self._listen(pubsub, stop)
            except Exception as error:  # the thread must survive any connection failure
                failure = error
            finally:
                subscribed.clear()
                # Released before the backoff, not held open through it.
                self._close(pubsub, subscriber)
            if failure is not None:
                delay = self._reconnect_delay(failures)
                failures += 1
                logger.warning("Realtime subscriber disconnected (%r); reconnecting in %.1fs", failure, delay)
                stop.wait(timeout=delay)

    def _await_subscribed(self, pubsub: Any, stop: threading.Event) -> None:
        """Block until Redis confirms the SUBSCRIBE, so no hint published afterwards is missed."""
        deadline = time.monotonic() + _START_WAIT_SECONDS
        while not stop.is_set():
            message = pubsub.get_message(timeout=_POLL_TIMEOUT_SECONDS)
            if message is not None and message.get("type") == "subscribe":
                return
            if time.monotonic() >= deadline:
                raise ConnectionError("Redis did not confirm the realtime subscription")

    def _listen(self, pubsub: Any, stop: threading.Event) -> None:
        """Read and deliver until stopped; raise when the connection fails or goes silent."""
        last_received = last_ping = time.monotonic()
        while not stop.is_set():
            message = pubsub.get_message(timeout=_POLL_TIMEOUT_SECONDS)
            now = time.monotonic()
            if message is not None:
                last_received = now
                if message.get("type") == "message":
                    self._deliver(message.get("data"))
                continue
            if now - last_received > 2 * self._keepalive:
                raise _StaleSubscriptionError(f"no traffic for {now - last_received:.0f}s")
            if now - last_received >= self._keepalive and now - last_ping >= self._keepalive:
                pubsub.ping()
                last_ping = now

    def _close(self, pubsub: Any, subscriber: Any) -> None:
        for resource in (pubsub, subscriber if subscriber is not self._client else None):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as error:
                logger.debug("Ignoring an error closing the realtime subscriber: %r", error)

    def _deliver(self, data: Any) -> None:
        decoded = realtime_envelope.decode(data)
        if decoded is None:
            return
        self._handlers.deliver(*decoded)
