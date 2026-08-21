"""The platform Redis capability — the single owner of Redis connections.

This is the *only* module in the codebase that imports the ``redis`` package,
and it does so **lazily** (inside :func:`create_redis_client`, with the exception
classes exposed through the module-level :func:`__getattr__`). The platform
``state`` and ``events`` Redis backends import *this* module at their top and
borrow a shared client from :func:`get_shared_client`; because nothing here
touches ``redis`` until a ``redis://`` URI is actually resolved, a ``local``
deployment (or one where ``redis`` is not installed — it is the optional
``distributed`` extra) loads zero Redis.

``get_shared_client`` memoizes clients so the process owns *one* connection per
distinct ``(uri, decode_responses)`` pair: in the common case (a single Redis URL
configured) the keyed state and the event bus share a single
``decode_responses=True`` client, while the byte-oriented ``StateProvider`` gets
its own ``decode_responses=False`` client.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis import Redis

logger = logging.getLogger(__name__)

DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS: float = 2.0

_LAZY_REDIS_ATTRS = frozenset({"Redis", "RedisError", "ResponseError"})


def __getattr__(name: str) -> Any:
    """Lazily expose the ``redis`` exception/client classes.

    Lets callers write clean top-level ``import jasil._core.redis_clients as
    redis_clients`` and ``except redis_clients.RedisError`` without importing
    ``redis`` until the attribute is actually accessed at runtime (i.e. only on
    the Redis code path). Keeps ``redis`` a genuinely optional dependency.
    """
    if name in _LAZY_REDIS_ATTRS:
        import redis

        return getattr(redis, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Redis matches ``SCAN``/``KEYS`` patterns with glob semantics (``stringmatchlen``),
# where a backslash escapes the character after it.
_GLOB_METACHARACTERS = frozenset("\\*?[]")


def glob_escape(literal: str) -> str:
    """
    Escape literal text for use inside a Redis ``MATCH`` pattern.

    A caller's literal key prefix is not a literal to Redis: ``*`` and ``?``
    become wildcards and ``[...]`` a character class. Interpolated unescaped, a
    prefix therefore matches keys the caller never named — ``delete_prefix("*")``
    would empty the keyspace — while missing the ones it did. The in-memory
    backend compares with ``str.startswith``, so escaping here is also what keeps
    the two state backends behaviourally identical.

    Args:
        literal: The exact text to match.

    Returns:
        The same text with every glob metacharacter backslash-escaped.
    """
    return "".join(f"\\{character}" if character in _GLOB_METACHARACTERS else character for character in literal)


def delete_matching_keys(
    redis_client: Any,
    key_pattern: str,
    scan_count: int = 100,
) -> int:
    """
    Delete Redis keys matching a scan pattern in small batches.

    Args:
        redis_client: Redis client used for deletion.
        key_pattern: Redis glob-style key pattern.
        scan_count: Requested Redis SCAN batch size, and the size of each DELETE.

    Returns:
        Number of keys deleted.

    Raises:
        RedisError: When Redis scan or delete fails.
    """
    # The scan is drained before anything is deleted. SCAN's cursor is defined
    # over the keyspace hash table, so deleting mid-scan can shrink that table
    # and make the cursor skip buckets — leaving matching keys behind, silently.
    # Callers use this to invalidate a whole prefix, where a partial delete means
    # stale data survives.
    keys_to_delete: list[str] = list(redis_client.scan_iter(match=key_pattern, count=scan_count))

    deleted_count = 0
    for start in range(0, len(keys_to_delete), scan_count):
        deleted_count += redis_client.delete(*keys_to_delete[start : start + scan_count])
    return deleted_count


def create_redis_client(
    storage_uri: str,
    purpose: str,
    socket_timeout: float = DEFAULT_REDIS_SOCKET_TIMEOUT_SECONDS,
    *,
    decode_responses: bool = True,
) -> Redis:
    """
    Create and verify a Redis client.

    Args:
        storage_uri: Redis storage URI.
        purpose: Human-readable use case for error messages.
        socket_timeout: Connection and read timeout in seconds.
        decode_responses: When True (default) responses are decoded to ``str``;
            pass False for byte-oriented callers (e.g. the platform StateProvider)
            that need raw ``bytes`` back.

    Returns:
        Connected Redis client.

    Raises:
        RuntimeError: When Redis cannot be initialized.
    """
    # Imported lazily: ``redis`` is the optional ``distributed`` extra, so this
    # is the single point where the package is actually loaded and only on the
    # Redis code path.
    from redis import Redis, RedisError

    try:
        redis_client = Redis.from_url(
            storage_uri,
            decode_responses=decode_responses,
            socket_connect_timeout=socket_timeout,
            socket_timeout=socket_timeout,
        )
        redis_client.ping()
    except (RedisError, ValueError) as redis_error:
        raise RuntimeError(f"Unable to initialize Redis storage for {purpose}.") from redis_error
    return redis_client


_shared_clients: dict[tuple[str, bool], Redis] = {}
_shared_clients_lock = threading.Lock()


def get_shared_client(
    storage_uri: str,
    *,
    purpose: str,
    decode_responses: bool = True,
) -> Redis:
    """
    Return a process-wide shared Redis client, creating it once per config.

    The platform Redis backends resolve their connection through here so the
    process opens *one* client per distinct ``(storage_uri, decode_responses)``
    pair instead of one per consumer.

    Args:
        storage_uri: Redis storage URI selecting the server.
        purpose: Human-readable use case for connection-error messages.
        decode_responses: Whether the client decodes responses to ``str``.

    Returns:
        The shared, connectivity-verified Redis client for this config.

    Raises:
        RuntimeError: When Redis cannot be initialized.
    """
    client_key = (storage_uri, decode_responses)
    with _shared_clients_lock:
        client = _shared_clients.get(client_key)
        if client is None:
            client = create_redis_client(storage_uri, purpose, decode_responses=decode_responses)
            _shared_clients[client_key] = client
        return client


def close_shared_clients() -> None:
    """
    Close every memoized client's connections, then discard them.

    Called from :meth:`jasil.container.Platform.close` so a process that shuts a
    platform down does not leave sockets open. A client that fails to close is
    logged and dropped anyway: shutdown must not raise.

    Returns:
        None.
    """
    with _shared_clients_lock:
        for (uri, _decode), client in _shared_clients.items():
            try:
                client.close()
            except Exception as error:
                scheme, _, _ = uri.partition("://")
                logger.warning("Failed to close the shared %s client: %r", scheme or "redis", error)
        _shared_clients.clear()


def reset_shared_clients() -> None:
    """
    Discard the memoized shared clients *without* closing them.

    For tests that inject a fake client, where closing would be meaningless or
    would break a fixture still holding it. Production shutdown wants
    :func:`close_shared_clients`.

    Raises:
        None.
    """
    with _shared_clients_lock:
        _shared_clients.clear()
