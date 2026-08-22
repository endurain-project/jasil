"""Async Redis-backed ``AsyncStateProvider`` backend.

This is the awaitable twin of :mod:`jasil.backends.state_redis`, following the
rule that every async backend mirrors its synchronous counterpart
method-for-method and name-for-name, with only the execution model changed.

The underlying client is ``redis.asyncio`` — the same package as ``redis``, but
with a coroutine API.  All network calls are therefore non-blocking: the event
loop is free while the command round-trips to Redis, which is the only reason to
prefer this backend over running the sync one in a threadpool.

**Construction and the factory pattern**

``__init__`` cannot be ``async``, so it cannot call ``await redis.ping()`` to
verify connectivity.  Doing the I/O in ``__init__`` synchronously would block the
loop.  The solution adopted here — and the same one used by the event-bus async
backend — is a module-level ``async def create_async_redis_state(...)`` factory
that awaits :func:`jasil._core.redis_clients.get_shared_async_client` (which
itself calls ``ping``) before handing the verified client to ``__init__``.
Callers that already hold a client (e.g. test fixtures) can construct directly.

**Lua script and script registration**

``register_script`` on ``redis.asyncio`` returns an async-callable script object
whose ``__call__`` is a coroutine, so all call-sites that invoke the script
become ``await``-expressions. The script source is identical to the sync
backend's; only the wiring differs. That identity is what guarantees the
tiered-failure semantics are exactly the same across execution models.

**iter_keys**

The :class:`~jasil.providers_async.AsyncStateProvider` protocol declares
``iter_keys`` as returning ``AsyncIterator[str]``, so this method is an
``async def`` generator.  ``redis.asyncio``'s ``scan_iter`` is itself an async
iterator, so we iterate it with ``async for`` and decode each key in the same
place the sync backend does.  Unlike the sync backend we do not materialise the
whole key list first: because the event loop is non-blocking, we can page
through ``scan_iter`` lazily without holding a lock.  A Redis failure mid-scan
will propagate out of the generator and be caught by the ``_translate_state_errors``
decorator on the calling async method — but because generators cannot be
decorated the same way, the error translation is done explicitly inside the method.
"""

import functools
import time
from collections.abc import AsyncIterator, Callable
from typing import Any, Concatenate

import jasil._core.redis_clients as redis_clients
from jasil.providers import StateBackendUnavailableError, TieredFailureOutcome

# Identical to the sync backend's script — the Lua semantics are the same on any
# Redis server regardless of which client library issues the EVALSHA.
_RECORD_TIERED_FAILURE_SCRIPT = """
local gate_key = KEYS[1]
local counter_key = KEYS[2]
local now = tonumber(ARGV[1])
local counter_ttl = tonumber(ARGV[2])

local gate = redis.call("GET", gate_key)
if gate and tonumber(gate) > now then
    local current = redis.call("GET", counter_key)
    return {tonumber(current) or 0, tonumber(gate), 0}
end
if gate then
    redis.call("DEL", gate_key)
end

local count = redis.call("INCR", counter_key)
redis.call("EXPIRE", counter_key, counter_ttl)

local lock_seconds = 0
local i = 3
while i < #ARGV do
    if count >= tonumber(ARGV[i]) then
        lock_seconds = tonumber(ARGV[i + 1])
    end
    i = i + 2
end

if lock_seconds > 0 then
    local gate_until = now + lock_seconds
    redis.call("SETEX", gate_key, lock_seconds, gate_until)
    return {count, gate_until, 1}
end

return {count, 0, 0}
"""


def _translate_state_errors[**P, R](
    method: Callable[Concatenate["AsyncRedisState", P], R],
) -> Callable[Concatenate["AsyncRedisState", P], R]:
    """Surface Redis outages from an ``AsyncRedisState`` method as a provider error.

    Wraps every public coroutine so that a ``RedisError`` raised by ``redis.asyncio``
    becomes the neutral :class:`~jasil.providers.StateBackendUnavailableError`
    that domain stores catch.  Domain code has no business knowing which client
    library backs the state store.

    Args:
        method: An ``AsyncRedisState`` coroutine method.

    Returns:
        The same method wrapped to translate ``RedisError``.
    """

    @functools.wraps(method)
    async def wrapper(self: "AsyncRedisState", *args: P.args, **kwargs: P.kwargs) -> Any:
        try:
            return await method(self, *args, **kwargs)  # type: ignore[misc]
        except redis_clients.RedisError as err:
            raise StateBackendUnavailableError("Redis state backend is unavailable") from err

    return wrapper  # type: ignore[return-value]


class AsyncRedisState:
    """``AsyncStateProvider`` backed by Redis — shared across workers and replicas.

    Correct for the ``distributed`` profile and multi-worker deployments.
    ``get`` / ``set`` store opaque ``bytes``; ``incr`` is an atomic server-side
    ``INCRBY``; ``record_tiered_failure`` runs the same atomic Lua script as the
    sync backend; TTLs map to Redis key expiry.

    Do not construct directly from a URI — use :func:`create_async_redis_state`,
    which awaits the connectivity check before returning an instance. Direct
    construction is reserved for test fixtures that supply an already-verified
    client.
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        # register_script on redis.asyncio returns an async-callable; awaiting
        # the result of calling it sends EVALSHA to Redis.
        self._record_failure_script = client.register_script(_RECORD_TIERED_FAILURE_SCRIPT)

    @_translate_state_errors
    async def get(self, key: str) -> bytes | None:
        """Return the value stored at *key*, or ``None`` if absent or expired.

        Args:
            key: The state key to look up.

        Returns:
            The stored bytes, or ``None``.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        return await self._client.get(key)

    @_translate_state_errors
    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        """Store *value* at *key*, optionally with a TTL.

        Args:
            key: The state key to write.
            value: Raw bytes to store.
            ttl_seconds: Seconds until the key expires; ``None`` means no expiry.

        Returns:
            None.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        if ttl_seconds is not None and ttl_seconds > 0:
            await self._client.set(key, value, ex=ttl_seconds)
        else:
            await self._client.set(key, value)

    @_translate_state_errors
    async def delete(self, key: str) -> None:
        """Remove *key* from the store; no-op if absent.

        Args:
            key: The state key to remove.

        Returns:
            None.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        await self._client.delete(key)

    @_translate_state_errors
    async def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        """Increment the integer stored at *key* by *amount*, returning the new value.

        ``INCRBY`` is atomic and leaves any existing TTL untouched; only (re)set
        expiry when the caller asks for one, mirroring the memory backend.

        Args:
            key: The state key to increment.
            amount: How much to add; defaults to 1.
            ttl_seconds: New TTL to apply after the increment; ``None`` preserves
                any existing expiry and sets none on a new key.

        Returns:
            The new integer value.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        new_value = int(await self._client.incrby(key, amount))
        if ttl_seconds is not None and ttl_seconds > 0:
            await self._client.expire(key, ttl_seconds)
        return new_value

    @_translate_state_errors
    async def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool:
        """Store *value* at *key* only if the key is absent or expired.

        Args:
            key: The state key to conditionally write.
            value: Raw bytes to store on success.
            ttl_seconds: TTL to apply if the key is written; ``None`` means no expiry.

        Returns:
            ``True`` if the value was stored, ``False`` if the key already existed.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        if ttl_seconds is not None and ttl_seconds > 0:
            return bool(await self._client.set(key, value, ex=ttl_seconds, nx=True))
        return bool(await self._client.set(key, value, nx=True))

    @_translate_state_errors
    async def get_and_delete(self, key: str) -> bytes | None:
        """Return the value at *key* and atomically remove it.

        Args:
            key: The state key to retrieve and delete.

        Returns:
            The stored bytes, or ``None`` if the key was absent or expired.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        return await self._client.getdel(key)

    @_translate_state_errors
    async def delete_prefix(self, prefix: str) -> int:
        """Delete all keys that start with *prefix*.

        The prefix is glob-escaped before being passed to Redis so a literal
        ``*`` in the prefix does not wipe the entire keyspace.

        Args:
            prefix: The key prefix to match for deletion.

        Returns:
            Number of keys deleted.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        # The sync helper ``delete_matching_keys`` is synchronous; for the async
        # backend we implement the same two-phase (scan-then-delete) strategy
        # inline using ``scan_iter`` from ``redis.asyncio``.
        pattern = f"{redis_clients.glob_escape(prefix)}*"
        scan_count = 100
        keys_to_delete: list[bytes | str] = []
        async for key in self._client.scan_iter(match=pattern, count=scan_count):
            keys_to_delete.append(key)

        deleted_count = 0
        for start in range(0, len(keys_to_delete), scan_count):
            deleted_count += await self._client.delete(*keys_to_delete[start : start + scan_count])
        return deleted_count

    async def iter_keys(self, prefix: str) -> AsyncIterator[str]:  # type: ignore[override]
        """Yield all keys whose names start with *prefix*.

        Unlike the sync backend, keys are streamed lazily via ``scan_iter``
        rather than materialised in one shot.  A ``RedisError`` raised mid-scan
        is translated to :class:`~jasil.providers.StateBackendUnavailableError`
        here rather than by the decorator (which cannot wrap an async generator).

        Args:
            prefix: Only keys that start with this string are yielded.

        Returns:
            An async iterator of matching key strings.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        pattern = f"{redis_clients.glob_escape(prefix)}*"
        try:
            async for key in self._client.scan_iter(match=pattern):
                yield key.decode() if isinstance(key, bytes) else key
        except redis_clients.RedisError as err:
            raise StateBackendUnavailableError("Redis state backend is unavailable") from err

    @_translate_state_errors
    async def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome:
        """Record a failure and return the current lockout outcome.

        Runs the same atomic Lua script as the sync backend so the tiered-failure
        semantics — no counter inflation while locked, last-match-wins tier
        selection — are guaranteed even under concurrent async calls on separate
        event loops (which share the Redis server).

        Args:
            counter_key: State key holding the running failure count.
            gate_key: State key holding the lockout-expiry timestamp.
            tiers: Ascending ``(threshold, lock_seconds)`` pairs; last match wins.
            counter_ttl_seconds: TTL to apply to the counter key.

        Returns:
            A :class:`~jasil.providers.TieredFailureOutcome` describing the
            current count, gate expiry, and whether this call newly locked the key.

        Raises:
            StateBackendUnavailableError: When Redis cannot be reached.
        """
        script_args = [str(int(time.time())), str(counter_ttl_seconds)]
        for threshold, tier_lock_seconds in tiers:
            script_args.extend((str(threshold), str(tier_lock_seconds)))
        result = await self._record_failure_script(keys=[gate_key, counter_key], args=script_args)
        count = int(result[0])
        gate_until = int(result[1])
        newly_locked = bool(int(result[2]))
        return TieredFailureOutcome(count, gate_until or None, newly_locked)


async def create_async_redis_state(uri: str) -> "AsyncRedisState":
    """Build an :class:`AsyncRedisState` from a ``redis://…`` URI.

    Awaits the shared-client cache to verify connectivity before constructing the
    backend, so a misconfigured URI fails at startup rather than at first use.
    Uses a ``decode_responses=False`` client so ``get`` returns ``bytes`` (not
    ``str``) per the :class:`~jasil.providers_async.AsyncStateProvider` contract.

    Args:
        uri: A ``redis://…`` URI selecting the Redis server.

    Returns:
        A connected, verified :class:`AsyncRedisState` instance.

    Raises:
        RuntimeError: When Redis cannot be initialized.
    """
    client = await redis_clients.get_shared_async_client(uri, purpose="platform state", decode_responses=False)
    return AsyncRedisState(client)
