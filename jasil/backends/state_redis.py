"""Redis-backed ``StateProvider`` backend.

Only imported by the composition root when ``state_uri`` selects Redis, so the
``local`` profile keeps using the in-memory backend. Backed by a ``decode_responses=False`` client so values round-trip as
raw ``bytes`` per the ``StateProvider`` contract; ``incr`` uses Redis ``INCRBY`` for
atomic, cross-process counters (rate limits, escalating gates) that a single-process dict
cannot provide.
"""

import functools
import time
from collections.abc import Callable, Iterator
from typing import Any, Concatenate

import jasil._core.redis_clients as redis_clients
from jasil.providers import StateBackendUnavailableError, TieredFailureOutcome

# Atomic escalating-gate script (KEYS: gate, counter; ARGV: now, counter_ttl,
# then (threshold, lock_seconds) pairs ascending). Mirrors the in-memory backend:
# an already-locked caller returns its current count WITHOUT incrementing, an
# expired gate is cleared, otherwise the counter is bumped and the highest
# crossed tier (last match wins) sets the gate.
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


# What Redis says when it refuses a command because of the key's value rather than its own condition.
_VALUE_REFUSALS = ("WRONGTYPE", "not an integer", "would overflow")


def _translate_state_errors[**P, R](
    method: Callable[Concatenate["RedisState", P], R],
) -> Callable[Concatenate["RedisState", P], R]:
    """Surface Redis failures from a ``RedisState`` method as provider-neutral errors.

    Keeps domain stores free of any Redis knowledge. Redis refusing a command for the key's value
    (``WRONGTYPE``, an ``INCR`` of a non-integer or past the integer range) is a ``ValueError``, as the memory
    backend raises. Everything else is ``StateBackendUnavailableError``: the ``ResponseError`` subclasses
    redis-py uses for server conditions (a read-only replica, out of memory, a missing permission) and the
    ones it leaves as a plain ``ResponseError`` (``MISCONF``, ``BUSY``, ``NOREPLICAS``).
    """

    @functools.wraps(method)
    def wrapper(self: "RedisState", *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return method(self, *args, **kwargs)
        except redis_clients.RedisError as err:
            if type(err) is redis_clients.ResponseError and any(text in str(err) for text in _VALUE_REFUSALS):
                raise ValueError(f"Redis refused the state operation: {err}") from err
            raise StateBackendUnavailableError("Redis state backend is unavailable") from err

    return wrapper


class RedisState:
    """``StateProvider`` backed by Redis — shared across workers and replicas.

    Correct for the ``distributed`` profile and multi-worker deployments.
    ``get`` / ``set`` store opaque ``bytes``; ``incr`` is an atomic server-side
    ``INCRBY``; ``record_tiered_failure`` runs an atomic Lua script; TTLs map to
    Redis key expiry.
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        self._record_failure_script = client.register_script(_RECORD_TIERED_FAILURE_SCRIPT)

    @classmethod
    def from_uri(cls, uri: str) -> "RedisState":
        """Build from a ``redis://…`` URI, verifying connectivity eagerly.

        Uses a ``decode_responses=False`` client so ``get`` returns ``bytes``
        (not ``str``) per the ``StateProvider`` contract.
        """
        client = redis_clients.get_shared_client(uri, purpose="platform state", decode_responses=False)
        return cls(client)

    @_translate_state_errors
    def get(self, key: str) -> bytes | None:
        """Return the value under ``key``, or ``None`` when absent or expired."""
        return self._client.get(key)

    @_translate_state_errors
    def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        """Store ``value`` under ``key``, expiring after ``ttl_seconds`` when given."""
        _check_ttl(ttl_seconds)
        if ttl_seconds is not None:
            self._client.set(key, value, ex=ttl_seconds)
        else:
            self._client.set(key, value)

    @_translate_state_errors
    def delete(self, key: str) -> None:
        """Delete ``key`` if present."""
        self._client.delete(key)

    @_translate_state_errors
    def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        """Add ``amount`` with ``INCRBY``; restart the expiry only when ``ttl_seconds`` is given."""
        # INCRBY is atomic and leaves any existing TTL untouched; only (re)set
        # expiry when the caller asks for one, mirroring the memory backend.
        _check_ttl(ttl_seconds)
        new_value = int(self._client.incrby(key, amount))
        if ttl_seconds is not None:
            self._client.expire(key, ttl_seconds)
        return new_value

    @_translate_state_errors
    def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool:
        """Store ``value`` with ``SET NX``; return True when this call stored it."""
        _check_ttl(ttl_seconds)
        if ttl_seconds is not None:
            return bool(self._client.set(key, value, ex=ttl_seconds, nx=True))
        return bool(self._client.set(key, value, nx=True))

    @_translate_state_errors
    def get_and_delete(self, key: str) -> bytes | None:
        """Return and delete the value with ``GETDEL``."""
        return self._client.getdel(key)

    @_translate_state_errors
    def delete_prefix(self, prefix: str) -> int:
        """Delete every key starting with ``prefix``, matched literally; ``prefix`` must not be empty."""
        # Escaped, because Redis reads the pattern as a glob: an unescaped ``*``
        # in a caller's prefix would delete far beyond what it named.
        _check_prefix(prefix)
        return redis_clients.delete_matching_keys(self._client, f"{redis_clients.glob_escape(prefix)}*")

    @_translate_state_errors
    def iter_keys(self, prefix: str) -> Iterator[str]:
        """Scan the keys starting with ``prefix`` and return them as an iterator."""
        # Materialised eagerly so a mid-scan Redis failure is translated here
        # rather than leaking out of a half-consumed generator.
        pattern = f"{redis_clients.glob_escape(prefix)}*"
        keys = [key.decode() if isinstance(key, bytes) else key for key in self._client.scan_iter(match=pattern)]
        return iter(keys)

    @_translate_state_errors
    def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome:
        """Count one occurrence and close the gate, atomically in a Lua script."""
        script_args = [str(int(time.time())), str(counter_ttl_seconds)]
        for threshold, tier_lock_seconds in tiers:
            script_args.extend((str(threshold), str(tier_lock_seconds)))
        result = self._record_failure_script(keys=[gate_key, counter_key], args=script_args)
        count = int(result[0])
        gate_until = int(result[1])
        newly_locked = bool(int(result[2]))
        return TieredFailureOutcome(count, gate_until or None, newly_locked)


def _check_ttl(ttl_seconds: int | None) -> None:
    if ttl_seconds is not None and ttl_seconds <= 0:
        raise ValueError(f"ttl_seconds must be positive, or None for no expiry, not {ttl_seconds}")


def _check_prefix(prefix: str) -> None:
    if not prefix:
        raise ValueError("delete_prefix needs a non-empty prefix")
