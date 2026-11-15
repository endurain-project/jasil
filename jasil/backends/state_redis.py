"""Redis-backed ``StateProvider`` backend.

Only imported by the composition root when ``state_uri`` selects Redis, so the
``local`` profile keeps using the in-memory backend. Backed by a ``decode_responses=False`` client so values round-trip as
raw ``bytes`` per the ``StateProvider`` contract; ``incr`` uses Redis ``INCRBY`` for
atomic, cross-process counters (rate limits, escalating gates) that a single-process dict
cannot provide.
"""

import functools
import math
import time
from collections.abc import Callable, Iterator
from typing import Any, Concatenate

import jasil._core.redis_clients as redis_clients
import jasil._core.state_keys as state_keys
from jasil.providers import SlotReservation, StateBackendUnavailableError, TieredFailureOutcome

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

# Increment and re-arm the expiry (KEYS: counter; ARGV: amount, ttl_seconds). A refused INCRBY raises and ends
# the script before EXPIRE, so a refused increment leaves the key's expiry as it was; a MULTI would still run it.
_INCR_WITH_TTL_SCRIPT = """
local count = redis.call("INCRBY", KEYS[1], ARGV[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
return count
"""

# Compare-and-delete and compare-and-expire (KEYS: key; ARGV: expected, then ttl_seconds). GET refuses a key of
# another type with WRONGTYPE, as the memory backend refuses an index.
_DELETE_IF_EQUALS_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"""

_EXPIRE_IF_EQUALS_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
end
return 0
"""

# Reserve a rate-gate slot (KEYS: gate; ARGV: interval, max_wait, both in microseconds). The slot is counted in
# microseconds of the server's own clock, so replicas whose clocks disagree still share one gate; at today's epoch
# that is below 2^53, where a Lua number is still an exact integer. GET refuses an index with WRONGTYPE, and a value
# that is not a whole number is refused as INCR refuses one. Returns {reserved, wait}.
_RESERVE_SLOT_SCRIPT = """
local time = redis.call("TIME")
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local interval = tonumber(ARGV[1])
local slot = now
local last = redis.call("GET", KEYS[1])
if last then
    local previous = tonumber(last)
    if previous == nil or previous ~= math.floor(previous) then
        return redis.error_reply("ERR value is not an integer or out of range")
    end
    slot = math.max(now, previous + interval)
end
local wait = slot - now
if wait > tonumber(ARGV[2]) then
    return {0, wait}
end
redis.call("SET", KEYS[1], string.format("%.0f", slot), "PX", math.ceil((slot + interval - now) / 1000))
return {1, wait}
"""

# Opens every index script (KEYS: index, deadlines; ARGV[1]: the caller's epoch time). It refuses a key of another
# type, discards deadlines an index left behind (``delete`` and ``delete_prefix`` remove only the index), and drops
# every member whose deadline has passed, so no script sees an expired member. ``rearm`` keeps both keys expiring
# with the latest deadline, so an index nobody writes to leaves nothing behind.
_INDEX_PRELUDE = """
local key, deadlines = KEYS[1], KEYS[2]
local now = tonumber(ARGV[1])
local kind = redis.call("TYPE", key)["ok"]
if kind == "none" then
    redis.call("DEL", deadlines)
elseif kind ~= "zset" then
    return redis.error_reply("WRONGTYPE Operation against a key holding the wrong kind of value")
end
for _, member in ipairs(redis.call("ZRANGEBYSCORE", deadlines, "-inf", now)) do
    redis.call("ZREM", key, member)
end
redis.call("ZREMRANGEBYSCORE", deadlines, "-inf", now)

local function rearm()
    local latest = redis.call("ZRANGE", deadlines, -1, -1, "WITHSCORES")
    if #latest > 0 then
        local ttl_ms = math.ceil((tonumber(latest[2]) - now) * 1000)
        redis.call("PEXPIRE", key, ttl_ms)
        redis.call("PEXPIRE", deadlines, ttl_ms)
    end
end
"""

# ARGV: now, ttl_seconds, score, member.
_INDEX_ADD_SCRIPT = (
    _INDEX_PRELUDE
    + """
redis.call("ZADD", key, ARGV[3], ARGV[4])
redis.call("ZADD", deadlines, now + tonumber(ARGV[2]), ARGV[4])
rearm()
return 1
"""
)

# ARGV: now, member.
_INDEX_REMOVE_SCRIPT = (
    _INDEX_PRELUDE
    + """
redis.call("ZREM", key, ARGV[2])
redis.call("ZREM", deadlines, ARGV[2])
rearm()
return 1
"""
)

# ARGV: now, limit, then the cursor's score and member when paging. Members tied at the cursor's score are compared
# byte by byte, the order Redis keeps them in, rather than with Lua's locale-dependent string comparison.
_INDEX_RANGE_SCRIPT = (
    _INDEX_PRELUDE
    + """
local limit = tonumber(ARGV[2])
if #ARGV == 2 then
    return redis.call("ZREVRANGE", key, 0, limit - 1, "WITHSCORES")
end

local function precedes(a, b)
    for i = 1, math.min(#a, #b) do
        local x, y = string.byte(a, i), string.byte(b, i)
        if x ~= y then
            return x < y
        end
    end
    return #a < #b
end

local page = {}
local tied = redis.call("ZREVRANGEBYSCORE", key, ARGV[3], ARGV[3], "WITHSCORES")
for i = 1, #tied, 2 do
    if #page == 2 * limit then
        return page
    end
    if precedes(tied[i], ARGV[4]) then
        page[#page + 1] = tied[i]
        page[#page + 1] = tied[i + 1]
    end
end
local remaining = limit - #page / 2
if remaining > 0 then
    local lower = redis.call("ZREVRANGEBYSCORE", key, "(" .. ARGV[3], "-inf", "WITHSCORES", "LIMIT", 0, remaining)
    for _, value in ipairs(lower) do
        page[#page + 1] = value
    end
end
return page
"""
)


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
    ``INCRBY``; ``record_tiered_failure`` and ``reserve_slot`` run atomic Lua scripts; TTLs map to
    Redis key expiry.
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        self._record_failure_script = client.register_script(_RECORD_TIERED_FAILURE_SCRIPT)
        self._incr_with_ttl_script = client.register_script(_INCR_WITH_TTL_SCRIPT)
        self._delete_if_equals_script = client.register_script(_DELETE_IF_EQUALS_SCRIPT)
        self._expire_if_equals_script = client.register_script(_EXPIRE_IF_EQUALS_SCRIPT)
        self._index_add_script = client.register_script(_INDEX_ADD_SCRIPT)
        self._index_remove_script = client.register_script(_INDEX_REMOVE_SCRIPT)
        self._index_range_script = client.register_script(_INDEX_RANGE_SCRIPT)
        self._reserve_slot_script = client.register_script(_RESERVE_SLOT_SCRIPT)

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
        if ttl_seconds is None:
            return int(self._client.incrby(key, amount))
        # One script, so a failure between the two cannot leave a counter that never expires.
        return int(self._incr_with_ttl_script(keys=[key], args=[str(amount), str(ttl_seconds)]))

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
    def delete_if_equals(self, key: str, expected: bytes) -> bool:
        """Delete ``key`` while it holds ``expected``, in one Lua script; return True when deleted."""
        return bool(self._delete_if_equals_script(keys=[key], args=[expected]))

    @_translate_state_errors
    def expire_if_equals(self, key: str, expected: bytes, ttl_seconds: int) -> bool:
        """Restart the expiry of ``key`` while it holds ``expected``, in one Lua script; return True when restarted."""
        _check_ttl(ttl_seconds)
        return bool(self._expire_if_equals_script(keys=[key], args=[expected, str(ttl_seconds)]))

    @_translate_state_errors
    def delete_prefix(self, prefix: str) -> int:
        """Delete every key starting with ``prefix``, matched literally; ``prefix`` must not be empty."""
        # Escaped, because Redis reads the pattern as a glob: an unescaped ``*``
        # in a caller's prefix would delete far beyond what it named.
        state_keys.check_delete_prefix(prefix)
        return redis_clients.delete_matching_keys(self._client, f"{redis_clients.glob_escape(prefix)}*")

    @_translate_state_errors
    def iter_keys(self, prefix: str) -> Iterator[str]:
        """Scan the keys starting with ``prefix`` and return them as an iterator."""
        # Materialised eagerly so a mid-scan Redis failure is translated here
        # rather than leaking out of a half-consumed generator.
        pattern = f"{redis_clients.glob_escape(prefix)}*"
        keys: dict[str, None] = {}  # SCAN may return a key more than once; order is kept
        for key in self._client.scan_iter(match=pattern):
            try:
                keys[key.decode() if isinstance(key, bytes) else key] = None
            except UnicodeDecodeError:
                continue  # not a key this provider wrote: it only ever stores text names
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

    @_translate_state_errors
    def reserve_slot(self, key: str, interval_seconds: float, *, max_wait_seconds: float) -> SlotReservation:
        """Reserve the gate's next free slot in one Lua script, on the Redis server's clock."""
        interval, max_wait = state_keys.slot_microseconds(interval_seconds, max_wait_seconds)
        reserved, wait = self._reserve_slot_script(keys=[key], args=[str(interval), str(max_wait)])
        return SlotReservation(bool(int(reserved)), state_keys.to_seconds(int(wait)))

    @_translate_state_errors
    def index_add(self, key: str, member: str, score: float, ttl_seconds: int) -> None:
        """Insert or replace ``member`` and its deadline, in one Lua script over the index and its deadlines."""
        _check_ttl(ttl_seconds)
        _check_score(score)
        # The caller's clock sets the deadline, as ``record_tiered_failure`` takes the caller's time.
        self._index_add_script(
            keys=_index_keys(key), args=[repr(time.time()), str(ttl_seconds), repr(float(score)), member]
        )

    @_translate_state_errors
    def index_remove(self, key: str, member: str) -> None:
        """Remove ``member`` and its deadline, in one Lua script."""
        self._index_remove_script(keys=_index_keys(key), args=[repr(time.time()), member])

    @_translate_state_errors
    def index_range(self, key: str, *, limit: int, before: tuple[float, str] | None = None) -> list[tuple[str, float]]:
        """Return a page of live members, in one Lua script; O(log n + limit) plus the members tied at the cursor."""
        _check_limit(limit)
        args = [repr(time.time()), str(limit)]
        if before is not None:
            _check_score(before[0])
            args += [repr(float(before[0])), before[1]]
        flat = self._index_range_script(keys=_index_keys(key), args=args)
        return [(flat[i].decode(), float(flat[i + 1])) for i in range(0, len(flat), 2)]


def _index_keys(key: str) -> list[str]:
    """Return the index key and the key of its deadlines.

    The deadlines are a sorted set of the same members scored by the epoch second each one expires. They sit
    under the reserved prefix rather than the index's own, so ``iter_keys`` and ``delete_prefix`` over the host's
    prefix see one key per index, as on the memory backend.
    """
    return [key, state_keys.INDEX_EXPIRY_PREFIX + key]


def _check_ttl(ttl_seconds: int | None) -> None:
    if ttl_seconds is not None and ttl_seconds <= 0:
        raise ValueError(f"ttl_seconds must be positive, or None for no expiry, not {ttl_seconds}")


def _check_score(score: float) -> None:
    if not math.isfinite(score):
        raise ValueError(f"an index score must be a finite number, not {score}")


def _check_limit(limit: int) -> None:
    if limit < 1:
        raise ValueError(f"limit must be at least 1, not {limit}")
