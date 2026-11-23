"""The key namespace JASIL reserves in a state store, and the argument rules both state backends enforce.

Kept here rather than on either backend: neither should have to import the other to share a rule they both
enforce, and the memory backend must refuse the same prefixes, and count rate-gate slots in the same units, as Redis.
"""

import math

#: Where the Redis backend keeps an index's member deadlines: this prefix plus the index key.
INDEX_EXPIRY_PREFIX = "jasil:state:index-expiry:"

#: The longest interval a rate gate takes. Bounding it keeps a slot, and the gate's expiry, within what Redis
#: accepts as a key expiry.
MAX_SLOT_INTERVAL_SECONDS = 86_400

#: Where a rate gate keeps what ``reserve_send`` adds to its slot pointer: the instant of its last send, and each
#: caller's booked slot. Outside the caller's own prefix, so ``iter_keys`` and ``delete_prefix`` over it see the gate
#: alone. Deleting these keys only loses slots, so ``delete_prefix`` need not refuse them.
GATE_PREFIX = "jasil:state:gate:"

#: The longest booking name ``reserve_send`` takes, in characters.
MAX_BOOKING_LENGTH = 200

#: How long a booking outlives its slot beyond a tenth of the wait: the time a deferred caller may take to be run
#: again (a job queued behind others), so a booking does not expire just before its caller comes back for it.
BOOKING_GRACE_SECONDS = 600

#: Rate-gate slots are counted in whole microseconds, so both backends do exact integer arithmetic.
_MICROSECONDS = 1_000_000

__all__ = [
    "BOOKING_GRACE_SECONDS",
    "GATE_PREFIX",
    "INDEX_EXPIRY_PREFIX",
    "MAX_BOOKING_LENGTH",
    "MAX_SLOT_INTERVAL_SECONDS",
    "booking_key",
    "booking_ttl_microseconds",
    "check_booking",
    "check_delete_prefix",
    "check_max_wait",
    "last_send_key",
    "slot_microseconds",
    "to_seconds",
]


def check_delete_prefix(prefix: str) -> None:
    """Refuse a ``delete_prefix`` prefix that is empty or reaches the reserved index namespace.

    A prefix that starts the reserved namespace (``"jasil:"``) or lies inside it would delete index deadlines
    while keeping their indexes, whose members would then never expire on their own.

    Args:
        prefix: The prefix the caller asked to delete.

    Raises:
        ValueError: When ``prefix`` is empty or overlaps :data:`INDEX_EXPIRY_PREFIX`.
    """
    if not prefix:
        raise ValueError("delete_prefix needs a non-empty prefix")
    if INDEX_EXPIRY_PREFIX.startswith(prefix) or prefix.startswith(INDEX_EXPIRY_PREFIX):
        raise ValueError(f"delete_prefix cannot reach the reserved index namespace {INDEX_EXPIRY_PREFIX!r}")


def check_max_wait(max_wait_seconds: float) -> None:
    """Refuse a ``max_wait_seconds`` that is negative or not finite.

    Raises:
        ValueError: When ``max_wait_seconds`` is negative, infinite or NaN.
    """
    if not (math.isfinite(max_wait_seconds) and max_wait_seconds >= 0):
        raise ValueError(f"max_wait_seconds must be a finite number of seconds, zero or more, not {max_wait_seconds}")


def slot_microseconds(interval_seconds: float, max_wait_seconds: float) -> tuple[int, int]:
    """Validate a ``reserve_slot`` call and convert its arguments to whole microseconds.

    The interval rounds up, so a gate never admits faster than asked, and the wait bound rounds down, so a
    caller never waits longer than it accepted.

    Returns:
        ``(interval, max_wait)`` in microseconds; the interval is at least 1.

    Raises:
        ValueError: When ``interval_seconds`` is not positive, finite and at most
            :data:`MAX_SLOT_INTERVAL_SECONDS`, or ``max_wait_seconds`` is negative or not finite.
    """
    if not (math.isfinite(interval_seconds) and 0 < interval_seconds <= MAX_SLOT_INTERVAL_SECONDS):
        raise ValueError(
            f"interval_seconds must be positive, finite and at most {MAX_SLOT_INTERVAL_SECONDS}, not {interval_seconds}"
        )
    check_max_wait(max_wait_seconds)
    return math.ceil(interval_seconds * _MICROSECONDS), math.floor(max_wait_seconds * _MICROSECONDS)


def to_seconds(microseconds: int) -> float:
    """Convert a slot wait in microseconds back to seconds."""
    return microseconds / _MICROSECONDS


def check_booking(booking: str, *, name: str = "booking") -> None:
    """Refuse a ``reserve_send`` booking name that is not a non-empty string of at most :data:`MAX_BOOKING_LENGTH`.

    Args:
        booking: The booking name.
        name: The argument the caller took it as, for the error message.

    Raises:
        ValueError: When ``booking`` is not a string, is empty, or is longer than :data:`MAX_BOOKING_LENGTH`.
    """
    if not isinstance(booking, str) or not 0 < len(booking) <= MAX_BOOKING_LENGTH:
        raise ValueError(f"{name} must be a non-empty string of at most {MAX_BOOKING_LENGTH} characters")


def last_send_key(key: str) -> str:
    """Return the key holding the instant the gate ``key`` last let a caller send, in microseconds."""
    return f"{GATE_PREFIX}sent:{key}"


def booking_key(key: str, booking: str) -> str:
    """Return the key holding the slot ``booking`` booked on the gate ``key``.

    The gate key's length leads, so no gate key and booking name pair can spell another pair's key.
    """
    return f"{GATE_PREFIX}booking:{len(key)}:{key}{booking}"


def booking_ttl_microseconds(wait: int) -> int:
    """Return how long a booking whose slot is ``wait`` microseconds away is kept: the wait, a tenth more, a grace.

    The tenth covers the jitter ``DeferJob`` adds to the retry, and the grace a caller run late.
    """
    return -(-wait * 11 // 10) + BOOKING_GRACE_SECONDS * _MICROSECONDS
