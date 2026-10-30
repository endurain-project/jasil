"""Time-ordered UUIDs (version 7, RFC 9562).

Python 3.12 has no ``uuid.uuid7``. A v7 UUID leads with a millisecond timestamp, so ids sort by creation
time and an index on them is appended to rather than scattered across. Within one process the ids are also
strictly increasing: the 12 bits after the version hold a counter (RFC 9562, method 1), so two ids minted
in the same millisecond keep their order, which a row-ordering tiebreak relies on.

Pure: no I/O beyond the clock and the system random source.
"""

import os
import threading
import time
import uuid

__all__ = ["uuid7"]

_lock = threading.Lock()
_last_milliseconds = 0
_last_counter = 0

_COUNTER_BITS = 12


def uuid7() -> uuid.UUID:
    """Return a new version 7 UUID, greater than every one this process returned before.

    Returns:
        A UUID whose string form sorts by creation time. If the clock steps backwards the timestamp
        holds at the last value and the counter keeps the ids increasing.
    """
    global _last_milliseconds, _last_counter
    with _lock:
        milliseconds = time.time_ns() // 1_000_000
        if milliseconds > _last_milliseconds:
            # Start the counter low, so a burst within the millisecond has room before it overflows.
            counter = int.from_bytes(os.urandom(2), "big") & (2**_COUNTER_BITS // 2 - 1)
        else:
            milliseconds = _last_milliseconds
            counter = _last_counter + 1
            if counter >= 2**_COUNTER_BITS:
                milliseconds += 1
                counter = 0
        _last_milliseconds, _last_counter = milliseconds, counter
    random_bits = int.from_bytes(os.urandom(8), "big") & (2**62 - 1)
    value = (milliseconds & (2**48 - 1)) << 80 | 0x7 << 76 | counter << 64 | 0b10 << 62 | random_bits
    return uuid.UUID(int=value)
