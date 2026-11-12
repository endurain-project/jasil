"""Turn the UUIDs in a JSON-bound value into their canonical strings.

JSON has no UUID type, and the envelope's payload and metadata are written to JSON columns and a Redis
stream. Converting once, when the envelope is built, means every route (in-process, outbox, durable job,
event log, Redis) carries the same value, rather than a ``UUID`` on one and a ``str`` on another.

Pure: no I/O.
"""

import uuid
from typing import Any

__all__ = ["stringify_uuids"]


def stringify_uuids(value: Any) -> Any:
    """Return ``value`` with every :class:`uuid.UUID` replaced by ``str(uuid)``.

    Recurses through dicts (keys and values), lists and tuples, and keeps each container's type, so a tuple
    stays a tuple. The input is never mutated; a container is copied. Any other value, including one JSON
    cannot encode, is returned as it is for the writer to refuse.

    Args:
        value: A JSON-bound value.

    Returns:
        The value with its UUIDs as strings.

    Raises:
        ValueError: When a dict holds a UUID key and the string of that UUID as another key, which would merge
            into one entry and lose a value. The message never names the keys or values.
    """
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        converted = {stringify_uuids(key): stringify_uuids(item) for key, item in value.items()}
        if len(converted) != len(value):
            raise ValueError("two dict keys become the same string once UUIDs are converted")
        return converted
    if isinstance(value, list):
        return [stringify_uuids(item) for item in value]
    if isinstance(value, tuple):
        return tuple(stringify_uuids(item) for item in value)
    return value
