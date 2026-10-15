"""Opaque keyset-pagination cursors over ``(timestamp, id)``.

A cursor is the sort key of the last row of a page, so the next page is "rows
after this key": stable under concurrent inserts, unlike an offset. It is encoded
as URL-safe base64 of a small JSON array, so a host can hand it to a client
verbatim, and decoded defensively because it comes back from a request.

Pure: no I/O, no infrastructure.
"""

import base64
import binascii
import json
from datetime import datetime

from jasil._core.timestamps import as_utc

__all__ = ["decode_cursor", "encode_cursor"]

_MAX_CURSOR_LENGTH = 512


def encode_cursor(moment: datetime, identifier: str) -> str:
    """Encode the sort key of a page's last row.

    Args:
        moment: The row's timestamp (naive values are taken as UTC).
        identifier: The row's unique id, the tiebreaker within one timestamp.

    Returns:
        An opaque URL-safe string.
    """
    payload = json.dumps([as_utc(moment).isoformat(), identifier], separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_cursor(cursor: str, *, name: str) -> tuple[datetime, str]:
    """Decode a cursor produced by :func:`encode_cursor`.

    Args:
        cursor: The opaque string returned as a previous page's ``next_cursor``.
        name: What the cursor pages through, for the error message.

    Returns:
        ``(timestamp, id)``, the timestamp timezone-aware.

    Raises:
        ValueError: When ``cursor`` is not one this module produced.
    """
    try:
        if not cursor or len(cursor) > _MAX_CURSOR_LENGTH:
            raise ValueError
        padding = "=" * (-len(cursor) % 4)
        decoded = base64.b64decode(cursor + padding, altchars=b"-_", validate=True)
        values = json.loads(decoded)
        if not isinstance(values, list) or len(values) != 2 or not all(isinstance(value, str) for value in values):
            raise ValueError
        moment = datetime.fromisoformat(values[0])
        if moment.tzinfo is None or not values[1]:
            raise ValueError
        return moment, values[1]
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as error:
        raise ValueError(f"invalid {name} cursor") from error
