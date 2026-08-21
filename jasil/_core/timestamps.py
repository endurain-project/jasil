"""Timestamp coercion for values that have round-tripped through a database.

SQLite has no timezone type, so a ``DateTime(timezone=True)`` column reads back
*naive*; PostgreSQL and MySQL return it aware. Every comparison against a
provider clock therefore has to normalise first, or it raises ``TypeError`` on
SQLite and silently compares wrong offsets elsewhere.

MySQL's ``DATETIME`` also keeps only whole seconds, and **rounds** rather than
truncates — a timestamp written at ``.65`` reads back a third of a second in the
future. Nothing JASIL persists may therefore be compared for equality with the
value that was written, and an age measured against one can come out negative;
:func:`age_seconds` is where that is absorbed.
"""

from datetime import UTC, datetime

__all__ = ["age_seconds", "as_utc"]


def as_utc(moment: datetime) -> datetime:
    """Return ``moment`` as an aware UTC datetime, assuming UTC when it is naive.

    Args:
        moment: A timestamp, possibly naive after a SQLite round-trip.

    Returns:
        The same instant, guaranteed timezone-aware.
    """
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def age_seconds(moment: datetime | None, now: datetime) -> float | None:
    """Return how many seconds have passed between ``moment`` and ``now``.

    Args:
        moment: The earlier instant, or ``None``.
        now: The reference instant.

    Returns:
        The age in seconds, never negative, or ``None`` when ``moment`` is
        ``None``. A row written moments ago can carry a timestamp fractionally
        *ahead* of ``now`` — MySQL rounds a stored ``DATETIME`` to the nearest
        second — and "this has been pending for -0.3 seconds" is not something a
        dashboard should have to explain.
    """
    if moment is None:
        return None
    return max(0.0, (now - as_utc(moment)).total_seconds())
