"""Timestamp coercion for values that have round-tripped through a database.

SQLite has no timezone type, so a ``DateTime(timezone=True)`` column reads back
*naive*; PostgreSQL and MySQL return it aware. Every comparison against a
provider clock therefore has to normalise first, or it raises ``TypeError`` on
SQLite and silently compares wrong offsets elsewhere.
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
        The age in seconds, or ``None`` when ``moment`` is ``None``.
    """
    if moment is None:
        return None
    return (now - as_utc(moment)).total_seconds()
