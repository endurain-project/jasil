"""Timestamp coercion for values that have round-tripped through a database.

SQLite has no timezone type, so a ``DateTime(timezone=True)`` column reads back
*naive*; PostgreSQL and MySQL return it aware. Every comparison against a
provider clock therefore has to normalise first, or it raises ``TypeError`` on
SQLite and silently compares wrong offsets elsewhere.

JASIL's columns keep microseconds on every database (``DATETIME(6)`` on MySQL and
MariaDB from migration ``rev0012``). A MySQL database not yet migrated keeps whole
seconds and **rounds** rather than truncates, so a timestamp written at ``.65``
reads back a third of a second in the future, and a row stamped by the database's
clock can be ahead of the process's: an age measured against a stored timestamp
can come out negative, and :func:`age_seconds` is where that is absorbed.
"""

from datetime import UTC, datetime

__all__ = ["age_seconds", "as_utc"]


def as_utc(moment: datetime) -> datetime:
    """Return ``moment`` as an aware UTC datetime, assuming UTC when it is naive.

    An aware value is converted, not just passed through: PostgreSQL hands a
    ``timestamptz`` back in the session's ``TimeZone``, and anything rendered from
    it (a hashed or signed string, a file name) must not depend on that setting.

    Args:
        moment: A timestamp, possibly naive after a SQLite round-trip.

    Returns:
        The same instant, timezone-aware with a UTC offset.
    """
    return moment.astimezone(UTC) if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def age_seconds(moment: datetime | None, now: datetime) -> float | None:
    """Return how many seconds have passed between ``moment`` and ``now``.

    Args:
        moment: The earlier instant, or ``None``.
        now: The reference instant.

    Returns:
        The age in seconds, never negative, or ``None`` when ``moment`` is
        ``None``. A row written moments ago can carry a timestamp fractionally
        *ahead* of ``now`` — stamped by another clock, or rounded to the nearest
        second by a MySQL ``DATETIME`` from before ``rev0012`` — and "this has been
        pending for -0.3 seconds" is not something a dashboard should have to explain.
    """
    if moment is None:
        return None
    return max(0.0, (now - as_utc(moment)).total_seconds())
