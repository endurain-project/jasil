"""Bounds on the values JASIL persists.

The envelope's ``event_id`` / ``event_type`` / ``source`` and a durable
``subscriber_id`` all land in fixed-width columns. Left unchecked, an over-long
value raises a truncation error on PostgreSQL and MySQL, is silently accepted on
SQLite, and — because the publish seam swallows delivery failures — costs the
event either way. Checking where the value enters the system turns that into one
clear error at the producing call site, identically on every database.

Stored failure text is bounded too, but by truncation rather than refusal: an
exception message is diagnostic, so clipping it loses nothing that matters, while
refusing it would lose the failure record itself.
"""

__all__ = ["MAX_STORED_ERROR_LENGTH", "check_length"]

#: Cap on the failure text written to ``event_log.error_message`` and
#: ``processing_jobs.last_error``. Both are unbounded ``Text`` columns, so this
#: exists to stop one pathological exception (a driver dumping a whole query, a
#: deeply nested cause chain) from bloating every retry's row.
MAX_STORED_ERROR_LENGTH = 4000


def check_length(value: str, *, field: str, limit: int) -> None:
    """Raise when ``value`` would not fit the ``limit``-character column for ``field``.

    Args:
        value: The identifier to check.
        field: Its name, used in the error message.
        limit: The character limit of the column it is stored in.

    Raises:
        ValueError: When ``value`` is longer than ``limit``. These identifiers are
            written by the producing code, never by users, so an over-long one is
            a bug to fix rather than a value to clip: a truncated ``event_type``
            would route to the wrong subscribers, or to none at all.
    """
    if len(value) > limit:
        raise ValueError(
            f"{field} is {len(value)} characters, which exceeds the {limit}-character limit "
            f"and would be rejected or truncated on write: {value[:limit]!r}..."
        )
