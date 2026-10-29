"""Bounds on the values JASIL persists.

Two shapes of bound, because the values differ in kind:

* :func:`check_length` **raises**. The envelope's ``event_id`` / ``event_type`` /
  ``source`` and a durable ``subscriber_id`` are written by the producing code,
  never by a user, so an over-long one is a bug to fix rather than a value to
  clip — a truncated ``event_type`` routes to the wrong subscribers, or to none.
* :func:`fit_length` **truncates**, leaving a marker. Failure text, the joined
  subscriber list, and a worker's identity are derived and diagnostic, so
  clipping them loses nothing that matters while refusing would lose the record
  itself. The marker is what stops a reader assuming the value is complete.

Either way the bound is applied where the value enters the system, so an
over-long value is one clear outcome instead of a truncation error on PostgreSQL
and MySQL, silent acceptance on SQLite, and — because the publish seam swallows
delivery failures — a lost event on all three.

**Every constant here is imported by the model declaring the column it bounds**,
so a width and the code protecting it cannot drift apart. The one pair that was
allowed to drift is what produced the bug recorded on
:data:`MAX_HANDLER_NAME_LENGTH`.
"""

__all__ = [
    "MAX_AUDIT_ACTION_ID_LENGTH",
    "MAX_AUDIT_ID_LENGTH",
    "MAX_AUDIT_NAME_LENGTH",
    "MAX_AUDIT_RETENTION_DAYS",
    "MAX_HANDLER_NAME_LENGTH",
    "MAX_IP_ADDRESS_LENGTH",
    "MAX_PARTITION_DAYS_AHEAD",
    "MAX_REQUEST_ID_LENGTH",
    "MAX_STORED_ERROR_LENGTH",
    "MAX_USER_AGENT_LENGTH",
    "MAX_WORKER_ID_LENGTH",
    "MAX_WORKER_LABEL_LENGTH",
    "MAX_WORKER_ROLE_LENGTH",
    "check_length",
    "fit_length",
]

#: Cap on the failure text written to ``event_log.error_message`` and
#: ``processing_jobs.last_error``. Both are unbounded ``Text`` columns, so this
#: exists to stop one pathological exception (a driver dumping a whole query, a
#: deeply nested cause chain) from bloating every retry's row.
MAX_STORED_ERROR_LENGTH = 4000

#: Width of ``event_log.handler_name``, which holds the comma-joined list of every
#: subscriber that ran for one event — so its length grows with the subscriber
#: count and is unbounded from the writing layer's point of view. Overflowing it
#: made PostgreSQL reject the whole UPDATE, and because event-log writes are
#: deliberately best-effort (swallowed by the recorder so observability never
#: breaks processing) the failure was silent: the handlers had already run, so
#: the work completed while the row stayed ``published`` forever.
MAX_HANDLER_NAME_LENGTH = 500

#: Width of ``event_log.worker_id`` and ``processing_jobs.locked_by``. The event
#: bus writes a hostname-derived process identity; durable jobs write a UUIDv4
#: instance id. The former cannot be shortened by a deployment, so the library
#: guarantees the fit itself.
MAX_WORKER_ID_LENGTH = 100

#: Widths of optional operator-facing worker metadata. Unlike ``locked_by``,
#: these values are supplied directly by the host and are rejected if overlong.
MAX_WORKER_ROLE_LENGTH = 100
MAX_WORKER_LABEL_LENGTH = 200

_TRUNCATION_MARKER = "..."


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


def fit_length(value: str | None, limit: int) -> str | None:
    """Clamp ``value`` to ``limit`` characters, marking it when it had to be cut.

    For the derived, diagnostic values :func:`check_length` would be the wrong
    tool for: refusing one discards the very record being written.

    Args:
        value: The text about to be stored, or ``None``.
        limit: The character limit of the column it is stored in.

    Returns:
        ``value`` unchanged when it is ``None`` or already fits, otherwise
        exactly ``limit`` characters ending in ``...``, so a reader can tell the
        value was cut rather than assume it is complete.
    """
    if value is None or len(value) <= limit:
        return value
    return value[: limit - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER


#: Widths of the ``jasil_audit_log`` columns holding host-supplied identifiers. The
#: host writes the action id, the actor, credential and target ids, so an over-long
#: one is rejected (:func:`check_length`) rather than clipped: a truncated target id
#: would point an investigator at the wrong record.
MAX_AUDIT_ACTION_ID_LENGTH = 100
MAX_AUDIT_ID_LENGTH = 64
MAX_AUDIT_NAME_LENGTH = 50

#: Widths of the audit columns copied from the request. The client controls a
#: ``User-Agent`` and may influence a request id, so these are clipped
#: (:func:`fit_length`): refusing the request would lose the very row that records it.
MAX_REQUEST_ID_LENGTH = 100
MAX_USER_AGENT_LENGTH = 512
#: Longest textual IPv6 address, including an IPv4-mapped suffix.
MAX_IP_ADDRESS_LENGTH = 45

#: How many days of audit partitions may be created ahead: ``audit.partition_days_ahead`` is validated
#: against it, and ``jasil_audit_ensure_partitions`` refuses more.
MAX_PARTITION_DAYS_AHEAD = 366

#: Longest audit retention, and retention floor, in days (a century). ``audit.retention_days`` and
#: ``audit.retention_floor_days`` are validated against it, so every cutoff stays a representable date.
MAX_AUDIT_RETENTION_DAYS = 36_500
