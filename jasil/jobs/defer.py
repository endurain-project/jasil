"""``DeferJob`` — how a durable handler asks to be run again later.

Pure on purpose (no ORM, no I/O), so it can be imported from anywhere a handler
lives, ahead of the models being mapped.
"""

from datetime import datetime


class DeferJob(Exception):  # noqa: N818 - a control-flow signal, not an error
    """Raised by a durable handler to run again at ``retry_at``, without failing.

    Meant for a transient, time-bounded condition the handler can name, such as a
    provider rate limit that clears in a few minutes. Unlike any other exception
    it is not a failure: the job returns to ``pending``, available from
    ``retry_at``, and the claim does **not** count against ``max_attempts``. It
    never dead-letters and never runs ``on_dead_letter``.

    The runner clamps ``retry_at`` to between now and ``jobs.defer_max_seconds``
    ahead, and spreads it by up to 10% so jobs deferred to the same instant do not
    wake together. A job that has been deferred ``jobs.max_deferrals`` times is
    failed like any other exception on its next ``DeferJob``, so it cannot defer
    forever.

    The handler's side effects before raising are not rolled back; as with any
    retried handler they must be idempotent.

    Attributes:
        retry_at: Earliest instant the job may run again. A naive value is read
            as UTC.
        reason: Why the job was deferred, kept as the job's ``last_error`` and on
            the attempt.
    """

    def __init__(self, retry_at: datetime, reason: str | None = None) -> None:
        super().__init__(reason or f"deferred until {retry_at.isoformat()}")
        self.retry_at = retry_at
        self.reason = reason
