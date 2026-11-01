"""``DeferJob`` and ``ContinueJob`` — how a durable handler asks to be run again later.

Pure on purpose (no ORM, no I/O), so it can be imported from anywhere a handler
lives, ahead of the models being mapped.
"""

import math
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


class ContinueJob(Exception):  # noqa: N818 - a control-flow signal, not an error
    """Raised by a durable handler that made progress and has work left, to run again after ``delay_seconds``.

    Meant for a handler that works through a large backlog in bounded passes (rate-limited sends, a
    paginated export, a large import) and requeues itself for the rest, so the worker stays free for other
    jobs in between. Raise it only when the pass settled something: a handler that is waiting for a
    condition raises :class:`DeferJob` instead.

    The job returns to ``pending``, available ``delay_seconds`` from now, clamped to between 0 and
    ``jobs.defer_max_seconds`` and not jittered. The claim counts neither against ``max_deferrals`` nor
    against ``max_attempts``, and it restarts the failure budget: ``max_attempts`` bounds the failures
    since the last continuation, not the length of a long but healthy run. A job that has continued
    ``jobs.max_continuations`` times is failed like any other exception on its next ``ContinueJob``, so it
    cannot continue forever.

    The handler's side effects before raising are not rolled back; as with any retried handler they must
    be idempotent.

    Attributes:
        delay_seconds: How long to wait before the next pass.
        reason: What the pass did or what is left, kept as the job's ``last_error`` and on the attempt.
    """

    def __init__(self, delay_seconds: float = 0.0, reason: str | None = None) -> None:
        """Validate the delay and build the signal.

        Args:
            delay_seconds: How long to wait before the next pass; clamped by the runner.
            reason: What the pass did or what is left.

        Raises:
            ValueError: When ``delay_seconds`` is not a finite number.
        """
        if not math.isfinite(delay_seconds):
            raise ValueError(f"ContinueJob.delay_seconds must be a finite number, not {delay_seconds}")
        super().__init__(reason or f"continuing after {delay_seconds} s")
        self.delay_seconds = delay_seconds
        self.reason = reason
