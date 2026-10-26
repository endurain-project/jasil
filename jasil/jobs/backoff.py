"""Retry backoff schedule for durable jobs.

The exponential schedule is deterministic; *equal jitter* is layered on top so a
burst of jobs that fail together (e.g. a downstream outage) do not all retry at
the same instant and stampede the recovering dependency. The worker adds the
computed delay to the current instant to set a failed job's ``available_at``.
"""

import random

_MAX_SHIFT = 30  # cap the exponent so ``base * 2**n`` cannot overflow


def defer_seconds(
    requested_seconds: float,
    *,
    max_seconds: float,
    jitter: bool = True,
) -> float:
    """
    Compute how long a deferred job waits.

    The request is clamped to ``[0, max_seconds]``; when ``jitter`` is set up to
    10% of it is added, so jobs deferred to the same instant (a shared rate limit)
    do not wake together. The result never exceeds ``max_seconds``, jitter included.

    Args:
        requested_seconds: How far ahead the handler asked to run again; may be
            negative or absurdly large.
        max_seconds: Ceiling on the wait.
        jitter: Whether to add up to 10% of the delay.

    Returns:
        The wait in seconds, between 0 and ``max_seconds``.
    """
    delay = min(max(requested_seconds, 0.0), max_seconds)
    if jitter:
        delay += random.uniform(0.0, delay * 0.1)  # noqa: S311 - timing spread, not security
    return min(delay, max_seconds)


def backoff_seconds(
    attempts: int,
    *,
    base_seconds: float,
    max_seconds: float,
    jitter: bool = True,
) -> float:
    """
    Compute the retry delay after ``attempts`` failed attempts.

    Exponential: ``base_seconds * 2 ** (attempts - 1)``, clamped to
    ``max_seconds``. The first retry (``attempts == 1``) waits ``base_seconds``.
    When ``jitter`` is set (the default) *equal jitter* is applied — the delay is
    randomised to between 50% and 100% of the computed value — so many jobs that
    fail at once spread their retries instead of retrying in lockstep (avoids a
    thundering-herd storm at scale). Pass ``jitter=False`` for the deterministic
    delay (used in tests).

    Args:
        attempts: Number of attempts made so far (>= 1 for a retry).
        base_seconds: Delay before the first retry.
        max_seconds: Upper bound on the delay.
        jitter: Whether to apply equal jitter to the computed delay.

    Returns:
        The retry delay in seconds, never negative and never above ``max_seconds``.
    """
    if attempts <= 1:
        delay = base_seconds
    else:
        shift = min(attempts - 1, _MAX_SHIFT)
        delay = base_seconds * (2**shift)
    delay = min(delay, max_seconds)
    if jitter:
        # Equal jitter: keep half the delay, randomise the other half. Retry
        # timing is not security-sensitive, so the stdlib PRNG is fine.
        delay = delay * 0.5 + random.uniform(0.0, delay * 0.5)  # noqa: S311
    return max(0.0, delay)
