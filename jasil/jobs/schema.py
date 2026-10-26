"""Pydantic response schemas for the durable-jobs admin dashboard."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class DurationBuckets(BaseModel):
    """
    How long completed attempts took, in fixed ranges.

    The boundaries are part of this schema, so two deployments' dashboards are
    comparable. Counts are non-cumulative and sum to the completed attempts in
    the window. Hosts needing other boundaries or true percentiles compute them
    from the ``duration_ms`` log field.

    Attributes:
        under_100ms: ``< 100 ms``.
        under_1s: ``100 ms`` up to ``< 1 s``.
        under_10s: ``1 s`` up to ``< 10 s``.
        under_1m: ``10 s`` up to ``< 60 s``.
        under_5m: ``60 s`` up to ``< 300 s``.
        over_5m: ``>= 300 s``.
    """

    under_100ms: int = 0
    under_1s: int = 0
    under_10s: int = 0
    under_1m: int = 0
    under_5m: int = 0
    over_5m: int = 0


class JobSubscriberStats(BaseModel):
    """
    Per-subscriber job counts by status within the window.

    Attributes:
        subscriber_id: The durable subscriber.
        event_type: The event channel it reacts to.
        total: Total jobs for this subscriber in the window.
        pending: Count waiting to be claimed (includes backoff).
        claimed: Count currently leased by a worker.
        completed: Count that finished successfully.
        dead_letter: Count that exhausted retries.
        avg_duration_ms: Mean handler time of attempts that completed in the
            window; ``None`` when none did or attempts are not recorded.
        max_duration_ms: Slowest such attempt.
        duration_buckets: The same attempts in fixed duration ranges; ``None``
            when attempts are not recorded.
        superseded: Attempts that ended in the window by finishing after their
            job was taken (the handler ran twice); ``None`` when attempts are not
            recorded.
    """

    subscriber_id: str
    event_type: str
    total: int
    pending: int
    claimed: int
    completed: int
    dead_letter: int
    avg_duration_ms: float | None = None
    max_duration_ms: int | None = None
    duration_buckets: DurationBuckets | None = None
    superseded: int | None = None


class JobQueueStats(BaseModel):
    """Windowed job counts and all-time current backlog age for one named queue."""

    queue: str
    total: int
    pending: int
    claimed: int
    completed: int
    dead_letter: int
    oldest_pending_seconds: float | None
    avg_duration_ms: float | None = None
    max_duration_ms: int | None = None
    duration_buckets: DurationBuckets | None = None
    superseded: int | None = None


class DeadLetterJob(BaseModel):
    """
    A dead-lettered job, shown for inspection and replay.

    Attributes:
        id: The job id (used to replay it).
        event_id: The originating envelope event_id.
        event_type: The event channel.
        subscriber_id: The subscriber that failed.
        source: Where the originating event came from.
        attempts: Attempts made before dead-lettering.
        max_attempts: The attempt ceiling that was reached.
        last_error: The final failure reason.
        created_at: When the job was enqueued.
        updated_at: When the job was dead-lettered.
        completed_at: When the job reached its terminal state.
        last_worker_id: The worker instance that ran its final attempt; ``None``
            when attempts are not recorded or the history was pruned.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    event_id: str
    event_type: str
    subscriber_id: str
    source: str
    attempts: int
    max_attempts: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    last_worker_id: str | None = None


class JobsSummary(BaseModel):
    """
    The durable-jobs admin-dashboard payload.

    Attributes:
        window_hours: The look-back window applied to the counts.
        total_jobs: Total jobs enqueued within the window.
        pending: Window count waiting to be claimed.
        claimed: Window count currently leased.
        completed: Window count finished successfully.
        dead_letter: Window count that exhausted retries.
        oldest_pending_seconds: Age of the oldest unfinished job, in seconds.
        by_subscriber: Per-subscriber breakdown within the window.
        by_queue: Per-queue breakdown within the window.
        recent_dead_letter: The current dead-letter queue contents (most recent first).
    """

    window_hours: int
    total_jobs: int
    pending: int
    claimed: int
    completed: int
    dead_letter: int
    oldest_pending_seconds: float | None
    by_subscriber: list[JobSubscriberStats]
    by_queue: list[JobQueueStats] = Field(default_factory=list)
    recent_dead_letter: list[DeadLetterJob]


class JobReplayResult(BaseModel):
    """
    The outcome of replaying a dead-lettered job.

    Attributes:
        replayed: True when the job was requeued for a fresh run.
    """

    replayed: bool


WorkerStatus = Literal["running", "stale", "stopped"]


class JobAttemptInfo(BaseModel):
    """
    One attempt to run a durable job.

    Attributes:
        id: The attempt id.
        job_id: The job it ran.
        event_id: The originating event.
        event_type: The event channel.
        subscriber_id: The subscriber that ran.
        queue: The queue the job was claimed from.
        attempt: The attempt number at claim.
        worker_id: The worker instance that claimed it.
        claimed_at: When the worker took the lease.
        started_at: When the handler began; ``None`` if it never ran.
        finished_at: When the attempt ended; ``None`` while it is open.
        outcome: One of ``completed``, ``failed``, ``dead_letter``,
            ``lease_expired``, ``superseded``, ``released``, ``abandoned``;
            ``None`` while open.
        duration_ms: Handler wall time.
        error: The failure reason, if it failed.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    job_id: str
    event_id: str
    event_type: str
    subscriber_id: str
    queue: str
    attempt: int
    worker_id: str
    claimed_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    outcome: str | None
    duration_ms: int | None
    error: str | None


class JobDetail(BaseModel):
    """
    One job and its attempt history, newest attempt first.

    Attributes:
        id: The job id.
        event_id: The originating event.
        event_type: The event channel.
        subscriber_id: The durable subscriber.
        queue: The named queue.
        source: Where the event came from.
        status: ``pending``, ``claimed``, ``completed`` or ``dead_letter``.
        attempts: Claims made so far, deferred ones included.
        deferrals: Of those, the claims the handler deferred with ``DeferJob``;
            they do not count against ``max_attempts``.
        max_attempts: The attempt ceiling.
        locked_by: The worker holding it now, while claimed.
        last_error: The most recent failure reason.
        available_at: Earliest instant it may be claimed.
        lease_expires_at: When the current lease expires, while claimed.
        created_at: When it was enqueued.
        updated_at: When it last changed state.
        completed_at: When it reached a terminal state.
        history: Its attempts, newest first, bounded by the caller's limit.
        attempts_recorded: Whether this process records attempts. ``False`` means
            ``history`` may be empty because nothing was written, not because
            nothing ran.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    event_id: str
    event_type: str
    subscriber_id: str
    queue: str
    source: str
    status: str
    attempts: int
    deferrals: int = 0
    max_attempts: int
    locked_by: str | None
    last_error: str | None
    available_at: datetime
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    history: list[JobAttemptInfo] = Field(default_factory=list)
    attempts_recorded: bool = True


class EventJobs(BaseModel):
    """Every subscriber's job for one event, each with its attempts.

    Answers "which worker processed this event, for each subscriber?". An event's
    ``event_log`` row only records publication, so this is where its outcome lives.
    """

    event_id: str
    jobs: list[JobDetail]
    attempts_recorded: bool = True


class WorkerAttempts(BaseModel):
    """One bounded page of a worker instance's attempts, newest first."""

    instance_id: str
    attempts: list[JobAttemptInfo]
    next_cursor: str | None = None
    attempts_recorded: bool = True


class WorkerInfo(BaseModel):
    """Operator-facing state for one restart-unique worker instance.

    The ``completed`` .. ``abandoned`` counts are attempts that *ended* within
    the summary's window, and are ``None`` when attempts are not recorded.
    """

    instance_id: str
    started_at: datetime
    last_heartbeat_at: datetime
    stopped_at: datetime | None
    queues: list[str] | None
    role: str | None
    label: str | None
    metadata: dict[str, Any] | None
    active_claimed_jobs: int
    status: WorkerStatus
    completed: int | None = None
    failed: int | None = None
    dead_lettered: int | None = None
    lease_expired: int | None = None
    superseded: int | None = None
    released: int | None = None
    abandoned: int | None = None
    last_finished_at: datetime | None = None


class WorkersSummary(BaseModel):
    """Global worker totals and one bounded page of retained telemetry."""

    stale_after_seconds: float
    window_hours: int = 24
    claimed_by_stale_workers: int = 0
    total_workers: int
    running: int
    stale: int
    stopped: int
    workers: list[WorkerInfo]
    next_cursor: str | None = None
