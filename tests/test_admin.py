"""The operator-facing facade in front of the two CRUD modules.

What is under test is not the aggregation itself (:mod:`tests.test_jobs` and
:mod:`tests.test_event_log` cover that) but the two properties the facade exists
to provide: it is importable from anywhere in a host's import graph, and it never
commits a session the caller handed it — because it is handed none.
"""

import base64
import inspect
import json
from datetime import timedelta

import pytest

import jasil.admin as jasil_admin
import jasil.container as container
import jasil.event_log.crud as event_log_crud
import jasil.jobs.crud as jobs_crud
import jasil.runtime as platform_runtime
import jasil.settings as settings
from jasil.events import new_event
from jasil.jobs.models import JobWorker

SUBSCRIBER = "invoice.render"


def _opaque_worker_cursor(value: object) -> str:
    payload = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


@pytest.fixture
def platform(tmp_path, session_factory, monkeypatch):
    """A real local-profile platform, published process-wide for this test only."""
    built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
    monkeypatch.setattr(platform_runtime, "_active_platform", built)
    return built


@pytest.fixture
def now(platform):
    return platform.clock.now()


def _dead_letter_job(db, now) -> str:
    """Drive a job to ``dead_letter`` through the real state machine.

    The claim happens a second after the enqueue, as a polling worker's would.
    Claiming at the *same* instant is not a case that occurs in practice, and it
    is unreproducible on MySQL, whose DATETIME rounds to the nearest second and
    so can place ``available_at`` fractionally in the future.
    """
    event = new_event("order.created", {"order_id": 1}, source="api:create_order")
    job = jobs_crud.enqueue_job(event, SUBSCRIBER, max_attempts=1, now=now, db=db)
    claimed_at = now + timedelta(seconds=1)
    claimed = jobs_crud.claim_jobs(worker_id="worker-1", limit=10, lease_seconds=60, now=claimed_at, db=db)[0]
    jobs_crud.mark_job_failed(
        job.id,
        "boom",
        worker_id="worker-1",
        attempt=claimed.attempts,
        base_seconds=1,
        max_seconds=1,
        now=claimed_at,
        db=db,
    )
    return job.id


class TestJobsSummary:
    def test_it_reports_a_seeded_job(self, platform, db, now):
        _dead_letter_job(db, now)

        summary = jasil_admin.get_jobs_summary()

        assert summary.total_jobs == 1
        assert summary.dead_letter == 1
        assert summary.recent_dead_letter[0].subscriber_id == SUBSCRIBER

    def test_the_window_is_configurable(self, platform, db):
        assert jasil_admin.get_jobs_summary(hours=6).window_hours == 6

    def test_the_dead_letter_limit_is_configurable(self, platform, db, now):
        _dead_letter_job(db, now)

        assert jasil_admin.get_jobs_summary(dead_letter_limit=0).recent_dead_letter == []

    def test_an_empty_database_summarizes_to_zero(self, platform, db):
        assert jasil_admin.get_jobs_summary().total_jobs == 0

    def test_it_reports_queue_counts(self, platform, db, now):
        jobs_crud.enqueue_job(
            new_event("order.created", {"order_id": 1}, source="api:create_order"),
            SUBSCRIBER,
            queue="campaign",
            max_attempts=3,
            now=now,
            db=db,
        )

        queue = jasil_admin.get_jobs_summary().by_queue[0]

        assert queue.queue == "campaign"
        assert queue.pending == 1
        assert queue.total == 1


class TestWorkersSummary:
    def test_it_derives_running_stale_and_stopped(self, platform, db, now):
        db.add_all(
            [
                JobWorker(instance_id="running", started_at=now, last_heartbeat_at=now, active_claimed_jobs=2),
                JobWorker(
                    instance_id="stale",
                    started_at=now - timedelta(hours=2),
                    last_heartbeat_at=now - timedelta(hours=1),
                    active_claimed_jobs=1,
                ),
                JobWorker(
                    instance_id="stopped",
                    started_at=now - timedelta(hours=2),
                    last_heartbeat_at=now,
                    stopped_at=now,
                    queues=["maintenance"],
                    role="maintenance",
                    label="nightly",
                    worker_metadata={"zone": "a"},
                    active_claimed_jobs=0,
                ),
            ]
        )
        db.commit()

        summary = jasil_admin.get_workers_summary(stale_after_seconds=60)

        assert (summary.running, summary.stale, summary.stopped) == (1, 1, 1)
        stopped = next(worker for worker in summary.workers if worker.instance_id == "stopped")
        assert stopped.queues == ["maintenance"]
        assert stopped.metadata == {"zone": "a"}

    def test_an_empty_registry_summarizes_to_zero(self, platform, db):
        assert jasil_admin.get_workers_summary().total_workers == 0

    def test_the_default_stale_threshold_is_three_heartbeat_intervals(self, platform, db):
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(heartbeat_interval_seconds=7)))

        assert jasil_admin.get_workers_summary().stale_after_seconds == 21

    def test_active_claims_are_derived_from_jobs_not_a_stale_heartbeat_snapshot(self, platform, db, now):
        db.add(
            JobWorker(
                instance_id="worker-1",
                started_at=now,
                last_heartbeat_at=now,
                active_claimed_jobs=7,
            )
        )
        db.commit()

        worker = jasil_admin.get_workers_summary().workers[0]

        assert worker.active_claimed_jobs == 0

    def test_workers_are_cursor_paginated_with_global_totals(self, platform, db, now):
        db.add_all(
            [
                JobWorker(instance_id=instance_id, started_at=now, last_heartbeat_at=now, active_claimed_jobs=0)
                for instance_id in ("worker-a", "worker-b", "worker-c")
            ]
        )
        db.commit()

        first = jasil_admin.get_workers_summary(limit=2)
        second = jasil_admin.get_workers_summary(limit=2, cursor=first.next_cursor)

        assert first.total_workers == second.total_workers == 3
        assert first.running == second.running == 3
        assert [worker.instance_id for worker in first.workers] == ["worker-c", "worker-b"]
        assert [worker.instance_id for worker in second.workers] == ["worker-a"]
        assert first.next_cursor is not None
        assert second.next_cursor is None

    @pytest.mark.parametrize("limit", [0, 501, True])
    def test_worker_page_size_is_bounded(self, platform, limit):
        with pytest.raises(ValueError, match="limit"):
            jasil_admin.get_workers_summary(limit=limit)

    @pytest.mark.parametrize("stale_after_seconds", [0, -1])
    def test_the_stale_threshold_must_be_positive(self, platform, stale_after_seconds):
        with pytest.raises(ValueError, match="stale_after_seconds"):
            jasil_admin.get_workers_summary(stale_after_seconds=stale_after_seconds)

    @pytest.mark.parametrize(
        "cursor",
        [
            "",
            "not-a-cursor",
            _opaque_worker_cursor({"started_at": "2026-01-01T00:00:00+00:00"}),
            _opaque_worker_cursor(["2026-01-01T00:00:00", "worker-1"]),
            _opaque_worker_cursor(["2026-01-01T00:00:00+00:00", ""]),
        ],
    )
    def test_an_invalid_worker_cursor_is_refused(self, platform, cursor):
        with pytest.raises(ValueError, match="cursor"):
            jasil_admin.get_workers_summary(cursor=cursor)


class TestEventLogSummary:
    def test_it_reports_a_recorded_event(self, platform, db):
        event_log_crud.record_published(new_event("order.created", {}, source="api:create_order"), db)

        summary = jasil_admin.get_event_log_summary()

        assert summary.total_events == 1
        assert summary.by_type[0].event_type == "order.created"

    def test_the_window_is_configurable(self, platform, db):
        assert jasil_admin.get_event_log_summary(hours=6).window_hours == 6

    def test_the_failure_limit_is_configurable(self, platform, db):
        event = new_event("order.created", {}, source="api:create_order")
        event_log_crud.record_published(event, db)
        event_log_crud.mark_failed(event.event_id, "h", "boom", 5, db)

        assert jasil_admin.get_event_log_summary(failure_limit=0).recent_failures == []


class TestReplay:
    def test_a_dead_letter_job_is_requeued(self, platform, db, now):
        job_id = _dead_letter_job(db, now)

        assert jasil_admin.replay_dead_letter_job(job_id).replayed is True

        db.rollback()  # the facade committed on its own session, not this one
        assert jobs_crud.get_job(job_id, db).status == jobs_crud.STATUS_PENDING

    def test_replaying_a_job_that_is_not_dead_lettered_reports_false(self, platform, db, now):
        job_id = _dead_letter_job(db, now)
        jasil_admin.replay_dead_letter_job(job_id)

        assert jasil_admin.replay_dead_letter_job(job_id).replayed is False

    def test_an_unknown_job_reports_false(self, platform, db):
        assert jasil_admin.replay_dead_letter_job("nope").replayed is False


class TestItOwnsItsSessions:
    """The reason this facade exists rather than a re-export of the CRUD modules.

    Every function in :mod:`jasil.jobs.crud` and :mod:`jasil.event_log.crud`
    takes a session and commits it. A host wiring an admin route would naturally
    pass the one its request already holds, and so have JASIL commit work it
    never meant to commit. Nothing here accepts a session, so that mistake is not
    available to make.
    """

    @pytest.mark.parametrize(
        "function",
        [
            jasil_admin.get_jobs_summary,
            jasil_admin.get_event_log_summary,
            jasil_admin.get_workers_summary,
            jasil_admin.replay_dead_letter_job,
        ],
    )
    def test_no_entry_point_accepts_a_session(self, function):
        assert "db" not in inspect.signature(function).parameters

    def test_every_entry_point_works_with_no_session_open(self, platform, session_factory, now):
        with session_factory() as seed:
            job_id = _dead_letter_job(seed, now)

        assert jasil_admin.get_jobs_summary().dead_letter == 1
        assert jasil_admin.get_event_log_summary() is not None
        assert jasil_admin.get_workers_summary() is not None
        assert jasil_admin.replay_dead_letter_job(job_id).replayed is True


class TestSchemasAreReExported:
    """A host types its routes from here, not from an internal module."""

    @pytest.mark.parametrize(
        "name",
        [
            "DeadLetterJob",
            "EventLogFailure",
            "EventLogPending",
            "EventLogSummary",
            "EventTypeStats",
            "JobReplayResult",
            "JobQueueStats",
            "JobSubscriberStats",
            "JobsSummary",
            "WorkerInfo",
            "WorkersSummary",
        ],
    )
    def test_every_response_schema_is_reachable(self, name):
        assert hasattr(jasil_admin, name)
        assert name in jasil_admin.__all__
