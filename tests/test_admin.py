"""The operator-facing facade in front of the two CRUD modules.

What is under test is not the aggregation itself (:mod:`tests.test_jobs` and
:mod:`tests.test_event_log` cover that) but the two properties the facade exists
to provide: it is importable from anywhere in a host's import graph, and it never
commits a session the caller handed it — because it is handed none.
"""

import inspect

import pytest

import jasil.admin as jasil_admin
import jasil.container as container
import jasil.event_log.crud as event_log_crud
import jasil.jobs.crud as jobs_crud
import jasil.runtime as platform_runtime
import jasil.settings as settings
from jasil.events import new_event

SUBSCRIBER = "invoice.render"


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
    """Drive a job to ``dead_letter`` through the real state machine."""
    event = new_event("order.created", {"order_id": 1}, source="api:create_order")
    job = jobs_crud.enqueue_job(event, SUBSCRIBER, max_attempts=1, now=now, db=db)
    jobs_crud.claim_jobs(worker_id="worker-1", limit=10, lease_seconds=60, now=now, db=db)
    jobs_crud.mark_job_failed(job.id, "boom", base_seconds=1, max_seconds=1, now=now, db=db)
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
            "JobSubscriberStats",
            "JobsSummary",
        ],
    )
    def test_every_response_schema_is_reachable(self, name):
        assert hasattr(jasil_admin, name)
        assert name in jasil_admin.__all__
