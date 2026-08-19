"""The durable-jobs wiring: worker lifecycle and the scheduled maintenance jobs.

This module is what an application actually calls, and everything in it reaches
for process-wide state — the active platform, the session factory, the durable
subscriber registry, and a module-global worker handle. The tests therefore drive
it against a real assembled platform and a real database rather than mocks; what
is asserted is the wiring, since the behaviour underneath is covered by
:mod:`tests.test_jobs`.
"""

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import jasil.container as container
import jasil.jobs.crud as jobs_crud
import jasil.jobs.outbox as jobs_outbox
import jasil.jobs.registry as jobs_registry
import jasil.jobs.relay as jobs_relay
import jasil.jobs.service as jobs_service
import jasil.runtime as platform_runtime
import jasil.settings as settings
from jasil.events import new_event
from jasil.jobs.models import EventOutbox, ProcessingJob

WAIT_TIMEOUT = 5.0


@pytest.fixture
def platform(tmp_path, session_factory, monkeypatch):
    """A real local-profile platform, published process-wide for this test only."""
    built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
    monkeypatch.setattr(platform_runtime, "_active_platform", built)
    return built


@pytest.fixture(autouse=True)
def _clear_registry():
    """The durable-subscriber registry is a process-wide singleton."""
    yield
    jobs_registry.registry.clear()


@pytest.fixture(autouse=True)
def _clear_worker(monkeypatch):
    """So is the worker handle; a leaked thread would outlive its test."""
    monkeypatch.setattr(jobs_service, "_worker", None)
    yield
    jobs_service.stop_job_worker()


class IdleRunner:
    """A runner that touches nothing.

    The lifecycle tests are about the module-global handle, not about claiming;
    :mod:`tests.test_worker` drives the loop itself. Keeping the background
    thread away from the database also keeps it away from SQLite's per-thread
    connection, which the main thread's ``dispose()`` cannot close.
    """

    def run_once(self) -> int:
        return 0


class TestBuildRunner:
    def test_it_wires_the_runner_from_settings_and_the_platform(self, platform, session_factory):
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(lease_seconds=17, batch_size=3)))

        runner = jobs_service.build_runner()

        assert runner._lease_seconds == 17
        assert runner._batch_size == 3
        assert runner._clock is platform.clock
        assert runner._registry is jobs_registry.registry

    def test_the_worker_id_identifies_the_process(self, platform, session_factory):
        """Competing consumers have to be distinguishable in ``locked_by``."""
        import os

        assert str(os.getpid()) in jobs_service.build_runner()._worker_id


class TestWorkerLifecycle:
    @pytest.fixture(autouse=True)
    def idle_runner(self, monkeypatch):
        monkeypatch.setattr(jobs_service, "build_runner", IdleRunner)

    def test_starting_installs_a_worker(self):
        jobs_service.start_job_worker()

        assert jobs_service._worker is not None

    def test_starting_twice_keeps_one_worker(self):
        jobs_service.start_job_worker()
        first = jobs_service._worker

        jobs_service.start_job_worker()

        assert jobs_service._worker is first

    def test_stopping_clears_the_worker(self):
        jobs_service.start_job_worker()

        jobs_service.stop_job_worker()

        assert jobs_service._worker is None

    def test_stopping_when_none_is_running_is_a_no_op(self):
        jobs_service.stop_job_worker()

        assert jobs_service._worker is None

    def test_the_poll_interval_comes_from_settings(self):
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(poll_interval_seconds=0.25)))

        jobs_service.start_job_worker()

        assert jobs_service._worker._poll_interval_seconds == 0.25


class TestScheduledRelay:
    def test_it_relays_pending_outbox_rows_into_jobs(self, platform, session_factory, db):
        jobs_registry.registry.register("order.created", "invoice.render", lambda _e: None)
        jobs_outbox.add_to_outbox(new_event("order.created", {"id": 1}, source="api"), now=platform.clock.now(), db=db)

        jobs_service.relay_outbox_scheduled()

        db.rollback()
        assert db.query(ProcessingJob).one().subscriber_id == "invoice.render"

    def test_it_keeps_draining_until_the_outbox_is_empty(self, platform, session_factory, db):
        """One pass is bounded by the batch size, so a backlog needs several."""
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(batch_size=2)))
        jobs_registry.registry.register("order.created", "invoice.render", lambda _e: None)
        for index in range(5):
            jobs_outbox.add_to_outbox(
                new_event("order.created", {"id": index}, source="api"), now=platform.clock.now(), db=db
            )

        jobs_service.relay_outbox_scheduled()

        db.rollback()
        assert db.query(EventOutbox).filter(EventOutbox.relayed_at.is_(None)).count() == 0
        assert db.query(ProcessingJob).count() == 5

    def test_an_empty_outbox_costs_one_pass(self, platform, session_factory, monkeypatch):
        """It must stop at the first empty batch, not run the full bound."""
        passes = []
        monkeypatch.setattr(jobs_relay, "relay_outbox_once", lambda **kwargs: passes.append(kwargs) or 0)

        jobs_service.relay_outbox_scheduled()

        assert len(passes) == 1

    def test_one_run_is_bounded(self, platform, session_factory, monkeypatch):
        """A pathological backlog must yield rather than monopolise the scheduler."""
        passes = []
        monkeypatch.setattr(jobs_relay, "relay_outbox_once", lambda **kwargs: passes.append(kwargs) or 1)

        jobs_service.relay_outbox_scheduled()

        assert len(passes) == jobs_service._MAX_RELAY_BATCHES


class TestScheduledReaper:
    def test_it_requeues_an_expired_lease(self, platform, session_factory, db):
        event = new_event("order.created", {"id": 1}, source="api")
        now = platform.clock.now()
        jobs_crud.enqueue_job(event, "invoice.render", max_attempts=3, now=now, db=db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=-1, now=now, db=db)

        jobs_service.reap_expired_jobs_scheduled()

        db.rollback()
        assert db.query(ProcessingJob).one().status == jobs_crud.STATUS_PENDING

    def test_it_is_quiet_when_there_is_nothing_to_reap(self, platform, session_factory, caplog):
        with caplog.at_level("INFO"):
            jobs_service.reap_expired_jobs_scheduled()

        assert "Reaped" not in caplog.text


class TestScheduleJobMaintenance:
    @pytest.fixture
    async def scheduler(self):
        """A started-but-paused scheduler.

        It has to be started for jobs to reach the jobstore — a pending scheduler
        just queues ``add_job`` calls, where ``replace_existing`` has no meaning.
        Async, because ``AsyncIOScheduler.start`` binds the running loop, and
        paused so nothing fires while the test inspects it.
        """
        scheduler = AsyncIOScheduler()
        scheduler.start(paused=True)
        yield scheduler
        scheduler.shutdown(wait=False)

    async def test_it_registers_the_relay_and_the_reaper(self, scheduler):
        jobs_service.schedule_job_maintenance(scheduler)

        assert {job.id for job in scheduler.get_jobs()} == {jobs_service._RELAY_JOB_ID, jobs_service._REAP_JOB_ID}

    async def test_registering_twice_replaces_rather_than_duplicates(self, scheduler):
        """Otherwise a re-entrant startup would run every maintenance job twice."""
        jobs_service.schedule_job_maintenance(scheduler)

        jobs_service.schedule_job_maintenance(scheduler)

        assert len(scheduler.get_jobs()) == 2
