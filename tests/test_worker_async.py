"""The async durable-job worker loop, background task and service wiring.

This mirrors ``test_worker.py`` and the async additions to
``test_jobs_service.py``. The runner is scripted for loop tests so no test sleeps:
large poll intervals prove productive batches are not followed by an idle wait,
and ``asyncio.wait_for`` bounds shutdown paths that must remain cancellable.
"""

import asyncio
import inspect
from datetime import UTC, datetime

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import func, select

import jasil.jobs.crud_async as jobs_crud
import jasil.jobs.outbox_async as jobs_outbox
import jasil.jobs.registry as jobs_registry
import jasil.jobs.relay_async as jobs_relay
import jasil.jobs.service as jobs_service
import jasil.runtime as platform_runtime
import jasil.settings as settings
import jasil.testing as jasil_testing
from jasil.events import new_event
from jasil.jobs.models import EventOutbox, ProcessingJob
from jasil.jobs.worker_async import AsyncBackgroundWorker, run_worker_async

# Long enough that an unwanted poll would hang the test rather than pass slowly.
NEVER = 3600.0
WAIT_TIMEOUT = 5.0
T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class ScriptedRunner:
    """An ``AsyncJobRunner`` stand-in that plays a script, then ends the loop."""

    def __init__(self, script, stop: asyncio.Event) -> None:
        self._script = list(script)
        self._stop = stop
        self.calls = 0

    async def run_once(self) -> int:
        self.calls += 1
        if not self._script:
            self._stop.set()
            return 0
        outcome = self._script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class SignallingAsyncRunner:
    """Reports that it ran on the event loop."""

    def __init__(self) -> None:
        self.ran = asyncio.Event()
        self.calls = 0

    async def run_once(self) -> int:
        self.calls += 1
        self.ran.set()
        return 0


class IdleRunner:
    """A runner that touches nothing."""

    async def run_once(self) -> int:
        return 0


@pytest.fixture(autouse=True)
async def _clear_process_jobs_state(monkeypatch):
    """The registries and async worker handle are process-wide singletons."""
    jobs_registry.registry.clear()
    jobs_registry.async_registry.clear()
    monkeypatch.setattr(jobs_service, "_async_worker", None)
    yield
    await jobs_service.stop_async_job_worker()
    jobs_registry.registry.clear()
    jobs_registry.async_registry.clear()
    platform_runtime.reset()


@pytest.fixture
async def platform(tmp_path, async_session_factory):
    """A real local-profile async platform, published process-wide for this test only."""
    installed = await jasil_testing.install_async_test_platform(tmp_path, clock=jasil_testing.FixedClock(T0))
    yield installed
    await installed.aclose()


async def _noop(_event) -> None:
    return None


async def _processing_job_count(async_db) -> int:
    await async_db.rollback()
    return (await async_db.execute(select(func.count()).select_from(ProcessingJob))).scalar_one()


class TestRunWorkerAsync:
    async def test_an_already_stopped_worker_never_claims(self):
        stop = asyncio.Event()
        stop.set()
        runner = ScriptedRunner([1], stop)

        await run_worker_async(runner, poll_interval_seconds=NEVER, stop=stop)

        assert runner.calls == 0

    async def test_it_drains_a_backlog_without_pausing(self):
        """A productive batch loops straight into the next one."""
        stop = asyncio.Event()
        runner = ScriptedRunner([3, 2], stop)

        await run_worker_async(runner, poll_interval_seconds=NEVER, stop=stop)

        assert runner.calls == 3

    async def test_it_waits_when_the_queue_is_empty(self):
        stop = asyncio.Event()
        runner = ScriptedRunner([], stop)

        await run_worker_async(runner, poll_interval_seconds=NEVER, stop=stop)

        assert runner.calls == 1

    async def test_a_failing_iteration_does_not_end_the_loop(self):
        stop = asyncio.Event()
        runner = ScriptedRunner([RuntimeError("connection reset"), 1], stop)

        await run_worker_async(runner, poll_interval_seconds=0, stop=stop)

        assert runner.calls == 3

    async def test_a_failing_iteration_is_logged(self, caplog):
        stop = asyncio.Event()
        runner = ScriptedRunner([RuntimeError("connection reset"), 1], stop)

        with caplog.at_level("ERROR"):
            await run_worker_async(runner, poll_interval_seconds=0, stop=stop)

        assert "Async durable job worker iteration failed" in caplog.text

    async def test_cancelled_error_is_not_swallowed(self):
        stop = asyncio.Event()
        runner = ScriptedRunner([asyncio.CancelledError()], stop)

        with pytest.raises(asyncio.CancelledError):
            await run_worker_async(runner, poll_interval_seconds=0, stop=stop)


class TestAsyncBackgroundWorker:
    @pytest.fixture
    async def worker(self):
        worker = AsyncBackgroundWorker(SignallingAsyncRunner(), poll_interval_seconds=NEVER)
        yield worker
        await asyncio.wait_for(worker.stop(), timeout=WAIT_TIMEOUT)

    async def test_it_runs_the_loop_as_a_named_task(self, worker):
        worker._runner.ran.clear()

        await worker.start()

        await asyncio.wait_for(worker._runner.ran.wait(), timeout=WAIT_TIMEOUT)
        assert worker._task.get_name() == "async-durable-job-worker"

    async def test_starting_twice_leaves_one_task(self, worker):
        await worker.start()
        task = worker._task

        await worker.start()

        assert worker._task is task

    async def test_stopping_releases_the_task_promptly(self, worker):
        await worker.start()
        task = worker._task

        await asyncio.wait_for(worker.stop(), timeout=WAIT_TIMEOUT)

        assert worker._task is None
        assert task.done()

    async def test_stopping_a_worker_that_never_started_is_a_no_op(self, worker):
        await asyncio.wait_for(worker.stop(), timeout=WAIT_TIMEOUT)

        assert worker._task is None

    async def test_it_can_be_restarted(self, worker):
        await worker.start()
        await asyncio.wait_for(worker.stop(), timeout=WAIT_TIMEOUT)
        worker._runner.ran.clear()

        await worker.start()

        assert worker._task is not None
        await asyncio.wait_for(worker._runner.ran.wait(), timeout=WAIT_TIMEOUT)


class TestBuildAsyncRunner:
    async def test_it_wires_the_runner_from_settings_and_the_async_platform(self, platform, async_session_factory):
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(lease_seconds=17, batch_size=3)))

        runner = jobs_service.build_async_runner()

        assert runner._lease_seconds == 17
        assert runner._batch_size == 3
        assert runner._clock is platform.clock
        assert runner._registry is jobs_registry.async_registry

    async def test_the_worker_id_identifies_the_process(self, platform, async_session_factory):
        import os

        assert str(os.getpid()) in jobs_service.build_async_runner()._worker_id


class TestAsyncWorkerLifecycle:
    @pytest.fixture(autouse=True)
    def idle_runner(self, monkeypatch):
        monkeypatch.setattr(jobs_service, "build_async_runner", IdleRunner)

    async def test_starting_installs_a_worker(self):
        await jobs_service.start_async_job_worker()

        assert jobs_service._async_worker is not None

    async def test_starting_twice_keeps_one_worker(self):
        await jobs_service.start_async_job_worker()
        first = jobs_service._async_worker

        await jobs_service.start_async_job_worker()

        assert jobs_service._async_worker is first

    async def test_stopping_clears_the_worker(self):
        await jobs_service.start_async_job_worker()

        await asyncio.wait_for(jobs_service.stop_async_job_worker(), timeout=WAIT_TIMEOUT)

        assert jobs_service._async_worker is None

    async def test_stopping_when_none_is_running_is_a_no_op(self):
        await asyncio.wait_for(jobs_service.stop_async_job_worker(), timeout=WAIT_TIMEOUT)

        assert jobs_service._async_worker is None

    async def test_the_poll_interval_comes_from_settings(self):
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(poll_interval_seconds=0.25)))

        await jobs_service.start_async_job_worker()

        assert jobs_service._async_worker._poll_interval_seconds == 0.25


class TestAsyncScheduledRelay:
    async def test_it_relays_pending_outbox_rows_into_jobs(self, platform, async_session_factory, async_db):
        jobs_registry.async_registry.register("order.created", "invoice.render", _noop)
        await jobs_outbox.add_to_outbox(new_event("order.created", {"id": 1}, source="api"), now=T0, db=async_db)

        await jobs_service.relay_outbox_async_scheduled()

        await async_db.rollback()
        assert (await async_db.execute(select(ProcessingJob))).scalar_one().subscriber_id == "invoice.render"

    async def test_it_keeps_draining_until_the_outbox_is_empty(self, platform, async_session_factory, async_db):
        """One pass is bounded by the batch size, so a backlog needs several."""
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(batch_size=2)))
        jobs_registry.async_registry.register("order.created", "invoice.render", _noop)
        for index in range(5):
            await jobs_outbox.add_to_outbox(
                new_event("order.created", {"id": index}, source="api"), now=T0, db=async_db
            )

        await jobs_service.relay_outbox_async_scheduled()

        await async_db.rollback()
        pending_outbox = (
            await async_db.execute(
                select(func.count()).select_from(EventOutbox).where(EventOutbox.relayed_at.is_(None))
            )
        ).scalar_one()
        assert pending_outbox == 0
        assert await _processing_job_count(async_db) == 5

    async def test_an_empty_outbox_costs_one_pass(self, platform, async_session_factory, monkeypatch):
        """It must stop at the first empty batch, not run the full bound."""
        passes = []

        async def relay_once(**kwargs) -> int:
            passes.append(kwargs)
            return 0

        monkeypatch.setattr(jobs_relay, "relay_outbox_once", relay_once)

        await jobs_service.relay_outbox_async_scheduled()

        assert len(passes) == 1

    async def test_one_run_is_bounded(self, platform, async_session_factory, monkeypatch):
        """A pathological backlog must yield rather than monopolise the scheduler."""
        passes = []

        async def relay_once(**kwargs) -> int:
            passes.append(kwargs)
            return 1

        monkeypatch.setattr(jobs_relay, "relay_outbox_once", relay_once)

        await jobs_service.relay_outbox_async_scheduled()

        assert len(passes) == jobs_service._MAX_RELAY_BATCHES


class TestAsyncScheduledReaper:
    async def test_it_requeues_an_expired_lease(self, platform, async_session_factory, async_db):
        event = new_event("order.created", {"id": 1}, source="api")
        await jobs_crud.enqueue_job(event, "invoice.render", max_attempts=3, now=T0, db=async_db)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=-1, now=T0, db=async_db)

        await jobs_service.reap_expired_jobs_async_scheduled()

        await async_db.rollback()
        assert (await async_db.execute(select(ProcessingJob))).scalar_one().status == jobs_crud.STATUS_PENDING

    async def test_it_is_quiet_when_there_is_nothing_to_reap(self, platform, async_session_factory, caplog):
        with caplog.at_level("INFO"):
            await jobs_service.reap_expired_jobs_async_scheduled()

        assert "Async reaper: reaped" not in caplog.text


class TestScheduleAsyncJobMaintenance:
    @pytest.fixture
    async def scheduler(self):
        """A started-but-paused scheduler bound to the running event loop."""
        scheduler = AsyncIOScheduler()
        scheduler.start(paused=True)
        yield scheduler
        scheduler.shutdown(wait=False)

    async def test_it_registers_the_async_relay_and_reaper_coroutines(self, scheduler):
        jobs_service.schedule_async_job_maintenance(scheduler)

        jobs = {job.id: job for job in scheduler.get_jobs()}
        assert set(jobs) == {jobs_service._ASYNC_RELAY_JOB_ID, jobs_service._ASYNC_REAP_JOB_ID}
        assert all(inspect.iscoroutinefunction(job.func) for job in jobs.values())

    async def test_it_is_separate_from_the_sync_maintenance_schedule(self, scheduler):
        jobs_service.schedule_async_job_maintenance(scheduler)

        assert {job.id for job in scheduler.get_jobs()}.isdisjoint(
            {jobs_service._RELAY_JOB_ID, jobs_service._REAP_JOB_ID}
        )

    async def test_registering_twice_replaces_rather_than_duplicates(self, scheduler):
        jobs_service.schedule_async_job_maintenance(scheduler)

        jobs_service.schedule_async_job_maintenance(scheduler)

        assert len(scheduler.get_jobs()) == 2
