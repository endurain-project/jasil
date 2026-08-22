"""The async durable-job runner and outbox relay.

This mirrors ``test_runner_and_dispatch.py`` and the relay coverage in
``test_jobs.py``: the async face should claim, dispatch, retry, dead-letter and
fan out events exactly like the sync face, only awaiting coroutine handlers.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

import jasil.jobs.crud_async as jobs_crud
import jasil.jobs.outbox_async as jobs_outbox
import jasil.jobs.registry as jobs_registry
import jasil.jobs.relay_async as jobs_relay
import jasil.jobs.runner_async as runner_async
from jasil._core.timestamps import as_utc
from jasil.events import MAX_EVENT_TYPE_LENGTH, new_event
from jasil.jobs.models import EventOutbox, ProcessingJob
from jasil.jobs.registry import MAX_SUBSCRIBER_ID_LENGTH, AsyncJobHandlerRegistry
from jasil.jobs.runner_async import AsyncJobRunner, ClaimedJob

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class FixedClock:
    def __init__(self, now: datetime = T0) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


@pytest.fixture(autouse=True)
def _clear_registries():
    """Both durable-subscriber registries are process-wide singletons."""
    yield
    jobs_registry.registry.clear()
    jobs_registry.async_registry.clear()


async def _noop(_event) -> None:
    return None


async def _enqueue(async_db, subscriber="s", max_attempts=3):
    event = new_event("activity.created", {"id": 1}, source="test", metadata={"request_id": "req-1"})
    return await jobs_crud.enqueue_job(event, subscriber, max_attempts=max_attempts, now=T0, db=async_db)


async def _stored_job(async_db) -> ProcessingJob:
    """Re-read after the runner committed in its own short-lived sessions."""
    await async_db.rollback()
    return (await async_db.execute(select(ProcessingJob))).scalar_one()


async def _job_count(async_db) -> int:
    await async_db.rollback()
    return (await async_db.execute(select(func.count()).select_from(ProcessingJob))).scalar_one()


class TestAsyncJobHandlerRegistry:
    def test_the_async_registry_is_re_exported_from_runner_async(self):
        assert runner_async.AsyncJobHandlerRegistry is jobs_registry.AsyncJobHandlerRegistry
        assert runner_async.async_registry is jobs_registry.async_registry

    def test_an_over_long_subscriber_id_is_refused(self):
        with pytest.raises(ValueError, match="subscriber_id is 201 characters"):
            AsyncJobHandlerRegistry().register("a.b", "s" * 201, _noop)

    def test_an_over_long_event_type_is_refused(self):
        with pytest.raises(ValueError, match="event_type is 101 characters"):
            AsyncJobHandlerRegistry().register("e" * 101, "sub", _noop)

    def test_identifiers_at_the_limit_are_accepted(self):
        registry = AsyncJobHandlerRegistry()
        subscriber_id = "s" * MAX_SUBSCRIBER_ID_LENGTH

        registry.register("a" * MAX_EVENT_TYPE_LENGTH, subscriber_id, _noop)

        assert registry.subscribers_for("a" * MAX_EVENT_TYPE_LENGTH) == (subscriber_id,)
        assert registry.subscriber_ids() == frozenset({subscriber_id})
        assert registry.get(subscriber_id) is _noop

    def test_clear_removes_every_registration(self):
        registry = AsyncJobHandlerRegistry()
        registry.register("a.b", "sub", _noop)

        registry.clear()

        assert registry.subscribers_for("a.b") == ()
        assert registry.get("sub") is None


class TestClaimedJob:
    def test_it_is_a_detached_frozen_snapshot(self):
        job = ClaimedJob(
            id="job-1",
            event_id="event-1",
            event_type="activity.created",
            subscriber_id="s",
            source="test",
            payload={"id": 1},
            metadata={"request_id": "req-1"},
            attempts=1,
            timestamp=T0.isoformat(),
            schema_version=1,
        )

        with pytest.raises(Exception, match=r"cannot assign|FrozenInstanceError"):
            job.attempts = 2  # type: ignore[misc]


class TestAsyncJobRunner:
    @pytest.fixture
    def registry(self):
        return AsyncJobHandlerRegistry()

    @pytest.fixture
    def clock(self):
        return FixedClock()

    @pytest.fixture
    def runner(self, registry, clock, async_session_factory):
        return AsyncJobRunner(
            registry=registry,
            clock=clock,
            session_factory=async_session_factory,
            worker_id="worker-1",
            lease_seconds=60,
            batch_size=10,
            backoff_base_seconds=10,
            backoff_max_seconds=100,
        )

    async def test_an_empty_queue_processes_nothing(self, runner):
        assert await runner.run_once() == 0

    async def test_a_due_job_runs_its_handler(self, runner, registry, async_db):
        seen = []

        async def handler(event) -> None:
            seen.append(event)

        registry.register("activity.created", "s", handler)
        await _enqueue(async_db)

        processed = await runner.run_once()

        assert processed == 1
        assert len(seen) == 1

    async def test_the_handler_receives_the_rebuilt_envelope(self, runner, registry, async_db):
        seen = []

        async def handler(event) -> None:
            seen.append(event)

        registry.register("activity.created", "s", handler)
        await _enqueue(async_db)

        await runner.run_once()

        assert seen[0].event_type == "activity.created"
        assert seen[0].payload == {"id": 1}
        assert seen[0].metadata == {"request_id": "req-1"}
        assert seen[0].retry_count == 1

    async def test_a_successful_run_completes_the_job(self, runner, registry, async_db):
        registry.register("activity.created", "s", _noop)
        await _enqueue(async_db)

        await runner.run_once()

        assert (await _stored_job(async_db)).status == jobs_crud.STATUS_COMPLETED

    async def test_a_failing_handler_reschedules_the_job(self, runner, registry, async_db):
        async def _boom(_event) -> None:
            raise RuntimeError("handler failed")

        registry.register("activity.created", "s", _boom)
        await _enqueue(async_db)

        await runner.run_once()

        job = await _stored_job(async_db)
        assert job.status == jobs_crud.STATUS_PENDING
        assert as_utc(job.available_at) > T0
        assert "handler failed" in job.last_error

    async def test_a_failing_handler_dead_letters_once_attempts_are_exhausted(self, runner, registry, async_db):
        async def _boom(_event) -> None:
            raise RuntimeError("handler failed")

        registry.register("activity.created", "s", _boom)
        await _enqueue(async_db, max_attempts=1)

        await runner.run_once()

        assert (await _stored_job(async_db)).status == jobs_crud.STATUS_DEAD_LETTER

    async def test_a_job_with_no_registered_handler_fails_rather_than_vanishing(self, runner, async_db):
        await _enqueue(async_db, subscriber="gone", max_attempts=1)

        await runner.run_once()

        job = await _stored_job(async_db)
        assert job.status == jobs_crud.STATUS_DEAD_LETTER
        assert "no durable handler" in job.last_error

    async def test_one_failing_job_does_not_abort_the_batch(self, runner, registry, async_db):
        seen = []

        async def good(event) -> None:
            seen.append(event)

        async def bad(_event) -> None:
            raise RuntimeError("boom")

        registry.register("activity.created", "good", good)
        registry.register("activity.created", "bad", bad)
        event = new_event("activity.created", {"id": 1}, source="test")
        await jobs_crud.enqueue_job(event, "good", max_attempts=3, now=T0, db=async_db)
        await jobs_crud.enqueue_job(event, "bad", max_attempts=3, now=T0, db=async_db)

        processed = await runner.run_once()

        assert processed == 2
        assert len(seen) == 1

    async def test_a_finalize_failure_is_logged_and_does_not_escape(
        self, runner, registry, async_db, monkeypatch, caplog
    ):
        registry.register("activity.created", "s", _noop)
        await _enqueue(async_db)

        async def explode(*args, **kwargs) -> None:
            raise RuntimeError("database went away")

        monkeypatch.setattr(runner_async.jobs_crud, "mark_job_completed", explode)

        with caplog.at_level("ERROR"):
            assert await runner.run_once() == 1

        assert "Durable job could not be finalized" in caplog.text

    async def test_the_batch_size_bounds_a_pass(self, registry, clock, async_session_factory, async_db):
        registry.register("activity.created", "s", _noop)
        for index in range(5):
            await jobs_crud.enqueue_job(
                new_event("activity.created", {"i": index}, source="test"),
                "s",
                max_attempts=3,
                now=T0,
                db=async_db,
            )
        runner = AsyncJobRunner(
            registry=registry,
            clock=clock,
            session_factory=async_session_factory,
            worker_id="w",
            lease_seconds=60,
            batch_size=2,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
        )

        assert await runner.run_once() == 2

    async def test_reaping_requeues_an_expired_lease(self, runner, registry, clock, async_db):
        registry.register("activity.created", "s", _noop)
        await _enqueue(async_db)
        await jobs_crud.claim_jobs(worker_id="other", limit=10, lease_seconds=60, now=T0, db=async_db)
        clock.advance(61)

        assert await runner.reap_once() == 1
        assert (await _stored_job(async_db)).status == jobs_crud.STATUS_PENDING


class TestAsyncOutboxRelay:
    @pytest.fixture
    def registry(self):
        return AsyncJobHandlerRegistry()

    @pytest.fixture
    def clock(self):
        return FixedClock()

    async def test_an_outbox_row_fans_out_to_every_registered_async_subscriber(
        self, async_db, async_session_factory, clock, registry
    ):
        registry.register("activity.created", "a", _noop)
        registry.register("activity.created", "b", _noop)
        event = new_event("activity.created", {"activity_id": 7}, source="test")
        await jobs_outbox.add_to_outbox(event, now=T0, db=async_db)

        relayed = await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
        )

        await async_db.rollback()
        jobs = (await async_db.execute(select(ProcessingJob))).scalars().all()
        assert relayed == 1
        assert {job.subscriber_id for job in jobs} == {"a", "b"}

    async def test_a_relayed_row_is_stamped_and_not_relayed_twice(
        self, async_db, async_session_factory, clock, registry
    ):
        registry.register("activity.created", "a", _noop)
        await jobs_outbox.add_to_outbox(new_event("activity.created", {}, source="test"), now=T0, db=async_db)
        await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
        )

        second = await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
        )

        await async_db.rollback()
        outbox = (await async_db.execute(select(EventOutbox))).scalar_one()
        assert second == 0
        assert outbox.relayed_at is not None
        assert await _job_count(async_db) == 1

    async def test_an_event_with_no_subscribers_is_still_marked_relayed(
        self, async_db, async_session_factory, clock, registry
    ):
        await jobs_outbox.add_to_outbox(new_event("activity.created", {}, source="test"), now=T0, db=async_db)

        await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
        )

        await async_db.rollback()
        assert (await async_db.execute(select(EventOutbox))).scalar_one().relayed_at is not None
        assert await _job_count(async_db) == 0

    async def test_relaying_is_idempotent_across_overlapping_passes(
        self, async_db, async_session_factory, clock, registry
    ):
        registry.register("activity.created", "a", _noop)
        outbox_id = await jobs_outbox.add_to_outbox(
            new_event("activity.created", {}, source="test"), now=T0, db=async_db
        )
        await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
        )
        await async_db.execute(
            EventOutbox.__table__.update().where(EventOutbox.id == outbox_id).values(relayed_at=None)
        )
        await async_db.commit()

        await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
        )

        assert await _job_count(async_db) == 1

    async def test_a_failure_mid_pass_relays_nothing(
        self, async_db, async_session_factory, clock, registry, monkeypatch
    ):
        registry.register("activity.created", "a", _noop)
        await jobs_outbox.add_to_outbox(new_event("activity.created", {}, source="test"), now=T0, db=async_db)

        async def explode(*args, **kwargs):
            raise RuntimeError("database went away")

        monkeypatch.setattr(jobs_relay.jobs_crud, "enqueue_job", explode)

        with pytest.raises(RuntimeError):
            await jobs_relay.relay_outbox_once(
                registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=10
            )

        await async_db.rollback()
        assert (await async_db.execute(select(EventOutbox))).scalar_one().relayed_at is None
        assert await _job_count(async_db) == 0

    async def test_the_batch_size_bounds_a_pass(self, async_db, async_session_factory, clock, registry):
        for index in range(5):
            await jobs_outbox.add_to_outbox(
                new_event("activity.created", {"i": index}, source="test"), now=T0, db=async_db
            )

        relayed = await jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=async_session_factory, max_attempts=3, batch_size=2
        )

        assert relayed == 2
