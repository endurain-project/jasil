"""The async publish seam: outbox vs. bus routing, and its failure contract.

The async counterpart of ``test_publisher.py``, protecting the same two
behaviours:

* **Routing** — an event goes to the durable outbox only when durable jobs are
  on *and* an async durable subscriber is registered; otherwise it goes on the
  bus.
* **Swallowing** — ``apublish`` must never raise into the producer, because the
  domain row is the source of truth and a failed publish is recovered by the
  subscriber's reconciliation net.

The routing tests carry extra weight here. The two faces keep independent
registries (an ``async def`` handler cannot be typed into the sync one), so the
async path has to consult the async registry. Reading the wrong one would not
fail — it would quietly downgrade every durable async event to best-effort,
losing the retry budget, the lease and the dead-letter queue while still
appearing to work.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

import jasil.jobs.registry as jobs_registry
import jasil.publisher as publisher
import jasil.runtime as runtime
import jasil.settings as settings
from jasil.events import META_REQUEST_ID
from jasil.jobs.models import EventOutbox

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class RecordingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, event) -> None:
        self.published.append(event)


class ExplodingBus:
    async def publish(self, event) -> None:
        raise RuntimeError("bus is down")


class RecordingRecorder:
    def __init__(self) -> None:
        self.queued: list = []

    async def record_queued(self, event) -> None:
        self.queued.append(event)


class FixedClock:
    def now(self) -> datetime:
        return T0


class FakeAsyncPlatform:
    def __init__(self, events=None, recorder=None) -> None:
        self.events = events if events is not None else RecordingBus()
        self.recorder = recorder
        self.clock = FixedClock()


@pytest.fixture
def platform(monkeypatch):
    """Install a fake async platform and clear both durable-subscriber registries."""
    installed = FakeAsyncPlatform()
    monkeypatch.setattr(runtime, "_active_async_platform", installed)
    jobs_registry.registry.clear()
    jobs_registry.async_registry.clear()
    yield installed
    jobs_registry.registry.clear()
    jobs_registry.async_registry.clear()


def _enable_durable_jobs():
    settings.configure(settings.JasilSettings(jobs=settings.JobSettings(enabled=True)))


async def _handler(_event) -> None:
    return None


async def _outbox_count(db) -> int:
    return (await db.execute(select(func.count()).select_from(EventOutbox))).scalar_one()


class TestBestEffortRouting:
    async def test_an_event_goes_on_the_bus_by_default(self, platform):
        await publisher.apublish("activity.created", {"id": 1}, source="test")

        assert len(platform.events.published) == 1
        assert platform.events.published[0].event_type == "activity.created"

    async def test_the_payload_and_source_reach_the_bus(self, platform):
        await publisher.apublish("activity.created", {"id": 1}, source="api:store")

        event = platform.events.published[0]
        assert event.payload == {"id": 1}
        assert event.source == "api:store"

    async def test_a_session_alone_does_not_trigger_durable_delivery(self, platform, async_db):
        """Durable jobs are off, so the session is ignored and nothing is queued."""
        await publisher.apublish("activity.created", {"id": 1}, source="test", db=async_db)

        assert len(platform.events.published) == 1
        assert await _outbox_count(async_db) == 0

    async def test_durable_jobs_without_a_subscriber_still_use_the_bus(self, platform, async_db):
        """Writing to the outbox with nothing to relay to would strand the row."""
        _enable_durable_jobs()

        await publisher.apublish("activity.created", {"id": 1}, source="test", db=async_db)

        assert len(platform.events.published) == 1
        assert await _outbox_count(async_db) == 0


class TestDurableRouting:
    async def test_an_event_goes_to_the_outbox_when_durably_subscribed(self, platform, async_db):
        _enable_durable_jobs()
        jobs_registry.async_registry.register("activity.created", "s", _handler)

        await publisher.apublish("activity.created", {"id": 1}, source="test", db=async_db)

        assert await _outbox_count(async_db) == 1
        assert platform.events.published == []

    async def test_a_sync_only_subscriber_does_not_make_the_async_path_durable(self, platform, async_db):
        """The two registries are independent; a sync registration is not an async one."""
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        await publisher.apublish("activity.created", {"id": 1}, source="test", db=async_db)

        assert len(platform.events.published) == 1
        assert await _outbox_count(async_db) == 0

    async def test_a_durable_event_is_recorded_as_queued(self, platform, async_db, monkeypatch):
        """The outbox path bypasses the bus, which is what normally records the
        lifecycle, so the dashboard would go dark without this."""
        recorder = RecordingRecorder()
        monkeypatch.setattr(runtime, "_active_async_platform", FakeAsyncPlatform(recorder=recorder))
        _enable_durable_jobs()
        jobs_registry.async_registry.register("activity.created", "s", _handler)

        await publisher.apublish("activity.created", {"id": 1}, source="test", db=async_db)

        assert len(recorder.queued) == 1

    async def test_durable_delivery_needs_a_session(self, platform, async_db):
        _enable_durable_jobs()
        jobs_registry.async_registry.register("activity.created", "s", _handler)

        await publisher.apublish("activity.created", {"id": 1}, source="test")

        assert len(platform.events.published) == 1
        assert await _outbox_count(async_db) == 0


class TestCorrelation:
    async def test_the_ambient_correlation_id_is_stamped(self, platform):
        import jasil.correlation as correlation

        correlation.set_correlation_id("req-42")

        await publisher.apublish("activity.created", {}, source="test")

        assert platform.events.published[0].metadata[META_REQUEST_ID] == "req-42"

    async def test_explicit_metadata_is_merged(self, platform):
        await publisher.apublish("activity.created", {}, source="test", metadata={"user_id": 9})

        assert platform.events.published[0].metadata["user_id"] == 9

    async def test_no_correlation_key_is_added_when_there_is_no_id(self, platform):
        await publisher.apublish("activity.created", {}, source="test")

        assert META_REQUEST_ID not in platform.events.published[0].metadata


class TestFailuresAreSwallowed:
    async def test_a_bus_failure_does_not_reach_the_producer(self, monkeypatch):
        monkeypatch.setattr(runtime, "_active_async_platform", FakeAsyncPlatform(events=ExplodingBus()))

        await publisher.apublish("activity.created", {}, source="test")

    async def test_a_bus_failure_is_logged(self, monkeypatch, caplog):
        monkeypatch.setattr(runtime, "_active_async_platform", FakeAsyncPlatform(events=ExplodingBus()))

        with caplog.at_level("ERROR"):
            await publisher.apublish("activity.created", {}, source="test")

        assert "Failed to publish event activity.created" in caplog.text

    async def test_publishing_before_startup_does_not_raise(self, monkeypatch):
        """A publish from a code path that runs before the platform is built must
        not take the process down."""
        monkeypatch.setattr(runtime, "_active_async_platform", None)

        await publisher.apublish("activity.created", {}, source="test")


class TestApublishCommitting:
    async def test_the_domain_commit_runs_on_the_best_effort_path(self, platform, async_db):
        committed = []

        async def _commit():
            committed.append(True)

        await publisher.apublish_committing("activity.created", {}, source="test", db=async_db, commit=_commit)

        assert committed == [True]
        assert len(platform.events.published) == 1

    async def test_the_commit_runs_before_the_bus_dispatch(self, platform, async_db):
        """The domain row is the source of truth, so it must be durable even if
        the dispatch then fails."""
        order = []

        async def _publish(_event):
            order.append("publish")

        async def _commit():
            order.append("commit")

        platform.events.publish = _publish

        await publisher.apublish_committing("activity.created", {}, source="test", db=async_db, commit=_commit)

        assert order == ["commit", "publish"]

    async def test_a_dispatch_failure_still_leaves_the_domain_committed(self, monkeypatch, async_db):
        monkeypatch.setattr(runtime, "_active_async_platform", FakeAsyncPlatform(events=ExplodingBus()))
        committed = []

        async def _commit():
            committed.append(True)

        await publisher.apublish_committing("activity.created", {}, source="test", db=async_db, commit=_commit)

        assert committed == [True]

    async def test_the_durable_path_stages_before_committing(self, platform, async_db):
        """Atomic delivery: the outbox row joins the caller's transaction, so it
        cannot be lost relative to the change that produced it."""
        _enable_durable_jobs()
        jobs_registry.async_registry.register("activity.created", "s", _handler)
        staged_at_commit = {}

        async def _commit():
            staged_at_commit["count"] = len((await async_db.execute(select(EventOutbox))).scalars().all())
            await async_db.commit()

        await publisher.apublish_committing("activity.created", {}, source="test", db=async_db, commit=_commit)

        assert staged_at_commit["count"] == 1

    async def test_a_staging_failure_propagates_so_the_caller_rolls_back(self, platform, async_db, monkeypatch):
        """Unlike the best-effort path, this one must *not* swallow: the domain
        change is still uncommitted, so the caller has to hear about it."""
        _enable_durable_jobs()
        jobs_registry.async_registry.register("activity.created", "s", _handler)

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("outbox is unavailable")

        monkeypatch.setattr(publisher, "_stage_in_outbox_async", _boom)

        async def _commit():
            raise AssertionError("commit must not run after a staging failure")

        with pytest.raises(RuntimeError, match="outbox is unavailable"):
            await publisher.apublish_committing("activity.created", {}, source="test", db=async_db, commit=_commit)


class TestApublishManyCommitting:
    async def test_every_payload_reaches_the_bus(self, platform, async_db):
        async def _commit():
            return None

        await publisher.apublish_many_committing(
            "activity.created", [{"id": 1}, {"id": 2}], source="test", db=async_db, commit=_commit
        )

        assert [event.payload["id"] for event in platform.events.published] == [1, 2]

    async def test_an_empty_batch_still_commits_exactly_once(self, platform, async_db):
        """Otherwise a caller with nothing to publish would leave work uncommitted."""
        committed = []

        async def _commit():
            committed.append(True)

        await publisher.apublish_many_committing("activity.created", [], source="test", db=async_db, commit=_commit)

        assert committed == [True]
        assert platform.events.published == []

    async def test_every_payload_is_staged_on_the_durable_path(self, platform, async_db):
        _enable_durable_jobs()
        jobs_registry.async_registry.register("activity.created", "s", _handler)

        async def _commit():
            await async_db.commit()

        await publisher.apublish_many_committing(
            "activity.created", [{"id": 1}, {"id": 2}, {"id": 3}], source="test", db=async_db, commit=_commit
        )

        assert await _outbox_count(async_db) == 3

    async def test_per_payload_metadata_is_applied(self, platform, async_db):
        async def _commit():
            return None

        await publisher.apublish_many_committing(
            "activity.created",
            [{"id": 1}, {"id": 2}],
            source="test",
            metadata_for=lambda payload: {"seq": payload["id"]},
            db=async_db,
            commit=_commit,
        )

        assert [event.metadata["seq"] for event in platform.events.published] == [1, 2]
