"""The publish seam: outbox vs. bus routing, and its failure contract.

Two behaviours matter most and are easy to regress:

* **Routing** — an event goes to the durable outbox only when durable jobs are
  on *and* a durable subscriber is registered; otherwise it goes on the bus.
* **Swallowing** — ``publish`` must never raise into the producer, because the
  domain row is the source of truth and a failed publish is recovered by the
  subscriber's reconciliation net.
"""

from datetime import UTC, datetime

import pytest

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

    def publish(self, event) -> None:
        self.published.append(event)


class ExplodingBus:
    def publish(self, event) -> None:
        raise RuntimeError("bus is down")


class RecordingRecorder:
    def __init__(self) -> None:
        self.queued: list = []

    def record_queued(self, event) -> None:
        self.queued.append(event)


class FixedClock:
    def now(self) -> datetime:
        return T0


class FakePlatform:
    def __init__(self, events=None, recorder=None) -> None:
        self.events = events if events is not None else RecordingBus()
        self.recorder = recorder
        self.clock = FixedClock()


@pytest.fixture
def platform(monkeypatch):
    """Install a fake platform and clear the durable-subscriber registry."""
    installed = FakePlatform()
    monkeypatch.setattr(runtime, "_active_platform", installed)
    jobs_registry.registry.clear()
    yield installed
    jobs_registry.registry.clear()


def _enable_durable_jobs():
    settings.configure(settings.JasilSettings(jobs=settings.JobSettings(enabled=True)))


class TestBestEffortRouting:
    def test_an_event_goes_on_the_bus_by_default(self, platform):
        publisher.publish("activity.created", {"id": 1}, source="test")

        assert len(platform.events.published) == 1
        assert platform.events.published[0].event_type == "activity.created"

    def test_the_payload_and_source_reach_the_bus(self, platform):
        publisher.publish("activity.created", {"id": 1}, source="api:store")

        event = platform.events.published[0]
        assert event.payload == {"id": 1}
        assert event.source == "api:store"

    def test_a_session_alone_does_not_trigger_durable_delivery(self, platform, db):
        """Durable jobs are off, so the session is ignored and nothing is queued."""
        publisher.publish("activity.created", {"id": 1}, source="test", db=db)

        assert len(platform.events.published) == 1
        assert db.query(EventOutbox).count() == 0

    def test_durable_jobs_without_a_subscriber_still_use_the_bus(self, platform, db):
        """Writing to the outbox with nothing to relay to would strand the row."""
        _enable_durable_jobs()

        publisher.publish("activity.created", {"id": 1}, source="test", db=db)

        assert len(platform.events.published) == 1
        assert db.query(EventOutbox).count() == 0


class TestDurableRouting:
    def test_an_event_goes_to_the_outbox_when_durably_subscribed(self, platform, db):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish("activity.created", {"id": 1}, source="test", db=db)

        assert db.query(EventOutbox).count() == 1
        assert platform.events.published == []

    def test_a_durable_event_is_recorded_as_queued(self, platform, db, monkeypatch):
        """The outbox path bypasses the bus, which is what normally records the
        lifecycle, so the dashboard would go dark without this."""
        recorder = RecordingRecorder()
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=recorder))
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish("activity.created", {"id": 1}, source="test", db=db)

        assert len(recorder.queued) == 1

    def test_durable_delivery_needs_a_session(self, platform, db):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish("activity.created", {"id": 1}, source="test")

        assert len(platform.events.published) == 1
        assert db.query(EventOutbox).count() == 0


class TestCorrelation:
    def test_the_ambient_correlation_id_is_stamped(self, platform):
        import jasil.correlation as correlation

        correlation.set_correlation_id("req-42")

        publisher.publish("activity.created", {}, source="test")

        assert platform.events.published[0].metadata[META_REQUEST_ID] == "req-42"

    def test_explicit_metadata_is_merged(self, platform):
        publisher.publish("activity.created", {}, source="test", metadata={"user_id": 9})

        assert platform.events.published[0].metadata["user_id"] == 9

    def test_no_correlation_key_is_added_when_there_is_no_id(self, platform):
        publisher.publish("activity.created", {}, source="test")

        assert META_REQUEST_ID not in platform.events.published[0].metadata


class TestFailuresAreSwallowed:
    def test_a_bus_failure_does_not_reach_the_producer(self, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=ExplodingBus()))

        publisher.publish("activity.created", {}, source="test")

    def test_a_bus_failure_is_logged(self, monkeypatch, caplog):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=ExplodingBus()))

        with caplog.at_level("ERROR"):
            publisher.publish("activity.created", {}, source="test")

        assert "Failed to publish event activity.created" in caplog.text

    def test_publishing_before_startup_does_not_raise(self, monkeypatch):
        """A publish from a code path that runs before the platform is built must
        not take the process down."""
        monkeypatch.setattr(runtime, "_active_platform", None)

        publisher.publish("activity.created", {}, source="test")


class TestPublishCommitting:
    def test_the_domain_commit_runs_on_the_best_effort_path(self, platform, db):
        committed = []

        publisher.publish_committing(
            "activity.created", {}, source="test", db=db, commit=lambda: committed.append(True)
        )

        assert committed == [True]
        assert len(platform.events.published) == 1

    def test_the_commit_runs_before_the_bus_dispatch(self, platform, db):
        """The domain row is the source of truth, so it must be durable even if
        the dispatch then fails."""
        order = []
        platform.events.publish = lambda _e: order.append("publish")

        publisher.publish_committing(
            "activity.created", {}, source="test", db=db, commit=lambda: order.append("commit")
        )

        assert order == ["commit", "publish"]

    def test_a_dispatch_failure_still_leaves_the_domain_committed(self, monkeypatch, db):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=ExplodingBus()))
        committed = []

        publisher.publish_committing(
            "activity.created", {}, source="test", db=db, commit=lambda: committed.append(True)
        )

        assert committed == [True]

    def test_the_durable_path_stages_before_committing(self, platform, db):
        """Atomic delivery: the outbox row joins the caller's transaction, so it
        cannot be lost relative to the change that produced it."""
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)
        staged_at_commit = {}

        def _commit():
            staged_at_commit["count"] = db.query(EventOutbox).count()
            db.commit()

        publisher.publish_committing("activity.created", {}, source="test", db=db, commit=_commit)

        assert staged_at_commit["count"] == 1

    def test_a_staging_failure_propagates_so_the_caller_rolls_back(self, platform, db, monkeypatch):
        """All-or-nothing: a half-written unit of work must not be committed."""
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)
        monkeypatch.setattr(
            publisher.jobs_outbox,
            "add_to_outbox",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db down")),
        )

        with pytest.raises(RuntimeError, match="db down"):
            publisher.publish_committing("activity.created", {}, source="test", db=db, commit=lambda: None)


class TestPublishManyCommitting:
    def test_every_payload_is_published_on_the_bus(self, platform, db):
        publisher.publish_many_committing(
            "activity.deleted", [{"id": 1}, {"id": 2}], source="test", db=db, commit=lambda: None
        )

        assert len(platform.events.published) == 2

    def test_an_empty_batch_still_commits_exactly_once(self, platform, db):
        """The caller's unit of work must be committed either way."""
        committed = []

        publisher.publish_many_committing(
            "activity.deleted", [], source="test", db=db, commit=lambda: committed.append(True)
        )

        assert committed == [True]

    def test_the_durable_path_stages_one_outbox_row_per_payload(self, platform, db):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.deleted", "s", lambda _e: None)

        publisher.publish_many_committing(
            "activity.deleted", [{"id": 1}, {"id": 2}, {"id": 3}], source="test", db=db, commit=db.commit
        )

        assert db.query(EventOutbox).count() == 3

    def test_per_payload_metadata_is_applied(self, platform, db):
        publisher.publish_many_committing(
            "activity.deleted",
            [{"id": 1}, {"id": 2}],
            source="test",
            metadata_for=lambda payload: {"user_id": payload["id"]},
            db=db,
            commit=lambda: None,
        )

        assert [event.metadata["user_id"] for event in platform.events.published] == [1, 2]

    def test_one_failing_event_does_not_stop_the_batch(self, monkeypatch, db):
        """Best-effort per event: a single bad payload must not drop the rest."""
        bus = RecordingBus()
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=bus))
        calls = {"n": 0}

        def _flaky(event):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            bus.published.append(event)

        bus.publish = _flaky

        publisher.publish_many_committing(
            "activity.deleted", [{"id": 1}, {"id": 2}], source="test", db=db, commit=lambda: None
        )

        assert len(bus.published) == 1
