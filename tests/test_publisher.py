"""The publish seam: outbox vs. bus routing, and its failure contract.

Two behaviours matter most and are easy to regress:

* **Routing** — an event goes to the durable outbox only when durable jobs are
  on *and* a durable subscriber is registered; otherwise it goes on the bus.
* **Swallowing** — ``publish`` must never raise into the producer, because the
  domain row is the source of truth and a failed publish is recovered by the
  subscriber's reconciliation net.
"""

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session

import jasil.correlation as correlation
import jasil.jobs.outbox as jobs_outbox
import jasil.jobs.registry as jobs_registry
import jasil.publisher as publisher
import jasil.runtime as runtime
import jasil.settings as settings
import jasil.testing as jasil_testing
from jasil.event_log.models import EventLog
from jasil.event_log.recorder import EventLogRecorder
from jasil.events import META_REQUEST_ID
from jasil.jobs.models import EventOutbox
from jasil.testing import FixedClock as ClockForRecorder

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


class SessionAwareRecorder(RecordingRecorder):
    def __init__(self) -> None:
        super().__init__()
        self.staged: list = []

    def record_queued_in(self, event, db) -> None:
        self.staged.append((event, db))


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

    def test_a_session_aware_recorder_stages_on_the_callers_session(self, platform, db, monkeypatch):
        """The queued row must ride the caller's transaction, not its own."""
        recorder = SessionAwareRecorder()
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=recorder))
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish("activity.created", {"id": 1}, source="test", db=db)

        assert [staged_db for _event, staged_db in recorder.staged] == [db]
        assert recorder.queued == []

    def test_durable_delivery_needs_a_session(self, platform, db):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish("activity.created", {"id": 1}, source="test")

        assert len(platform.events.published) == 1
        assert db.query(EventOutbox).count() == 0


class TestTheEventIsStampedByThePlatformClock:
    def test_on_the_bus(self, platform):
        publisher.publish("activity.created", {}, source="test")

        assert [event.timestamp for event in platform.events.published] == [T0.isoformat()]

    def test_in_the_outbox(self, platform, db):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish_on_commit("activity.created", {}, source="test", db=db)

        assert db.query(EventOutbox).one().timestamp == T0.isoformat()


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

    @pytest.mark.parametrize("returned", [uuid.UUID(int=1), 7], ids=["uuid", "int"])
    def test_a_provider_returning_a_non_string_is_no_id_and_the_event_still_publishes(self, platform, caplog, returned):
        correlation.configure_provider(lambda: returned)

        with caplog.at_level("WARNING"):
            assert correlation.get_correlation_id() is None
            assert correlation.get_correlation_id() is None
        event_id = publisher.publish("activity.created", {}, source="test")

        assert event_id is not None
        assert META_REQUEST_ID not in platform.events.published[0].metadata
        assert caplog.text.count(type(returned).__name__) == 1
        assert str(returned) not in caplog.text


class TestDisabledEventBus:
    """``events_uri=none://`` drops best-effort events silently; the durable route never touches the bus."""

    def test_a_best_effort_publish_logs_no_error(self, tmp_path, caplog):
        jasil_testing.install_test_platform(tmp_path, settings=settings.JasilSettings(events_uri="none://"))

        caplog.clear()
        with caplog.at_level("ERROR"):
            publisher.publish("activity.created", {"id": 1}, source="test")

        assert caplog.records == []

    def test_a_durable_event_is_still_staged_in_the_outbox(self, tmp_path, db):
        jasil_testing.install_test_platform(
            tmp_path,
            settings=settings.JasilSettings(events_uri="none://", jobs=settings.JobSettings(enabled=True)),
        )
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

        publisher.publish("activity.created", {"id": 1}, source="test", db=db)

        assert db.query(EventOutbox).count() == 1


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


SECRET_PARAMETER = "s3cret-payload-value"


def _statement_error(*_args, **_kwargs):
    raise StatementError("insert failed", "INSERT INTO t VALUES (?)", {"payload": SECRET_PARAMETER}, ValueError())


class TestFailureLogsCarryNoStatementParameters:
    """A SQLAlchemy error's text carries the statement's parameters: the event's payload and metadata."""

    def test_a_bus_failure_is_logged_by_type(self, monkeypatch, caplog):
        bus = RecordingBus()
        bus.publish = _statement_error
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=bus))

        with caplog.at_level("ERROR"):
            publisher.publish("activity.created", {}, source="test")

        assert "StatementError (ValueError)" in caplog.text
        assert SECRET_PARAMETER not in caplog.text

    def test_a_durable_staging_failure_is_logged_by_type(self, platform, db, monkeypatch, caplog):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)
        monkeypatch.setattr(jobs_outbox, "add_to_outbox", _statement_error)

        with caplog.at_level("ERROR"):
            publisher.publish("activity.created", {}, source="test", db=db)
            with pytest.raises(StatementError):
                publisher.publish_on_commit("activity.created", {}, source="test", db=db)

        assert caplog.text.count("StatementError (ValueError)") == 2
        assert SECRET_PARAMETER not in caplog.text


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
            jobs_outbox,
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

    def test_a_metadata_builder_failure_after_the_commit_skips_only_that_event(self, platform, db, caplog):
        """The domain change is committed by then, so raising would tell the caller its unit of work failed."""

        def _metadata(payload):
            if payload["id"] == 1:
                raise RuntimeError("bad metadata")
            return None

        with caplog.at_level("ERROR"):
            publisher.publish_many_committing(
                "activity.deleted",
                [{"id": 1}, {"id": 2}],
                source="test",
                metadata_for=_metadata,
                db=db,
                commit=db.commit,
            )

        assert [event.payload["id"] for event in platform.events.published] == [2]
        assert "Failed to publish event activity.deleted" in caplog.text

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

    def test_a_mint_failure_on_the_bus_path_is_logged_and_swallowed(self, platform, db, monkeypatch, caplog):
        def _refuse(*_args, **_kwargs):
            raise RuntimeError("cannot mint")

        monkeypatch.setattr(publisher, "new_event", _refuse)

        with caplog.at_level("ERROR"):
            publisher.publish_many_committing(
                "activity.deleted", [{"id": 1}], source="test", db=db, commit=lambda: None
            )

        assert platform.events.published == []
        assert "Failed to publish event activity.deleted" in caplog.text


def _bus_failure_platform(monkeypatch):
    monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=ExplodingBus()))


def _outbox_count(engine) -> int:
    with Session(engine) as other:
        return other.query(EventOutbox).count()


class TestPublishOnCommitDurable:
    @pytest.fixture(autouse=True)
    def _durable(self, platform):
        _enable_durable_jobs()
        jobs_registry.registry.register("activity.created", "s", lambda _e: None)

    def test_the_row_is_staged_in_the_open_transaction_before_commit(self, platform, db, db_engine):
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)

        assert db.in_transaction()
        assert db.query(EventOutbox).count() == 1

    def test_the_row_is_absent_after_rollback(self, platform, db, db_engine):
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)
        db.rollback()

        assert _outbox_count(db_engine) == 0

    def test_the_row_is_present_after_commit(self, platform, db, db_engine):
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)
        db.commit()

        assert _outbox_count(db_engine) == 1
        assert platform.events.published == []

    def test_domain_rows_and_outbox_row_share_one_transaction(self, platform, db, db_engine):
        """A stand-in domain row and the outbox row land or vanish together."""
        db.add(EventOutbox(**_outbox_kwargs("domain-row")))
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)
        db.commit()
        assert _outbox_count(db_engine) == 2

        db.add(EventOutbox(**_outbox_kwargs("domain-row-2")))
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)
        db.rollback()
        assert _outbox_count(db_engine) == 2

    def test_a_staging_failure_raises_to_the_caller(self, platform, db, monkeypatch):
        monkeypatch.setattr(
            jobs_outbox, "add_to_outbox", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db down"))
        )

        with pytest.raises(RuntimeError, match="db down"):
            publisher.publish_on_commit("activity.created", {}, source="test", db=db)


def _outbox_kwargs(event_id: str) -> dict:
    return {
        "id": event_id,
        "event_id": event_id,
        "event_type": "domain.row",
        "source": "test",
        "timestamp": T0.isoformat(),  # the envelope's ISO-8601 string, as the column stores it
        "payload": {},
        "schema_version": 1,
        "created_at": T0,
    }


class TestPublishOnCommitBestEffort:
    def test_nothing_is_dispatched_before_commit(self, platform, db):
        publisher.publish_on_commit("activity.created", {"id": 1}, source="test", db=db)

        assert platform.events.published == []

    def test_one_dispatch_after_commit(self, platform, db):
        publisher.publish_on_commit("activity.created", {"id": 1}, source="test", db=db)
        db.commit()

        assert [e.payload for e in platform.events.published] == [{"id": 1}]

    def test_nothing_is_dispatched_after_rollback(self, platform, db):
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)
        db.rollback()
        db.commit()

        assert platform.events.published == []

    def test_nothing_is_dispatched_when_the_session_is_closed_uncommitted(self, platform, session_factory):
        with session_factory() as session:
            publisher.publish_on_commit("activity.created", {}, source="test", db=session)
            session.close()
            session.commit()

        assert platform.events.published == []

    def test_a_subscriber_failure_does_not_reach_the_committer(self, monkeypatch, db, caplog):
        _bus_failure_platform(monkeypatch)
        publisher.publish_on_commit("activity.created", {}, source="test", db=db)

        with caplog.at_level("ERROR"):
            db.commit()

        assert "Failed to publish event activity.created" in caplog.text

    def test_ambient_request_id_is_merged_like_publish_committing(self, platform, db):
        correlation.set_correlation_id("req-9")
        publisher.publish_on_commit("activity.created", {}, source="test", metadata={"user_id": 3}, db=db)
        correlation.reset()  # the commit may happen outside the request context
        db.commit()

        assert platform.events.published[0].metadata == {META_REQUEST_ID: "req-9", "user_id": 3}

    def test_a_session_that_commits_twice_dispatches_each_batch_once(self, platform, db):
        publisher.publish_on_commit("activity.created", {"id": 1}, source="test", db=db)
        db.commit()
        db.commit()
        publisher.publish_on_commit("activity.created", {"id": 2}, source="test", db=db)
        db.commit()

        assert [e.payload["id"] for e in platform.events.published] == [1, 2]

    def test_events_dispatch_in_publish_order(self, platform, db):
        publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        publisher.publish_many_on_commit("a.two", [{"n": 2}, {"n": 3}], source="test", db=db)
        publisher.publish_on_commit("a.one", {"n": 4}, source="test", db=db)
        db.commit()

        assert [e.payload["n"] for e in platform.events.published] == [1, 2, 3, 4]

    def test_an_event_published_during_dispatch_waits_for_the_next_commit(self, platform, db):
        def _republish(event):
            platform.events.published.append(event)
            if event.payload["n"] == 1:
                publisher.publish_on_commit("a.one", {"n": 2}, source="test", db=db)

        platform.events.publish = _republish
        publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        db.commit()
        assert [e.payload["n"] for e in platform.events.published] == [1]

        db.commit()
        assert [e.payload["n"] for e in platform.events.published] == [1, 2]

    def test_an_empty_batch_is_a_no_op(self, platform, db):
        publisher.publish_many_on_commit("activity.deleted", [], source="test", db=db)

        assert not db.in_transaction()
        assert "jasil.after_commit.pending" not in db.info

    def test_a_missing_session_is_rejected(self, platform):
        with pytest.raises(ValueError):
            publisher.publish_on_commit("activity.created", {}, source="test", db=None)

    def test_a_non_session_is_rejected_before_anything_is_queued(self, platform, session_factory):
        from sqlalchemy.orm import scoped_session

        scoped = scoped_session(session_factory)
        with pytest.raises(TypeError):
            publisher.publish_on_commit("activity.created", {}, source="test", db=scoped)
        scoped.remove()

        with session_factory() as other:
            other.commit()  # no listener leaked onto the factory
        assert platform.events.published == []

    def test_a_session_that_never_publishes_gets_no_listeners(self, platform, session_factory):
        with session_factory() as session:
            session.commit()

            assert session.info == {}


class TestPublishOnCommitSavepoints:
    def test_a_rolled_back_savepoint_drops_only_its_events(self, platform, db):
        publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        savepoint = db.begin_nested()
        publisher.publish_on_commit("a.one", {"n": 2}, source="test", db=db)
        savepoint.rollback()
        publisher.publish_on_commit("a.one", {"n": 3}, source="test", db=db)
        db.commit()

        assert [e.payload["n"] for e in platform.events.published] == [1, 3]

    def test_a_released_savepoint_dispatches_only_on_the_outer_commit(self, platform, db):
        with db.begin_nested():
            publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        assert platform.events.published == []

        publisher.publish_on_commit("a.one", {"n": 2}, source="test", db=db)
        db.commit()

        assert [e.payload["n"] for e in platform.events.published] == [1, 2]

    def test_nested_savepoints_release_into_their_parents_in_order(self, platform, db):
        publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        with db.begin_nested():
            publisher.publish_on_commit("a.one", {"n": 2}, source="test", db=db)
            with db.begin_nested():
                publisher.publish_on_commit("a.one", {"n": 3}, source="test", db=db)
            publisher.publish_on_commit("a.one", {"n": 4}, source="test", db=db)
        db.commit()

        assert [e.payload["n"] for e in platform.events.published] == [1, 2, 3, 4]

    def test_a_released_savepoint_is_discarded_by_an_outer_rollback(self, platform, db):
        with db.begin_nested():
            publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        db.rollback()
        db.commit()

        assert platform.events.published == []

    def test_rolling_back_an_outer_savepoint_drops_its_released_children(self, platform, db):
        outer = db.begin_nested()
        with db.begin_nested():
            publisher.publish_on_commit("a.one", {"n": 1}, source="test", db=db)
        outer.rollback()
        db.commit()

        assert platform.events.published == []


class TestPublishOnCommitIsolation:
    def test_two_sessions_do_not_leak_events_into_each_other(self, platform, session_factory):
        with session_factory() as first, session_factory() as second:
            publisher.publish_on_commit("a.one", {"owner": "first"}, source="test", db=first)
            publisher.publish_on_commit("a.one", {"owner": "second"}, source="test", db=second)

            second.rollback()
            first.commit()
            second.commit()

        assert [e.payload["owner"] for e in platform.events.published] == ["first"]

    def test_concurrent_threads_each_dispatch_only_their_own_events(self, platform, session_factory):
        import threading

        barrier = threading.Barrier(4)

        def _work(n: int) -> None:
            with session_factory() as session:
                publisher.publish_on_commit("a.one", {"n": n}, source="test", db=session)
                barrier.wait(timeout=5)
                if n % 2:
                    session.rollback()
                else:
                    session.commit()

        threads = [threading.Thread(target=_work, args=(n,)) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sorted(e.payload["n"] for e in platform.events.published) == [0, 2]


class TestIdentifiersAreValidatedOnEveryRoute:
    """An identifier too long for its column raises before the commit, whether or not durable jobs are on."""

    # The registry refuses an oversize event type, so only the source can be oversize on the durable route.
    @pytest.mark.parametrize(("field", "durable"), [("event_type", False), ("source", False), ("source", True)])
    def test_publish_many_committing_raises_before_committing(self, platform, db, durable, field):
        if durable:
            _enable_durable_jobs()
            jobs_registry.registry.register("activity.deleted", "s", lambda _e: None)
        arguments = {"event_type": "activity.deleted", "source": "test", field: "a" * 300}
        committed = []

        with pytest.raises(ValueError, match=field):
            publisher.publish_many_committing(
                arguments["event_type"],
                [{"id": 1}],
                source=arguments["source"],
                db=db,
                commit=lambda: committed.append(True),
            )

        assert committed == []
        assert platform.events.published == []

    @pytest.mark.parametrize("field", ["event_type", "source"])
    def test_publish_on_commit_raises_on_the_bus_route_too(self, platform, db, field):
        arguments = {"event_type": "activity.created", "source": "test", field: "a" * 300}

        with pytest.raises(ValueError, match=field):
            publisher.publish_on_commit(arguments["event_type"], {}, source=arguments["source"], db=db)
        db.commit()

        assert platform.events.published == []


class TestPublishOnCommitCapturesThePayload:
    def test_a_payload_changed_after_publishing_is_dispatched_as_it_was(self, platform, db):
        """The event is minted when it is published; the commit may come much later."""
        payload = {"id": 1, "tags": ["a"]}
        publisher.publish_on_commit("activity.created", payload, source="test", db=db)
        payload["id"] = 2
        payload["tags"].append("b")

        db.commit()

        assert [event.payload for event in platform.events.published] == [{"id": 1, "tags": ["a"]}]


class TestPublishCommittingStillOwnsItsCommit:
    def test_it_does_not_register_session_listeners(self, platform, db):
        publisher.publish_committing("activity.created", {}, source="test", db=db, commit=lambda: None)

        assert db.info == {}
        assert len(platform.events.published) == 1


class TestPublishOnCommitMintFailure:
    def test_a_metadata_builder_failure_is_logged_and_skips_only_that_event(self, platform, db, caplog):
        def _metadata(payload):
            if payload["n"] == 1:
                raise RuntimeError("bad metadata")
            return None

        with caplog.at_level("ERROR"):
            publisher.publish_many_on_commit(
                "a.one", [{"n": 1}, {"n": 2}], source="test", metadata_for=_metadata, db=db
            )
        db.commit()

        assert [e.payload["n"] for e in platform.events.published] == [2]
        assert "Failed to publish event a.one" in caplog.text


def _register_durable(event_type: str = "activity.created") -> None:
    _enable_durable_jobs()
    jobs_registry.registry.register(event_type, "s", lambda _e: None)


def _queued_rows(db) -> int:
    return db.query(EventLog).filter(EventLog.status == "queued").count()


class TestAnExplicitEventId:
    def test_it_reaches_the_bus(self, platform):
        publisher.publish("activity.created", {}, source="test", event_id="evt-1")

        assert [event.event_id for event in platform.events.published] == ["evt-1"]

    def test_it_reaches_the_outbox(self, platform, db):
        _register_durable()

        publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="evt-1")

        assert db.query(EventOutbox).one().event_id == "evt-1"

    def test_a_builder_supplies_one_per_payload(self, platform, db):
        publisher.publish_many_committing(
            "activity.deleted",
            [{"id": 1}, {"id": 2}],
            source="test",
            event_id_for=lambda payload: f"evt-{payload['id']}",
            db=db,
            commit=lambda: None,
        )

        assert [event.event_id for event in platform.events.published] == ["evt-1", "evt-2"]

    def test_no_id_still_mints_a_uuid(self, platform):
        returned = publisher.publish("activity.created", {}, source="test")

        assert returned == platform.events.published[0].event_id
        assert len(returned) == 36


class TestPublishReturnsTheEventId:
    def test_publish_on_the_bus(self, platform):
        assert publisher.publish("activity.created", {}, source="test", event_id="evt-1") == "evt-1"

    def test_publish_on_the_durable_route(self, platform, db):
        _register_durable()

        assert publisher.publish("activity.created", {}, source="test", db=db, event_id="evt-1") == "evt-1"

    def test_publish_returns_none_when_the_bus_failed(self, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=ExplodingBus()))

        assert publisher.publish("activity.created", {}, source="test") is None

    def test_publish_returns_the_id_after_a_partial_delivery(self, monkeypatch):
        """A producer retrying on None would run the handlers that succeeded a second time."""
        from jasil.backends.events_inprocess import InProcessEventBus

        bus = InProcessEventBus()
        calls = []
        bus.subscribe("activity.created", lambda _e: (_ for _ in ()).throw(RuntimeError("boom")))
        bus.subscribe("activity.created", lambda _e: calls.append("second"))
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=bus))

        assert publisher.publish("activity.created", {}, source="test", event_id="evt-1") == "evt-1"
        assert calls == ["second"]

    def test_publish_returns_none_when_the_platform_is_missing(self, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", None)

        assert publisher.publish("activity.created", {}, source="test") is None

    def test_publish_committing_on_the_bus(self, platform, db):
        returned = publisher.publish_committing(
            "activity.created", {}, source="test", db=db, commit=lambda: None, event_id="evt-1"
        )

        assert returned == "evt-1"

    def test_publish_committing_on_the_durable_route(self, platform, db):
        _register_durable()

        returned = publisher.publish_committing(
            "activity.created", {}, source="test", db=db, commit=db.commit, event_id="evt-1"
        )

        assert returned == "evt-1"

    def test_publish_committing_returns_none_when_the_dispatch_failed(self, monkeypatch, db):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=ExplodingBus()))

        assert publisher.publish_committing("activity.created", {}, source="test", db=db, commit=lambda: None) is None

    def test_publish_on_commit_on_the_bus_returns_the_id_before_the_commit(self, platform, db):
        returned = publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="evt-1")

        assert returned == "evt-1"
        assert platform.events.published == []

    def test_publish_on_commit_on_the_durable_route(self, platform, db):
        _register_durable()

        assert publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="evt-1") == "evt-1"

    def test_publish_many_committing_on_the_bus_returns_the_ids_in_payload_order(self, platform, db):
        returned = publisher.publish_many_committing(
            "activity.deleted",
            [{"id": 1}, {"id": 2}],
            source="test",
            event_id_for=lambda payload: f"evt-{payload['id']}",
            db=db,
            commit=lambda: None,
        )

        assert returned == ["evt-1", "evt-2"]

    def test_publish_many_committing_has_none_for_a_failed_dispatch(self, monkeypatch, db):
        bus = RecordingBus()
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(events=bus))

        def _fail_the_first(event):
            if event.payload["id"] == 1:
                raise RuntimeError("transient")
            bus.published.append(event)

        bus.publish = _fail_the_first

        returned = publisher.publish_many_committing(
            "activity.deleted",
            [{"id": 1}, {"id": 2}],
            source="test",
            event_id_for=lambda payload: f"evt-{payload['id']}",
            db=db,
            commit=lambda: None,
        )

        assert returned == [None, "evt-2"]

    def test_publish_many_committing_has_none_for_a_mint_failure(self, platform, db):
        def _refuse_the_first(payload):
            if payload["id"] == 1:
                raise RuntimeError("bad id")
            return f"evt-{payload['id']}"

        returned = publisher.publish_many_committing(
            "activity.deleted",
            [{"id": 1}, {"id": 2}],
            source="test",
            event_id_for=_refuse_the_first,
            db=db,
            commit=lambda: None,
        )

        assert returned == [None, "evt-2"]

    def test_publish_many_committing_on_the_durable_route(self, platform, db):
        _register_durable("activity.deleted")

        returned = publisher.publish_many_committing(
            "activity.deleted",
            [{"id": 1}, {"id": 2}],
            source="test",
            event_id_for=lambda payload: f"evt-{payload['id']}",
            db=db,
            commit=db.commit,
        )

        assert returned == ["evt-1", "evt-2"]

    def test_an_empty_batch_returns_an_empty_list_and_still_commits(self, platform, db):
        committed = []

        returned = publisher.publish_many_committing(
            "activity.deleted", [], source="test", db=db, commit=lambda: committed.append(True)
        )

        assert returned == []
        assert committed == [True]

    def test_publish_many_on_commit_on_the_bus(self, platform, db):
        returned = publisher.publish_many_on_commit(
            "a.one",
            [{"n": 1}, {"n": 2}],
            source="test",
            event_id_for=lambda payload: f"evt-{payload['n']}",
            db=db,
        )

        assert returned == ["evt-1", "evt-2"]

    def test_publish_many_on_commit_has_none_for_a_mint_failure(self, platform, db):
        def _refuse_the_first(payload):
            if payload["n"] == 1:
                raise RuntimeError("bad id")
            return f"evt-{payload['n']}"

        returned = publisher.publish_many_on_commit(
            "a.one", [{"n": 1}, {"n": 2}], source="test", event_id_for=_refuse_the_first, db=db
        )
        db.commit()

        assert returned == [None, "evt-2"]
        assert [event.event_id for event in platform.events.published] == ["evt-2"]

    def test_publish_many_on_commit_on_the_durable_route(self, platform, db):
        _register_durable("a.one")

        returned = publisher.publish_many_on_commit(
            "a.one", [{"n": 1}, {"n": 2}], source="test", event_id_for=lambda payload: f"evt-{payload['n']}", db=db
        )

        assert returned == ["evt-1", "evt-2"]

    def test_an_empty_on_commit_batch_returns_an_empty_list(self, platform, db):
        assert publisher.publish_many_on_commit("a.one", [], source="test", db=db) == []


class TestARepeatedEventIdIsIdempotent:
    @pytest.fixture(autouse=True)
    def _durable(self, platform):
        _register_durable()

    def test_publishing_twice_leaves_one_outbox_row_and_returns_the_id_both_times(self, platform, db):
        first = publisher.publish("activity.created", {"n": 1}, source="test", db=db, event_id="evt-1")
        second = publisher.publish("activity.created", {"n": 1}, source="test", db=db, event_id="evt-1")

        assert (first, second) == ("evt-1", "evt-1")
        assert db.query(EventOutbox).count() == 1

    def test_the_queued_record_is_written_once(self, db, monkeypatch):
        recorder = SessionAwareRecorder()
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=recorder))

        for _ in range(2):
            publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="evt-1")

        assert len(recorder.staged) == 1

    def test_a_legacy_recorder_is_called_once(self, db, monkeypatch):
        recorder = RecordingRecorder()
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=recorder))

        for _ in range(2):
            publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="evt-1")

        assert len(recorder.queued) == 1

    def test_the_real_event_log_has_one_queued_row(self, db, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=EventLogRecorder(ClockForRecorder())))

        for _ in range(2):
            publisher.publish_committing(
                "activity.created", {}, source="test", db=db, commit=db.commit, event_id="evt-1"
            )

        assert db.query(EventOutbox).count() == 1
        assert _queued_rows(db) == 1

    def test_a_repeat_inside_one_batch_and_transaction(self, db, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=EventLogRecorder(ClockForRecorder())))

        returned = publisher.publish_many_on_commit(
            "activity.created", [{"n": 1}, {"n": 1}], source="test", event_id_for=lambda _p: "evt-1", db=db
        )
        db.commit()

        assert returned == ["evt-1", "evt-1"]
        assert db.query(EventOutbox).count() == 1
        assert _queued_rows(db) == 1

    def test_the_outbox_row_and_the_queued_row_commit_together(self, db, db_engine, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=EventLogRecorder(ClockForRecorder())))

        publisher.publish("activity.created", {}, source="test", db=db, event_id="evt-1")

        with Session(db_engine) as other:
            assert other.query(EventOutbox).count() == 1
            assert _queued_rows(other) == 1

    def test_without_committing_both_stay_in_the_callers_transaction(self, db, db_engine, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(recorder=EventLogRecorder(ClockForRecorder())))

        publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="evt-1")
        db.rollback()

        with Session(db_engine) as other:
            assert other.query(EventOutbox).count() == 0
            assert _queued_rows(other) == 0

    def test_a_different_payload_is_refused_on_publish_on_commit(self, db):
        publisher.publish_on_commit("activity.created", {"n": 1}, source="test", db=db, event_id="evt-1")

        with pytest.raises(ValueError, match=r"evt-1.*payload"):
            publisher.publish_on_commit("activity.created", {"n": 2}, source="test", db=db, event_id="evt-1")

    def test_a_different_event_type_is_refused_on_publish_on_commit(self, db):
        jobs_registry.registry.register("other.type", "other", lambda _e: None)
        publisher.publish_on_commit("activity.created", {"n": 1}, source="test", db=db, event_id="evt-1")

        with pytest.raises(ValueError, match=r"evt-1.*event_type"):
            publisher.publish_on_commit("other.type", {"n": 1}, source="test", db=db, event_id="evt-1")

    def test_a_different_payload_is_refused_on_publish_committing_before_committing(self, db):
        publisher.publish_committing(
            "activity.created", {"n": 1}, source="test", db=db, commit=db.commit, event_id="evt-1"
        )
        committed = []

        with pytest.raises(ValueError, match="payload"):
            publisher.publish_committing(
                "activity.created",
                {"n": 2},
                source="test",
                db=db,
                commit=lambda: committed.append(True),
                event_id="evt-1",
            )

        assert committed == []

    def test_a_different_payload_is_swallowed_and_logged_by_publish_without_the_payload(self, db, caplog):
        publisher.publish("activity.created", {"n": 1}, source="test", db=db, event_id="evt-1")

        with caplog.at_level("ERROR"):
            returned = publisher.publish(
                "activity.created", {"secret": SECRET_PARAMETER}, source="test", db=db, event_id="evt-1"
            )

        assert returned is None
        assert "Failed to publish event activity.created" in caplog.text
        assert SECRET_PARAMETER not in caplog.text

    def test_the_refusal_does_not_carry_the_payload(self, db):
        publisher.publish_on_commit("activity.created", {"n": 1}, source="test", db=db, event_id="evt-1")

        with pytest.raises(ValueError) as exc_info:
            publisher.publish_on_commit(
                "activity.created", {"secret": SECRET_PARAMETER}, source="test", db=db, event_id="evt-1"
            )

        assert SECRET_PARAMETER not in str(exc_info.value)

    def test_a_tuple_equals_the_list_the_column_stores(self, db):
        publisher.publish_on_commit("activity.created", {"tags": ("a", "b")}, source="test", db=db, event_id="evt-1")

        returned = publisher.publish_on_commit(
            "activity.created", {"tags": ["a", "b"]}, source="test", db=db, event_id="evt-1"
        )

        assert returned == "evt-1"
        assert db.query(EventOutbox).count() == 1

    def test_metadata_does_not_count_as_a_difference(self, db):
        publisher.publish_on_commit(
            "activity.created", {}, source="test", db=db, event_id="evt-1", metadata={"request_id": "a"}
        )

        returned = publisher.publish_on_commit(
            "activity.created", {}, source="test", db=db, event_id="evt-1", metadata={"request_id": "b"}
        )

        assert returned == "evt-1"


class TestAnEmptyEventIdIsRejected:
    def test_publish_committing_raises_before_committing(self, platform, db):
        committed = []

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_committing(
                "activity.created", {}, source="test", db=db, commit=lambda: committed.append(True), event_id=""
            )

        assert committed == []

    def test_publish_on_commit_raises_before_staging(self, platform, db):
        _register_durable()

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id="")

        assert db.query(EventOutbox).count() == 0

    def test_publish_swallows_it_like_any_failure(self, platform, caplog):
        with caplog.at_level("ERROR"):
            returned = publisher.publish("activity.created", {}, source="test", event_id="")

        assert returned is None
        assert platform.events.published == []
        assert "Failed to publish event activity.created" in caplog.text

    def test_a_builder_returning_it_is_refused_on_the_durable_route(self, platform, db):
        _register_durable("a.one")

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_many_on_commit("a.one", [{}], source="test", event_id_for=lambda _p: "", db=db)

    def test_a_builder_returning_it_is_skipped_on_the_bus_route(self, platform, db):
        returned = publisher.publish_many_on_commit("a.one", [{}], source="test", event_id_for=lambda _p: "", db=db)

        assert returned == [None]


class TestEventIdBuilderFailuresMirrorMetadataBuilderFailures:
    @staticmethod
    def _boom(_payload):
        raise RuntimeError("bad id")

    def test_publish_many_committing_skips_only_that_event_on_the_bus_route(self, platform, db, caplog):
        def _id(payload):
            return self._boom(payload) if payload["id"] == 1 else None

        with caplog.at_level("ERROR"):
            publisher.publish_many_committing(
                "activity.deleted", [{"id": 1}, {"id": 2}], source="test", event_id_for=_id, db=db, commit=db.commit
            )

        assert [event.payload["id"] for event in platform.events.published] == [2]
        assert "Failed to publish event activity.deleted" in caplog.text

    def test_publish_many_committing_raises_on_the_durable_route_without_committing(self, platform, db):
        _register_durable("activity.deleted")
        committed = []

        with pytest.raises(RuntimeError, match="bad id"):
            publisher.publish_many_committing(
                "activity.deleted",
                [{"id": 1}],
                source="test",
                event_id_for=self._boom,
                db=db,
                commit=lambda: committed.append(True),
            )

        assert committed == []

    def test_publish_many_on_commit_skips_only_that_event_on_the_bus_route(self, platform, db, caplog):
        def _id(payload):
            return self._boom(payload) if payload["n"] == 1 else None

        with caplog.at_level("ERROR"):
            publisher.publish_many_on_commit("a.one", [{"n": 1}, {"n": 2}], source="test", event_id_for=_id, db=db)
        db.commit()

        assert [e.payload["n"] for e in platform.events.published] == [2]
        assert "Failed to publish event a.one" in caplog.text

    def test_publish_many_on_commit_raises_on_the_durable_route(self, platform, db):
        _register_durable("a.one")

        with pytest.raises(RuntimeError, match="bad id"):
            publisher.publish_many_on_commit("a.one", [{}], source="test", event_id_for=self._boom, db=db)


_BAD_EVENT_IDS = [
    pytest.param("Evt-1", id="uppercase"),
    pytest.param("evt 1", id="space"),
    pytest.param("evt-1\n", id="trailing-newline"),
    pytest.param("evt-1\nevt-2", id="newline"),
    pytest.param("-evt", id="leading-dash"),
    pytest.param("é", id="accent"),
    pytest.param("a" * 37, id="too-long"),
    pytest.param(5, id="not-a-string"),
]


class TestAnEventIdIsPortableAndBounded:
    @pytest.mark.parametrize("event_id", _BAD_EVENT_IDS)
    def test_publish_committing_raises_before_committing_on_the_bus_route(self, platform, db, event_id):
        committed = []

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_committing(
                "activity.created",
                {},
                source="test",
                db=db,
                commit=lambda: committed.append(True),
                event_id=event_id,
            )

        assert committed == []
        assert platform.events.published == []

    @pytest.mark.parametrize("event_id", _BAD_EVENT_IDS)
    def test_publish_committing_raises_before_committing_on_the_durable_route(self, platform, db, event_id):
        _register_durable()
        committed = []

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_committing(
                "activity.created",
                {},
                source="test",
                db=db,
                commit=lambda: committed.append(True),
                event_id=event_id,
            )

        assert committed == []
        assert db.query(EventOutbox).count() == 0

    @pytest.mark.parametrize("event_id", _BAD_EVENT_IDS)
    @pytest.mark.parametrize("durable", [False, True])
    def test_publish_on_commit_raises_before_staging_on_either_route(self, platform, db, event_id, durable):
        if durable:
            _register_durable()

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_on_commit("activity.created", {}, source="test", db=db, event_id=event_id)
        db.commit()

        assert db.query(EventOutbox).count() == 0
        assert platform.events.published == []

    @pytest.mark.parametrize("event_id", _BAD_EVENT_IDS)
    @pytest.mark.parametrize("durable", [False, True])
    def test_publish_swallows_it_like_any_failure(self, platform, db, caplog, event_id, durable):
        if durable:
            _register_durable()

        with caplog.at_level("ERROR"):
            returned = publisher.publish("activity.created", {}, source="test", db=db, event_id=event_id)

        assert returned is None
        assert platform.events.published == []
        assert db.query(EventOutbox).count() == 0
        assert "Failed to publish event activity.created" in caplog.text

    @pytest.mark.parametrize("event_id", _BAD_EVENT_IDS)
    def test_a_builder_returning_one_is_refused_on_the_durable_route(self, platform, db, event_id):
        _register_durable("a.one")

        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_many_on_commit("a.one", [{}], source="test", event_id_for=lambda _p: event_id, db=db)
        with pytest.raises(ValueError, match="event_id"):
            publisher.publish_many_committing(
                "a.one", [{}], source="test", event_id_for=lambda _p: event_id, db=db, commit=db.commit
            )

        assert db.query(EventOutbox).count() == 0

    @pytest.mark.parametrize("event_id", _BAD_EVENT_IDS)
    def test_a_builder_returning_one_is_skipped_on_the_bus_route(self, platform, db, event_id):
        returned = publisher.publish_many_on_commit(
            "a.one", [{}], source="test", event_id_for=lambda _p: event_id, db=db
        )
        committed = publisher.publish_many_committing(
            "a.one", [{}], source="test", event_id_for=lambda _p: event_id, db=db, commit=db.commit
        )

        assert (returned, committed) == ([None], [None])
        assert platform.events.published == []

    @pytest.mark.parametrize(
        "event_id",
        [
            "2f1c3a8e-3d6b-4f0e-9b4e-0a1b2c3d4e5f",
            "6ba7b810-9dad-51d1-80b4-00c04fd430c8",
            "a",
            "0.a_b-c",
            "a" * 36,
        ],
    )
    def test_a_uuid_or_a_portable_slug_is_accepted(self, platform, event_id):
        assert publisher.publish("activity.created", {}, source="test", event_id=event_id) == event_id

    def test_a_generated_uuid4_and_uuid5_are_accepted(self, platform):
        import uuid

        for generated in (uuid.uuid4(), uuid.uuid5(uuid.NAMESPACE_DNS, "example.org")):
            assert publisher.publish("activity.created", {}, source="test", event_id=str(generated)) == str(generated)


class TestAReusedIdThroughTheRelay:
    @staticmethod
    def _relay(session_factory, registry):
        import jasil.jobs.relay as jobs_relay

        return jobs_relay.relay_outbox_once(
            registry=registry, clock=FixedClock(), session_factory=session_factory, max_attempts=3, batch_size=10
        )

    def test_one_job_per_subscriber_however_often_the_id_is_published(self, platform, db, session_factory):
        from jasil.jobs.models import ProcessingJob
        from jasil.jobs.registry import registry

        _register_durable()
        registry.register("activity.created", "t", lambda _e: None)
        for _ in range(2):
            publisher.publish_committing(
                "activity.created", {"n": 1}, source="test", db=db, commit=db.commit, event_id="evt-1"
            )

        assert self._relay(session_factory, registry) == 1
        assert sorted(job.subscriber_id for job in db.query(ProcessingJob).all()) == ["s", "t"]

        publisher.publish_committing(
            "activity.created", {"n": 1}, source="test", db=db, commit=db.commit, event_id="evt-1"
        )

        assert self._relay(session_factory, registry) == 0
        assert db.query(ProcessingJob).count() == 2
        assert db.query(EventOutbox).count() == 1
