"""The Redis Streams event bus — the ``distributed`` profile's delivery path.

This backend only runs when `events_uri` selects Redis, which means it is absent
from a development machine and present in production: exactly the code that has
to be pinned by tests rather than by use. It is exercised against fakeredis,
which implements streams and consumer groups, so the suite stays hermetic.

Most tests drive `_poll_once` directly instead of starting the consumer thread.
The thread adds nothing to what is being asserted — it is a `while not stopped`
loop around that one call — and driving it directly makes each test deterministic
rather than timing-dependent. The thread's own lifecycle is covered separately at
the bottom, where it is the subject.
"""

import json
import logging
import threading

import fakeredis
import pytest
import redis

from jasil.backends.events_redis import (
    RedisStreamEventBus,
    deserialize_event,
    serialize_event,
)
from jasil.events import Event, new_event

STREAM = "test:events"
GROUP = "test"


@pytest.fixture
def client():
    return fakeredis.FakeStrictRedis(decode_responses=True)


@pytest.fixture
def bus(client):
    bus = RedisStreamEventBus(client, stream=STREAM, group=GROUP, consumer="consumer-1")
    bus._ensure_group()
    return bus


class RecordingRecorder:
    """Stands in for the event-log recorder, capturing the lifecycle calls."""

    def __init__(self) -> None:
        self.published: list[Event] = []
        self.tracked: list[tuple[str, str, str | None]] = []
        self.failed: list[str] = []

    def record_published(self, event):
        self.published.append(event)

    def record_queued(self, event):  # pragma: no cover - the bus never queues
        raise AssertionError("the bus must not record 'queued'")

    def track(self, event, *, worker_id, handler_name, record_processing=True):
        recorder = self

        class _Tracked:
            def __enter__(self):
                recorder.tracked.append((event.event_id, worker_id, handler_name))

            def __exit__(self, exc_type, exc, traceback):
                if exc is not None:
                    recorder.failed.append(event.event_id)
                return False

        return _Tracked()


def _pending_count(client) -> int:
    return client.xpending(STREAM, GROUP)["pending"]


class TestSerialization:
    def test_an_envelope_survives_the_round_trip(self):
        event = new_event(
            "order.created",
            {"id": 42, "nested": {"ok": True}},
            source="api:create_order",
            metadata={"request_id": "r-1"},
            schema_version=3,
        )

        assert deserialize_event(serialize_event(event)) == event

    def test_every_field_is_a_string_on_the_wire(self):
        """Redis stream fields are flat strings; a non-string would fail at XADD."""
        fields = serialize_event(new_event("order.created", {"id": 42}, source="api"))

        assert all(isinstance(value, str) for value in fields.values())

    def test_the_payload_and_metadata_travel_as_json(self):
        fields = serialize_event(new_event("order.created", {"id": 42}, source="api", metadata={"k": "v"}))

        assert json.loads(fields["payload"]) == {"id": 42}
        assert json.loads(fields["metadata"]) == {"k": "v"}

    def test_an_entry_written_before_versioning_reads_as_version_one(self):
        """A stream has no migration: entries predating the field are still in it."""
        fields = serialize_event(new_event("order.created", {}, source="api"))
        del fields["schema_version"]

        assert deserialize_event(fields).schema_version == 1


class TestPublish:
    def test_the_event_lands_on_the_stream(self, bus, client):
        bus.publish(new_event("order.created", {"id": 42}, source="api"))

        assert client.xlen(STREAM) == 1

    def test_it_is_recorded_published_from_the_producer(self, client):
        """The producer records 'published'; the consumer records the rest."""
        recorder = RecordingRecorder()
        bus = RedisStreamEventBus(client, stream=STREAM, group=GROUP, recorder=recorder)
        event = new_event("order.created", {}, source="api")

        bus.publish(event)

        assert recorder.published == [event]

    def test_publishing_without_a_recorder_is_fine(self, bus, client):
        """The event log is opt-in, so the bus must work with no recorder at all."""
        bus.publish(new_event("order.created", {}, source="api"))

        assert client.xlen(STREAM) == 1


class TestConsumerGroup:
    def test_the_group_is_created_on_start(self, client):
        bus = RedisStreamEventBus(client, stream=STREAM, group=GROUP)

        bus._ensure_group()

        assert [group["name"] for group in client.xinfo_groups(STREAM)] == [GROUP]

    def test_creating_it_twice_is_a_no_op(self, bus):
        """Every replica calls this at startup; only one of them can win."""
        bus._ensure_group()

    def test_a_real_redis_error_is_not_swallowed(self, bus, monkeypatch):
        """Only BUSYGROUP means 'someone beat me to it'; anything else is a fault."""

        def _explode(*args, **kwargs):
            raise redis.ResponseError("WRONGTYPE not a stream")

        monkeypatch.setattr(bus._client, "xgroup_create", _explode)

        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            bus._ensure_group()


class TestDispatch:
    def test_a_subscriber_receives_its_event_type(self, bus):
        seen = []
        bus.subscribe("order.created", seen.append)
        bus.publish(new_event("order.created", {"id": 42}, source="api"))

        bus._poll_once()

        assert [event.payload for event in seen] == [{"id": 42}]

    def test_every_subscriber_of_a_type_runs(self, bus):
        """In-process fan-out still happens on whichever replica claims the entry."""
        first, second = [], []
        bus.subscribe("order.created", first.append)
        bus.subscribe("order.created", second.append)
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()

        assert len(first) == len(second) == 1

    def test_another_event_type_is_not_delivered(self, bus):
        seen = []
        bus.subscribe("order.created", seen.append)
        bus.publish(new_event("invoice.rendered", {}, source="api"))

        bus._poll_once()

        assert seen == []

    def test_an_event_nobody_subscribed_to_is_acked(self, bus, client):
        """Otherwise an unhandled type would pile up in the pending list forever."""
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()

        assert _pending_count(client) == 0

    def test_a_handled_entry_is_acked(self, bus, client):
        bus.subscribe("order.created", lambda _event: None)
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()

        assert _pending_count(client) == 0

    def test_polling_an_empty_stream_does_nothing(self, bus):
        bus._poll_once()

    def test_a_batch_is_dispatched_in_order(self, bus):
        seen = []
        bus.subscribe("order.created", lambda event: seen.append(event.payload["id"]))
        for index in range(3):
            bus.publish(new_event("order.created", {"id": index}, source="api"))

        bus._poll_once()

        assert seen == [0, 1, 2]


class TestAckFailure:
    def test_a_failed_ack_does_not_strand_the_rest_of_the_batch(self, bus, client, monkeypatch, caplog):
        """The batch is read with ``>``, so an entry skipped here is never read again."""
        seen = []
        bus.subscribe("order.created", lambda event: seen.append(event.payload["id"]))
        for index in range(3):
            bus.publish(new_event("order.created", {"id": index}, source="api"))
        acks = []
        real_xack = client.xack

        def flaky_xack(stream, group, entry_id):
            acks.append(entry_id)
            if len(acks) == 1:
                raise redis.exceptions.ConnectionError("blip")
            return real_xack(stream, group, entry_id)

        monkeypatch.setattr(client, "xack", flaky_xack)

        with caplog.at_level(logging.ERROR):
            bus._poll_once()

        assert seen == [0, 1, 2]
        assert len(acks) == 3
        assert "ack" in caplog.text


class TestFailureIsolation:
    def test_a_raising_handler_does_not_kill_the_consumer(self, bus):
        """One poisoned entry must not take the whole replica's bus down."""
        seen = []
        bus.subscribe("order.created", lambda event: seen.append(event.payload["n"]) or 1 / event.payload["n"])
        bus.publish(new_event("order.created", {"n": 0}, source="api"))
        bus._poll_once()

        bus.publish(new_event("order.created", {"n": 1}, source="api"))
        bus._poll_once()

        assert seen == [0, 1]

    def test_every_handler_runs_when_one_raises(self, bus):
        calls = []
        bus.subscribe("order.created", lambda _event: calls.append("first"))
        bus.subscribe("order.created", lambda _event: (_ for _ in ()).throw(RuntimeError("boom")))
        bus.subscribe("order.created", lambda _event: calls.append("third"))
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()

        assert calls == ["first", "third"]

    def test_a_failed_entry_is_logged_and_acked(self, bus, client, caplog):
        """Best-effort: nothing reads a claimed entry again, so leaving it pending would only grow the PEL."""
        bus.subscribe("order.created", lambda _event: (_ for _ in ()).throw(RuntimeError("boom")))
        bus.publish(new_event("order.created", {}, source="api"))

        with caplog.at_level("ERROR"):
            bus._poll_once()

        assert _pending_count(client) == 0
        assert "Event handling failed" in caplog.text
        assert "boom" in caplog.text

    def test_a_second_poll_does_not_redeliver_it(self, bus):
        """Pins the best-effort contract: retrying is what durable jobs are for."""
        attempts = []

        def _always_fails(_event):
            attempts.append(1)
            raise RuntimeError("boom")

        bus.subscribe("order.created", _always_fails)
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()
        bus._poll_once()

        assert len(attempts) == 1

    def test_an_undeserializable_entry_is_survived(self, bus, client, caplog):
        """A malformed entry is a data problem, not a reason to stop consuming."""
        client.xadd(STREAM, {"event_id": "1", "nonsense": "yes"})

        with caplog.at_level("ERROR"):
            bus._poll_once()

        assert _pending_count(client) == 0
        assert "Event handling failed" in caplog.text

    def test_a_lost_consumer_group_is_recreated(self, bus, client, monkeypatch):
        """A Redis restarted without persistence, or flushed, keeps the stream (XADD recreates it) but not the group."""
        client.delete(STREAM)
        bus.subscribe("order.created", lambda _event: bus._stop.set())
        bus.publish(new_event("order.created", {}, source="api"))
        waits = []

        def _wait(timeout=None):
            waits.append(timeout)
            if len(waits) > 3:  # stop a consumer that never recovers, rather than hang the suite
                bus._stop.set()

        monkeypatch.setattr(bus._stop, "wait", _wait)

        bus._run(bus._stop)

        assert len(waits) == 1  # one failed poll, then the group is back and the event delivered

    def test_a_redis_outage_is_logged_and_retried(self, bus, monkeypatch, caplog):
        """The consumer loop must survive Redis going away and come back on its own."""
        polls = []

        def _flaky():
            polls.append(1)
            if len(polls) == 1:
                raise redis.RedisError("connection lost")
            bus._stop.set()

        monkeypatch.setattr(bus, "_poll_once", _flaky)
        monkeypatch.setattr(bus._stop, "wait", lambda timeout=None: None)

        with caplog.at_level("ERROR"):
            bus._run(bus._stop)

        assert len(polls) == 2
        assert "consumer poll failed" in caplog.text


class TestRecorderIntegration:
    @pytest.fixture
    def recorder(self):
        return RecordingRecorder()

    @pytest.fixture
    def bus(self, client, recorder):
        bus = RedisStreamEventBus(client, stream=STREAM, group=GROUP, consumer="consumer-1", recorder=recorder)
        bus._ensure_group()
        return bus

    def test_processing_is_tracked_with_the_consumer_and_handlers(self, bus, recorder):
        bus.subscribe("order.created", lambda _event: None)
        event = new_event("order.created", {}, source="api")
        bus.publish(event)

        bus._poll_once()

        assert recorder.tracked == [(event.event_id, "consumer-1", "<lambda>")]

    def test_the_handler_names_are_joined(self, bus, recorder):
        def render_invoice(_event):
            pass

        def send_receipt(_event):
            pass

        bus.subscribe("order.created", render_invoice)
        bus.subscribe("order.created", send_receipt)
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()

        assert recorder.tracked[0][2] == "render_invoice,send_receipt"

    def test_no_handlers_means_no_handler_name(self, bus, recorder):
        bus.publish(new_event("order.created", {}, source="api"))

        bus._poll_once()

        assert recorder.tracked[0][2] is None

    def test_an_envelope_that_cannot_be_serialized_is_not_recorded_as_published(self, bus, recorder):
        """The row would stay ``published`` with nothing on the stream to move it on."""
        with pytest.raises(TypeError):
            bus.publish(new_event("order.created", {"when": object()}, source="api"))

        assert recorder.published == []

    def test_a_failure_reaches_the_recorder(self, bus, recorder):
        event = new_event("order.created", {}, source="api")
        bus.subscribe("order.created", lambda _event: (_ for _ in ()).throw(RuntimeError("boom")))
        bus.publish(event)

        bus._poll_once()

        assert recorder.failed == [event.event_id]


class TestConsumerLifecycle:
    def test_start_runs_a_consumer_thread_that_delivers(self, bus):
        delivered = threading.Event()
        bus.subscribe("order.created", lambda _event: delivered.set())
        bus.publish(new_event("order.created", {}, source="api"))

        bus.start()
        try:
            assert delivered.wait(timeout=5), "the consumer thread never dispatched the event"
        finally:
            bus.stop()

    def test_starting_twice_keeps_one_thread(self, bus):
        bus.start()
        try:
            thread = bus._thread

            bus.start()

            assert bus._thread is thread
        finally:
            bus.stop()

    def test_stop_joins_the_thread(self, bus):
        bus.start()
        thread = bus._thread

        bus.stop()

        assert not thread.is_alive()
        assert bus._thread is None

    def test_a_restart_after_a_stop_that_timed_out_leaves_the_old_thread_stopped(self, bus):
        """A handler outlasting the stop timeout keeps its thread alive; restarting must not revive that loop."""
        bus.start()
        old_thread, old_stop = bus._thread, bus._stop
        # What a timed-out stop leaves: the thread forgotten, its stop event set, the thread perhaps still running.
        bus._thread = None
        old_stop.set()

        bus.start()
        try:
            assert old_stop.is_set()
            old_thread.join(timeout=5)
            assert not old_thread.is_alive()
            assert bus._thread is not old_thread and bus._thread.is_alive()
        finally:
            bus.stop()

    def test_stopping_a_bus_that_never_started_is_a_no_op(self, bus):
        """``Platform.close`` calls this unconditionally."""
        bus.stop()

    def test_the_consumer_defaults_to_the_process_identity(self, client):
        """Two replicas must not share a consumer name, or they steal each other's entries."""
        bus = RedisStreamEventBus(client, stream=STREAM, group=GROUP)

        assert bus._consumer
