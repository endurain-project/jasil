"""The Redis Pub/Sub realtime backend — fan-out of hints to every replica.

Exercised against fakeredis, like the stream bus, so the suite stays hermetic.
Unlike the stream bus tests, most of these start the real subscriber thread:
the fan-out across instances, the reconnect and the shutdown *are* the subject,
and they only exist on that thread. Assertions wait on a condition with a
deadline rather than sleeping a fixed time.
"""

import json
import logging
import threading
import time

import fakeredis
import pytest

import jasil._core.redis_clients as redis_clients
import jasil.backends.realtime_redis as realtime_redis
from jasil.backends.realtime_redis import RedisRealtime
from jasil.providers import RealtimeProvider

CHANNEL = "test:realtime"
DEADLINE_SECONDS = 5.0


class Inbox:
    """A thread-safe handler that records what it receives."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._arrived = threading.Condition(self._lock)
        self.items: list[tuple[str, dict]] = []

    def __call__(self, key: str, hint: dict) -> None:
        with self._arrived:
            self.items.append((key, hint))
            self._arrived.notify_all()

    def wait_for(self, count: int) -> list[tuple[str, dict]]:
        with self._arrived:
            assert self._arrived.wait_for(lambda: len(self.items) >= count, timeout=DEADLINE_SECONDS), self.items
            return list(self.items)


@pytest.fixture(autouse=True)
def _fast_poll(monkeypatch):
    """Shorten the read window so every stop() returns promptly; behaviour is unchanged."""
    monkeypatch.setattr(realtime_redis, "_POLL_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(realtime_redis, "_START_WAIT_SECONDS", 0.2)


@pytest.fixture
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def make_realtime(server):
    """Build providers on one fake server, each with its own subscriber connection."""
    built: list[RedisRealtime] = []

    def make(**kwargs) -> RedisRealtime:
        options = {
            "subscriber_factory": lambda: fakeredis.FakeStrictRedis(server=server, decode_responses=True),
            "channel": CHANNEL,
            "reconnect_base_seconds": 0.01,
            "reconnect_max_seconds": 0.05,
            **kwargs,
        }
        realtime = RedisRealtime(fakeredis.FakeStrictRedis(server=server, decode_responses=True), **options)
        built.append(realtime)
        return realtime

    yield make
    for realtime in built:
        realtime.stop()


def _publish_raw(server, payload) -> None:
    fakeredis.FakeStrictRedis(server=server).publish(CHANNEL, payload)


class TestPublish:
    def test_it_satisfies_the_port(self, make_realtime):
        assert isinstance(make_realtime(), RealtimeProvider)

    def test_the_envelope_lands_on_the_channel(self, server, make_realtime):
        listener = fakeredis.FakeStrictRedis(server=server, decode_responses=True).pubsub(
            ignore_subscribe_messages=True
        )
        listener.subscribe(CHANNEL)
        listener.get_message(timeout=0.1)  # consume the subscribe confirmation

        make_realtime().notify("7", {"kind": "activity"})

        message = listener.get_message(timeout=1.0)
        assert json.loads(message["data"]) == {"v": 2, "key": "7", "hint": {"kind": "activity"}}

    def test_an_oversized_hint_raises(self, make_realtime):
        with pytest.raises(ValueError, match="cap"):
            make_realtime(max_bytes=64).notify("7", {"p": "x" * 64})

    def test_a_hint_that_is_not_json_raises(self, make_realtime):
        with pytest.raises(ValueError, match="not JSON-serialisable"):
            make_realtime().notify("7", {"when": object()})

    def test_a_transport_failure_is_logged_and_dropped(self, server, make_realtime, caplog):
        realtime = make_realtime()
        server.connected = False

        with caplog.at_level(logging.WARNING):
            realtime.notify("7", {"kind": "activity"})

        assert "Dropped a realtime hint for key '7'" in caplog.text

    def test_any_client_error_is_dropped(self, caplog):
        class Exploding:
            def publish(self, channel, payload):
                raise OSError("socket closed")

        with caplog.at_level(logging.WARNING):
            RedisRealtime(Exploding()).notify("7", {})

        assert "socket closed" in caplog.text


class TestValidation:
    """The Redis backend refuses what the in-process one refuses."""

    @pytest.mark.parametrize("key", [7, None, b"7", True, "", "x" * 257])
    def test_an_invalid_key_raises(self, make_realtime, key):
        with pytest.raises(ValueError):
            make_realtime().notify(key, {})

    @pytest.mark.parametrize("hint", [[], "a", None, {"x": float("nan")}, {"x": float("inf")}, {"o": object()}])
    def test_an_invalid_hint_raises(self, make_realtime, hint):
        with pytest.raises(ValueError):
            make_realtime().notify("k", hint)

    def test_any_non_empty_str_key_reaches_the_handler_unchanged(self, make_realtime):
        realtime = make_realtime()
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()
        key = "6f1c2a9e:é"

        realtime.notify(key, {"a": 1})

        assert inbox.wait_for(1) == [(key, {"a": 1})]


class TestFanOut:
    def test_every_started_instance_receives_a_hint_from_either(self, make_realtime):
        first, second = make_realtime(), make_realtime()
        first_inbox, second_inbox = Inbox(), Inbox()
        first.subscribe(first_inbox)
        second.subscribe(second_inbox)
        first.start()
        second.start()

        first.notify("1", {"from": "first"})
        second.notify("2", {"from": "second"})

        expected = [("1", {"from": "first"}), ("2", {"from": "second"})]
        assert sorted(first_inbox.wait_for(2)) == expected
        assert sorted(second_inbox.wait_for(2)) == expected

    def test_a_publish_only_instance_reaches_the_started_ones(self, make_realtime):
        """A dedicated job worker publishes without ever calling start()."""
        replica, worker = make_realtime(), make_realtime()
        inbox = Inbox()
        replica.subscribe(inbox)
        replica.start()

        worker.notify("7", {"kind": "import.done"})

        assert inbox.wait_for(1) == [("7", {"kind": "import.done"})]

    def test_every_handler_of_an_instance_runs(self, make_realtime):
        realtime = make_realtime()
        first, second = Inbox(), Inbox()
        realtime.subscribe(first)
        realtime.subscribe(second)
        realtime.start()

        realtime.notify("7", {})

        assert first.wait_for(1) == second.wait_for(1) == [("7", {})]

    def test_a_failing_handler_does_not_kill_the_subscriber(self, make_realtime, caplog):
        realtime = make_realtime()
        inbox = Inbox()

        def broken(key, hint):
            raise RuntimeError("boom")

        realtime.subscribe(broken)
        realtime.subscribe(inbox)
        realtime.start()

        with caplog.at_level(logging.ERROR):
            realtime.notify("1", {})
            realtime.notify("2", {})
            assert inbox.wait_for(2) == [("1", {}), ("2", {})]
        assert "Realtime hint handler" in caplog.text


class TestReceive:
    @pytest.mark.parametrize(
        "payload",
        [
            "not json",
            json.dumps({"v": 3, "key": "7", "hint": {}}),
            json.dumps({"v": 2, "key": 7, "hint": {}}),
            json.dumps({"v": 2, "key": "7", "hint": "x"}),
            json.dumps([1, 2, 3]),
        ],
    )
    def test_a_bad_payload_is_ignored_and_the_next_still_arrives(self, server, make_realtime, payload, caplog):
        realtime = make_realtime()
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()

        with caplog.at_level(logging.WARNING):
            _publish_raw(server, payload)
            realtime.notify("7", {"ok": True})
            assert inbox.wait_for(1) == [("7", {"ok": True})]
        assert "Ignoring" in caplog.text

    def test_a_bytes_payload_is_decoded(self, server, make_realtime):
        """A subscriber client without decode_responses hands over bytes."""
        realtime = make_realtime(subscriber_factory=lambda: fakeredis.FakeStrictRedis(server=server))
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()

        _publish_raw(server, b'{"v":2,"key":"7","hint":{"a":1}}')

        assert inbox.wait_for(1) == [("7", {"a": 1})]


class TestLifecycle:
    def test_a_restart_does_not_revive_a_thread_stop_gave_up_on(self, make_realtime, monkeypatch):
        realtime = make_realtime()
        realtime.start()
        old_thread, old_stop = realtime._thread, realtime._stop
        monkeypatch.setattr(realtime_redis, "signal_and_join", lambda thread, stop: stop.set())
        realtime.stop()
        monkeypatch.undo()

        realtime.start()

        assert old_stop.is_set()
        assert realtime._stop is not old_stop
        old_thread.join(timeout=DEADLINE_SECONDS)
        assert not old_thread.is_alive()

    def test_stop_from_a_handler_does_not_join_itself(self, make_realtime, caplog):
        realtime = make_realtime()
        stopped = threading.Event()
        realtime.subscribe(lambda key, hint: (realtime.stop(), stopped.set()))
        realtime.start()

        with caplog.at_level(logging.WARNING):
            realtime.notify("7", {})
            assert stopped.wait(timeout=DEADLINE_SECONDS)
        assert "Failed to stop" not in caplog.text

    def test_start_returns_only_once_redis_confirmed_the_subscription(self, server, make_realtime):
        realtime = make_realtime()
        inbox = Inbox()
        realtime.subscribe(inbox)

        realtime.start()
        _publish_raw(server, b'{"v":2,"key":"1","hint":{}}')

        assert inbox.wait_for(1) == [("1", {})]

    def test_a_non_utf8_payload_does_not_drop_the_subscription(self, server, make_realtime, caplog):
        realtime = make_realtime(subscriber_factory=lambda: fakeredis.FakeStrictRedis(server=server))
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()

        with caplog.at_level(logging.WARNING):
            _publish_raw(server, b"\xff\xfe")
            realtime.notify("7", {"ok": True})
            assert inbox.wait_for(1) == [("7", {"ok": True})]
        assert "disconnected" not in caplog.text

    def test_start_is_idempotent(self, make_realtime):
        realtime = make_realtime()
        realtime.start()
        thread = realtime._thread

        realtime.start()

        assert realtime._thread is thread

    def test_stop_is_idempotent_and_safe_before_start(self, make_realtime):
        realtime = make_realtime()
        realtime.stop()
        realtime.start()
        thread = realtime._thread

        realtime.stop()
        realtime.stop()

        assert not thread.is_alive()

    def test_it_can_be_restarted_after_stop(self, make_realtime):
        realtime = make_realtime()
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()
        realtime.stop()

        realtime.start()
        realtime.notify("7", {})

        assert inbox.wait_for(1) == [("7", {})]

    def test_a_stopped_instance_receives_nothing(self, make_realtime):
        listener, publisher = make_realtime(), make_realtime()
        inbox, witness = Inbox(), Inbox()
        listener.subscribe(inbox)
        listener.start()
        listener.stop()
        other = make_realtime()
        other.subscribe(witness)
        other.start()

        publisher.notify("7", {})

        witness.wait_for(1)
        assert inbox.items == []

    def test_start_does_not_raise_when_redis_is_down(self, server, make_realtime, caplog):
        realtime = make_realtime()
        server.connected = False

        with caplog.at_level(logging.WARNING):
            realtime.start()

        assert "not connected yet" in caplog.text
        server.connected = True
        assert realtime._subscribed.wait(timeout=DEADLINE_SECONDS)


class TestReconnect:
    def test_the_subscriber_reconnects_after_the_connection_drops(self, server, make_realtime, caplog):
        realtime = make_realtime()
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()

        with caplog.at_level(logging.INFO):
            server.connected = False
            deadline = time.monotonic() + DEADLINE_SECONDS
            while realtime._subscribed.is_set():
                assert time.monotonic() < deadline, "the subscriber never noticed the drop"
                time.sleep(0.01)
            server.connected = True
            assert realtime._subscribed.wait(timeout=DEADLINE_SECONDS)

            realtime.notify("7", {"after": "reconnect"})
            assert inbox.wait_for(1) == [("7", {"after": "reconnect"})]

        assert "Realtime subscriber disconnected" in caplog.text
        assert "reconnected after" in caplog.text

    def test_a_silent_connection_is_pinged_then_torn_down(self, server, make_realtime, caplog):
        """A half-open connection delivers nothing, so silence itself must trigger a reconnect.

        The PING is swallowed here, which is exactly the silence a dead
        connection produces; a live one answers with a PONG.
        """

        class Silent(fakeredis.FakeStrictRedis):
            def pubsub(self, **kwargs):
                pubsub = super().pubsub(**kwargs)
                pubsub.ping = lambda *args, **kw: None
                return pubsub

        realtime = make_realtime(
            keepalive_seconds=0.05,
            subscriber_factory=lambda: Silent(server=server, decode_responses=True),
        )
        realtime.start()

        with caplog.at_level(logging.WARNING):
            deadline = time.monotonic() + DEADLINE_SECONDS
            while "no traffic" not in caplog.text:
                assert time.monotonic() < deadline, "a silent subscription was never torn down"
                time.sleep(0.01)

    def test_a_failing_factory_is_retried(self, server, make_realtime):
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("Unable to initialize Redis storage for realtime hint subscriber.")
            return fakeredis.FakeStrictRedis(server=server, decode_responses=True)

        realtime = make_realtime(subscriber_factory=flaky)
        inbox = Inbox()
        realtime.subscribe(inbox)
        realtime.start()
        assert realtime._subscribed.wait(timeout=DEADLINE_SECONDS)

        realtime.notify("7", {})

        assert inbox.wait_for(1) == [("7", {})]
        assert len(attempts) == 3

    def test_the_backoff_is_capped_and_jittered(self):
        realtime = RedisRealtime(None, reconnect_base_seconds=1.0, reconnect_max_seconds=8.0)

        delays = [realtime._reconnect_delay(failures) for failures in range(100)]

        assert all(0.5 <= delay <= 8.0 for delay in delays)
        assert max(delays[10:]) > 4.0  # capped at 8, jittered within its upper half
        assert realtime._reconnect_delay(0) <= 1.0


class TestFromUri:
    def test_it_builds_on_the_shared_client(self, monkeypatch):
        shared = fakeredis.FakeStrictRedis(decode_responses=True)
        monkeypatch.setattr(redis_clients, "get_shared_client", lambda uri, *, purpose, decode_responses=True: shared)

        realtime = RedisRealtime.from_uri("redis://cache:6379/0", channel="custom")

        assert realtime._client is shared
        assert realtime._channel == "custom"

    def test_the_subscriber_gets_a_dedicated_client(self, monkeypatch):
        """The long-lived subscription must not hold a connection of the shared pool."""
        shared = fakeredis.FakeStrictRedis(decode_responses=True)
        dedicated = []
        monkeypatch.setattr(redis_clients, "get_shared_client", lambda uri, *, purpose, decode_responses=True: shared)
        monkeypatch.setattr(
            redis_clients,
            "create_redis_client",
            lambda uri, purpose, **kwargs: dedicated.append((uri, purpose, kwargs)) or fakeredis.FakeStrictRedis(),
        )

        realtime = RedisRealtime.from_uri("redis://cache:6379/0")
        client = realtime._subscriber_factory()

        assert client is not shared
        # Raw bytes, so a foreign payload on the server-wide channel cannot raise in the client.
        assert dedicated == [("redis://cache:6379/0", "realtime hint subscriber", {"decode_responses": False})]

    def test_an_unreachable_server_fails_eagerly(self):
        redis_clients.reset_shared_clients()
        try:
            with pytest.raises(RuntimeError, match="realtime hints"):
                RedisRealtime.from_uri("redis://127.0.0.1:1/0")
        finally:
            redis_clients.reset_shared_clients()

    def test_the_dedicated_client_is_closed_on_stop(self, server):
        closed = []

        class Tracked(fakeredis.FakeStrictRedis):
            def close(self):
                closed.append(self)
                super().close()

        realtime = RedisRealtime(
            fakeredis.FakeStrictRedis(server=server),
            subscriber_factory=lambda: Tracked(server=server, decode_responses=True),
            channel=CHANNEL,
        )
        realtime.start()
        realtime.stop()

        assert len(closed) == 1

    def test_a_close_failure_is_ignored(self, server):
        class Sticky(fakeredis.FakeStrictRedis):
            def close(self):
                raise OSError("already gone")

        realtime = RedisRealtime(
            fakeredis.FakeStrictRedis(server=server),
            subscriber_factory=lambda: Sticky(server=server, decode_responses=True),
            channel=CHANNEL,
        )
        realtime.start()

        realtime.stop()

    def test_the_publish_client_is_never_closed_by_the_subscriber(self, server):
        """Without a factory the subscriber borrows the publish client and must leave it open."""
        client = fakeredis.FakeStrictRedis(server=server, decode_responses=True)
        realtime = RedisRealtime(client, channel=CHANNEL)
        realtime.start()
        realtime.stop()

        assert client.ping()
