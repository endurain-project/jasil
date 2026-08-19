"""The Redis client owner — creation, sharing, and shutdown.

One module owns every Redis connection in the process, and it memoizes clients so
a deployment with one Redis URL opens one connection per response mode rather
than one per consumer. Two properties matter: the memo key has to include
``decode_responses`` (the state backend needs raw ``bytes``, the event bus needs
``str``, and handing either the wrong one corrupts values silently), and shutdown
must never raise — it runs while something else is already going wrong.

``redis.Redis`` is replaced with a recording double rather than fakeredis here:
what is under test is *how this module drives the client* — the connect
parameters, the eager ping, the close-then-clear ordering — not Redis behaviour.
"""

from typing import ClassVar

import fakeredis
import pytest
import redis

import jasil.redis as platform_redis


class RecordingClient:
    """Stands in for ``redis.Redis``, recording how it was built and closed."""

    created: ClassVar[list[dict]] = []

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.closed = False
        self.pinged = False
        self.close_error: Exception | None = None

    @classmethod
    def from_url(cls, url, **kwargs):
        client = cls(url=url, **kwargs)
        cls.created.append(client.kwargs)
        return client

    def ping(self):
        self.pinged = True
        return True

    def close(self):
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


@pytest.fixture(autouse=True)
def _no_leaked_clients():
    """The memo is process-wide, so a test that fills it must empty it."""
    platform_redis.reset_shared_clients()
    RecordingClient.created = []
    yield
    platform_redis.reset_shared_clients()


@pytest.fixture
def recording(monkeypatch):
    monkeypatch.setattr(redis, "Redis", RecordingClient)
    return RecordingClient


class TestClientCreation:
    def test_connectivity_is_verified_eagerly(self, recording):
        """A URL that cannot be reached should fail at startup, not on first use."""
        client = platform_redis.create_redis_client("redis://cache:6379/0", "test")

        assert client.pinged is True

    def test_the_url_and_timeouts_are_passed_through(self, recording):
        platform_redis.create_redis_client("redis://cache:6379/0", "test", socket_timeout=7.5)

        created = recording.created[0]
        assert created["url"] == "redis://cache:6379/0"
        assert created["socket_timeout"] == 7.5
        assert created["socket_connect_timeout"] == 7.5

    def test_responses_are_decoded_by_default(self, recording):
        platform_redis.create_redis_client("redis://cache:6379/0", "test")

        assert recording.created[0]["decode_responses"] is True

    def test_a_byte_oriented_caller_can_opt_out(self, recording):
        """The StateProvider contract is raw ``bytes``, not ``str``."""
        platform_redis.create_redis_client("redis://cache:6379/0", "test", decode_responses=False)

        assert recording.created[0]["decode_responses"] is False

    def test_a_connection_failure_names_the_purpose(self, monkeypatch):
        class _Unreachable(RecordingClient):
            def ping(self):
                raise redis.RedisError("connection refused")

        monkeypatch.setattr(redis, "Redis", _Unreachable)

        with pytest.raises(RuntimeError, match="platform state"):
            platform_redis.create_redis_client("redis://cache:6379/0", "platform state")

    def test_a_malformed_url_is_reported_the_same_way(self, monkeypatch):
        class _Invalid(RecordingClient):
            @classmethod
            def from_url(cls, url, **kwargs):
                raise ValueError("invalid URL scheme")

        monkeypatch.setattr(redis, "Redis", _Invalid)

        with pytest.raises(RuntimeError, match="Unable to initialize Redis"):
            platform_redis.create_redis_client("nonsense://", "test")

    def test_the_url_is_not_echoed_into_the_error(self, monkeypatch):
        """A Redis URL carries the password; it must not reach a log or a traceback."""

        class _Unreachable(RecordingClient):
            def ping(self):
                raise redis.RedisError("connection refused")

        monkeypatch.setattr(redis, "Redis", _Unreachable)

        with pytest.raises(RuntimeError) as raised:
            platform_redis.create_redis_client("redis://user:hunter2@cache:6379/0", "test")

        assert "hunter2" not in str(raised.value)


class TestSharedClients:
    def test_the_same_config_is_created_once(self, recording):
        first = platform_redis.get_shared_client("redis://cache:6379/0", purpose="test")
        second = platform_redis.get_shared_client("redis://cache:6379/0", purpose="test")

        assert first is second
        assert len(recording.created) == 1

    def test_a_different_url_gets_its_own_client(self, recording):
        platform_redis.get_shared_client("redis://a:6379/0", purpose="test")
        platform_redis.get_shared_client("redis://b:6379/0", purpose="test")

        assert len(recording.created) == 2

    def test_the_two_response_modes_do_not_share_a_client(self, recording):
        """Handing the state backend a decoding client would corrupt every value."""
        text = platform_redis.get_shared_client("redis://cache:6379/0", purpose="bus", decode_responses=True)
        raw = platform_redis.get_shared_client("redis://cache:6379/0", purpose="state", decode_responses=False)

        assert text is not raw
        assert len(recording.created) == 2


class TestShutdown:
    def test_closing_releases_and_forgets_every_client(self, recording):
        client = platform_redis.get_shared_client("redis://cache:6379/0", purpose="test")

        platform_redis.close_shared_clients()

        assert client.closed is True
        assert platform_redis._shared_clients == {}

    def test_a_client_that_will_not_close_is_dropped_anyway(self, recording, caplog):
        """Shutdown runs while something else is already going wrong; it cannot raise."""
        client = platform_redis.get_shared_client("redis://cache:6379/0", purpose="test")
        client.close_error = RuntimeError("socket already gone")

        with caplog.at_level("WARNING"):
            platform_redis.close_shared_clients()

        assert "Failed to close the shared redis client" in caplog.text
        assert platform_redis._shared_clients == {}

    def test_closing_twice_is_a_no_op(self, recording):
        """``Platform.close`` is documented as safe to call more than once."""
        platform_redis.get_shared_client("redis://cache:6379/0", purpose="test")

        platform_redis.close_shared_clients()
        platform_redis.close_shared_clients()

    def test_closing_with_nothing_open_is_a_no_op(self):
        platform_redis.close_shared_clients()

    def test_resetting_discards_without_closing(self, recording):
        """For tests holding an injected fake, where closing would break the fixture."""
        client = platform_redis.get_shared_client("redis://cache:6379/0", purpose="test")

        platform_redis.reset_shared_clients()

        assert client.closed is False
        assert platform_redis._shared_clients == {}


class TestDeleteMatchingKeys:
    @pytest.fixture
    def client(self):
        return fakeredis.FakeStrictRedis(decode_responses=True)

    def test_matching_keys_are_deleted(self, client):
        client.set("session:a", "1")
        client.set("session:b", "1")

        assert platform_redis.delete_matching_keys(client, "session:*") == 2
        assert client.keys("session:*") == []

    def test_other_keys_are_untouched(self, client):
        client.set("session:a", "1")
        client.set("other:a", "1")

        platform_redis.delete_matching_keys(client, "session:*")

        assert client.exists("other:a") == 1

    def test_no_match_deletes_nothing(self, client):
        assert platform_redis.delete_matching_keys(client, "absent:*") == 0

    def test_a_large_match_is_deleted_in_batches(self, client):
        """One unbounded DEL would block the server for the length of the list."""
        for index in range(25):
            client.set(f"session:{index}", "1")

        assert platform_redis.delete_matching_keys(client, "session:*", scan_count=10) == 25
        assert client.keys("session:*") == []

    def test_no_key_is_missed_when_the_match_outgrows_one_batch(self, client):
        """Deleting mid-scan shrinks the keyspace table and makes SCAN skip buckets.

        A prefix invalidation that leaves keys behind is worse than one that
        fails: the caller believes the data is gone.
        """
        for index in range(500):
            client.set(f"session:{index}", "1")

        deleted = platform_redis.delete_matching_keys(client, "session:*", scan_count=10)

        assert deleted == 500
        assert client.keys("session:*") == []
