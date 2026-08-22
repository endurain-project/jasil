"""Async backend conformance and lifecycle coverage.

The async backends mirror the synchronous provider backends, so these tests keep
the same contract-oriented shape as the sync suites while awaiting every public
operation. Redis is exercised through ``fakeredis.aioredis`` and S3 through
``botocore.stub.Stubber`` so the suite stays hermetic.
"""

import asyncio
import io
import time
from typing import ClassVar

import boto3
import fakeredis
import pytest
import redis
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber

import jasil._core.redis_clients as redis_clients
from jasil._core.tasks import signal_and_cancel
from jasil.backends.events_inprocess_async import AsyncInProcessEventBus
from jasil.backends.events_redis_async import (
    AsyncRedisStreamEventBus,
    create_async_redis_event_bus,
    deserialize_event,
    serialize_event,
)
from jasil.backends.lock_noop_async import AsyncNoopLock
from jasil.backends.state_memory_async import AsyncMemoryState
from jasil.backends.state_redis_async import AsyncRedisState, create_async_redis_state
from jasil.backends.storage_local_async import AsyncLocalStorage
from jasil.backends.storage_s3_async import AsyncS3Storage
from jasil.events import Event, new_event
from jasil.providers import StateBackendUnavailableError, TieredFailureOutcome
from jasil.providers_async import (
    AsyncEventBusProvider,
    AsyncLockProvider,
    AsyncStateProvider,
    AsyncStorageProvider,
)

STREAM = "test:async:events"
GROUP = "test"
BUCKET = "blobs"
PREFIX = "jasil"
AREA = "avatars"
KEY = "42.webp"
OBJECT_KEY = f"{PREFIX}/{AREA}/{KEY}"
UNSAFE_SEGMENTS = ["../escape", "/etc/passwd", "a/../../b", ".."]


async def _collect_keys(state, prefix: str) -> list[str]:
    return [key async for key in state.iter_keys(prefix)]


@pytest.fixture(params=["memory", "redis"])
async def async_state(request):
    if request.param == "memory":
        yield AsyncMemoryState()
    else:
        client = fakeredis.aioredis.FakeRedis(decode_responses=False)
        try:
            yield AsyncRedisState(client)
        finally:
            await client.aclose()


@pytest.fixture
async def redis_client():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def redis_bus(redis_client):
    bus = AsyncRedisStreamEventBus(redis_client, stream=STREAM, group=GROUP, consumer="consumer-1")
    await bus._ensure_group()
    return bus


@pytest.fixture
def s3_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def s3_stub(s3_client):
    with Stubber(s3_client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture
def s3_storage(s3_client):
    return AsyncS3Storage(s3_client, BUCKET, PREFIX)


def _body(data: bytes) -> StreamingBody:
    return StreamingBody(io.BytesIO(data), len(data))


class AsyncRecordingRecorder:
    """Stands in for the async event-log recorder, capturing lifecycle calls."""

    def __init__(self) -> None:
        self.published: list[Event] = []
        self.tracked: list[tuple[str, str, str | None, bool]] = []
        self.failed: list[str] = []

    async def record_published(self, event):
        self.published.append(event)

    async def record_queued(self, event):  # pragma: no cover - these buses never queue
        raise AssertionError("the async buses must not record 'queued'")

    def track(self, event, *, worker_id, handler_name, record_processing=True):
        recorder = self

        class _Tracked:
            async def __aenter__(self):
                recorder.tracked.append((event.event_id, worker_id, handler_name, record_processing))

            async def __aexit__(self, exc_type, exc, traceback):
                if exc is not None:
                    recorder.failed.append(event.event_id)
                return False

        return _Tracked()


async def _pending_count(client) -> int:
    return (await client.xpending(STREAM, GROUP))["pending"]


class TestAsyncStateProviderConformance:
    def test_both_backends_satisfy_the_protocol(self, async_state):
        assert isinstance(async_state, AsyncStateProvider)

    async def test_getting_a_missing_key_returns_none(self, async_state):
        assert await async_state.get("absent") is None

    async def test_a_value_round_trips_as_bytes(self, async_state):
        await async_state.set("k", b"value")

        assert await async_state.get("k") == b"value"

    async def test_setting_an_existing_key_overwrites_it(self, async_state):
        await async_state.set("k", b"first")

        await async_state.set("k", b"second")

        assert await async_state.get("k") == b"second"

    async def test_deleting_removes_the_key(self, async_state):
        await async_state.set("k", b"value")

        await async_state.delete("k")

        assert await async_state.get("k") is None

    async def test_deleting_a_missing_key_is_a_no_op(self, async_state):
        await async_state.delete("absent")

    async def test_incr_starts_from_zero(self, async_state):
        assert await async_state.incr("counter") == 1

    async def test_incr_accumulates(self, async_state):
        await async_state.incr("counter")

        assert await async_state.incr("counter") == 2

    async def test_incr_accepts_an_amount(self, async_state):
        assert await async_state.incr("counter", amount=5) == 5

    async def test_set_if_absent_claims_a_free_key(self, async_state):
        assert await async_state.set_if_absent("lock", b"owner") is True

    async def test_set_if_absent_refuses_a_taken_key(self, async_state):
        await async_state.set_if_absent("lock", b"first")

        assert await async_state.set_if_absent("lock", b"second") is False
        assert await async_state.get("lock") == b"first"

    async def test_get_and_delete_returns_the_value_then_clears_it(self, async_state):
        await async_state.set("once", b"value")

        assert await async_state.get_and_delete("once") == b"value"
        assert await async_state.get("once") is None

    async def test_get_and_delete_on_a_missing_key_returns_none(self, async_state):
        assert await async_state.get_and_delete("absent") is None

    async def test_delete_prefix_removes_only_matching_keys(self, async_state):
        await async_state.set("session:a", b"1")
        await async_state.set("session:b", b"2")
        await async_state.set("other:c", b"3")

        deleted = await async_state.delete_prefix("session:")

        assert deleted == 2
        assert await async_state.get("other:c") == b"3"

    async def test_delete_prefix_on_no_matches_returns_zero(self, async_state):
        assert await async_state.delete_prefix("nothing:") == 0

    async def test_iter_keys_yields_only_matching_keys(self, async_state):
        await async_state.set("session:a", b"1")
        await async_state.set("session:b", b"2")
        await async_state.set("other:c", b"3")

        assert sorted(await _collect_keys(async_state, "session:")) == ["session:a", "session:b"]

    async def test_iter_keys_yields_strings(self, async_state):
        await async_state.set("session:a", b"1")

        assert all(isinstance(key, str) for key in await _collect_keys(async_state, "session:"))

    async def test_iter_keys_on_no_matches_is_empty(self, async_state):
        assert await _collect_keys(async_state, "nothing:") == []

    @pytest.mark.parametrize("prefix", ["tenant:a*b:", "user:[1]:", "q?:", "back\\slash:"])
    async def test_a_prefix_is_matched_literally_and_not_as_a_glob(self, async_state, prefix):
        await async_state.set(f"{prefix}kept", b"1")
        await async_state.set("tenant:aXb:other", b"2")
        await async_state.set("user:1:other", b"3")
        await async_state.set("qZ:other", b"4")
        await async_state.set("backslash:other", b"5")

        assert sorted(await _collect_keys(async_state, prefix)) == [f"{prefix}kept"]

    async def test_delete_prefix_cannot_be_widened_into_the_whole_keyspace(self, async_state):
        await async_state.set("session:a", b"1")
        await async_state.set("other:b", b"2")

        assert await async_state.delete_prefix("*") == 0
        assert await async_state.get("session:a") == b"1"
        assert await async_state.get("other:b") == b"2"

    async def test_delete_prefix_removes_a_key_holding_a_metacharacter(self, async_state):
        await async_state.set("tenant:a*b:one", b"1")
        await async_state.set("tenant:aXb:two", b"2")

        assert await async_state.delete_prefix("tenant:a*b:") == 1
        assert await async_state.get("tenant:aXb:two") == b"2"

    async def test_a_ttl_bearing_value_is_readable_before_it_expires(self, async_state):
        await async_state.set("k", b"value", ttl_seconds=60)

        assert await async_state.get("k") == b"value"


class TestAsyncTieredFailureConformance:
    TIERS = ((3, 60), (5, 300))

    async def _record(self, state) -> TieredFailureOutcome:
        return await state.record_tiered_failure("counter", "gate", self.TIERS, 900)

    async def test_the_first_failure_counts_without_locking(self, async_state):
        outcome = await self._record(async_state)

        assert outcome == TieredFailureOutcome(1, None, False)

    async def test_failures_accumulate_below_the_first_threshold(self, async_state):
        await self._record(async_state)

        assert (await self._record(async_state)).count == 2

    async def test_reaching_a_threshold_locks(self, async_state):
        for _ in range(2):
            await self._record(async_state)

        outcome = await self._record(async_state)

        assert outcome.count == 3
        assert outcome.newly_locked is True
        assert outcome.locked_until_epoch > int(time.time())

    async def test_a_locked_caller_does_not_inflate_the_counter(self, async_state):
        for _ in range(3):
            await self._record(async_state)

        outcome = await self._record(async_state)

        assert outcome.count == 3
        assert outcome.newly_locked is False

    async def test_the_highest_crossed_tier_wins(self, async_state):
        outcome = await async_state.record_tiered_failure("c", "g", ((1, 60), (1, 300)), 900)

        assert outcome.locked_until_epoch - int(time.time()) > 60


class TestAsyncMemoryBackendExpiry:
    @pytest.fixture
    def clock(self, monkeypatch):
        current = {"now": 1000.0}
        monkeypatch.setattr("jasil.backends.state_memory_async.time.monotonic", lambda: current["now"])
        return current

    async def test_a_value_expires_once_its_ttl_elapses(self, clock):
        state = AsyncMemoryState()
        await state.set("k", b"value", ttl_seconds=10)

        clock["now"] += 11

        assert await state.get("k") is None

    async def test_a_value_without_a_ttl_never_expires(self, clock):
        state = AsyncMemoryState()
        await state.set("k", b"value")

        clock["now"] += 10_000

        assert await state.get("k") == b"value"

    async def test_an_expired_key_frees_its_slot_for_set_if_absent(self, clock):
        state = AsyncMemoryState()
        await state.set_if_absent("lock", b"first", ttl_seconds=10)

        clock["now"] += 11

        assert await state.set_if_absent("lock", b"second") is True

    async def test_incr_preserves_an_existing_expiry(self, clock):
        state = AsyncMemoryState()
        await state.incr("counter", ttl_seconds=10)

        await state.incr("counter")
        clock["now"] += 11

        assert await state.get("counter") is None

    async def test_expired_keys_are_not_listed(self, clock):
        state = AsyncMemoryState()
        await state.set("session:a", b"1", ttl_seconds=10)
        await state.set("session:b", b"2")

        clock["now"] += 11

        assert await _collect_keys(state, "session:") == ["session:b"]

    async def test_expired_gate_is_cleared_before_counting_again(self, clock, monkeypatch):
        current_wall = {"now": 1000}
        monkeypatch.setattr("jasil.backends.state_memory_async.time.time", lambda: current_wall["now"])
        state = AsyncMemoryState()
        outcome = await state.record_tiered_failure("counter", "gate", ((1, 10),), 900)
        assert outcome.newly_locked is True

        current_wall["now"] += 11
        clock["now"] += 11

        assert await state.record_tiered_failure("counter", "gate", ((2, 10),), 900) == TieredFailureOutcome(
            2, current_wall["now"] + 10, True
        )


class TestAsyncRedisBackend:
    @pytest.fixture
    def failing_state(self):
        from redis.exceptions import ConnectionError as RedisConnectionError

        class _Broken:
            def register_script(self, script):
                return None

            async def get(self, *args, **kwargs):
                raise RedisConnectionError("connection refused")

        return AsyncRedisState(_Broken())

    async def test_a_redis_outage_surfaces_as_a_provider_error(self, failing_state):
        with pytest.raises(StateBackendUnavailableError):
            await failing_state.get("k")

    async def test_the_original_redis_error_is_chained(self, failing_state):
        with pytest.raises(StateBackendUnavailableError) as excinfo:
            await failing_state.get("k")

        assert excinfo.value.__cause__ is not None

    async def test_iter_keys_translates_a_mid_scan_outage(self):
        from redis.exceptions import ConnectionError as RedisConnectionError

        class _Broken:
            def register_script(self, script):
                return None

            async def scan_iter(self, *args, **kwargs):
                raise RedisConnectionError("connection refused")
                yield "unreachable"

        with pytest.raises(StateBackendUnavailableError):
            await _collect_keys(AsyncRedisState(_Broken()), "k")

    async def test_factory_uses_shared_raw_client(self, monkeypatch):
        class _Client:
            def register_script(self, script):
                return script

        client = _Client()
        calls = []

        async def _get_shared(uri, *, purpose, decode_responses=True):
            calls.append((uri, purpose, decode_responses))
            return client

        monkeypatch.setattr(redis_clients, "get_shared_async_client", _get_shared)

        state = await create_async_redis_state("redis://cache:6379/0")

        assert state._client is client
        assert calls == [("redis://cache:6379/0", "platform state", False)]


class TestAsyncInProcessEventBus:
    def test_it_satisfies_the_protocol(self):
        assert isinstance(AsyncInProcessEventBus(), AsyncEventBusProvider)

    async def test_a_subscriber_receives_its_event_type(self):
        bus = AsyncInProcessEventBus()
        seen = []

        async def handler(event):
            seen.append(event)

        event = new_event("order.created", {"id": 42}, source="api")
        bus.subscribe("order.created", handler)

        await bus.publish(event)

        assert seen == [event]

    async def test_handlers_run_in_registration_order(self):
        bus = AsyncInProcessEventBus()
        calls = []

        async def first(_event):
            calls.append("first")

        async def second(_event):
            calls.append("second")

        bus.subscribe("order.created", first)
        bus.subscribe("order.created", second)

        await bus.publish(new_event("order.created", {}, source="api"))

        assert calls == ["first", "second"]

    async def test_another_event_type_is_not_delivered(self):
        bus = AsyncInProcessEventBus()
        seen = []

        async def handler(event):
            seen.append(event)

        bus.subscribe("order.created", handler)

        await bus.publish(new_event("invoice.rendered", {}, source="api"))

        assert seen == []

    async def test_a_handler_failure_is_recorded_and_reraised(self):
        recorder = AsyncRecordingRecorder()
        bus = AsyncInProcessEventBus(recorder=recorder)
        event = new_event("order.created", {}, source="api")

        async def handler(_event):
            raise RuntimeError("boom")

        bus.subscribe("order.created", handler)

        with pytest.raises(RuntimeError, match="boom"):
            await bus.publish(event)

        assert recorder.published == [event]
        assert recorder.tracked == [(event.event_id, "inprocess", "handler", False)]
        assert recorder.failed == [event.event_id]

    async def test_no_handlers_are_recorded_with_no_handler_name(self):
        recorder = AsyncRecordingRecorder()
        bus = AsyncInProcessEventBus(recorder=recorder)
        event = new_event("order.created", {}, source="api")

        await bus.publish(event)

        assert recorder.tracked == [(event.event_id, "inprocess", None, False)]

    async def test_start_and_stop_are_no_ops(self):
        bus = AsyncInProcessEventBus()

        await bus.start()
        await bus.stop()


class TestAsyncRedisSerialization:
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
        fields = serialize_event(new_event("order.created", {"id": 42}, source="api"))

        assert all(isinstance(value, str) for value in fields.values())

    def test_an_entry_written_before_versioning_reads_as_version_one(self):
        fields = serialize_event(new_event("order.created", {}, source="api"))
        del fields["schema_version"]

        assert deserialize_event(fields).schema_version == 1


class TestAsyncRedisStreamEventBus:
    async def test_the_event_lands_on_the_stream(self, redis_bus, redis_client):
        await redis_bus.publish(new_event("order.created", {"id": 42}, source="api"))

        assert await redis_client.xlen(STREAM) == 1

    async def test_it_is_recorded_published_from_the_producer(self, redis_client):
        recorder = AsyncRecordingRecorder()
        bus = AsyncRedisStreamEventBus(redis_client, stream=STREAM, group=GROUP, recorder=recorder)
        event = new_event("order.created", {}, source="api")

        await bus.publish(event)

        assert recorder.published == [event]

    async def test_the_group_is_created_on_start(self, redis_client):
        bus = AsyncRedisStreamEventBus(redis_client, stream=STREAM, group=GROUP)

        await bus._ensure_group()

        assert [group["name"] for group in await redis_client.xinfo_groups(STREAM)] == [GROUP]

    async def test_creating_it_twice_is_a_no_op(self, redis_bus):
        await redis_bus._ensure_group()

    async def test_a_real_redis_error_is_not_swallowed(self, redis_bus, monkeypatch):
        async def _explode(*args, **kwargs):
            raise redis.ResponseError("WRONGTYPE not a stream")

        monkeypatch.setattr(redis_bus._client, "xgroup_create", _explode)

        with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
            await redis_bus._ensure_group()

    async def test_a_subscriber_receives_its_event_type(self, redis_bus):
        seen = []

        async def handler(event):
            seen.append(event)

        redis_bus.subscribe("order.created", handler)
        await redis_bus.publish(new_event("order.created", {"id": 42}, source="api"))

        await redis_bus._poll_once()

        assert [event.payload for event in seen] == [{"id": 42}]

    async def test_every_subscriber_of_a_type_runs(self, redis_bus):
        first, second = [], []

        async def first_handler(event):
            first.append(event)

        async def second_handler(event):
            second.append(event)

        redis_bus.subscribe("order.created", first_handler)
        redis_bus.subscribe("order.created", second_handler)
        await redis_bus.publish(new_event("order.created", {}, source="api"))

        await redis_bus._poll_once()

        assert len(first) == len(second) == 1

    async def test_an_event_nobody_subscribed_to_is_acked(self, redis_bus, redis_client):
        await redis_bus.publish(new_event("order.created", {}, source="api"))

        await redis_bus._poll_once()

        assert await _pending_count(redis_client) == 0

    async def test_polling_an_empty_stream_does_nothing(self, redis_bus):
        await redis_bus._poll_once()

    async def test_a_batch_is_dispatched_in_order(self, redis_bus):
        seen = []

        async def handler(event):
            seen.append(event.payload["id"])

        redis_bus.subscribe("order.created", handler)
        for index in range(3):
            await redis_bus.publish(new_event("order.created", {"id": index}, source="api"))

        await redis_bus._poll_once()

        assert seen == [0, 1, 2]

    async def test_a_failed_entry_is_left_pending(self, redis_bus, redis_client, caplog):
        async def handler(_event):
            raise RuntimeError("boom")

        redis_bus.subscribe("order.created", handler)
        await redis_bus.publish(new_event("order.created", {}, source="api"))

        with caplog.at_level("ERROR"):
            await redis_bus._poll_once()

        assert await _pending_count(redis_client) == 1
        assert "leaving it pending" in caplog.text

    async def test_a_second_poll_does_not_redeliver_a_failed_entry(self, redis_bus):
        attempts = []

        async def handler(_event):
            attempts.append(1)
            raise RuntimeError("boom")

        redis_bus.subscribe("order.created", handler)
        await redis_bus.publish(new_event("order.created", {}, source="api"))

        await redis_bus._poll_once()
        await redis_bus._poll_once()

        assert len(attempts) == 1

    async def test_an_undeserializable_entry_is_survived(self, redis_bus, redis_client, caplog):
        await redis_client.xadd(STREAM, {"event_id": "1", "nonsense": "yes"})

        with caplog.at_level("ERROR"):
            await redis_bus._poll_once()

        assert await _pending_count(redis_client) == 1
        assert "Async event handler failed" in caplog.text

    async def test_a_redis_outage_is_logged_and_retried(self, redis_bus, monkeypatch, caplog):
        polls = []

        async def _flaky():
            polls.append(1)
            if len(polls) == 1:
                raise redis.RedisError("connection lost")
            redis_bus._stop.set()

        monkeypatch.setattr(redis_bus, "_poll_once", _flaky)

        with caplog.at_level("ERROR"):
            await redis_bus._run()

        assert len(polls) == 2
        assert "consumer poll failed" in caplog.text.lower()

    async def test_run_remains_cancellable(self, redis_bus, monkeypatch):
        never = asyncio.Event()

        async def _blocked_poll():
            await never.wait()

        monkeypatch.setattr(redis_bus, "_poll_once", _blocked_poll)
        task = asyncio.create_task(redis_bus._run())
        await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)

    async def test_start_runs_a_consumer_task(self, redis_bus, monkeypatch):
        delivered = asyncio.Event()

        async def _poll_once():
            delivered.set()
            redis_bus._stop.set()

        monkeypatch.setattr(redis_bus, "_poll_once", _poll_once)

        await redis_bus.start()
        try:
            await asyncio.wait_for(delivered.wait(), timeout=5)
        finally:
            await redis_bus.stop()

    async def test_starting_twice_keeps_one_task(self, redis_bus):
        await redis_bus.start()
        try:
            task = redis_bus._task

            await redis_bus.start()

            assert redis_bus._task is task
        finally:
            await redis_bus.stop()

    async def test_stop_returns_promptly_while_the_consumer_is_polling(self, redis_bus):
        await redis_bus.start()

        started = time.monotonic()
        await redis_bus.stop()

        assert time.monotonic() - started < 2.0
        assert redis_bus._task is None

    async def test_stopping_a_bus_that_never_started_is_a_no_op(self, redis_bus):
        await redis_bus.stop()

    async def test_the_consumer_defaults_to_the_process_identity(self, redis_client):
        bus = AsyncRedisStreamEventBus(redis_client, stream=STREAM, group=GROUP)

        assert bus._consumer

    async def test_processing_is_tracked_with_the_consumer_and_handlers(self, redis_client):
        recorder = AsyncRecordingRecorder()
        bus = AsyncRedisStreamEventBus(
            redis_client, stream=STREAM, group=GROUP, consumer="consumer-1", recorder=recorder
        )
        await bus._ensure_group()

        async def handler(_event):
            pass

        event = new_event("order.created", {}, source="api")
        bus.subscribe("order.created", handler)
        await bus.publish(event)

        await bus._poll_once()

        assert recorder.tracked == [(event.event_id, "consumer-1", "handler", True)]

    async def test_a_failure_reaches_the_recorder(self, redis_client):
        recorder = AsyncRecordingRecorder()
        bus = AsyncRedisStreamEventBus(
            redis_client, stream=STREAM, group=GROUP, consumer="consumer-1", recorder=recorder
        )
        await bus._ensure_group()
        event = new_event("order.created", {}, source="api")

        async def handler(_event):
            raise RuntimeError("boom")

        bus.subscribe("order.created", handler)
        await bus.publish(event)

        await bus._poll_once()

        assert recorder.failed == [event.event_id]

    async def test_factory_uses_shared_text_client(self, monkeypatch):
        client = object()
        calls = []

        async def _get_shared(uri, *, purpose, decode_responses=True):
            calls.append((uri, purpose, decode_responses))
            return client

        monkeypatch.setattr(redis_clients, "get_shared_async_client", _get_shared)

        bus = await create_async_redis_event_bus("redis://cache:6379/0", stream="s", group="g")

        assert bus._client is client
        assert (bus._stream, bus._group) == ("s", "g")
        assert calls == [("redis://cache:6379/0", "async event bus", True)]


class TestAsyncLocalStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        return AsyncLocalStorage(str(tmp_path), url_prefix="/media")

    def test_it_satisfies_the_protocol(self, storage):
        assert isinstance(storage, AsyncStorageProvider)

    async def test_a_blob_round_trips(self, storage):
        await storage.save("thumbnails", "1.webp", b"bytes")

        assert await storage.get("thumbnails", "1.webp") == b"bytes"

    async def test_saving_returns_the_key(self, storage):
        assert await storage.save("thumbnails", "1.webp", b"x") == "1.webp"

    async def test_a_missing_blob_reads_as_none(self, storage):
        assert await storage.get("thumbnails", "absent.webp") is None

    async def test_existence_is_reported(self, storage):
        assert await storage.exists("thumbnails", "1.webp") is False

        await storage.save("thumbnails", "1.webp", b"x")

        assert await storage.exists("thumbnails", "1.webp") is True

    async def test_deleting_removes_the_blob(self, storage):
        await storage.save("thumbnails", "1.webp", b"x")

        await storage.delete("thumbnails", "1.webp")

        assert await storage.exists("thumbnails", "1.webp") is False

    async def test_deleting_a_missing_blob_is_a_no_op(self, storage):
        await storage.delete("thumbnails", "absent.webp")

    async def test_keys_are_listed_sorted_and_filtered(self, storage):
        for key in ("c.webp", "a.webp", "b.webp", "user-1.webp"):
            await storage.save("thumbnails", key, b"x")

        assert await storage.list_keys("thumbnails") == ["a.webp", "b.webp", "c.webp", "user-1.webp"]
        assert await storage.list_keys("thumbnails", prefix="user-") == ["user-1.webp"]

    async def test_listing_an_unknown_area_is_empty(self, storage):
        assert await storage.list_keys("nothing") == []

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("a b.webp", "/media/thumbnails/a%20b.webp"),
            ("a?b.webp", "/media/thumbnails/a%3Fb.webp"),
            ("a#b.webp", "/media/thumbnails/a%23b.webp"),
            ("100%.webp", "/media/thumbnails/100%25.webp"),
            ("2026/01/1.webp", "/media/thumbnails/2026/01/1.webp"),
        ],
    )
    async def test_a_key_is_percent_encoded(self, storage, key, expected):
        assert await storage.url("thumbnails", key) == expected

    async def test_a_requested_expiry_is_reported_as_ignored_once(self, storage, caplog):
        with caplog.at_level("WARNING"):
            for index in range(5):
                await storage.url("thumbnails", f"{index}.webp", expires_in=60)

        assert caplog.text.count("was ignored") == 1

    async def test_not_asking_for_an_expiry_is_quiet(self, storage, caplog):
        with caplog.at_level("WARNING"):
            await storage.url("thumbnails", "1.webp")

        assert caplog.text == ""

    async def test_a_nested_key_round_trips_and_is_listed(self, storage):
        await storage.save("thumbnails", "2026/01/1.webp", b"x")
        await storage.save("thumbnails", "flat.webp", b"x")

        assert await storage.get("thumbnails", "2026/01/1.webp") == b"x"
        assert await storage.list_keys("thumbnails") == ["2026/01/1.webp", "flat.webp"]
        assert await storage.list_keys("thumbnails", prefix="2026/") == ["2026/01/1.webp"]

    async def test_an_empty_directory_and_escaping_symlink_contribute_no_key(self, storage, tmp_path):
        (tmp_path / "thumbnails" / "empty").mkdir(parents=True)
        outside = tmp_path.parent / "secret.txt"
        outside.write_text("secret")
        (tmp_path / "thumbnails" / "link.webp").symlink_to(outside)

        assert await storage.list_keys("thumbnails") == []


class TestAsyncStorageSegmentValidation:
    @pytest.fixture(params=["local", "s3"])
    def storage(self, request, tmp_path, s3_client):
        if request.param == "local":
            return AsyncLocalStorage(str(tmp_path))
        return AsyncS3Storage(s3_client, "blobs", "jasil")

    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize("operation", ["save", "get", "exists", "delete", "url"])
    async def test_a_traversing_segment_is_refused(self, storage, unsafe, field, operation):
        arguments = {"area": "avatars", "key": "42.webp", field: unsafe}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else ()

        with pytest.raises(ValueError, match="escapes base directory"):
            await call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize("operation", ["save", "get", "exists", "delete", "url"])
    async def test_an_empty_segment_is_refused(self, storage, field, operation):
        arguments = {"area": "avatars", "key": "42.webp", field: ""}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else ()

        with pytest.raises(ValueError, match="must not be empty"):
            await call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    async def test_listing_refuses_a_traversing_area(self, storage, unsafe):
        with pytest.raises(ValueError, match="escapes base directory"):
            await storage.list_keys(unsafe)

    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    async def test_listing_refuses_a_traversing_prefix(self, storage, unsafe):
        with pytest.raises(ValueError, match="escapes base directory"):
            await storage.list_keys("avatars", unsafe)

    async def test_listing_refuses_an_empty_area(self, storage):
        with pytest.raises(ValueError, match="must not be empty"):
            await storage.list_keys("")


class TestAsyncS3Storage:
    def test_from_uri_reads_bucket_prefix_region_and_endpoint(self):
        storage = AsyncS3Storage.from_uri(
            "s3://my-bucket/nested/prefix?region=eu-west-1&endpoint_url=https://minio.test"
        )

        assert storage._bucket == "my-bucket"
        assert storage._prefix == "nested/prefix"
        assert storage._client.meta.region_name == "eu-west-1"
        assert storage._client.meta.endpoint_url == "https://minio.test"

    def test_a_uri_without_a_bucket_is_refused(self):
        with pytest.raises(ValueError, match="missing a bucket"):
            AsyncS3Storage.from_uri("s3://")

    async def test_it_writes_under_the_composed_key_and_returns_the_bare_one(self, s3_storage, s3_stub):
        s3_stub.add_response("put_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY, "Body": b"bytes"})

        assert await s3_storage.save(AREA, KEY, b"bytes") == KEY

    async def test_a_content_type_is_forwarded(self, s3_storage, s3_stub):
        s3_stub.add_response(
            "put_object",
            {},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "Body": b"bytes", "ContentType": "image/webp"},
        )

        await s3_storage.save(AREA, KEY, b"bytes", content_type="image/webp")

    async def test_no_prefix_yields_an_area_rooted_key(self, s3_client, s3_stub):
        storage = AsyncS3Storage(s3_client, BUCKET)
        s3_stub.add_response("put_object", {}, {"Bucket": BUCKET, "Key": f"{AREA}/{KEY}", "Body": b"bytes"})

        await storage.save(AREA, KEY, b"bytes")

    async def test_it_returns_the_stored_bytes(self, s3_storage, s3_stub):
        s3_stub.add_response("get_object", {"Body": _body(b"bytes")}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        assert await s3_storage.get(AREA, KEY) == b"bytes"

    @pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
    async def test_a_missing_blob_is_none(self, s3_storage, s3_stub, code):
        s3_stub.add_client_error("get_object", service_error_code=code, http_status_code=404)

        assert await s3_storage.get(AREA, KEY) is None

    async def test_an_access_failure_is_not_a_missing_blob(self, s3_storage, s3_stub):
        s3_stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(ClientError):
            await s3_storage.get(AREA, KEY)

    async def test_a_present_blob_is_true(self, s3_storage, s3_stub):
        s3_stub.add_response("head_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        assert await s3_storage.exists(AREA, KEY) is True

    async def test_a_missing_blob_is_false(self, s3_storage, s3_stub):
        s3_stub.add_client_error("head_object", service_error_code="404", http_status_code=404)

        assert await s3_storage.exists(AREA, KEY) is False

    async def test_a_throttling_failure_surfaces(self, s3_storage, s3_stub):
        s3_stub.add_client_error("head_object", service_error_code="SlowDown", http_status_code=503)

        with pytest.raises(ClientError):
            await s3_storage.exists(AREA, KEY)

    async def test_it_deletes_the_composed_key(self, s3_storage, s3_stub):
        s3_stub.add_response("delete_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        await s3_storage.delete(AREA, KEY)

    async def test_it_strips_the_bucket_prefix_and_the_area(self, s3_storage, s3_stub):
        s3_stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/b.webp"}, {"Key": f"{PREFIX}/{AREA}/a.webp"}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"},
        )

        assert await s3_storage.list_keys(AREA) == ["a.webp", "b.webp"]

    async def test_a_key_prefix_narrows_the_listing(self, s3_storage, s3_stub):
        s3_stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/user-1.webp"}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/user-"},
        )

        assert await s3_storage.list_keys(AREA, "user-") == ["user-1.webp"]

    async def test_every_page_is_read(self, s3_storage, s3_stub):
        s3_stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/a.webp"}], "IsTruncated": True, "NextContinuationToken": "next"},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"},
        )
        s3_stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/b.webp"}], "IsTruncated": False},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}", "ContinuationToken": "next"},
        )

        assert await s3_storage.list_keys(AREA) == ["a.webp", "b.webp"]

    async def test_an_empty_area_lists_nothing(self, s3_storage, s3_stub):
        s3_stub.add_response("list_objects_v2", {}, {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"})

        assert await s3_storage.list_keys(AREA) == []

    async def test_a_nested_key_is_listed_by_its_full_path(self, s3_storage, s3_stub):
        s3_stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/2026/01/a.webp"}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"},
        )

        assert await s3_storage.list_keys(AREA) == ["2026/01/a.webp"]

    async def test_it_presigns_the_composed_key_and_honours_expiry(self, s3_storage):
        short = await s3_storage.url(AREA, KEY, expires_in=60)
        long = await s3_storage.url(AREA, KEY, expires_in=86_400)

        assert OBJECT_KEY in short
        assert BUCKET in short
        assert "Signature" in short
        assert short != long


class TestAsyncNoopLock:
    def test_it_satisfies_the_protocol(self):
        assert isinstance(AsyncNoopLock(), AsyncLockProvider)

    async def test_it_always_acquires(self):
        async with AsyncNoopLock().try_acquire("retention", ttl_seconds=60) as acquired:
            assert acquired is True

    async def test_it_can_be_reentered(self):
        lock = AsyncNoopLock()
        async with lock.try_acquire("a") as first, lock.try_acquire("b") as second:
            assert first is second is True


class TestSignalAndCancel:
    async def test_a_missing_task_only_sets_the_stop_event(self):
        stop = asyncio.Event()

        await signal_and_cancel(None, stop)

        assert stop.is_set()

    async def test_a_cooperative_task_is_awaited_without_cancellation(self):
        stop = asyncio.Event()
        finished = asyncio.Event()

        async def _worker():
            await stop.wait()
            finished.set()

        task = asyncio.create_task(_worker(), name="cooperative")

        await signal_and_cancel(task, stop, timeout=0.2)

        assert finished.is_set()
        assert task.done()
        assert not task.cancelled()

    async def test_an_unresponsive_task_is_cancelled_and_logged(self, caplog):
        stop = asyncio.Event()

        async def _worker():
            await asyncio.Event().wait()

        task = asyncio.create_task(_worker(), name="stuck")

        with caplog.at_level("WARNING"):
            await signal_and_cancel(task, stop, timeout=0.01)

        assert stop.is_set()
        assert task.cancelled()
        assert "cancelling it" in caplog.text

    async def test_a_task_error_is_logged_and_swallowed(self, caplog):
        stop = asyncio.Event()

        async def _worker():
            raise RuntimeError("boom")

        task = asyncio.create_task(_worker(), name="broken")
        await asyncio.sleep(0)

        with caplog.at_level("WARNING"):
            await signal_and_cancel(task, stop, timeout=0.2)

        assert "stopped with an error" in caplog.text


class RecordingAsyncClient:
    """Stands in for ``redis.asyncio.Redis``, recording async construction and close."""

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

    async def ping(self):
        self.pinged = True
        return True

    async def aclose(self):
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


@pytest.fixture(autouse=True)
def _no_leaked_async_clients():
    redis_clients.reset_shared_async_clients()
    RecordingAsyncClient.created = []
    yield
    redis_clients.reset_shared_async_clients()


@pytest.fixture
def async_recording(monkeypatch):
    monkeypatch.setattr(redis.asyncio, "Redis", RecordingAsyncClient)
    return RecordingAsyncClient


class TestAsyncRedisClients:
    async def test_connectivity_is_verified_eagerly(self, async_recording):
        client = await redis_clients.create_async_redis_client("redis://cache:6379/0", "test")

        assert client.pinged is True

    async def test_the_url_timeouts_and_response_mode_are_passed_through(self, async_recording):
        await redis_clients.create_async_redis_client(
            "redis://cache:6379/0", "test", socket_timeout=7.5, decode_responses=False
        )

        created = async_recording.created[0]
        assert created["url"] == "redis://cache:6379/0"
        assert created["socket_timeout"] == 7.5
        assert created["socket_connect_timeout"] == 7.5
        assert created["decode_responses"] is False

    async def test_a_connection_failure_names_the_purpose_and_closes_the_client(self, monkeypatch):
        class _Unreachable(RecordingAsyncClient):
            async def ping(self):
                raise redis.RedisError("connection refused")

        monkeypatch.setattr(redis.asyncio, "Redis", _Unreachable)

        with pytest.raises(RuntimeError, match="platform state"):
            await redis_clients.create_async_redis_client("redis://cache:6379/0", "platform state")

        assert _Unreachable.created[0]["url"] == "redis://cache:6379/0"

    async def test_a_malformed_url_is_reported_the_same_way(self, monkeypatch):
        class _Invalid(RecordingAsyncClient):
            @classmethod
            def from_url(cls, url, **kwargs):
                raise ValueError("invalid URL scheme")

        monkeypatch.setattr(redis.asyncio, "Redis", _Invalid)

        with pytest.raises(RuntimeError, match="Unable to initialize Redis"):
            await redis_clients.create_async_redis_client("nonsense://", "test")

    async def test_the_url_is_not_echoed_into_the_error(self, monkeypatch):
        class _Unreachable(RecordingAsyncClient):
            async def ping(self):
                raise redis.RedisError("connection refused")

        monkeypatch.setattr(redis.asyncio, "Redis", _Unreachable)

        with pytest.raises(RuntimeError) as raised:
            await redis_clients.create_async_redis_client("redis://:hunter2@cache:6379/0", "test")

        assert "hunter2" not in str(raised.value)

    async def test_the_same_config_is_created_once(self, async_recording):
        first = await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="test")
        second = await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="test")

        assert first is second
        assert len(async_recording.created) == 1

    async def test_a_different_url_gets_its_own_client(self, async_recording):
        await redis_clients.get_shared_async_client("redis://a:6379/0", purpose="test")
        await redis_clients.get_shared_async_client("redis://b:6379/0", purpose="test")

        assert len(async_recording.created) == 2

    async def test_the_two_response_modes_do_not_share_a_client(self, async_recording):
        text = await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="bus", decode_responses=True)
        raw = await redis_clients.get_shared_async_client(
            "redis://cache:6379/0", purpose="state", decode_responses=False
        )

        assert text is not raw
        assert len(async_recording.created) == 2

    async def test_closing_releases_and_forgets_every_client(self, async_recording):
        client = await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="test")

        await redis_clients.close_shared_async_clients()

        assert client.closed is True
        assert redis_clients._shared_async_clients == {}

    async def test_a_client_that_will_not_close_is_dropped_anyway(self, async_recording, caplog):
        client = await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="test")
        client.close_error = RuntimeError("socket already gone")

        with caplog.at_level("WARNING"):
            await redis_clients.close_shared_async_clients()

        assert "Failed to close the shared async redis client" in caplog.text
        assert redis_clients._shared_async_clients == {}

    async def test_closing_twice_is_a_no_op(self, async_recording):
        await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="test")

        await redis_clients.close_shared_async_clients()
        await redis_clients.close_shared_async_clients()

    async def test_resetting_discards_without_closing(self, async_recording):
        client = await redis_clients.get_shared_async_client("redis://cache:6379/0", purpose="test")

        redis_clients.reset_shared_async_clients()

        assert client.closed is False
        assert redis_clients._shared_async_clients == {}
