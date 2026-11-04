"""The ``none://`` backends: a disabled capability fails loudly or drops quietly, per its contract."""

import io
import logging

import pytest

import jasil
from jasil import CapabilityDisabledError
from jasil.backends.events_none import NullEventBus
from jasil.backends.realtime_none import NullRealtime
from jasil.backends.storage_none import NullStorage
from jasil.events import new_event
from jasil.providers import (
    EventBusProvider,
    PartRef,
    RealtimeProvider,
    StorageBackendUnavailableError,
    StorageProvider,
    UploadSession,
)

SESSION = UploadSession(
    area="a", key="k", session_id="s", max_bytes=None, min_part_size=1, max_part_size=2, max_parts=3
)


class TestNullStorage:
    def test_it_is_a_storage_provider(self):
        assert isinstance(NullStorage(), StorageProvider)

    @pytest.mark.parametrize(
        ("method", "args", "kwargs"),
        [
            ("save", ("a", "k", b"x"), {}),
            ("get", ("a", "k"), {}),
            ("stat", ("a", "k"), {}),
            ("exists", ("a", "k"), {}),
            ("delete", ("a", "k"), {}),
            ("list_keys", ("a",), {}),
            ("iter_objects", ("a",), {}),
            ("save_stream", ("a", "k", io.BytesIO(b"x")), {}),
            ("open_stream", ("a", "k"), {}),
            ("serve", ("a", "k"), {}),
            ("url", ("a", "k"), {}),
            ("delete_prefix", ("a", "p"), {}),
            ("copy", ("a", "k", "b", "k"), {}),
            ("check_writable", (), {}),
            ("begin_upload", ("a", "k"), {}),
            ("upload_part", (SESSION, 1, io.BytesIO(b"x")), {"size": 1}),
            ("complete_upload", (SESSION, [PartRef(part_number=1, size=1, validator="v")]), {}),
            ("abort_upload", (SESSION,), {}),
            ("cleanup_uploads", (), {"older_than_epoch": 0.0}),
        ],
    )
    def test_every_operation_reports_the_backend_unavailable(self, method, args, kwargs):
        with pytest.raises(StorageBackendUnavailableError, match="Storage is disabled"):
            getattr(NullStorage(), method)(*args, **kwargs)

    def test_the_message_names_the_setting_that_disabled_it(self):
        with pytest.raises(StorageBackendUnavailableError) as exc_info:
            NullStorage().check_writable()

        assert "storage_uri=none://" in str(exc_info.value)

    def test_arguments_are_not_validated_first(self):
        """An invalid address still reports the disabled capability, not a ValueError."""
        with pytest.raises(StorageBackendUnavailableError):
            NullStorage().get("../escape", "")


class TestNullEventBus:
    def test_it_is_an_event_bus_provider(self):
        assert isinstance(NullEventBus(), EventBusProvider)

    def test_publish_drops_the_event_and_logs_it_at_debug(self, caplog):
        event = new_event("order.created", {"id": 1}, source="test")

        with caplog.at_level(logging.DEBUG):
            result = NullEventBus().publish(event)

        assert result is None
        assert "order.created" in caplog.text
        assert event.event_id in caplog.text
        assert {record.levelno for record in caplog.records} == {logging.DEBUG}

    def test_subscribe_is_refused_because_the_handler_would_never_run(self):
        with pytest.raises(CapabilityDisabledError, match=r"events_uri=none://") as exc_info:
            NullEventBus().subscribe("order.created", lambda _event: None)

        assert "durable" in str(exc_info.value)

    def test_start_and_stop_do_nothing(self):
        bus = NullEventBus()

        assert bus.start() is None
        assert bus.stop() is None


class TestNullRealtime:
    def test_it_is_a_realtime_provider(self):
        assert isinstance(NullRealtime(), RealtimeProvider)

    def test_notify_drops_a_valid_hint_without_logging_its_key(self, caplog):
        with caplog.at_level(logging.DEBUG):
            result = NullRealtime().notify("user-secret-key", {"changed": "secret-hint"})

        assert result is None
        assert caplog.records
        assert "user-secret-key" not in caplog.text
        assert "secret-hint" not in caplog.text

    @pytest.mark.parametrize(("key", "hint"), [("", {}), ("k", ["not", "a", "dict"]), ("k", "text")])
    def test_notify_still_rejects_a_programmer_error(self, key, hint):
        with pytest.raises(ValueError):
            NullRealtime().notify(key, hint)

    def test_notify_still_enforces_the_size_cap(self):
        with pytest.raises(ValueError):
            NullRealtime(max_bytes=32).notify("k", {"data": "x" * 100})

    def test_subscribe_is_refused_because_the_handler_would_never_run(self):
        with pytest.raises(CapabilityDisabledError, match="realtime"):
            NullRealtime().subscribe(lambda _key, _hint: None)

    def test_start_and_stop_do_nothing(self):
        realtime = NullRealtime()

        assert realtime.start() is None
        assert realtime.stop() is None


class TestCapabilityDisabledError:
    def test_it_is_exported_from_the_package(self):
        assert jasil.CapabilityDisabledError is CapabilityDisabledError
        assert "CapabilityDisabledError" in jasil.__all__

    def test_it_is_a_runtime_error(self):
        assert issubclass(CapabilityDisabledError, RuntimeError)
