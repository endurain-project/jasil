"""The realtime hint channel — the port's contract and the in-process backend.

A hint is a latency optimisation: delivery is best-effort, but a hint that is
not JSON or is too large is a programmer error and must fail loudly, in the
in-process backend exactly as against Redis, so the bug shows up in a local
test rather than as a silently dropped push in production.
"""

import json
import logging
import threading

import pytest

import jasil._core.realtime_envelope as realtime_envelope
from jasil.backends.realtime_inprocess import InProcessRealtime
from jasil.providers import RealtimeProvider


@pytest.fixture
def realtime():
    return InProcessRealtime()


class TestEnvelope:
    def test_a_hint_survives_the_round_trip(self):
        encoded = realtime_envelope.encode(7, {"kind": "activity", "id": 3})

        assert realtime_envelope.decode(encoded) == (7, {"kind": "activity", "id": 3})

    def test_the_wire_format_is_versioned_compact_json(self):
        assert realtime_envelope.encode(7, {"a": 1}) == b'{"v":1,"user_id":7,"hint":{"a":1}}'

    def test_an_envelope_at_the_cap_is_accepted(self):
        empty = len(realtime_envelope.encode(1, {"p": ""}))
        hint = {"p": "x" * (128 - empty)}

        assert len(realtime_envelope.encode(1, hint, max_bytes=128)) == 128

    def test_an_envelope_over_the_cap_is_refused(self):
        with pytest.raises(ValueError, match="over the 4096-byte cap"):
            realtime_envelope.encode(1, {"p": "x" * 5000})

    def test_the_cap_counts_encoded_bytes_not_characters(self):
        hint = {"p": "é" * 30}  # 30 characters, 60 bytes

        with pytest.raises(ValueError, match="cap"):
            realtime_envelope.encode(1, hint, max_bytes=80)

    @pytest.mark.parametrize("hint", [{"when": object()}, {"x": float("nan")}, {"x": {1, 2}}])
    def test_a_hint_that_is_not_json_is_refused(self, hint):
        with pytest.raises(ValueError, match="not JSON-serialisable"):
            realtime_envelope.encode(1, hint)

    @pytest.mark.parametrize("hint", [["a"], "a", None])
    def test_a_hint_that_is_not_a_dict_is_refused(self, hint):
        with pytest.raises(ValueError, match="hint must be a dict"):
            realtime_envelope.encode(1, hint)

    @pytest.mark.parametrize("user_id", ["7", 7.0, True, None])
    def test_a_user_id_that_is_not_an_int_is_refused(self, user_id):
        with pytest.raises(ValueError, match="user_id must be an int"):
            realtime_envelope.encode(user_id, {})

    @pytest.mark.parametrize(
        "raw",
        [
            b"not json",
            b"[1, 2]",
            b'{"v": 1, "user_id": "7", "hint": {}}',
            b'{"v": 1, "user_id": true, "hint": {}}',
            b'{"v": 1, "user_id": 7, "hint": []}',
            b'{"v": 1, "hint": {}}',
            None,
        ],
    )
    def test_a_malformed_payload_decodes_to_nothing(self, raw, caplog):
        with caplog.at_level(logging.WARNING):
            assert realtime_envelope.decode(raw) is None
        assert "Ignoring" in caplog.text

    @pytest.mark.parametrize("version", [0, 2, "1", None])
    def test_an_unknown_version_decodes_to_nothing(self, version, caplog):
        raw = json.dumps({"v": version, "user_id": 7, "hint": {}})

        with caplog.at_level(logging.WARNING):
            assert realtime_envelope.decode(raw) is None
        assert "unsupported envelope version" in caplog.text


class TestInProcessRealtime:
    def test_it_satisfies_the_port(self, realtime):
        assert isinstance(realtime, RealtimeProvider)

    def test_a_handler_receives_the_user_and_the_hint(self, realtime):
        received = []
        realtime.subscribe(lambda user_id, hint: received.append((user_id, hint)))

        realtime.notify_user(7, {"kind": "activity"})

        assert received == [(7, {"kind": "activity"})]

    def test_delivery_is_synchronous_on_the_callers_thread(self, realtime):
        threads = []
        realtime.subscribe(lambda user_id, hint: threads.append(threading.current_thread()))

        realtime.notify_user(1, {})

        assert threads == [threading.current_thread()]

    def test_every_handler_receives_every_hint(self, realtime):
        first, second = [], []
        realtime.subscribe(lambda user_id, hint: first.append(user_id))
        realtime.subscribe(lambda user_id, hint: second.append(user_id))

        realtime.notify_user(1, {})
        realtime.notify_user(2, {})

        assert first == second == [1, 2]

    def test_a_failing_handler_is_logged_and_the_next_still_runs(self, realtime, caplog):
        received = []

        def broken(user_id, hint):
            raise RuntimeError("boom")

        realtime.subscribe(broken)
        realtime.subscribe(lambda user_id, hint: received.append(user_id))

        with caplog.at_level(logging.ERROR):
            realtime.notify_user(7, {})

        assert received == [7]
        assert "Realtime hint handler" in caplog.text
        assert "boom" in caplog.text

    def test_notify_before_subscribe_is_a_no_op(self, realtime):
        realtime.notify_user(7, {"kind": "activity"})

    def test_an_invalid_hint_raises_even_with_no_handler(self, realtime):
        """The programmer error surfaces whether or not anyone is listening."""
        with pytest.raises(ValueError):
            realtime.notify_user(7, {"when": object()})

    def test_an_oversized_hint_raises(self):
        realtime = InProcessRealtime(max_bytes=64)

        with pytest.raises(ValueError, match="cap"):
            realtime.notify_user(7, {"p": "x" * 64})

    def test_the_handler_sees_what_the_wire_would_carry(self, realtime):
        """Tuples arrive as lists and keys as strings, as they would through Redis."""
        received = []
        realtime.subscribe(lambda user_id, hint: received.append(hint))

        realtime.notify_user(7, {"ids": (1, 2), 3: "three"})

        assert received == [{"ids": [1, 2], "3": "three"}]

    def test_the_handler_gets_a_copy_not_the_callers_dict(self, realtime):
        received = []
        realtime.subscribe(lambda user_id, hint: received.append(hint))
        hint = {"n": 1}

        realtime.notify_user(7, hint)
        hint["n"] = 2

        assert received == [{"n": 1}]

    def test_start_and_stop_are_idempotent_no_ops(self, realtime):
        realtime.start()
        realtime.start()
        realtime.stop()
        realtime.stop()

    def test_clear_drops_every_handler(self, realtime):
        received = []
        realtime.subscribe(lambda user_id, hint: received.append(user_id))

        realtime.clear()
        realtime.notify_user(7, {})

        assert received == []
