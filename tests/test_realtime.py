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
        encoded = realtime_envelope.encode("7", {"kind": "activity", "id": 3})

        assert realtime_envelope.decode(encoded) == ("7", {"kind": "activity", "id": 3})

    def test_the_wire_format_is_versioned_compact_json(self):
        assert realtime_envelope.encode("7", {"a": 1}) == b'{"v":2,"key":"7","hint":{"a":1}}'

    def test_an_envelope_at_the_cap_is_accepted(self):
        empty = len(realtime_envelope.encode("k", {"p": ""}))
        hint = {"p": "x" * (128 - empty)}

        assert len(realtime_envelope.encode("k", hint, max_bytes=128)) == 128

    def test_an_envelope_over_the_cap_is_refused(self):
        with pytest.raises(ValueError, match="over the 4096-byte cap"):
            realtime_envelope.encode("k", {"p": "x" * 5000})

    def test_the_cap_counts_encoded_bytes_not_characters(self):
        hint = {"p": "é" * 30}  # 30 characters, 60 bytes

        with pytest.raises(ValueError, match="cap"):
            realtime_envelope.encode("k", hint, max_bytes=80)

    @pytest.mark.parametrize("hint", [{"when": object()}, {"x": float("nan")}, {"x": {1, 2}}])
    def test_a_hint_that_is_not_json_is_refused(self, hint):
        with pytest.raises(ValueError, match="not JSON-serialisable"):
            realtime_envelope.encode("k", hint)

    @pytest.mark.parametrize("hint", [["a"], "a", None])
    def test_a_hint_that_is_not_a_dict_is_refused(self, hint):
        with pytest.raises(ValueError, match="hint must be a dict"):
            realtime_envelope.encode("k", hint)

    @pytest.mark.parametrize("key", [7, 7.0, True, None, b"7"])
    def test_a_key_that_is_not_a_str_is_refused(self, key):
        with pytest.raises(ValueError, match="key must be a str"):
            realtime_envelope.encode(key, {})

    def test_an_empty_key_is_refused(self):
        with pytest.raises(ValueError, match="must not be empty"):
            realtime_envelope.encode("", {})

    def test_a_key_at_the_cap_is_accepted_and_one_over_is_refused(self):
        at_cap = "k" * realtime_envelope.MAX_KEY_LENGTH

        assert realtime_envelope.decode(realtime_envelope.encode(at_cap, {})) == (at_cap, {})
        with pytest.raises(ValueError, match="256-character cap"):
            realtime_envelope.encode(at_cap + "k", {})

    def test_a_key_with_an_unpaired_surrogate_is_refused(self):
        with pytest.raises(ValueError, match="unpaired Unicode surrogate"):
            realtime_envelope.encode("\ud800", {})

    def test_an_escaped_surrogate_key_decodes_to_nothing(self, caplog):
        with caplog.at_level(logging.WARNING):
            assert realtime_envelope.decode(b'{"v":2,"key":"\\ud800","hint":{}}') is None
        assert "Ignoring" in caplog.text

    def test_the_key_cap_is_256_and_the_version_is_2(self):
        assert realtime_envelope.MAX_KEY_LENGTH == 256
        assert realtime_envelope.ENVELOPE_VERSION == 2

    @pytest.mark.parametrize("key", ["7", "6f1c2a9e-0b7d-4c1e-9a55-3d2f8e7b1c40", "device:42:é", "日本語", "a b"])
    def test_any_non_empty_str_key_round_trips_unchanged(self, key):
        assert realtime_envelope.decode(realtime_envelope.encode(key, {"a": 1})) == (key, {"a": 1})

    def test_nan_and_infinity_are_refused(self):
        for value in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValueError, match="not JSON-serialisable"):
                realtime_envelope.encode("k", {"x": value})

    @pytest.mark.parametrize(
        "raw",
        [
            b"not json",
            b"[1, 2]",
            b'{"v": 2, "key": 7, "hint": {}}',
            b'{"v": 2, "key": true, "hint": {}}',
            b'{"v": 2, "key": null, "hint": {}}',
            b'{"v": 2, "key": "", "hint": {}}',
            b'{"v": 2, "key": "' + b"k" * 257 + b'", "hint": {}}',
            b'{"v": 2, "key": "7", "hint": []}',
            b'{"v": 2, "hint": {}}',
            None,
        ],
    )
    def test_a_malformed_payload_decodes_to_nothing(self, raw, caplog):
        with caplog.at_level(logging.WARNING):
            assert realtime_envelope.decode(raw) is None
        assert "Ignoring" in caplog.text

    @pytest.mark.parametrize("version", [0, 1, 3, "2", None])
    def test_an_unknown_version_decodes_to_nothing(self, version, caplog):
        raw = json.dumps({"v": version, "key": "7", "hint": {}})

        with caplog.at_level(logging.WARNING):
            assert realtime_envelope.decode(raw) is None
        assert "unsupported envelope version" in caplog.text


class TestInProcessRealtime:
    def test_it_satisfies_the_port(self, realtime):
        assert isinstance(realtime, RealtimeProvider)

    def test_a_handler_receives_the_user_and_the_hint(self, realtime):
        received = []
        realtime.subscribe(lambda key, hint: received.append((key, hint)))

        realtime.notify("7", {"kind": "activity"})

        assert received == [("7", {"kind": "activity"})]

    @pytest.mark.parametrize("key", ["6f1c2a9e-0b7d-4c1e-9a55-3d2f8e7b1c40", "org:42:room", "salle-é", "x" * 256])
    def test_any_non_empty_str_key_reaches_the_handler_unchanged(self, realtime, key):
        received = []
        realtime.subscribe(lambda k, hint: received.append((k, hint)))

        realtime.notify(key, {"kind": "x"})

        assert received == [(key, {"kind": "x"})]

    @pytest.mark.parametrize("key", [7, None, b"7", True, "", "x" * 257])
    def test_an_invalid_key_raises(self, realtime, key):
        with pytest.raises(ValueError):
            realtime.notify(key, {})

    @pytest.mark.parametrize("hint", [[], "a", None, {"x": float("nan")}, {"x": float("inf")}, {"o": object()}])
    def test_an_invalid_hint_raises(self, realtime, hint):
        with pytest.raises(ValueError):
            realtime.notify("k", hint)

    def test_delivery_is_synchronous_on_the_callers_thread(self, realtime):
        threads = []
        realtime.subscribe(lambda key, hint: threads.append(threading.current_thread()))

        realtime.notify("1", {})

        assert threads == [threading.current_thread()]

    def test_every_handler_receives_every_hint(self, realtime):
        first, second = [], []
        realtime.subscribe(lambda key, hint: first.append(key))
        realtime.subscribe(lambda key, hint: second.append(key))

        realtime.notify("1", {})
        realtime.notify("2", {})

        assert first == second == ["1", "2"]

    def test_a_failing_handler_is_logged_and_the_next_still_runs(self, realtime, caplog):
        received = []

        def broken(key, hint):
            raise RuntimeError("boom")

        realtime.subscribe(broken)
        realtime.subscribe(lambda key, hint: received.append(key))

        with caplog.at_level(logging.ERROR):
            realtime.notify("7", {})

        assert received == ["7"]
        assert "Realtime hint handler" in caplog.text
        assert "boom" in caplog.text

    def test_notify_before_subscribe_is_a_no_op(self, realtime):
        realtime.notify("7", {"kind": "activity"})

    def test_an_invalid_hint_raises_even_with_no_handler(self, realtime):
        """The programmer error surfaces whether or not anyone is listening."""
        with pytest.raises(ValueError):
            realtime.notify("7", {"when": object()})

    def test_an_oversized_hint_raises(self):
        realtime = InProcessRealtime(max_bytes=64)

        with pytest.raises(ValueError, match="cap"):
            realtime.notify("7", {"p": "x" * 64})

    def test_the_handler_sees_what_the_wire_would_carry(self, realtime):
        """Tuples arrive as lists and keys as strings, as they would through Redis."""
        received = []
        realtime.subscribe(lambda key, hint: received.append(hint))

        realtime.notify("7", {"ids": (1, 2), 3: "three"})

        assert received == [{"ids": [1, 2], "3": "three"}]

    def test_the_handler_gets_a_copy_not_the_callers_dict(self, realtime):
        received = []
        realtime.subscribe(lambda key, hint: received.append(hint))
        hint = {"n": 1}

        realtime.notify("7", hint)
        hint["n"] = 2

        assert received == [{"n": 1}]

    def test_start_and_stop_are_idempotent_no_ops(self, realtime):
        realtime.start()
        realtime.start()
        realtime.stop()
        realtime.stop()

    def test_clear_drops_every_handler(self, realtime):
        received = []
        realtime.subscribe(lambda key, hint: received.append(key))

        realtime.clear()
        realtime.notify("7", {})

        assert received == []
