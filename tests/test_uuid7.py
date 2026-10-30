"""Time-ordered ids: the audit log orders rows that share a timestamp by id."""

import time
import uuid

import jasil._core.uuid7 as uuid7_module
from jasil._core.uuid7 import uuid7


class TestUuid7:
    def test_it_is_a_version_7_rfc_variant_uuid(self):
        value = uuid7()

        assert (value.version, value.variant) == (7, uuid.RFC_4122)

    def test_the_timestamp_is_the_current_unix_millisecond(self):
        before = time.time_ns() // 1_000_000
        value = uuid7()
        after = time.time_ns() // 1_000_000

        assert before <= value.int >> 80 <= after + 1

    def test_ids_minted_in_one_millisecond_stay_in_order(self):
        ids = [uuid7() for _ in range(10_000)]

        assert ids == sorted(ids)
        assert len(set(ids)) == len(ids)

    def test_ids_keep_increasing_when_the_clock_steps_back(self, monkeypatch):
        first = uuid7()
        monkeypatch.setattr(uuid7_module.time, "time_ns", lambda: 0)

        assert uuid7() > first

    def test_an_exhausted_counter_moves_the_timestamp_forward(self, monkeypatch):
        monkeypatch.setattr(uuid7_module.time, "time_ns", lambda: 5_000_000)
        monkeypatch.setattr(uuid7_module, "_last_milliseconds", 5)
        monkeypatch.setattr(uuid7_module, "_last_counter", 2**12 - 1)

        assert uuid7().int >> 80 == 6
