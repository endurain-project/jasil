"""Keyset-pagination cursors: they round-trip, and anything a client could send back is refused cleanly."""

import base64
import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from jasil._core.cursors import decode_cursor, encode_cursor

MOMENT = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)


def _raw(value) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")


class TestRoundTrip:
    def test_the_key_comes_back_unchanged(self):
        assert decode_cursor(encode_cursor(MOMENT, "job-1"), name="jobs") == (MOMENT, "job-1")

    def test_a_naive_moment_is_taken_as_utc(self):
        moment, _ = decode_cursor(encode_cursor(MOMENT.replace(tzinfo=None), "job-1"), name="jobs")

        assert moment == MOMENT

    def test_another_offset_is_the_same_instant(self):
        local = MOMENT.astimezone(timezone(timedelta(hours=5)))

        assert decode_cursor(encode_cursor(local, "job-1"), name="jobs")[0] == MOMENT

    @pytest.mark.parametrize("offset", ["+05:00", "-03:30", "+00:00", "Z"])
    def test_a_forged_offset_decodes_to_the_same_instant_in_utc(self, offset):
        moment, _ = decode_cursor(_raw([f"2026-01-01T12:00:00.123456{offset}", "job-1"]), name="jobs")

        assert moment.tzinfo is UTC
        assert moment == datetime.fromisoformat(f"2026-01-01T12:00:00.123456{offset}")

    def test_the_cursor_is_url_safe_and_unpadded(self):
        cursor = encode_cursor(MOMENT, "id/with+chars?")

        assert "=" not in cursor and "+" not in cursor and "/" not in cursor


class TestRefusal:
    @pytest.mark.parametrize(
        "cursor",
        [
            pytest.param("", id="empty"),
            pytest.param("x" * 513, id="too-long"),
            pytest.param("not base64!", id="not-base64"),
            pytest.param(base64.urlsafe_b64encode(b"\xff\xfe").decode(), id="not-utf8"),
            pytest.param(_raw({"at": "2026-01-01T00:00:00+00:00"}), id="not-a-list"),
            pytest.param(_raw(["2026-01-01T00:00:00+00:00"]), id="one-item"),
            pytest.param(_raw(["2026-01-01T00:00:00+00:00", 7]), id="id-not-a-string"),
            pytest.param(_raw(["2026-01-01T00:00:00+00:00", ""]), id="empty-id"),
            pytest.param(_raw(["yesterday", "job-1"]), id="not-a-timestamp"),
            pytest.param(_raw(["2026-01-01T00:00:00", "job-1"]), id="naive-timestamp"),
        ],
    )
    def test_anything_this_module_did_not_produce_is_a_value_error(self, cursor):
        with pytest.raises(ValueError, match="invalid workers cursor"):
            decode_cursor(cursor, name="workers")

    @pytest.mark.parametrize("stamp", ["0001-01-01T00:00:00+05:00", "9999-12-31T23:59:59-05:00"])
    def test_an_offset_that_leaves_the_representable_range_is_a_value_error(self, stamp):
        with pytest.raises(ValueError, match="invalid workers cursor"):
            decode_cursor(_raw([stamp, "job-1"]), name="workers")
