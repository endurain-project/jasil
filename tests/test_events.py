"""The event envelope and the payload-versioning skew rules."""

import uuid
from datetime import datetime
from typing import ClassVar

import pytest
from pydantic import ValidationError

from jasil.event_versioning import (
    UnsupportedEventVersionError,
    VersionedPayload,
    parse_payload,
)
from jasil.events import (
    INITIAL_SCHEMA_VERSION,
    MAX_EVENT_ID_LENGTH,
    MAX_EVENT_TYPE_LENGTH,
    MAX_SOURCE_LENGTH,
    META_REQUEST_ID,
    Event,
    new_event,
)


class TestNewEvent:
    def test_it_mints_a_uuid4_event_id(self):
        event = new_event("activity.created", {}, source="test")

        assert uuid.UUID(event.event_id).version == 4

    def test_each_event_gets_a_distinct_id(self):
        first = new_event("activity.created", {}, source="test")
        second = new_event("activity.created", {}, source="test")

        assert first.event_id != second.event_id

    def test_an_explicit_id_is_preserved(self):
        """A retry re-publishes under the original id so tracing stays stable."""
        event = new_event("activity.created", {}, source="test", event_id="fixed-id", retry_count=2)

        assert event.event_id == "fixed-id"
        assert event.retry_count == 2

    def test_the_timestamp_is_utc_iso8601(self):
        event = new_event("activity.created", {}, source="test")

        parsed = datetime.fromisoformat(event.timestamp)

        assert parsed.tzinfo is not None
        assert parsed.utcoffset().total_seconds() == 0

    def test_metadata_defaults_to_an_empty_dict(self):
        event = new_event("activity.created", {}, source="test")

        assert event.metadata == {}

    def test_metadata_is_carried_through(self):
        event = new_event("a.b", {}, source="test", metadata={META_REQUEST_ID: "req-1"})

        assert event.metadata[META_REQUEST_ID] == "req-1"

    def test_the_schema_version_defaults_to_the_initial_version(self):
        """Producers written before versioning existed must keep working unchanged."""
        assert new_event("a.b", {}, source="test").schema_version == INITIAL_SCHEMA_VERSION

    def test_the_envelope_is_immutable(self):
        """Handlers share one envelope; a mutation would be visible to the others."""
        event = new_event("a.b", {}, source="test")

        with pytest.raises(Exception, match=r"frozen|immutable|cannot assign"):
            event.event_type = "other"  # type: ignore[misc]


class TestEnvelopeLengthLimits:
    """Identifiers are checked at mint, not at write.

    An over-long value raises a truncation error on PostgreSQL/MySQL and none at
    all on SQLite — and the publish seam swallows delivery failures, so the event
    would simply vanish with an opaque driver error in the log.
    """

    def test_an_over_long_event_type_is_refused(self):
        with pytest.raises(ValueError, match="event_type is 101 characters"):
            new_event("e" * 101, {}, source="test")

    def test_an_over_long_source_is_refused(self):
        with pytest.raises(ValueError, match="source is 51 characters"):
            new_event("a.b", {}, source="s" * 51)

    def test_an_over_long_explicit_event_id_is_refused(self):
        with pytest.raises(ValueError, match="event_id is 37 characters"):
            new_event("a.b", {}, source="test", event_id="i" * 37)

    def test_a_generated_event_id_always_fits(self):
        assert len(new_event("a.b", {}, source="test").event_id) == MAX_EVENT_ID_LENGTH

    def test_values_at_the_limit_are_accepted(self):
        event = new_event("e" * MAX_EVENT_TYPE_LENGTH, {}, source="s" * MAX_SOURCE_LENGTH)

        assert len(event.event_type) == MAX_EVENT_TYPE_LENGTH
        assert len(event.source) == MAX_SOURCE_LENGTH


class Payload(VersionedPayload):
    """A payload at version 3, with the upgrade path from 1."""

    SCHEMA_VERSION: ClassVar[int] = 3
    UPGRADERS: ClassVar[dict] = {
        1: lambda payload: {**payload, "added_in_v2": "default"},
        2: lambda payload: {**payload, "added_in_v3": "default"},
    }

    name: str
    added_in_v2: str
    added_in_v3: str


class PayloadWithMissingUpgrader(VersionedPayload):
    SCHEMA_VERSION: ClassVar[int] = 2
    UPGRADERS: ClassVar[dict] = {}

    name: str


def _event_at(version: int, payload: dict) -> Event:
    return new_event("thing.happened", payload, source="test", schema_version=version)


class TestParsePayload:
    def test_a_current_version_payload_validates_directly(self):
        event = _event_at(3, {"name": "n", "added_in_v2": "a", "added_in_v3": "b"})

        parsed = parse_payload(Payload, event)

        assert parsed.name == "n"

    def test_an_older_payload_is_upgraded_one_step_at_a_time(self):
        """A v1 payload must walk 1->2->3, not jump straight to the target."""
        event = _event_at(1, {"name": "n"})

        parsed = parse_payload(Payload, event)

        assert parsed.added_in_v2 == "default"
        assert parsed.added_in_v3 == "default"

    def test_an_intermediate_version_runs_only_the_remaining_steps(self):
        event = _event_at(2, {"name": "n", "added_in_v2": "explicit"})

        parsed = parse_payload(Payload, event)

        assert parsed.added_in_v2 == "explicit"
        assert parsed.added_in_v3 == "default"

    def test_a_newer_payload_is_refused(self):
        """During a rolling deploy an old replica may see a new build's event.

        Ignoring the unknown fields would make it read a repurposed field as its
        default and do the wrong thing quietly, so it refuses instead.
        """
        event = _event_at(4, {"name": "n"})

        with pytest.raises(UnsupportedEventVersionError, match="version 4; this build understands 3"):
            parse_payload(Payload, event)

    def test_refusing_a_newer_payload_is_logged_with_context(self, caplog):
        event = _event_at(4, {"name": "n"})

        with caplog.at_level("ERROR"), pytest.raises(UnsupportedEventVersionError):
            parse_payload(Payload, event)

        record = caplog.records[-1]
        assert record.event_type == "thing.happened"
        assert record.event_version == 4
        assert record.supported_version == 3

    def test_a_missing_upgrader_is_refused(self):
        """An evolution that shipped without its migration must fail loudly."""
        event = _event_at(1, {"name": "n"})

        with pytest.raises(UnsupportedEventVersionError, match="no upgrader from payload version 1 to 2"):
            parse_payload(PayloadWithMissingUpgrader, event)

    def test_an_upgrade_is_logged(self, caplog):
        event = _event_at(1, {"name": "n"})

        with caplog.at_level("INFO"):
            parse_payload(Payload, event)

        assert "Upgrading an event payload" in caplog.text

    def test_a_payload_that_does_not_match_its_model_raises(self):
        event = _event_at(3, {"name": "n"})

        with pytest.raises(ValidationError):
            parse_payload(Payload, event)

    def test_version_markers_are_class_vars_not_fields(self):
        """Without ``ClassVar`` Pydantic would demand them in every payload dict."""
        assert "SCHEMA_VERSION" not in Payload.model_fields
        assert "UPGRADERS" not in Payload.model_fields
