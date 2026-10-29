"""The read-only query surface: filters, keyset pagination, the default time window, the summary."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

import jasil.audit as jasil_audit
from jasil.audit import AuditAction, AuditContext, AuditFilters, Category, Outcome, Severity

T0 = datetime(2026, 1, 1, 12, tzinfo=UTC)


@pytest.fixture
def actions(audit_env):
    catalog = jasil_audit.get_catalog()
    return {
        "login": catalog.register(AuditAction("auth.session.login", Category.AUTHENTICATION, Severity.LOW, "k", "d")),
        "delete": catalog.register(AuditAction("data.record.delete", Category.DATA_CHANGE, Severity.HIGH, "k", "d")),
    }


def _write(audit_env, action, *, minutes_ago: int = 0, **fields):
    """Record one row ``minutes_ago`` before the fixed clock's now."""
    context = fields.pop("context", {})
    audit_env.clock.moment = T0 - timedelta(minutes=minutes_ago)
    with jasil_audit.audit_context(**context):
        jasil_audit.record_detached(action, outcome=fields.pop("outcome", Outcome.SUCCESS), **fields)
    audit_env.clock.moment = T0


class TestFilters:
    @pytest.fixture(autouse=True)
    def _rows(self, audit_env, actions, session_factory):
        _write(audit_env, actions["login"], minutes_ago=50, context={"actor_id": "1", "actor_type": "user"})
        _write(
            audit_env,
            actions["delete"],
            minutes_ago=40,
            outcome=Outcome.DENIED,
            target_type="record",
            target_id="r1",
            target_owner_id="2",
            context={"actor_id": "1", "actor_type": "user", "source": "cli", "event_id": "evt-1"},
        )
        _write(
            audit_env,
            actions["delete"],
            minutes_ago=30,
            target_type="record",
            target_id="r2",
            context={"actor_id": "3", "actor_type": "api_key", "source": "worker"},
        )
        audit_env.clock.moment = T0

    def _ids(self, **filters) -> list[str | None]:
        return [entry.target_id for entry in jasil_audit.list_entries(AuditFilters(**filters)).entries]

    def test_with_no_filter_everything_in_the_window_is_listed_newest_first(self):
        entries = jasil_audit.list_entries().entries

        assert [entry.action_id for entry in entries] == [
            "data.record.delete",
            "data.record.delete",
            "auth.session.login",
        ]

    @pytest.mark.parametrize(
        ("filters", "expected"),
        [
            ({"action_ids": ["auth.session.login"]}, [None]),
            ({"action_ids": ["auth.session.login", "data.record.delete"]}, ["r2", "r1", None]),
            ({"categories": ["data_change"]}, ["r2", "r1"]),
            ({"severities": ["low"]}, [None]),
            ({"outcomes": ["denied"]}, ["r1"]),
            ({"actor_id": "1"}, ["r1", None]),
            ({"actor_type": "api_key"}, ["r2"]),
            ({"target_type": "record"}, ["r2", "r1"]),
            ({"target_id": "r1"}, ["r1"]),
            ({"target_owner_id": "2"}, ["r1"]),
            ({"event_id": "evt-1"}, ["r1"]),
            ({"source": "worker"}, ["r2"]),
            ({"actor_id": "1", "outcomes": ["success"]}, [None]),
            ({"actor_id": "nobody"}, []),
            ({"action_ids": []}, []),
        ],
    )
    def test_each_filter_narrows(self, filters, expected):
        assert self._ids(**filters) == expected

    def test_the_request_id_filter(self, audit_env, actions):
        import jasil.correlation as correlation

        correlation.set_correlation_id("req-9")
        _write(audit_env, actions["login"], minutes_ago=5)
        correlation.set_correlation_id(None)

        entries = jasil_audit.list_entries(AuditFilters(request_id="req-9")).entries

        assert [entry.request_id for entry in entries] == ["req-9"]

    def test_the_time_range_includes_both_ends(self):
        window = {"from_ts": T0 - timedelta(minutes=40), "to_ts": T0 - timedelta(minutes=30)}

        assert self._ids(**window) == ["r2", "r1"]

    def test_a_range_between_two_rows_is_empty(self):
        assert self._ids(from_ts=T0 - timedelta(minutes=39), to_ts=T0 - timedelta(minutes=31)) == []

    def test_a_range_given_in_another_offset_selects_the_same_instants(self):
        plus_five = timezone(timedelta(hours=5))
        window = {"from_ts": T0 - timedelta(minutes=40), "to_ts": T0 - timedelta(minutes=30)}

        assert self._ids(**{bound: moment.astimezone(plus_five) for bound, moment in window.items()}) == ["r2", "r1"]

    def test_the_default_window_is_the_last_24_hours(self, audit_env, actions):
        _write(audit_env, actions["login"], minutes_ago=24 * 60 + 1, context={"actor_id": "old"})

        assert "old" not in {entry.actor_id for entry in jasil_audit.list_entries().entries}
        assert "old" in {
            entry.actor_id for entry in jasil_audit.list_entries(AuditFilters(from_ts=T0 - timedelta(days=2))).entries
        }

    def test_the_default_window_ends_at_to_ts(self):
        assert self._ids(to_ts=T0 - timedelta(minutes=35)) == ["r1", None]

    def test_a_row_stamped_exactly_now_is_listed(self, audit_env, actions):
        _write(audit_env, actions["login"], target_id="now")

        assert "now" in self._ids()

    def test_an_inverted_range_is_refused(self):
        with pytest.raises(ValueError, match="from_ts"):
            self._ids(from_ts=T0, to_ts=T0 - timedelta(hours=1))

    def test_a_range_longer_than_the_maximum_is_refused(self):
        with pytest.raises(ValueError, match="31 days"):
            self._ids(from_ts=T0 - timedelta(days=31, seconds=1), to_ts=T0)

    def test_a_range_of_exactly_the_maximum_is_accepted(self):
        assert self._ids(from_ts=T0 - timedelta(days=31), to_ts=T0) == ["r2", "r1", None]


class TestPagination:
    @pytest.fixture(autouse=True)
    def _rows(self, audit_env, actions, session_factory):
        for index in range(7):
            # Two rows share each timestamp, so the cursor must break ties.
            _write(audit_env, actions["login"], minutes_ago=index // 2, target_id=f"t{index}")

    def _walk(self, limit: int) -> list[str | None]:
        seen: list[str | None] = []
        cursor = None
        while True:
            page = jasil_audit.list_entries(cursor=cursor, limit=limit)
            seen.extend(entry.target_id for entry in page.entries)
            if page.next_cursor is None:
                return seen
            cursor = page.next_cursor

    @pytest.mark.parametrize("limit", [1, 2, 3, 7, 8, 500])
    def test_walking_the_pages_yields_every_row_once_in_order(self, limit):
        seen = self._walk(limit)

        everything = [entry.target_id for entry in jasil_audit.list_entries(limit=500).entries]
        assert seen == everything
        assert sorted(seen) == [f"t{i}" for i in range(7)]

    def test_the_last_page_has_no_cursor(self):
        assert jasil_audit.list_entries(limit=7).next_cursor is None
        assert jasil_audit.list_entries(limit=6).next_cursor is not None

    def test_a_row_written_after_the_first_page_does_not_shift_the_next(self, audit_env, actions):
        first = jasil_audit.list_entries(limit=3)
        _write(audit_env, actions["login"], minutes_ago=0, target_id="late")

        second = jasil_audit.list_entries(cursor=first.next_cursor, limit=3)

        assert [e.target_id for e in first.entries + second.entries] == [
            e.target_id for e in jasil_audit.list_entries(limit=500).entries if e.target_id != "late"
        ][:6]

    @pytest.mark.parametrize("limit", [0, -1, 501])
    def test_a_limit_out_of_range_is_refused(self, limit):
        with pytest.raises(ValueError, match="limit"):
            jasil_audit.list_entries(limit=limit)

    def test_a_forged_cursor_is_refused(self):
        with pytest.raises(ValueError, match="audit cursor"):
            jasil_audit.list_entries(cursor="not-a-cursor")


class TestGetEntry:
    def test_it_returns_the_entry_by_key(self, audit_env, actions, session_factory):
        _write(audit_env, actions["login"], target_id="x")
        listed = jasil_audit.list_entries().entries[0]

        found = jasil_audit.get_entry(listed.id, listed.occurred_at)

        assert found == listed

    def test_a_wrong_id_or_time_finds_nothing(self, audit_env, actions, session_factory):
        _write(audit_env, actions["login"])
        listed = jasil_audit.list_entries().entries[0]

        assert jasil_audit.get_entry("nope", listed.occurred_at) is None
        assert jasil_audit.get_entry(listed.id, listed.occurred_at - timedelta(seconds=1)) is None


class TestSummary:
    def test_it_counts_by_category_outcome_and_severity_and_ranks_actions(self, audit_env, actions, session_factory):
        for _ in range(3):
            _write(audit_env, actions["login"], minutes_ago=10)
        _write(audit_env, actions["delete"], minutes_ago=10, outcome=Outcome.DENIED)
        _write(audit_env, actions["delete"], minutes_ago=60 * 30)  # outside the window

        result = jasil_audit.summary(hours=24)

        assert result.total == 4
        assert result.by_category == {"authentication": 3, "data_change": 1}
        assert result.by_outcome == {"success": 3, "denied": 1}
        assert result.by_severity == {"low": 3, "high": 1}
        assert [(a.action_id, a.count) for a in result.top_actions] == [
            ("auth.session.login", 3),
            ("data.record.delete", 1),
        ]

    def test_an_empty_trail_summarizes_to_zero(self, audit_env, session_factory):
        result = jasil_audit.summary()

        assert (result.total, result.by_category, result.top_actions) == (0, {}, [])

    def test_a_longer_window_includes_older_rows(self, audit_env, actions, session_factory):
        _write(audit_env, actions["delete"], minutes_ago=60 * 30)

        assert jasil_audit.summary(hours=48).total == 1
        assert jasil_audit.summary(hours=24).total == 0

    @pytest.mark.parametrize("hours", [0, 31 * 24 + 1, 10**9])
    def test_a_window_out_of_range_is_refused(self, hours):
        with pytest.raises(ValueError, match="hours"):
            jasil_audit.summary(hours=hours)


class TestCatalogListing:
    def test_it_lists_what_the_host_synchronized(self, audit_env, actions, session_factory):
        jasil_audit.sync_catalog()

        listed = {action.id: action for action in jasil_audit.list_catalog()}

        assert {"auth.session.login", "data.record.delete", "audit.catalog.sync"} <= set(listed)
        assert listed["data.record.delete"].severity == "high"
        assert listed["data.record.delete"].deprecated is False

    def test_it_is_empty_before_any_synchronization(self, audit_env, session_factory):
        assert jasil_audit.list_catalog() == []


def test_the_context_in_force_when_listing_is_irrelevant(audit_env, actions, session_factory):
    jasil_audit.set_context(AuditContext(actor_id="reader"))
    _write(audit_env, actions["login"], context={"actor_id": "writer"})

    assert jasil_audit.list_entries().entries[0].actor_id == "writer"
