"""The audit write paths.

The property everything else depends on: ``stage`` puts the row in the caller's transaction, so the
audited change and its audit row commit or roll back together. The contrast between the two paths is the
other thing worth pinning: ``stage`` fails closed (the transaction rolls back), ``record_detached`` fails
open (the request carries on, and the failure is counted).
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

import jasil.audit as jasil_audit
import jasil.correlation as correlation
import jasil.testing as jasil_testing
from jasil._core.uuid7 import uuid7
from jasil.audit import AuditAction, AuditContext, Category, Outcome, Severity
from jasil.audit.models import AuditLog
from jasil.event_log.models import EventLog
from jasil.event_versioning import VersionedPayload


class ProfileChange(VersionedPayload):
    SCHEMA_VERSION = 3

    fields: list[str]


@pytest.fixture
def change(audit_env):
    return jasil_audit.get_catalog().register(
        AuditAction(
            id="profile.user.update",
            category=Category.IDENTITY,
            severity=Severity.MEDIUM,
            description_key="audit.profile.user.update",
            description_default="A profile was updated",
            details_model=ProfileChange,
        )
    )


def _rows(db) -> list[AuditLog]:
    return list(db.scalars(select(AuditLog).order_by(AuditLog.occurred_at, AuditLog.id)))


def _domain_change(db) -> None:
    db.add(EventLog(id="domain-change", event_type="x.y", event_source="test", event_payload={}))


class TestTransactionalWrite:
    def test_the_change_and_its_row_commit_together(self, db, change):
        _domain_change(db)
        jasil_audit.stage(db, change, target_type="user", target_id="7", details={"fields": ["email"]})

        db.commit()

        assert db.get(EventLog, "domain-change") is not None
        assert [row.target_id for row in _rows(db)] == ["7"]

    def test_the_change_and_its_row_roll_back_together(self, db, change):
        _domain_change(db)
        jasil_audit.stage(db, change, target_type="user", target_id="7", details={"fields": ["email"]})

        db.rollback()

        assert db.get(EventLog, "domain-change") is None
        assert _rows(db) == []

    def test_a_savepoint_rollback_discards_only_its_own_rows(self, db, change):
        jasil_audit.stage(db, change, target_id="kept", details={"fields": []})
        savepoint = db.begin_nested()
        jasil_audit.stage(db, change, target_id="discarded", details={"fields": []})
        savepoint.rollback()

        db.commit()

        assert [row.target_id for row in _rows(db)] == ["kept"]

    def test_it_only_adds_the_row_and_neither_commits_nor_flushes(self, db, change):
        jasil_audit.stage(db, change, details={"fields": []})

        assert [type(obj) for obj in db.new] == [AuditLog]
        assert db.scalar(select(func.count()).select_from(AuditLog).execution_options(autoflush=False)) == 0

    def test_the_row_is_flushed_with_the_session(self, db, change):
        jasil_audit.stage(db, change, details={"fields": []})

        db.flush()

        assert db.scalar(select(func.count()).select_from(AuditLog)) == 1
        db.rollback()
        assert db.scalar(select(func.count()).select_from(AuditLog)) == 0

    def test_a_string_id_resolves_through_the_catalog(self, db, change):
        jasil_audit.stage(db, "profile.user.update", details={"fields": []})
        db.commit()

        assert _rows(db)[0].action_id == "profile.user.update"

    def test_the_row_stands_alone(self, db, change, audit_env):
        audit_env.configure(app_name="shop", app_version="1.2.3")
        correlation.set_correlation_id("req-1")
        jasil_audit.set_context(
            AuditContext(
                actor_type="user",
                actor_id="42",
                credential_type="password",
                credential_id="c-1",
                on_behalf_of_id="9",
                source="api",
                ip_address="203.0.113.5",
                user_agent="pytest",
                event_id="evt-1",
            )
        )

        jasil_audit.stage(
            db,
            change,
            outcome=Outcome.DENIED,
            target_type="user",
            target_id="7",
            target_owner_id="7",
            details={"fields": ["email"]},
        )
        db.commit()

        row = _rows(db)[0]
        assert (row.action_id, row.category, row.severity, row.outcome) == (
            "profile.user.update",
            "identity",
            "medium",
            "denied",
        )
        assert (row.actor_type, row.actor_id, row.credential_type, row.credential_id, row.on_behalf_of_id) == (
            "user",
            "42",
            "password",
            "c-1",
            "9",
        )
        assert (row.target_type, row.target_id, row.target_owner_id) == ("user", "7", "7")
        assert (row.request_id, row.event_id, row.source) == ("req-1", "evt-1", "api")
        assert (row.ip_address, row.user_agent) == ("203.0.113.5", "pytest")
        assert (row.details, row.details_version) == ({"fields": ["email"]}, 3)
        assert (row.app_name, row.app_version) == ("shop", "1.2.3")
        assert row.instance_id is not None

    def test_occurred_at_is_the_platform_clock(self, db, change, audit_env):
        jasil_audit.stage(db, change, details={"fields": []})
        db.commit()

        assert _rows(db)[0].occurred_at.replace(tzinfo=UTC) == jasil_testing.DEFAULT_NOW

    def test_the_default_outcome_is_success(self, db, change):
        jasil_audit.stage(db, change, details={"fields": []})
        db.commit()

        assert _rows(db)[0].outcome == "success"

    def test_ids_sort_in_creation_order(self):
        ids = [str(uuid7()) for _ in range(50)]

        assert ids == sorted(set(ids))
        assert {i[14] for i in ids} == {"7"}

    def test_every_row_has_the_same_instance_id(self, db, change):
        jasil_audit.stage(db, change, details={"fields": []})
        jasil_audit.stage(db, change, details={"fields": []})
        db.commit()

        assert len({row.instance_id for row in _rows(db)}) == 1


class TestFailClosed:
    def test_an_unregistered_action_raises(self, db, audit_env):
        with pytest.raises(jasil_audit.UnknownAuditActionError):
            jasil_audit.stage(db, "nope.nope.nope")

        assert list(db.new) == []

    def test_an_action_value_that_was_never_registered_raises(self, db, audit_env):
        stranger = AuditAction("a.b.c", Category.SYSTEM, Severity.INFO, "k", "d")

        with pytest.raises(jasil_audit.UnknownAuditActionError):
            jasil_audit.stage(db, stranger)

    def test_details_that_do_not_match_the_model_raise(self, db, change):
        with pytest.raises(ValidationError):
            jasil_audit.stage(db, change, details={"fields": "not-a-list"})

        with pytest.raises(ValidationError):
            jasil_audit.stage(db, change)  # the model has a required field

        assert list(db.new) == []

    def test_a_storage_error_propagates_so_the_transaction_rolls_back(self, db, change, db_engine):
        _domain_change(db)
        AuditLog.__table__.drop(db_engine)
        jasil_audit.stage(db, change, details={"fields": []})

        with pytest.raises(DBAPIError):
            db.commit()
        db.rollback()

        assert db.get(EventLog, "domain-change") is None

    def test_an_over_long_target_id_raises_rather_than_being_clipped(self, db, change):
        with pytest.raises(ValueError, match="target_id"):
            jasil_audit.stage(db, change, target_id="x" * 65, details={"fields": []})

    def test_an_unknown_outcome_raises(self, db, change):
        with pytest.raises(ValueError, match="not a valid Outcome"):
            jasil_audit.stage(db, change, outcome="maybe", details={"fields": []})


class TestDetails:
    def test_a_model_instance_is_accepted_and_extra_keys_are_dropped(self, db, change):
        jasil_audit.stage(db, change, details=ProfileChange(fields=["a"]))
        jasil_audit.stage(db, change, details={"fields": ["b"], "password": "hunter2"})
        db.commit()

        assert [row.details for row in _rows(db)] == [{"fields": ["a"]}, {"fields": ["b"]}]

    def test_an_action_without_details_stores_null(self, db, audit_env):
        action = jasil_audit.get_catalog().register(
            AuditAction("plain.thing.happen", Category.SYSTEM, Severity.INFO, "k", "d")
        )
        jasil_audit.stage(db, action)
        db.commit()

        assert _rows(db)[0].details is None

    def test_a_payload_over_the_cap_is_refused(self, db, change, audit_env):
        audit_env.configure(max_details_bytes=64)

        with pytest.raises(jasil_audit.AuditDetailsError, match="max_details_bytes"):
            jasil_audit.stage(db, change, details={"fields": ["x" * 100]})

        assert list(db.new) == []

    def test_a_payload_at_the_cap_is_accepted(self, db, change, audit_env):
        payload = {"fields": ["x" * 10]}
        size = len(b'{"fields":["xxxxxxxxxx"]}')
        audit_env.configure(max_details_bytes=size)

        jasil_audit.stage(db, change, details=payload)

        assert len(list(db.new)) == 1

    def test_the_size_is_counted_in_utf8_bytes(self, db, change, audit_env):
        audit_env.configure(max_details_bytes=30)

        with pytest.raises(jasil_audit.AuditDetailsError):
            jasil_audit.stage(db, change, details={"fields": ["é" * 10]})


class TestDisabled:
    def test_nothing_is_written_but_arguments_are_still_checked(self, db, change, audit_env):
        audit_env.configure(enabled=False)

        jasil_audit.stage(db, change, details={"fields": []})
        with pytest.raises(jasil_audit.UnknownAuditActionError):
            jasil_audit.stage(db, "nope.nope.nope")
        with pytest.raises(ValidationError):
            jasil_audit.stage(db, change, details={"fields": 3})

        assert list(db.new) == []

    def test_the_capture_helper_still_sees_the_entry(self, db, change, audit_env):
        audit_env.configure(enabled=False)

        with jasil_testing.capture_audit() as captured:
            jasil_audit.stage(db, change, target_id="7", details={"fields": ["email"]})
            jasil_audit.record_detached(change, outcome=Outcome.DENIED, details={"fields": []})

        captured.assert_audited("profile.user.update", outcome="success", target_id="7")
        captured.assert_audited("profile.user.update", outcome=Outcome.DENIED)
        assert _rows(db) == []

    def test_maintenance_does_nothing(self, audit_env, session_factory):
        audit_env.configure(enabled=False)

        jasil_audit.run_audit_maintenance()

        with session_factory() as db:
            assert _rows(db) == []


class TestStageMany:
    def test_it_stages_every_record(self, db, change):
        jasil_audit.stage_many(
            db,
            [
                jasil_audit.AuditRecord(change, target_id="1", details={"fields": []}),
                jasil_audit.AuditRecord("profile.user.update", Outcome.FAILURE, target_id="2", details={"fields": []}),
            ],
        )
        db.commit()

        assert [(row.target_id, row.outcome) for row in _rows(db)] == [("1", "success"), ("2", "failure")]

    def test_one_invalid_record_stages_nothing(self, db, change):
        with pytest.raises(ValidationError):
            jasil_audit.stage_many(
                db,
                [
                    jasil_audit.AuditRecord(change, target_id="1", details={"fields": []}),
                    jasil_audit.AuditRecord(change, target_id="2", details={"fields": 5}),
                ],
            )

        assert list(db.new) == []

    def test_an_empty_batch_is_a_no_op(self, db, change):
        jasil_audit.stage_many(db, [])

        assert list(db.new) == []


class TestFailOpen:
    def test_the_row_is_committed_on_its_own_session(self, db, change, session_factory):
        _domain_change(db)  # the caller's transaction, which then rolls back

        jasil_audit.record_detached(change, outcome=Outcome.DENIED, target_id="7", details={"fields": ["email"]})
        db.rollback()

        assert db.get(EventLog, "domain-change") is None
        assert [(row.outcome, row.target_id) for row in _rows(db)] == [("denied", "7")]

    def test_a_storage_error_is_logged_counted_and_swallowed(self, change, db_engine, session_factory, caplog):
        AuditLog.__table__.drop(db_engine)

        with caplog.at_level(logging.ERROR):
            jasil_audit.record_detached(change, outcome=Outcome.FAILURE, details={"fields": []})

        health = jasil_audit.get_audit_health()
        assert health["write_failures"] == 1
        assert health["last_failure_at"] is not None
        assert "profile.user.update" in caplog.text

    def test_an_unknown_action_is_logged_counted_and_swallowed(self, audit_env, caplog):
        with caplog.at_level(logging.ERROR):
            jasil_audit.record_detached("nope.nope.nope", outcome=Outcome.FAILURE)

        assert jasil_audit.get_audit_health()["write_failures"] == 1
        assert "nope.nope.nope" in caplog.text

    def test_a_storage_error_logs_the_type_never_the_statement_parameters(
        self, change, db_engine, session_factory, caplog
    ):
        AuditLog.__table__.drop(db_engine)
        jasil_audit.set_context(AuditContext(ip_address="203.0.113.99", user_agent="secret-agent/1.0"))

        with caplog.at_level(logging.ERROR):
            jasil_audit.record_detached(change, outcome=Outcome.FAILURE, details={"fields": ["private-field"]})

        assert "OperationalError" in caplog.text or "ProgrammingError" in caplog.text
        for leaked in ("203.0.113.99", "secret-agent", "private-field", "parameters"):
            assert leaked not in caplog.text

    def test_invalid_details_are_swallowed_and_never_logged(self, change, session_factory, caplog):
        with caplog.at_level(logging.ERROR):
            jasil_audit.record_detached(change, outcome=Outcome.FAILURE, details={"fields": "hunter2"})

        assert jasil_audit.get_audit_health()["write_failures"] == 1
        assert "hunter2" not in caplog.text

    def test_a_missing_session_factory_does_not_raise(self, change, audit_env, monkeypatch):
        import jasil.orm as jasil_orm

        jasil_orm._session_factory.reset()

        jasil_audit.record_detached(change, outcome=Outcome.SUCCESS, details={"fields": []})

        assert jasil_audit.get_audit_health()["write_failures"] == 1

    def test_health_starts_clean(self, audit_env):
        assert jasil_audit.get_audit_health() == {
            "write_failures": 0,
            "last_failure_at": None,
            "furthest_partition_date": None,
            "last_checkpoint_at": None,
        }

    def test_a_success_does_not_count(self, change, session_factory):
        jasil_audit.record_detached(change, outcome=Outcome.SUCCESS, details={"fields": []})

        assert jasil_audit.get_audit_health()["write_failures"] == 0


class TestCaptureHelper:
    def test_it_collects_entries_staged_inside_the_block_only(self, db, change):
        jasil_audit.stage(db, change, target_id="before", details={"fields": []})
        with jasil_testing.capture_audit() as captured:
            jasil_audit.stage(db, change, target_id="inside", details={"fields": []})
        jasil_audit.stage(db, change, target_id="after", details={"fields": []})

        assert [entry.target_id for entry in captured.entries] == ["inside"]

    def test_assert_audited_matches_on_extra_fields_and_returns_the_entry(self, db, change):
        jasil_audit.set_context(AuditContext(actor_type="user", actor_id="3"))
        with jasil_testing.capture_audit() as captured:
            jasil_audit.stage(db, change, target_id="7", details={"fields": ["email"]})

        entry = captured.assert_audited("profile.user.update", actor_id="3", target_id="7")

        assert entry.details == {"fields": ["email"]}

    def test_assert_audited_explains_a_miss(self, db, change):
        with jasil_testing.capture_audit() as captured:
            jasil_audit.stage(db, change, details={"fields": []})

        with pytest.raises(AssertionError, match=r"was not audited.*profile\.user\.update\[success\]"):
            captured.assert_audited("profile.user.update", outcome=Outcome.DENIED)

    def test_assert_not_audited(self, db, change):
        with jasil_testing.capture_audit() as captured:
            captured.assert_not_audited("profile.user.update")
            jasil_audit.stage(db, change, details={"fields": []})

        with pytest.raises(AssertionError, match="was audited"):
            captured.assert_not_audited("profile.user.update")

    def test_reset_all_removes_a_leaked_observer_and_the_registered_actions(self, change):
        with jasil_testing.capture_audit():
            jasil_testing.reset_all()

        assert "profile.user.update" not in jasil_audit.get_catalog()


class TestClock:
    def test_a_later_clock_gives_a_later_timestamp(self, db, change, audit_env):
        jasil_audit.stage(db, change, details={"fields": []})
        audit_env.clock.advance(3600)
        jasil_audit.stage(db, change, details={"fields": []})
        db.commit()

        first, second = _rows(db)
        assert second.occurred_at - first.occurred_at == timedelta(hours=1)
        assert first.occurred_at.replace(tzinfo=UTC) == datetime(2026, 1, 1, 12, tzinfo=UTC)
