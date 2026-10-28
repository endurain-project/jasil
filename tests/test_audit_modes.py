"""Audit rows are the same whether durable jobs are on or off.

The audit row rides the caller's own transaction, so neither the outbox nor the bus is involved in
writing it. This runs one business operation in both modes and requires identical audit rows, and the same
all-or-nothing behaviour when the operation fails.
"""

import pytest
from sqlalchemy import delete, select

import jasil.audit as jasil_audit
import jasil.jobs.registry as jobs_registry
import jasil.publisher as publisher
import jasil.settings as jasil_settings
import jasil.testing as jasil_testing
from jasil.audit import AuditAction, AuditContext, Category, Outcome, Severity
from jasil.audit.models import AuditLog
from jasil.event_log.models import EventLog
from jasil.jobs.models import EventOutbox
from jasil.testing import capture_audit


@pytest.fixture
def place_order(audit_env):
    action = jasil_audit.get_catalog().register(
        AuditAction("shop.order.place", Category.DATA_CHANGE, Severity.LOW, "k", "d")
    )

    def run(db, *, fail: bool = False) -> None:
        jasil_audit.set_context(AuditContext(actor_type="user", actor_id="42", source="api", ip_address="203.0.113.7"))
        db.add(EventLog(id="order-1", event_type="shop.order", event_source="test", event_payload={}))
        jasil_audit.stage(db, action, target_type="order", target_id="order-1", target_owner_id="42")
        if fail:
            raise RuntimeError("the operation failed after staging")
        # In durable mode this commits the caller's session, the audit row with it: both were staged in
        # the same transaction as the change, so all three become durable together.
        publisher.publish("shop.order.placed", {"id": "order-1"}, source="api:place_order", db=db)
        db.commit()

    return run


def _install(tmp_path, *, jobs_enabled: bool) -> None:
    jobs_registry.registry.clear()
    if jobs_enabled:
        jobs_registry.registry.register("shop.order.placed", "send_receipt", lambda event: None)
    jasil_testing.install_test_platform(
        tmp_path,
        settings=jasil_settings.JasilSettings(jobs=jasil_settings.JobSettings(enabled=jobs_enabled)),
    )


def _normalized(db) -> list[tuple]:
    rows = db.scalars(select(AuditLog).order_by(AuditLog.occurred_at, AuditLog.id)).all()
    return [
        (
            r.action_id,
            r.category,
            r.severity,
            r.outcome,
            r.actor_type,
            r.actor_id,
            r.source,
            r.ip_address,
            r.target_type,
            r.target_id,
            r.target_owner_id,
            r.details,
        )
        for r in rows
    ]


def test_the_same_audit_rows_are_produced_in_both_modes(tmp_path, place_order, db):
    produced = {}
    outbox = {}
    for jobs_enabled in (False, True):
        _install(tmp_path, jobs_enabled=jobs_enabled)
        with capture_audit() as captured:
            place_order(db)
        produced[jobs_enabled] = _normalized(db)
        outbox[jobs_enabled] = db.query(EventOutbox).count()
        assert [entry.action_id for entry in captured.entries] == ["shop.order.place"]
        db.execute(delete(AuditLog))
        db.execute(delete(EventLog))
        db.execute(delete(EventOutbox))
        db.commit()

    assert outbox == {False: 0, True: 1}  # the two modes really did route differently
    assert produced[False] == produced[True]
    assert produced[True] == [
        (
            "shop.order.place",
            "data_change",
            "low",
            "success",
            "user",
            "42",
            "api",
            "203.0.113.7",
            "order",
            "order-1",
            "42",
            None,
        )
    ]


@pytest.mark.parametrize("jobs_enabled", [False, True])
def test_a_failed_operation_leaves_no_audit_row_in_either_mode(tmp_path, place_order, db, jobs_enabled):
    _install(tmp_path, jobs_enabled=jobs_enabled)

    with pytest.raises(RuntimeError):
        place_order(db, fail=True)
    db.rollback()

    assert db.scalars(select(AuditLog)).all() == []
    assert db.get(EventLog, "order-1") is None


@pytest.mark.parametrize("jobs_enabled", [False, True])
def test_a_denial_recorded_detached_survives_the_rollback_in_either_mode(
    tmp_path, place_order, db, jobs_enabled, audit_env
):
    _install(tmp_path, jobs_enabled=jobs_enabled)
    action = jasil_audit.get_catalog().get("shop.order.place")

    with pytest.raises(RuntimeError):
        place_order(db, fail=True)
    jasil_audit.record_detached(action, outcome=Outcome.DENIED, target_type="order", target_id="order-1")
    db.rollback()

    assert [row.outcome for row in db.scalars(select(AuditLog))] == ["denied"]
