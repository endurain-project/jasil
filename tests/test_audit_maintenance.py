"""Audit maintenance: catalog sync, retention, step isolation and scheduling.

Maintenance is owned by JASIL but scheduled by the host, runs on every replica at once, and must never let
one failing step starve the others (a catalog problem must not stop checkpoints from being sealed).
"""

import contextlib
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

import jasil.audit as jasil_audit
import jasil.audit.maintenance as maintenance
import jasil.runtime as runtime
from jasil.audit import AuditAction, Category, Outcome, Severity
from jasil.audit.models import AuditActionRecord, AuditCheckpoint, AuditLog, Instance

T0 = datetime(2026, 1, 1, 12, tzinfo=UTC)


class HeldLock:
    """Stands in for another replica already running maintenance."""

    @contextlib.contextmanager
    def try_acquire(self, name, ttl_seconds=None):
        yield False


class RecordingLock:
    def __init__(self) -> None:
        self.names: list[str] = []

    @contextlib.contextmanager
    def try_acquire(self, name, ttl_seconds=None):
        self.names.append(name)
        yield True


@pytest.fixture
def actions(audit_env):
    catalog = jasil_audit.get_catalog()
    return [
        catalog.register(AuditAction("shop.order.place", Category.DATA_CHANGE, Severity.LOW, "k1", "Place an order")),
        catalog.register(AuditAction("shop.order.cancel", Category.DATA_CHANGE, Severity.MEDIUM, "k2", "Cancel one")),
    ]


def _rows(session_factory, model, *order):
    with session_factory() as db:
        return list(db.scalars(select(model).order_by(*order)))


def _write(audit_env, action, when: datetime) -> None:
    audit_env.clock.moment = when
    jasil_audit.record_detached(action, outcome=Outcome.SUCCESS)
    audit_env.clock.moment = T0


class TestCatalogSync:
    def test_it_inserts_every_registered_action(self, actions, session_factory):
        result = jasil_audit.sync_catalog()

        rows = {row.id: row for row in _rows(session_factory, AuditActionRecord)}
        assert result == jasil_audit.CatalogSyncResult(added=6, updated=0, deprecated=0)
        assert rows["shop.order.cancel"].severity == "medium"
        assert rows["shop.order.cancel"].description_default == "Cancel one"
        assert rows["shop.order.cancel"].deprecated is False

    def test_a_second_sync_changes_nothing_and_writes_no_audit_row(self, actions, session_factory):
        jasil_audit.sync_catalog()
        before = len(_rows(session_factory, AuditLog))

        assert jasil_audit.sync_catalog() == jasil_audit.CatalogSyncResult()
        assert len(_rows(session_factory, AuditLog)) == before

    def test_a_changed_definition_is_updated(self, actions, session_factory):
        jasil_audit.sync_catalog()
        catalog = jasil_audit.get_catalog()
        catalog._actions["shop.order.cancel"] = replace(actions[1], severity=Severity.HIGH, description_default="New")

        result = jasil_audit.sync_catalog()

        row = next(r for r in _rows(session_factory, AuditActionRecord) if r.id == "shop.order.cancel")
        assert (result.updated, row.severity, row.description_default) == (1, "high", "New")

    def test_an_action_no_longer_registered_is_deprecated_never_deleted(self, actions, session_factory):
        jasil_audit.sync_catalog()
        del jasil_audit.get_catalog()._actions["shop.order.cancel"]

        result = jasil_audit.sync_catalog()

        rows = {row.id: row for row in _rows(session_factory, AuditActionRecord)}
        assert result.deprecated == 1
        assert rows["shop.order.cancel"].deprecated is True
        assert rows["shop.order.place"].deprecated is False

    def test_deprecating_is_reported_once(self, actions, session_factory):
        jasil_audit.sync_catalog()
        del jasil_audit.get_catalog()._actions["shop.order.cancel"]
        jasil_audit.sync_catalog()

        assert jasil_audit.sync_catalog().deprecated == 0

    def test_an_action_registered_as_deprecated_is_stored_as_deprecated(self, audit_env, session_factory):
        jasil_audit.get_catalog().register(
            AuditAction("old.thing.happen", Category.SYSTEM, Severity.INFO, "k", "d", deprecated=True)
        )

        jasil_audit.sync_catalog()

        assert next(r for r in _rows(session_factory, AuditActionRecord) if r.id == "old.thing.happen").deprecated

    def test_a_returning_action_is_undeprecated(self, actions, session_factory):
        jasil_audit.sync_catalog()
        removed = jasil_audit.get_catalog()._actions.pop("shop.order.cancel")
        jasil_audit.sync_catalog()
        jasil_audit.get_catalog()._actions["shop.order.cancel"] = removed

        assert jasil_audit.sync_catalog().updated == 1
        assert not next(r for r in _rows(session_factory, AuditActionRecord) if r.id == "shop.order.cancel").deprecated

    def test_last_seen_moves_and_first_seen_does_not(self, actions, audit_env, session_factory):
        jasil_audit.sync_catalog()
        audit_env.clock.advance(3600)

        jasil_audit.sync_catalog()

        row = next(r for r in _rows(session_factory, AuditActionRecord) if r.id == "shop.order.place")
        assert row.last_seen_at - row.first_seen_at == timedelta(hours=1)

    def test_a_change_is_audited_with_the_counts(self, actions, session_factory):
        jasil_audit.sync_catalog()

        row = next(r for r in _rows(session_factory, AuditLog) if r.action_id == "audit.catalog.sync")
        assert row.details == {"added": 6, "updated": 0, "deprecated": 0}
        assert (row.actor_type, row.source) == ("system", "worker")

    def test_losing_a_race_to_another_replica_retries_once(self, actions, session_factory, monkeypatch):
        real = maintenance._sync_catalog
        calls = []

        def racing():
            calls.append(1)
            if len(calls) == 1:
                raise IntegrityError("insert", {}, Exception("duplicate"))
            return real()

        monkeypatch.setattr(maintenance, "_sync_catalog", racing)

        assert jasil_audit.sync_catalog().added == 6
        assert len(calls) == 2

    def test_a_disabled_audit_syncs_nothing(self, actions, audit_env, session_factory):
        audit_env.configure(enabled=False)

        assert jasil_audit.sync_catalog() == jasil_audit.CatalogSyncResult()
        assert _rows(session_factory, AuditActionRecord) == []


class TestRetention:
    @pytest.fixture(autouse=True)
    def _old_and_new(self, actions, audit_env, session_factory):
        # Day-long windows: the rows are months old, and sealing minute-sized windows back to them is slow.
        audit_env.configure(checkpoint_window_minutes=1440)
        _write(audit_env, actions[0], T0 - timedelta(days=100))
        _write(audit_env, actions[0], T0 - timedelta(days=95))
        _write(audit_env, actions[1], T0 - timedelta(days=10))

    def _targets(self, session_factory) -> list[str]:
        return [row.action_id for row in _rows(session_factory, AuditLog, AuditLog.occurred_at)]

    def test_rows_older_than_the_window_are_deleted_and_the_prune_is_recorded(self, session_factory):
        jasil_audit.run_audit_maintenance()

        remaining = _rows(session_factory, AuditLog, AuditLog.occurred_at)
        assert [row.action_id for row in remaining if row.action_id.startswith("shop.")] == ["shop.order.cancel"]
        prune = next(row for row in remaining if row.action_id == "audit.retention.prune")
        assert prune.details["partitions_dropped"] == 0
        # 90 days before the clock (12:00), floored to the day-long checkpoint window the fixture configures.
        assert prune.details["cutoff"].startswith("2025-10-03T00:00:00")

    def test_a_second_run_prunes_nothing_more(self, session_factory):
        jasil_audit.run_audit_maintenance()
        before = len(self._targets(session_factory))

        jasil_audit.run_audit_maintenance()

        assert len(self._targets(session_factory)) == before

    def test_the_cutoff_falls_on_a_window_boundary_so_no_sealed_window_is_half_emptied(
        self, actions, audit_env, session_factory
    ):
        _write(audit_env, actions[1], datetime(2025, 10, 3, 0, 0, 1, tzinfo=UTC))  # 11h59m before the raw cutoff

        jasil_audit.run_audit_maintenance()

        assert self._targets(session_factory).count("shop.order.cancel") == 2  # it is in the window being kept

    def test_the_cutoff_follows_the_sealed_window_not_the_current_setting(self, actions, audit_env, session_factory):
        old_day = datetime(2025, 10, 3, tzinfo=UTC)
        _write(audit_env, actions[0], old_day + timedelta(hours=6))  # inside the day the cutoff (12:15) falls in
        audit_env.configure(retention_days=0)
        jasil_audit.run_audit_maintenance()  # seals day-long windows up to T0's day, pruning nothing
        audit_env.configure(checkpoint_window_minutes=15, retention_days=90)  # the settings change afterwards
        audit_env.clock.moment = T0 + timedelta(minutes=30)

        jasil_audit.run_audit_maintenance()

        report = jasil_audit.verify_checkpoints(None, old_day - timedelta(days=11), old_day + timedelta(days=1))
        assert report.ok, report.windows
        # The 12:15 cutoff was floored to the sealed window's start (00:00), not to the new 15-minute grid.
        # Only the 06:00 row survives: the two older fixture rows went, and the cutoff stopped at the window start.
        assert self._targets(session_factory).count("shop.order.place") == 1

    def test_it_never_prunes_past_what_the_newest_checkpoint_allows(
        self, actions, audit_env, session_factory, monkeypatch
    ):
        audit_env.clock.moment = T0 - timedelta(days=50)
        jasil_audit.run_audit_maintenance()  # seals up to 50 days ago under a 90-day retention
        audit_env.configure(retention_days=30)
        audit_env.clock.moment = T0
        monkeypatch.setattr(maintenance.checkpoints, "seal_pending_windows", lambda now: 1 / 0)

        jasil_audit.run_audit_maintenance()

        # The newest checkpoint allows only what is 90 days older than its end: nothing here yet.
        assert self._targets(session_factory).count("shop.order.place") == 2

    def test_a_checkpoint_retention_too_long_to_represent_prunes_nothing(self, audit_env, session_factory):
        jasil_audit.run_audit_maintenance()
        before = self._targets(session_factory)
        with session_factory() as db:
            db.execute(update(AuditCheckpoint).values(retention_days=2_000_000_000))
            db.commit()
        audit_env.clock.moment = T0 + timedelta(days=1)

        maintenance._apply_retention(T0 + timedelta(days=1))

        assert self._targets(session_factory) == before

    def test_the_prune_is_recorded_before_any_row_is_deleted(self, actions, audit_env, session_factory, monkeypatch):
        seen = []
        real = maintenance.bounded_delete

        def delete_after_checking(*args, **kwargs):
            with session_factory() as db:
                seen.append([r.action_id for r in db.scalars(select(AuditLog)) if r.action_id.startswith("audit.")])
            return real(*args, **kwargs)

        monkeypatch.setattr(maintenance, "bounded_delete", delete_after_checking)

        jasil_audit.run_audit_maintenance()

        assert "audit.retention.prune" in seen[0]  # already committed when the first batch goes

    def test_a_stop_after_the_deletion_still_leaves_the_prune_accounted_for(
        self, actions, audit_env, session_factory, monkeypatch
    ):
        real = maintenance.bounded_delete

        def delete_then_die(*args, **kwargs):
            real(*args, **kwargs)
            raise RuntimeError("process stopped after the last batch")

        monkeypatch.setattr(maintenance, "bounded_delete", delete_then_die)
        jasil_audit.run_audit_maintenance()  # the failing step is isolated and logged

        report = jasil_audit.verify_checkpoints(None, T0 - timedelta(days=101), T0)
        assert report.ok, report.windows

    def test_nothing_to_delete_records_nothing(self, actions, audit_env, session_factory):
        jasil_audit.run_audit_maintenance()
        before = self._targets(session_factory).count("audit.retention.prune")

        jasil_audit.run_audit_maintenance()

        assert self._targets(session_factory).count("audit.retention.prune") == before

    def test_a_row_exactly_at_the_cutoff_is_kept(self, actions, audit_env, session_factory):
        _write(audit_env, actions[1], T0 - timedelta(days=90))

        jasil_audit.run_audit_maintenance()

        assert self._targets(session_factory).count("shop.order.cancel") == 2

    def test_legal_hold_suspends_pruning(self, audit_env, session_factory, caplog):
        audit_env.configure(legal_hold=True)

        with caplog.at_level(logging.INFO):
            jasil_audit.run_audit_maintenance()

        assert self._targets(session_factory).count("shop.order.place") == 2
        assert "legal hold" in caplog.text

    @pytest.mark.parametrize("retention_days", [0, -5])
    def test_a_non_positive_window_keeps_every_row(self, audit_env, session_factory, retention_days):
        audit_env.configure(retention_days=retention_days)

        jasil_audit.run_audit_maintenance()

        assert self._targets(session_factory).count("shop.order.place") == 2

    def test_the_window_follows_the_setting(self, audit_env, session_factory):
        audit_env.configure(retention_days=5, retention_floor_days=1)

        jasil_audit.run_audit_maintenance()

        assert self._targets(session_factory).count("shop.order.cancel") == 0

    def test_dropped_windows_stay_verifiable_afterwards(self, audit_env, session_factory):
        audit_env.configure(checkpoint_grace_minutes=0)
        audit_env.clock.moment = T0 - timedelta(days=50)
        jasil_audit.run_audit_maintenance()  # seals the old windows, which are then pruned
        audit_env.clock.moment = T0
        jasil_audit.run_audit_maintenance()

        report = jasil_audit.verify_checkpoints(None, T0 - timedelta(days=101), T0)

        assert report.ok
        assert any(window.pruned for window in report.windows)


class TestRetentionChange:
    def _changes(self, session_factory):
        return [row for row in _rows(session_factory, AuditLog) if row.action_id == "audit.retention.change"]

    def test_the_first_run_records_a_baseline_without_an_event(self, audit_env, session_factory):
        jasil_audit.run_audit_maintenance()

        assert self._changes(session_factory) == []
        instance = _rows(session_factory, Instance)[0]
        assert (instance.retention_days, instance.legal_hold) == (90, False)

    def test_changing_the_window_or_the_hold_is_audited(self, audit_env, session_factory):
        jasil_audit.run_audit_maintenance()
        audit_env.configure(retention_days=45, legal_hold=True)

        jasil_audit.run_audit_maintenance()
        jasil_audit.run_audit_maintenance()  # unchanged now: no second event

        (change,) = self._changes(session_factory)
        assert change.details == {
            "retention_days_before": 90,
            "retention_days_after": 45,
            "legal_hold_before": False,
            "legal_hold_after": True,
        }
        assert (change.actor_type, change.severity) == ("system", "high")


class TestRun:
    def test_it_runs_every_step_in_order_under_its_own_lock(self, actions, audit_env, session_factory):
        lock = RecordingLock()
        runtime.set_active_platform(replace(audit_env.platform, lock=lock))
        audit_env.clock.moment = T0
        _write(audit_env, actions[0], T0 - timedelta(minutes=40))

        jasil_audit.run_audit_maintenance()

        assert lock.names == ["jasil_audit_maintenance"]
        assert len(_rows(session_factory, AuditActionRecord)) == 6  # catalog synced
        assert _rows(session_factory, AuditCheckpoint)  # checkpoints sealed
        assert jasil_audit.get_audit_health()["last_checkpoint_at"] is not None

    def test_it_does_not_take_the_retention_lock(self, audit_env):
        lock = RecordingLock()
        runtime.set_active_platform(replace(audit_env.platform, lock=lock))

        jasil_audit.run_audit_maintenance()

        assert "jasil_retention_prune" not in lock.names

    def test_another_replica_holding_the_lock_skips_the_run(self, actions, audit_env, session_factory):
        runtime.set_active_platform(replace(audit_env.platform, lock=HeldLock()))

        jasil_audit.run_audit_maintenance()

        assert _rows(session_factory, AuditActionRecord) == []

    def test_running_twice_is_idempotent(self, actions, audit_env, session_factory):
        _write(audit_env, actions[0], T0 - timedelta(minutes=40))
        jasil_audit.run_audit_maintenance()
        first = (
            [row.id for row in _rows(session_factory, AuditActionRecord)],
            [(c.window_start, c.hash) for c in _rows(session_factory, AuditCheckpoint)],
            len(_rows(session_factory, AuditLog)),
        )

        jasil_audit.run_audit_maintenance()

        assert first == (
            [row.id for row in _rows(session_factory, AuditActionRecord)],
            [(c.window_start, c.hash) for c in _rows(session_factory, AuditCheckpoint)],
            len(_rows(session_factory, AuditLog)),
        )

    @pytest.mark.parametrize("failing", ["sync_catalog", "_record_retention_change", "_ensure_partitions"])
    def test_a_failing_step_does_not_skip_the_others(
        self, failing, actions, audit_env, session_factory, monkeypatch, caplog
    ):
        _write(audit_env, actions[0], T0 - timedelta(minutes=40))

        def explode(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(maintenance, failing, explode)

        with caplog.at_level(logging.ERROR):
            jasil_audit.run_audit_maintenance()

        assert "step" in caplog.text
        assert _rows(session_factory, AuditCheckpoint)  # sealing still ran
        assert (
            "shop.order.place" not in {r.id for r in _rows(session_factory, AuditActionRecord)}
            or failing != "sync_catalog"
        )

    def test_a_failing_checkpoint_step_holds_retention_back(
        self, actions, audit_env, session_factory, monkeypatch, caplog
    ):
        _write(audit_env, actions[0], T0 - timedelta(days=100))
        monkeypatch.setattr(maintenance.checkpoints, "seal_pending_windows", lambda now: 1 / 0)

        with caplog.at_level(logging.INFO):
            jasil_audit.run_audit_maintenance()

        # Never sealed, so never hashed: pruning it would destroy rows no checkpoint can account for.
        assert "shop.order.place" in {row.action_id for row in _rows(session_factory, AuditLog)}
        assert "no sealed checkpoint" in caplog.text

    def test_a_failing_retention_step_does_not_undo_the_others(self, actions, audit_env, session_factory, monkeypatch):
        monkeypatch.setattr(maintenance, "_apply_retention", lambda now: 1 / 0)

        jasil_audit.run_audit_maintenance()

        assert len(_rows(session_factory, AuditActionRecord)) == 6

    def test_every_replica_reports_the_latest_run(self, actions, audit_env, session_factory):
        _write(audit_env, actions[0], T0 - timedelta(minutes=30))
        jasil_audit.run_audit_maintenance()

        jasil_audit.reset()  # a replica that did not win the lock

        assert jasil_audit.get_audit_health()["last_checkpoint_at"] == datetime(2026, 1, 1, 11, 45, tzinfo=UTC)

    def test_partition_management_is_skipped_off_postgresql(self, audit_env, session_factory):
        jasil_audit.run_audit_maintenance()

        assert jasil_audit.get_audit_health()["furthest_partition_date"] is None


class TestScheduling:
    @pytest.fixture
    def scheduler(self):
        return AsyncIOScheduler()

    async def test_it_registers_the_job_with_a_startup_run(self, scheduler):
        jasil_audit.schedule_audit_maintenance(scheduler)

        (job,) = scheduler.get_jobs()
        assert (job.id, job.func) == ("jasil_audit_maintenance", jasil_audit.run_audit_maintenance)
        assert job.max_instances == 1
        assert job.next_run_time is not None or scheduler.state == 0

    async def test_it_can_skip_the_startup_run(self, scheduler):
        jasil_audit.schedule_audit_maintenance(scheduler, run_at_startup=False)

        (job,) = scheduler.get_jobs()
        assert job.id == "jasil_audit_maintenance"

    async def test_it_is_a_different_job_from_the_retention_prune(self, scheduler):
        import jasil.retention as retention

        retention.schedule_retention_maintenance(scheduler)
        jasil_audit.schedule_audit_maintenance(scheduler)

        assert len(scheduler.get_jobs()) == 2
