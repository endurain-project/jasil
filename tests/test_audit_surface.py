"""What `jasil.audit` exports, what importing it costs, and the small service seams.

The package is public API and must be importable anywhere in a host's import graph; that is pinned in
``test_import_order.py`` (before the ORM is mapped) and ``test_optional_deps.py`` (without ``cryptography``).
"""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

import jasil.audit as jasil_audit
import jasil.audit.service as audit_service
import jasil.orm as jasil_orm
from jasil.audit.models import Instance
from jasil.orm import jasil_table_names


class TestPublicSurface:
    def test_every_exported_name_exists(self):
        missing = [name for name in jasil_audit.__all__ if not hasattr(jasil_audit, name)]

        assert missing == []

    def test_the_export_list_has_no_duplicates(self):
        assert len(jasil_audit.__all__) == len(set(jasil_audit.__all__))

    def test_the_exports_are_what_the_stability_page_promises(self):
        for name in (
            "stage",
            "stage_many",
            "record_detached",
            "get_audit_health",
            "list_entries",
            "get_entry",
            "summary",
            "list_catalog",
            "verify_checkpoints",
            "run_audit_maintenance",
            "schedule_audit_maintenance",
            "sync_catalog",
            "grants_sql",
            "retention_floor_sql",
            "AuditAction",
            "AuditCatalog",
            "AuditContext",
            "set_context",
            "get_context",
            "audit_context",
            "configure_provider",
        ):
            assert name in jasil_audit.__all__

    def test_the_audit_tables_are_jasil_tables(self):
        assert {"jasil_audit_log", "jasil_audit_actions", "jasil_audit_checkpoints", "jasil_instance"} <= (
            jasil_table_names()
        )
        assert all(name.startswith("jasil_") for name in jasil_table_names())


class TestHealthSeam:
    def test_maintenance_figures_are_remembered_and_none_does_not_erase_them(self):
        audit_service.note_maintenance(furthest_partition_date=date(2026, 1, 8))
        audit_service.note_maintenance(last_checkpoint_at=datetime(2026, 1, 1, tzinfo=UTC))
        audit_service.note_maintenance()

        health = jasil_audit.get_audit_health()
        assert health["furthest_partition_date"] == date(2026, 1, 8)
        assert health["last_checkpoint_at"] == datetime(2026, 1, 1, tzinfo=UTC)

    def test_reset_forgets_them(self):
        audit_service.note_maintenance(furthest_partition_date=date(2026, 1, 8))

        jasil_audit.reset()

        assert jasil_audit.get_audit_health()["furthest_partition_date"] is None

    def test_removing_an_observer_twice_is_harmless(self):
        remove = audit_service.add_observer(lambda entry: None)

        remove()
        remove()


class TestClock:
    def test_without_a_platform_it_is_the_system_clock(self):
        assert abs(audit_service.now() - datetime.now(UTC)) < timedelta(seconds=5)

    def test_with_a_platform_it_is_the_platform_clock(self, audit_env):
        audit_env.clock.advance(3600)

        assert audit_service.now() == audit_env.clock.now()


class TestInstanceId:
    def test_it_is_generated_once_and_then_cached(self, audit_env, session_factory):
        with session_factory() as db:
            first = audit_service.ensure_instance_id(audit_service.engine_of(db))
            audit_service._instance_id = None  # forget the cache: the stored row is what answers now
            second = audit_service.ensure_instance_id(audit_service.engine_of(db))
            rows = db.scalars(select(Instance)).all()

        assert first == second
        assert [row.instance_id for row in rows] == [first]
        assert len(first) == 36

    def test_a_cached_id_does_no_io(self, audit_env, session_factory, monkeypatch):
        with session_factory() as db:
            engine = audit_service.engine_of(db)
        cached = audit_service.ensure_instance_id(engine)
        monkeypatch.setattr(audit_service, "Session", None)  # any database access would now raise

        assert audit_service.ensure_instance_id(engine) == cached

    def test_losing_the_race_to_another_replica_adopts_its_id(self, audit_env, session_factory, monkeypatch):
        with session_factory() as db:
            stored = db.scalars(select(Instance.instance_id)).one()
            engine = audit_service.engine_of(db)
        audit_service._instance_id = None

        class StaleSession(Session):
            """Its first read misses a row another replica has just committed."""

            stale = True

            def scalar(self, *args, **kwargs):
                if StaleSession.stale:
                    StaleSession.stale = False
                    return None
                return super().scalar(*args, **kwargs)

        monkeypatch.setattr(audit_service, "Session", StaleSession)

        assert audit_service.ensure_instance_id(engine) == stored

    def test_a_session_bound_to_a_connection_resolves_the_engine(self, db_engine):
        with db_engine.connect() as connection, Session(bind=connection) as session:
            assert audit_service.engine_of(session) is db_engine

    def test_a_second_instance_row_is_impossible(self, db):
        db.add(Instance(id=1, instance_id="a"))
        db.commit()
        db.add(Instance(id=2, instance_id="b"))

        with pytest.raises(Exception, match=r"(?i)constraint|check"):
            db.commit()
        db.rollback()

    def test_the_session_factory_is_not_needed(self, audit_env, db_engine):
        jasil_orm._session_factory.reset()
        audit_service._instance_id = None

        assert audit_service.ensure_instance_id(db_engine)
