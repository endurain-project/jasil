"""Option B: the host owns the declarative base and the engine."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

import jasil.orm as orm

JASIL_TABLES = {"event_log", "processing_jobs", "event_outbox"}


@pytest.fixture
def pristine_orm(mapped_base):
    """Run a test against an unconfigured ``jasil.orm``, then restore the session mapping.

    Model modules capture the base at import time, so the session-wide mapping is
    re-established on teardown rather than left cleared.
    """
    orm.reset()
    yield orm
    orm.reset()
    orm.map_models(mapped_base)


class TestModelMapping:
    def test_jasil_tables_land_in_the_hosts_registry(self, mapped_base):
        """The whole point of Option B: one metadata covers host and library."""
        assert set(mapped_base.metadata.tables) >= JASIL_TABLES

    def test_mapping_is_idempotent(self, mapped_base):
        """Hosts with several entry points must not have to coordinate."""
        orm.map_models(mapped_base)

        assert orm.is_models_mapped() is True

    def test_mapping_onto_a_second_base_is_refused(self, mapped_base):
        """Two registries would silently break cross-table foreign keys."""

        class OtherBase(DeclarativeBase):
            pass

        with pytest.raises(RuntimeError, match="already called with a different base"):
            orm.map_models(OtherBase)

    def test_reading_the_base_before_mapping_explains_the_fix(self, pristine_orm):
        with pytest.raises(RuntimeError, match="map_models"):
            pristine_orm.get_active_base()

    def test_is_models_mapped_reports_the_state(self, pristine_orm):
        assert pristine_orm.is_models_mapped() is False

        pristine_orm.map_models()

        assert pristine_orm.is_models_mapped() is True

    def test_mapping_defaults_to_jasils_own_base(self, pristine_orm):
        """A host that would rather not own a registry can omit the argument."""
        pristine_orm.map_models()

        assert pristine_orm.get_active_base() is orm.Base

    def test_a_failed_mapping_leaves_no_half_configured_base(self, pristine_orm, monkeypatch):
        """The host must be able to fix the problem and retry."""

        def _explode(_name):
            raise ImportError("boom")

        monkeypatch.setattr(orm.importlib, "import_module", _explode)

        with pytest.raises(ImportError):
            pristine_orm.map_models()

        assert pristine_orm.is_models_mapped() is False


class TestSessionFactory:
    def test_reading_the_factory_before_configuring_explains_the_fix(self, pristine_orm):
        with pytest.raises(RuntimeError, match="configure_sessionmaker"):
            pristine_orm.get_sessionmaker()

    def test_the_configured_factory_is_returned(self, session_factory):
        assert orm.get_sessionmaker() is session_factory

    def test_is_sessionmaker_configured_reports_the_state(self, pristine_orm):
        assert pristine_orm.is_sessionmaker_configured() is False

        pristine_orm.configure_sessionmaker(sessionmaker(bind=create_engine("sqlite://")))

        assert pristine_orm.is_sessionmaker_configured() is True


class TestEngineAccess:
    def test_the_hosts_engine_is_reachable(self, db_engine, session_factory):
        """The advisory-lock backend holds a raw connection and cannot go via a session."""
        assert orm.get_engine() is db_engine

    def test_an_unbound_factory_is_rejected_with_guidance(self, pristine_orm):
        pristine_orm.configure_sessionmaker(sessionmaker())

        with pytest.raises(RuntimeError, match="not bound to an engine"):
            pristine_orm.get_engine()

    def test_reading_the_engine_before_configuring_raises(self, pristine_orm):
        with pytest.raises(RuntimeError, match="configure_sessionmaker"):
            pristine_orm.get_engine()


class TestPersistence:
    def test_a_row_round_trips_through_the_hosts_session(self, db):
        from jasil.event_log.models import EventLog

        db.add(
            EventLog(
                id="event-1",
                event_type="thing.happened",
                event_source="test",
                status="published",
                event_payload={"a": 1},
                event_metadata={},
            )
        )
        db.commit()

        stored = db.query(EventLog).one()

        assert stored.id == "event-1"
        assert stored.event_payload == {"a": 1}

    def test_the_models_are_bound_to_the_hosts_base(self, mapped_base):
        from jasil.event_log.models import EventLog
        from jasil.jobs.models import EventOutbox, ProcessingJob

        for model in (EventLog, ProcessingJob, EventOutbox):
            assert model.metadata is mapped_base.metadata
