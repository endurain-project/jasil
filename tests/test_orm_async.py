"""Async SQLAlchemy session-factory plumbing."""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

import jasil.orm as orm


@pytest.fixture
def pristine_orm(mapped_base):
    """Run against unconfigured ``jasil.orm``, then restore the model mapping."""
    orm.reset()
    yield orm
    orm.reset()
    orm.map_models(mapped_base)


class TestAsyncSessionFactory:
    def test_jasil_table_names_reports_owned_tables(self):
        assert orm.jasil_table_names() == {"event_log", "processing_jobs", "event_outbox"}

    def test_reading_the_factory_before_configuring_names_the_async_slot(self, pristine_orm):
        with pytest.raises(RuntimeError, match=r"\*async\* session factory"):
            pristine_orm.get_async_sessionmaker()

    def test_the_configured_factory_is_returned(self, async_session_factory):
        assert orm.get_async_sessionmaker() is async_session_factory

    def test_is_async_sessionmaker_configured_reports_the_state(self, pristine_orm):
        assert pristine_orm.is_async_sessionmaker_configured() is False

        pristine_orm.configure_async_sessionmaker(async_sessionmaker(bind=create_async_engine("sqlite+aiosqlite://")))

        assert pristine_orm.is_async_sessionmaker_configured() is True

    def test_configuring_sync_does_not_configure_async(self, pristine_orm):
        pristine_orm.configure_sessionmaker(sessionmaker(bind=create_engine("sqlite://")))

        assert pristine_orm.is_sessionmaker_configured() is True
        assert pristine_orm.is_async_sessionmaker_configured() is False
        with pytest.raises(RuntimeError, match=r"\*async\* session factory"):
            pristine_orm.get_async_sessionmaker()

    def test_configuring_async_does_not_configure_sync(self, pristine_orm):
        pristine_orm.configure_async_sessionmaker(async_sessionmaker(bind=create_async_engine("sqlite+aiosqlite://")))

        assert pristine_orm.is_async_sessionmaker_configured() is True
        assert pristine_orm.is_sessionmaker_configured() is False
        with pytest.raises(RuntimeError, match=r"\*synchronous\* session factory"):
            pristine_orm.get_sessionmaker()


class TestAsyncEngineAccess:
    def test_the_sync_engine_still_uses_the_sync_slot(self, pristine_orm):
        engine = create_engine("sqlite://")
        pristine_orm.configure_sessionmaker(sessionmaker(bind=engine))

        assert pristine_orm.get_engine() is engine

    def test_the_hosts_async_engine_is_reachable(self, async_db_engine, async_session_factory):
        assert orm.get_async_engine() is async_db_engine

    def test_an_unbound_async_factory_is_rejected_with_guidance(self, pristine_orm):
        pristine_orm.configure_async_sessionmaker(async_sessionmaker())

        with pytest.raises(RuntimeError, match="async session factory is not bound to an engine"):
            pristine_orm.get_async_engine()

    def test_reading_the_async_engine_before_configuring_names_the_async_slot(self, pristine_orm):
        with pytest.raises(RuntimeError, match="configure_async_sessionmaker"):
            pristine_orm.get_async_engine()


class TestReset:
    def test_reset_clears_both_session_factory_slots(self, pristine_orm):
        orm.configure_sessionmaker(sessionmaker(bind=create_engine("sqlite://")))
        orm.configure_async_sessionmaker(async_sessionmaker(bind=create_async_engine("sqlite+aiosqlite://")))

        orm.reset()

        assert orm.is_sessionmaker_configured() is False
        assert orm.is_async_sessionmaker_configured() is False
        with pytest.raises(RuntimeError, match=r"\*synchronous\* session factory"):
            orm.get_sessionmaker()
        with pytest.raises(RuntimeError, match=r"\*async\* session factory"):
            orm.get_async_sessionmaker()
