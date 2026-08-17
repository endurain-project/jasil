"""Shared fixtures.

Every fixture here is hermetic: no network, no real Redis, no on-disk database.
Tests that need DNS or a Redis client get a fake injected rather than reaching
out, so the suite is deterministic and runs offline.
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import jasil.correlation as correlation
import jasil.orm as orm
import jasil.settings as settings

# Option B forbids importing a model module before the mapping exists, so this
# runs at conftest import time — the test suite's equivalent of host startup —
# rather than in a fixture, which would be too late for module-level imports in
# the test files themselves.
orm.map_models()


@pytest.fixture(autouse=True)
def _reset_process_state():
    """Clear the process-wide config slots so tests cannot leak into each other.

    ``jasil.orm``'s declarative base is deliberately *not* reset: model modules
    capture it at import time, so the mapping has to survive the whole session.
    """
    yield
    settings.reset()
    correlation.reset()


@pytest.fixture(scope="session")
def mapped_base():
    """The declarative base JASIL's models are mapped onto."""
    return orm.Base


@pytest.fixture
def db_engine(mapped_base):
    """A fresh in-memory SQLite database with JASIL's tables created."""
    engine = create_engine("sqlite://")
    mapped_base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(db_engine):
    """A session factory bound to the test database, installed on ``jasil.orm``."""
    factory = sessionmaker(bind=db_engine, expire_on_commit=False)
    orm.configure_sessionmaker(factory)
    return factory


@pytest.fixture
def db(session_factory):
    """An open session against the test database."""
    with session_factory() as session:
        yield session
