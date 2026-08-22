"""Shared fixtures.

Every fixture here is hermetic: no network, no real Redis, no on-disk database.
Tests that need DNS or a Redis client get a fake injected rather than reaching
out, so the suite is deterministic and runs offline.

The database is in-memory SQLite by default. Set ``JASIL_TEST_DATABASE_URL`` to
run the whole suite against a real PostgreSQL or MySQL server instead — the CI
matrix does exactly that, because the concurrency primitives (``ON CONFLICT DO
NOTHING``, ``FOR UPDATE SKIP LOCKED``) differ per dialect and SQLite exercises
neither.

The async fixtures mirror the sync ones one-for-one against the same database,
translating the URL to its asyncio driver. They are separate fixtures rather than
a parametrization because a test that exercises the async face wants *only* the
async face: the whole point of the port is that the two are independent.
"""

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

import jasil.correlation as correlation
import jasil.orm as orm
import jasil.settings as settings

#: In-memory SQLite by default; the CI matrix overrides it per database.
TEST_DATABASE_URL = os.environ.get("JASIL_TEST_DATABASE_URL", "sqlite://")

#: Sync driver -> asyncio driver, so both faces run against the same server in
#: the CI matrix without the matrix having to know about two URLs.
_ASYNC_DRIVERS = {
    "sqlite": "sqlite+aiosqlite",
    "postgresql": "postgresql+asyncpg",
    "postgresql+psycopg2": "postgresql+asyncpg",
    "mysql": "mysql+aiomysql",
    "mysql+pymysql": "mysql+aiomysql",
}


def _async_database_url(url: str) -> str:
    """Translate a synchronous SQLAlchemy URL to its asyncio-driver equivalent."""
    scheme, separator, rest = url.partition("://")
    return f"{_ASYNC_DRIVERS.get(scheme, scheme)}{separator}{rest}"


#: The same database as :data:`TEST_DATABASE_URL`, reached through an async driver.
TEST_ASYNC_DATABASE_URL = _async_database_url(TEST_DATABASE_URL)

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


@pytest.fixture(scope="session")
def database_url():
    """The database the suite runs against."""
    return TEST_DATABASE_URL


@pytest.fixture
def db_engine(mapped_base):
    """A database with JASIL's tables, empty at the start of every test.

    In-memory SQLite is private per engine, but a real server is shared across
    the run, so the schema is dropped and recreated rather than assumed clean.
    """
    engine = create_engine(TEST_DATABASE_URL)
    mapped_base.metadata.drop_all(engine)
    mapped_base.metadata.create_all(engine)
    yield engine
    mapped_base.metadata.drop_all(engine)
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


@pytest.fixture
def async_database_url():
    """The database the async suite runs against, through its asyncio driver."""
    return TEST_ASYNC_DATABASE_URL


@pytest.fixture
async def async_db_engine(mapped_base):
    """An async engine over a database with JASIL's tables, empty at the start of every test.

    The schema is created through the async engine's own connection so an
    in-memory SQLite database — which is private to the connection that made it —
    is the same one the test then queries.
    """
    engine = create_async_engine(TEST_ASYNC_DATABASE_URL)
    async with engine.begin() as connection:
        await connection.run_sync(mapped_base.metadata.drop_all)
        await connection.run_sync(mapped_base.metadata.create_all)
    yield engine
    async with engine.begin() as connection:
        await connection.run_sync(mapped_base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
def async_session_factory(async_db_engine):
    """An async session factory bound to the test database, installed on ``jasil.orm``.

    ``expire_on_commit=False`` matches what the async CRUD documents as required:
    an expired instance would need a lazy refresh on next access, which asyncio
    cannot perform implicitly.
    """
    factory = async_sessionmaker(bind=async_db_engine, expire_on_commit=False)
    orm.configure_async_sessionmaker(factory)
    return factory


@pytest.fixture
async def async_db(async_session_factory):
    """An open async session against the test database."""
    async with async_session_factory() as session:
        yield session
