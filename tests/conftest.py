"""Shared fixtures.

Every fixture here is hermetic: no network, no real Redis, no on-disk database.
Tests that need DNS or a Redis client get a fake injected rather than reaching
out, so the suite is deterministic and runs offline.

The database is in-memory SQLite by default. Set ``JASIL_TEST_DATABASE_URL`` to
run the whole suite against a real PostgreSQL or MySQL server instead — the CI
matrix does exactly that, because the concurrency primitives (``ON CONFLICT DO
NOTHING``, ``FOR UPDATE SKIP LOCKED``) differ per dialect and SQLite exercises
neither.
"""

import ipaddress
import os
import socket

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

import jasil.orm as orm
import jasil.testing as jasil_testing

#: In-memory SQLite by default; the CI matrix overrides it per database.
TEST_DATABASE_URL = os.environ.get("JASIL_TEST_DATABASE_URL", "sqlite://")

# Option B forbids importing a model module before the mapping exists, so this
# runs at conftest import time — the test suite's equivalent of host startup —
# rather than in a fixture, which would be too late for module-level imports in
# the test files themselves.
orm.map_models()


@pytest.fixture(autouse=True)
def _no_cloud_metadata():
    """Keep boto3 off the EC2 instance-metadata endpoint.

    A client built without credentials probes ``169.254.169.254`` for them, a real network call that also
    costs seconds per client. A private ``MonkeyPatch``, so a test's own ``monkeypatch`` is still created,
    and undone, in the order its fixtures ask for it.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("AWS_EC2_METADATA_DISABLED", "true")
        yield


@pytest.fixture(autouse=True)
def _no_real_dns():
    """Fail any lookup of a name that a test has not faked; an IP literal is answered here, not by the resolver.

    ``localhost`` is the exception and goes to the resolver: the database jobs connect to a service by that name.

    Answering a literal needs no round trip, but asking the system resolver to would make a test depend on the
    machine it runs on. A test that needs a name patches ``getaddrinfo`` itself, after this, and wins.
    A private ``MonkeyPatch``, like the fixture above, so a test's own ``monkeypatch`` keeps its teardown order.
    """

    real = socket.getaddrinfo

    def literal_only(host, port=None, *args, **kwargs):
        if str(host).lower() == "localhost":
            # The database CI jobs name their service this way, and a loopback name needs no network.
            return real(host, port, *args, **kwargs)
        try:
            address = ipaddress.ip_address(str(host).split("%")[0])
        except ValueError:
            raise AssertionError(f"a test resolved {host!r} through the real resolver; fake getaddrinfo") from None
        family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
        return [(family, socket.SOCK_STREAM, 6, "", (str(host), port or 0))]

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(socket, "getaddrinfo", literal_only)
        yield


@pytest.fixture(autouse=True)
def _reset_process_state():
    """Clear every process-wide slot so tests cannot leak into each other.

    The settings, correlation provider, audit state, published platform, durable
    subscriber registry and shared Redis clients (:func:`jasil.testing.reset_all`).
    ``jasil.orm``'s declarative base is deliberately *not* reset: model modules
    capture it at import time, so the mapping has to survive the whole session.
    """
    yield
    jasil_testing.reset_all()


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
def threaded_session_factory(mapped_base, database_url):
    """A session factory for sessions on several threads at once, empty at the start of the test.

    In-memory SQLite gives each thread its own empty database by default, so on SQLite every thread shares one
    connection to a single in-memory database, which a session holds alone until it closes: the others wait
    for it, as they would for SQLite's write lock. On a real server it is the suite's database.
    """
    if database_url == "sqlite://":
        engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=1,
            max_overflow=0,
            connect_args={"check_same_thread": False},
        )
    else:
        engine = create_engine(database_url)
    mapped_base.metadata.drop_all(engine)
    mapped_base.metadata.create_all(engine)
    yield sessionmaker(bind=engine, expire_on_commit=False)
    mapped_base.metadata.drop_all(engine)
    engine.dispose()


class AuditEnv:
    """A test platform with a fixed clock and the audit settings a test wants.

    Attributes:
        clock: The platform clock; advance it to move ``occurred_at`` and the checkpoint windows.
        platform: The installed platform.
    """

    def __init__(self, tmp_path, session_factory) -> None:
        import jasil.audit.service as audit_service
        import jasil.testing as jasil_testing

        self._tmp_path = tmp_path
        self._overrides: dict = {}
        self.clock = jasil_testing.FixedClock()
        self.configure()
        # The first audit write resolves the instance id on a session of its own. Resolve it before a
        # test holds an open transaction, so that session cannot commit the test's pending work (all
        # SQLite sessions in a test share one connection).
        with session_factory() as db:
            audit_service.ensure_instance_id(audit_service.engine_of(db))

    def configure(self, **audit_overrides) -> None:
        """Reinstall the platform with these audit settings on top of the ones set before."""
        import jasil.settings as jasil_settings
        import jasil.testing as jasil_testing

        self._overrides.update(audit_overrides)
        self.platform = jasil_testing.install_test_platform(
            self._tmp_path,
            clock=self.clock,
            settings=jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(**self._overrides)),
        )


@pytest.fixture
def audit_env(tmp_path, session_factory):
    """A platform on a fixed clock, with auditing on and the instance id already resolved."""
    import jasil.testing as jasil_testing

    environment = AuditEnv(tmp_path, session_factory)
    yield environment
    jasil_testing.reset_all()
