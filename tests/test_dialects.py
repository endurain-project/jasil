"""Dialect capability detection.

`supports_skip_locked` decides whether concurrent workers take disjoint batches
or can claim the same row twice. A false positive means emitting a clause the
server rejects; a false negative silently downgrades concurrency. Both matter,
so every branch is pinned here rather than being discovered on a real server.
"""

import pytest
from sqlalchemy import create_engine

from jasil._core.dialects import supports_skip_locked


class FakeDialect:
    def __init__(self, name, server_version_info=None, is_mariadb=False):
        self.name = name
        self.server_version_info = server_version_info
        self.is_mariadb = is_mariadb


class FakeBind:
    def __init__(self, dialect):
        self.dialect = dialect


def _bind(name, **kwargs):
    return FakeBind(FakeDialect(name, **kwargs))


class TestSupportsSkipLocked:
    def test_no_bind_is_unsupported(self):
        assert supports_skip_locked(None) is False

    def test_postgresql_is_supported(self):
        assert supports_skip_locked(_bind("postgresql")) is True

    def test_sqlite_is_unsupported(self):
        """SQLite has no row-level locking, and a single writer makes it moot."""
        assert supports_skip_locked(_bind("sqlite")) is False

    def test_an_unknown_dialect_is_unsupported(self):
        """Better to lose concurrency than to emit SQL the server rejects."""
        assert supports_skip_locked(_bind("oracle")) is False

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            pytest.param((8, 0, 1), True, id="8.0.1-first-supported"),
            pytest.param((8, 4, 0), True, id="8.4"),
            pytest.param((9, 0, 0), True, id="9.x"),
            pytest.param((8, 0, 0), False, id="8.0.0-too-old"),
            pytest.param((5, 7, 44), False, id="5.7"),
        ],
    )
    def test_mysql_depends_on_the_server_version(self, version, expected):
        assert supports_skip_locked(_bind("mysql", server_version_info=version)) is expected

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            pytest.param((10, 6, 0), True, id="10.6-first-supported"),
            pytest.param((11, 0, 0), True, id="11.x"),
            pytest.param((10, 5, 0), False, id="10.5-too-old"),
        ],
    )
    def test_mariadb_has_its_own_floor(self, version, expected):
        """MariaDB reports as 'mysql' but gained SKIP LOCKED at a different version."""
        assert supports_skip_locked(_bind("mysql", server_version_info=version, is_mariadb=True)) is expected

    def test_an_unknown_mysql_version_is_unsupported(self):
        """``server_version_info`` is only populated after connecting."""
        assert supports_skip_locked(_bind("mysql", server_version_info=None)) is False
        assert supports_skip_locked(_bind("mysql", server_version_info=())) is False


class TestAgainstARealEngine:
    def test_sqlite_reports_unsupported(self):
        engine = create_engine("sqlite://")
        with engine.connect() as connection:
            assert supports_skip_locked(connection) is False
        engine.dispose()
