"""Dialect capability detection.

`supports_skip_locked` decides whether concurrent workers take disjoint batches
or can claim the same row twice. A false positive means emitting a clause the
server rejects; a false negative silently downgrades concurrency. Both matter,
so every branch is pinned here rather than being discovered on a real server.
"""

import re

import pytest
from sqlalchemy import DateTime, create_engine
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateColumn, CreateTable

from jasil._core.dialects import needs_locking_read, supports_skip_locked
from jasil.orm import jasil_table_names


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

    @pytest.mark.parametrize(("version", "expected"), [((10, 6, 0), True), ((10, 5, 0), False)])
    def test_a_mariadb_url_is_recognised_too(self, version, expected):
        """A ``mariadb+...`` engine's dialect is named ``mariadb`` rather than ``mysql``."""
        assert supports_skip_locked(_bind("mariadb", server_version_info=version, is_mariadb=True)) is expected

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


class TestNeedsLockingRead:
    """A MySQL snapshot can hide a row another transaction committed first; the other databases cannot."""

    def test_no_bind_does_not_need_it(self):
        assert needs_locking_read(None) is False

    @pytest.mark.parametrize(
        ("name", "expected"), [("mysql", True), ("mariadb", True), ("postgresql", False), ("sqlite", False)]
    )
    def test_per_dialect(self, name, expected):
        assert needs_locking_read(_bind(name)) is expected


def _timestamp_columns(mapped_base) -> list:
    """Every timestamp column of JASIL's tables."""
    return [
        column
        for table_name in sorted(jasil_table_names())
        for column in mapped_base.metadata.tables[table_name].columns
        if isinstance(column.type, DateTime)
    ]


class TestTimestampColumns:
    """``DateTime`` alone is a whole-second ``DATETIME`` on MySQL and MariaDB, which rounds what JASIL writes."""

    MYSQL_DIALECTS = (mysql.dialect(), make_url("mariadb+pymysql://").get_dialect()())

    def test_every_jasil_table_has_timestamp_columns_to_check(self, mapped_base):
        assert {column.table.name for column in _timestamp_columns(mapped_base)} == jasil_table_names()

    @pytest.mark.parametrize("dialect", MYSQL_DIALECTS, ids=lambda dialect: dialect.name)
    def test_on_mysql_and_mariadb_every_timestamp_keeps_microseconds(self, mapped_base, dialect):
        for table_name in sorted(jasil_table_names()):
            ddl = str(CreateTable(mapped_base.metadata.tables[table_name]).compile(dialect=dialect))
            assert re.findall(r"DATETIME(?!\(6\))", ddl) == [], table_name
            assert re.findall(r"CURRENT_TIMESTAMP(?!\(6\))|now\(\)", ddl) == [], table_name

    @pytest.mark.parametrize("dialect", MYSQL_DIALECTS, ids=lambda dialect: dialect.name)
    def test_on_mysql_and_mariadb_a_server_default_is_the_microsecond_clock(self, mapped_base, dialect):
        """A ``DATETIME(6)`` column refuses a ``CURRENT_TIMESTAMP`` default of lower precision."""
        for column in _timestamp_columns(mapped_base):
            spec = str(CreateColumn(column).compile(dialect=dialect))
            assert spec.startswith(f"{column.name} DATETIME(6)"), spec
            assert ("DEFAULT CURRENT_TIMESTAMP(6)" in spec) is (column.server_default is not None), spec

    @pytest.mark.parametrize(
        ("dialect", "rendered_type", "rendered_default"),
        [
            (postgresql.dialect(), "TIMESTAMP WITH TIME ZONE", "DEFAULT now()"),
            (sqlite.dialect(), "DATETIME", "DEFAULT CURRENT_TIMESTAMP"),
        ],
        ids=["postgresql", "sqlite"],
    )
    def test_other_dialects_render_what_datetime_with_a_time_zone_always_did(
        self, mapped_base, dialect, rendered_type, rendered_default
    ):
        for column in _timestamp_columns(mapped_base):
            spec = str(CreateColumn(column).compile(dialect=dialect))
            assert spec.startswith(f"{column.name} {rendered_type}"), spec
            assert "(6)" not in spec, spec
            assert (rendered_default in spec) is (column.server_default is not None), spec
