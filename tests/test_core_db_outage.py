"""Telling a lost database from a defect, and logging each the way it deserves.

An unreachable database is a degraded dependency: once it is back nothing is wrong, so it is logged as a WARNING
without a traceback. Everything else is a defect someone has to fix and keeps its level and traceback. The
classification is built from the same exception objects SQLAlchemy raises, wrapping the real driver errors.
"""

import logging
import sqlite3

import psycopg
import pytest
from sqlalchemy import exc

from jasil._core.db_outage import log_failure
from jasil.orm import is_database_unavailable


def _wrapped(driver_error: Exception, base: type[Exception] = psycopg.Error) -> exc.DBAPIError:
    """Wrap ``driver_error`` the way SQLAlchemy does when a statement raises it."""
    return exc.DBAPIError.instance("SELECT 1", {}, driver_error, base)


class PyMySQLStyleError(Exception):
    """The shape of a PyMySQL or mysqlclient error: the MySQL error number first, and no SQLSTATE."""


class PyMySQLStyleOperationalError(PyMySQLStyleError):
    """PyMySQL raises its ``OperationalError`` for lost connections and for deadlocks alike."""


def _mysql(errno: int) -> exc.DBAPIError:
    return exc.OperationalError("SELECT 1", {}, PyMySQLStyleOperationalError(errno, "mysql error"))


class TestIsDatabaseUnavailable:
    def test_an_exhausted_pool_is_unavailable(self):
        assert is_database_unavailable(exc.TimeoutError("QueuePool limit reached")) is True

    def test_an_invalidated_connection_is_unavailable(self):
        """SQLAlchemy's dialect recognised a disconnect, whatever the driver error."""
        error = exc.DBAPIError("SELECT 1", {}, sqlite3.DatabaseError("gone"), connection_invalidated=True)

        assert is_database_unavailable(error) is True

    def test_a_refused_postgres_connection_is_unavailable(self):
        """Every PostgreSQL server error carries a SQLSTATE; a driver-side failure to connect does not."""
        error = _wrapped(psycopg.OperationalError("connection refused"))

        assert isinstance(error, exc.OperationalError)
        assert is_database_unavailable(error) is True

    @pytest.mark.parametrize(
        "sqlstate",
        [
            "08000",  # connection_exception
            "08006",  # connection_failure
            "57P01",  # admin_shutdown: pg_terminate_backend, a smart or fast shutdown
            "57P02",  # crash_shutdown
            "57P03",  # cannot_connect_now: starting up or in recovery
            "53300",  # too_many_connections
        ],
    )
    def test_a_server_that_is_going_away_or_full_is_unavailable(self, sqlstate, postgres_error):
        assert is_database_unavailable(postgres_error(sqlstate)) is True

    @pytest.mark.parametrize(
        "sqlstate",
        [
            "53100",  # disk_full: the server is reachable and something has to be fixed
            "23505",  # unique_violation
            "40P01",  # deadlock_detected
            "42P01",  # undefined_table
        ],
    )
    def test_a_server_side_defect_is_not_unavailable(self, sqlstate, postgres_error):
        assert is_database_unavailable(postgres_error(sqlstate)) is False

    def test_a_driver_error_without_a_sqlstate_that_is_not_operational_is_not_unavailable(self):
        """A missing SQLSTATE means a lost connection only on an ``OperationalError``."""
        error = _wrapped(psycopg.InterfaceError("the cursor is closed"))

        assert is_database_unavailable(error) is False

    @pytest.mark.parametrize(
        "errno",
        [
            2002,  # CR_CONNECTION_ERROR: the local socket refused
            2003,  # CR_CONN_HOST_ERROR: the server refused
            2006,  # CR_SERVER_GONE_ERROR
            2013,  # CR_SERVER_LOST
        ],
    )
    def test_a_mysql_client_error_is_unavailable(self, errno):
        assert is_database_unavailable(_mysql(errno)) is True

    @pytest.mark.parametrize(
        "errno",
        [
            1205,  # ER_LOCK_WAIT_TIMEOUT
            1213,  # ER_LOCK_DEADLOCK
            1146,  # ER_NO_SUCH_TABLE
        ],
    )
    def test_a_mysql_server_error_is_not_unavailable(self, errno):
        """PyMySQL reports these as ``OperationalError`` too, with no SQLSTATE to tell them apart."""
        assert is_database_unavailable(_mysql(errno)) is False

    def test_a_locked_sqlite_database_is_not_unavailable(self):
        error = _wrapped(sqlite3.OperationalError("database is locked"), sqlite3.Error)

        assert is_database_unavailable(error) is False

    def test_an_error_that_is_not_from_the_database_is_not_unavailable(self):
        assert is_database_unavailable(RuntimeError("handler bug")) is False


class TestLogFailure:
    def test_an_outage_is_a_warning_without_a_traceback(self, caplog, postgres_error):
        log = logging.getLogger("tests.db_outage")

        with caplog.at_level(logging.DEBUG, logger="tests.db_outage"):
            log_failure(log, "Outbox relay failed", postgres_error("57P01"), extra={"job_id": "j1"})

        [record] = caplog.records
        assert record.levelno == logging.WARNING
        assert record.exc_info is None
        assert record.getMessage() == "Outbox relay failed; the database is unavailable"
        assert record.error == "OperationalError"
        assert record.job_id == "j1"

    def test_a_defect_keeps_its_level_and_traceback(self, caplog, postgres_error):
        log = logging.getLogger("tests.db_outage")
        error = postgres_error("53100")

        with caplog.at_level(logging.DEBUG, logger="tests.db_outage"):
            log_failure(log, "Outbox relay failed", error, extra={"job_id": "j1"})

        [record] = caplog.records
        assert record.levelno == logging.ERROR
        assert record.exc_info[1] is error
        assert record.getMessage() == "Outbox relay failed"
        assert record.job_id == "j1"

    def test_a_defect_can_be_logged_at_a_lower_level(self, caplog):
        """Telemetry failures are warnings either way; only the traceback depends on the cause."""
        log = logging.getLogger("tests.db_outage")

        with caplog.at_level(logging.DEBUG, logger="tests.db_outage"):
            log_failure(log, "Durable worker heartbeat failed", RuntimeError("bug"), level=logging.WARNING)

        [record] = caplog.records
        assert record.levelno == logging.WARNING
        assert record.exc_info is not None
