"""Telling an unreachable database from a defect, and logging each the way it deserves.

ERROR means a defect someone has to fix. A database that is down, restarting or out of connection slots is a
degraded dependency: once it is back nothing is wrong. The background loops that retry on their own (the job
worker, its heartbeat, the scheduled maintenance) log it as a WARNING without a traceback instead.
"""

import logging
from collections.abc import Mapping

from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

__all__ = ["is_database_unavailable", "log_failure"]

# 08: connection exception. 57P01 admin_shutdown, 57P02 crash_shutdown, 57P03 cannot_connect_now (starting up or
# in recovery), 53300 too_many_connections.
_UNAVAILABLE_SQLSTATES = frozenset({"57P01", "57P02", "57P03", "53300"})

# The MySQL client's own error numbers (CR_*: cannot connect, server gone away, connection lost); a number below
# them is an error the server reported.
_MYSQL_CLIENT_ERRORS = range(2000, 3000)


def is_database_unavailable(error: BaseException) -> bool:
    """Return whether ``error`` means the database could not be reached, rather than that something is broken.

    True for an exhausted connection pool, for a connection SQLAlchemy invalidated as a disconnect, and for these
    driver errors:

    - A driver that reports a SQLSTATE (psycopg, psycopg2): SQLSTATE class ``08``, ``57P01``, ``57P02``, ``57P03``
      or ``53300``, or an ``OperationalError`` with no SQLSTATE at all. Every server error carries one, so a missing
      SQLSTATE means the driver lost the connection or was refused it.
    - A MySQL driver that reports only error numbers (PyMySQL, mysqlclient): an ``OperationalError`` with a client
      error number (2000-2999). Server errors such as a deadlock are ``OperationalError`` too and stay False.

    Args:
        error: The exception a database call raised.

    Returns:
        True when the database is unreachable, restarting or full; False for anything else.
    """
    if isinstance(error, PoolTimeoutError):
        return True
    if not isinstance(error, DBAPIError):
        return False
    if error.connection_invalidated:
        return True
    driver_error = error.orig
    if hasattr(driver_error, "sqlstate") or hasattr(driver_error, "pgcode"):
        sqlstate = getattr(driver_error, "sqlstate", None) or getattr(driver_error, "pgcode", None)
        if sqlstate is None:
            return isinstance(error, OperationalError)
        return sqlstate.startswith("08") or sqlstate in _UNAVAILABLE_SQLSTATES
    code = driver_error.args[0] if driver_error is not None and driver_error.args else None
    return isinstance(error, OperationalError) and isinstance(code, int) and code in _MYSQL_CLIENT_ERRORS


def log_failure(
    log: logging.Logger,
    message: str,
    error: BaseException,
    *,
    level: int = logging.ERROR,
    extra: Mapping[str, object] | None = None,
) -> None:
    """Log a failed step that is retried on its own: WARNING without a traceback when the database is unavailable.

    Args:
        log: The calling module's logger.
        message: What failed.
        error: Why it failed.
        level: The level for any other failure, which is logged with its traceback.
        extra: Structured fields for the record. An outage adds ``error``, the exception's class name.
    """
    if is_database_unavailable(error):
        log.warning("%s; the database is unavailable", message, extra={**(extra or {}), "error": type(error).__name__})
    else:
        log.log(level, message, exc_info=error, extra=extra)
