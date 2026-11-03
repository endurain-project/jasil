"""SQL the host runs itself: role grants and the retention floor.

JASIL never connects as a privileged role or names a role of its own. These helpers return statements
for the host to review and run with the account that owns the audit tables (the one that ran the
migration), in its own deployment tooling. PostgreSQL, and MySQL or MariaDB with ``dialect="mysql"``.
"""

from typing import Literal

from jasil.audit import _mysql as mysql
from jasil.audit import _postgres as pg

__all__ = ["grants_sql", "retention_floor_sql"]

#: The databases these helpers write SQL for; ``"mysql"`` covers MariaDB too.
type _Dialect = Literal["postgresql", "mysql"]

_TABLES = ("jasil_audit_log", "jasil_audit_actions", "jasil_audit_checkpoints", "jasil_instance")


def _quote(dialect: _Dialect, name: str) -> str:
    return mysql.quote_ident(name) if dialect == "mysql" else pg.quote_ident(name)


def _qualified(dialect: _Dialect, schema: str | None, name: str) -> str:
    return f"{_quote(dialect, schema)}.{name}" if schema else name


def _role(dialect: _Dialect, name: str) -> str:
    if not name:
        raise ValueError("a role name must not be empty")
    return _quote(dialect, name)


def _check(dialect: _Dialect) -> None:
    """Refuse a dialect these helpers do not write SQL for; a caller need not be type-checked."""
    if dialect not in ("postgresql", "mysql"):
        raise ValueError(f"dialect must be 'postgresql' or 'mysql' (MySQL and MariaDB), not {dialect!r}")


def grants_sql(
    runtime_role: str,
    reader_role: str,
    *,
    maintenance_role: str | None = None,
    schema: str | None = None,
    dialect: _Dialect = "postgresql",
) -> list[str]:
    """
    Return the statements that give each role exactly the access to the audit tables it needs.

    * ``runtime_role`` — the application: ``INSERT`` on ``jasil_audit_log`` and nothing else on it, so it
      cannot read, change or delete a row; ``SELECT`` and ``INSERT`` on ``jasil_instance`` and ``SELECT``,
      ``INSERT`` and ``UPDATE`` on ``jasil_audit_actions`` (the catalog synchronization); ``EXECUTE`` on
      ``jasil_audit_ensure_partitions``.
    * ``reader_role`` — the observability product: ``SELECT`` on all four tables.
    * ``maintenance_role`` (optional) — the process that calls ``run_audit_maintenance``: it must read the
      log to seal checkpoints, so it also gets ``SELECT`` on ``jasil_audit_log``, ``SELECT`` and
      ``INSERT`` on ``jasil_audit_checkpoints``, ``UPDATE`` on ``jasil_instance`` and ``EXECUTE`` on both
      routines. Passing the runtime role here gives the application read access to the trail; pass a
      separate role to keep it append-only.

    On PostgreSQL every statement first revokes what the role may already hold on these tables and functions,
    so running the list converges to the intended state; the two functions are already closed to ``PUBLIC`` by
    the migration. MySQL and MariaDB cannot revoke a privilege that was never granted without failing, so there
    the statements only grant: start from roles that hold nothing on these tables, and nothing database-wide,
    which a table-level grant cannot take away.

    Args:
        runtime_role: Role the application connects as.
        reader_role: Role the observability product connects as.
        maintenance_role: Role the maintenance job connects as, when it differs from the runtime role.
        schema: Schema (on MySQL, database) holding the tables; the connection's default when omitted.
        dialect: ``"postgresql"``, or ``"mysql"`` for MySQL and MariaDB, where a role name is an account or
            role at host ``%``.

    Returns:
        The statements, in the order to run them.

    Raises:
        ValueError: When a role name is empty, the reader role is also the runtime or the maintenance role
            (which would make the trail readable by the process it audits, or writable by its reader), or the
            dialect is not one of the two.
    """
    _check(dialect)
    if runtime_role == reader_role:
        raise ValueError("the runtime and reader roles must differ, or the application can read the trail it writes")
    if maintenance_role == reader_role:
        raise ValueError("the maintenance and reader roles must differ, or the reader can insert rows and checkpoints")
    log, actions, checkpoints, instance = (_qualified(dialect, schema, table) for table in _TABLES)
    if dialect == "mysql":
        routines = {
            name: f"PROCEDURE {_qualified(dialect, schema, name)}"
            for name in (mysql.ENSURE_PROCEDURE, mysql.DROP_PROCEDURE)
        }
    else:
        routines = {
            name: f"FUNCTION {_qualified(dialect, schema, name)}({arguments})"
            for name, arguments in ((pg.ENSURE_FUNCTION, "integer"), (pg.DROP_FUNCTION, pg.DROP_ARGUMENTS))
        }
    ensure, drop = routines
    statements: list[str] = []

    def role_access(role: str, grants: dict[str, str], executes: tuple[str, ...] = ()) -> None:
        quoted = _role(dialect, role)
        if dialect == "postgresql":
            for table in (log, actions, checkpoints, instance):
                statements.append(f"REVOKE ALL ON TABLE {table} FROM {quoted}")
        for table, privileges in grants.items():
            statements.append(f"GRANT {privileges} ON TABLE {table} TO {quoted}")
        if dialect == "postgresql":
            for routine in routines.values():
                statements.append(f"REVOKE ALL ON {routine} FROM {quoted}")
        for name in executes:
            statements.append(f"GRANT EXECUTE ON {routines[name]} TO {quoted}")

    role_access(runtime_role, {log: "INSERT", instance: "SELECT, INSERT", actions: "SELECT, INSERT, UPDATE"}, (ensure,))
    role_access(reader_role, {table: "SELECT" for table in (log, actions, checkpoints, instance)})
    if maintenance_role is not None:
        role_access(
            maintenance_role,
            {
                log: "SELECT, INSERT",
                actions: "SELECT, INSERT, UPDATE",
                checkpoints: "SELECT, INSERT",
                instance: "SELECT, INSERT, UPDATE",
            },
            (ensure, drop),
        )
    return statements


def retention_floor_sql(floor_days: int, *, schema: str, dialect: _Dialect = "postgresql") -> list[str]:
    """
    Return the statements that change the retention floor of ``jasil_audit_drop_partitions``.

    The floor is baked into the routine when its migration runs (``rev0006`` on PostgreSQL, ``rev0008`` on
    MySQL and MariaDB), from ``AuditSettings.retention_floor_days``. Changing the setting afterwards does not
    change the guard; run these statements, in order, as the account that owns the routine.

    On PostgreSQL this is one ``CREATE OR REPLACE FUNCTION``, which keeps the function's owner and grants.
    MySQL has no ``CREATE OR REPLACE PROCEDURE``, so there it is ``DROP PROCEDURE`` then ``CREATE PROCEDURE``;
    the procedure is recreated with the running account as its definer, and dropping it removes the
    ``EXECUTE`` grants on it, so run :func:`grants_sql` for the maintenance role again afterwards.

    Args:
        floor_days: The youngest age, in days, the routine will let a cutoff reach. Between 1 and 36500.
        schema: Schema (on MySQL, database) holding the audit tables, as the migration found it
            (``current_schema()`` or ``DATABASE()`` when it ran). Required: the routine is qualified with it,
            so there is no safe default.
        dialect: ``"postgresql"``, or ``"mysql"`` for MySQL and MariaDB.

    Returns:
        The statements, in the order to run them.

    Raises:
        ValueError: When ``floor_days`` is below 1 or above 36500, or the dialect is not one of the two.
    """
    _check(dialect)
    if dialect == "mysql":
        return [
            f"DROP PROCEDURE IF EXISTS {_qualified(dialect, schema, mysql.DROP_PROCEDURE)}",
            mysql.drop_partitions_procedure(schema, floor_days).strip(),
        ]
    return [pg.drop_partitions_function(schema, floor_days).strip()]
