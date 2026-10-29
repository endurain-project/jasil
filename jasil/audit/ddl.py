"""SQL the host runs itself: role grants and the retention floor.

JASIL never connects as a privileged role or names a role of its own. These helpers return statements
for the host to review and run with the role that owns the audit tables (the one that ran the
migration), in its own deployment tooling. PostgreSQL only.
"""

from jasil.audit import _postgres as pg

__all__ = ["grants_sql", "retention_floor_sql"]

_TABLES = ("jasil_audit_log", "jasil_audit_actions", "jasil_audit_checkpoints", "jasil_instance")


def _qualified(schema: str | None, name: str) -> str:
    return f"{pg.quote_ident(schema)}.{name}" if schema else name


def _role(name: str) -> str:
    if not name:
        raise ValueError("a role name must not be empty")
    return pg.quote_ident(name)


def grants_sql(
    runtime_role: str,
    reader_role: str,
    *,
    maintenance_role: str | None = None,
    schema: str | None = None,
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
      functions. Passing the runtime role here gives the application read access to the trail; pass a
      separate role to keep it append-only.

    Every statement first revokes what the role may already hold on these tables, so running the list
    converges to the intended state. The two functions are already closed to ``PUBLIC`` by the migration.

    Args:
        runtime_role: Role the application connects as.
        reader_role: Role the observability product connects as.
        maintenance_role: Role the maintenance job connects as, when it differs from the runtime role.
        schema: Schema holding the tables; the connection's default when omitted.

    Returns:
        The statements, in the order to run them.

    Raises:
        ValueError: When a role name is empty, or the reader role is also the runtime or the maintenance role
            (which would make the trail readable by the process it audits, or writable by its reader).
    """
    if runtime_role == reader_role:
        raise ValueError("the runtime and reader roles must differ, or the application can read the trail it writes")
    if maintenance_role == reader_role:
        raise ValueError("the maintenance and reader roles must differ, or the reader can insert rows and checkpoints")
    log, actions, checkpoints, instance = (_qualified(schema, table) for table in _TABLES)
    functions = {
        name: f"{_qualified(schema, name)}({arguments})"
        for name, arguments in (
            (pg.ENSURE_FUNCTION, "integer"),
            (pg.DROP_FUNCTION, "timestamptz"),
        )
    }
    statements: list[str] = []

    def role_access(role: str, grants: dict[str, str], executes: tuple[str, ...] = ()) -> None:
        quoted = _role(role)
        for table in (log, actions, checkpoints, instance):
            statements.append(f"REVOKE ALL ON TABLE {table} FROM {quoted}")
        for table, privileges in grants.items():
            statements.append(f"GRANT {privileges} ON TABLE {table} TO {quoted}")
        for function in functions.values():
            statements.append(f"REVOKE ALL ON FUNCTION {function} FROM {quoted}")
        for name in executes:
            statements.append(f"GRANT EXECUTE ON FUNCTION {functions[name]} TO {quoted}")

    role_access(
        runtime_role,
        {log: "INSERT", instance: "SELECT, INSERT", actions: "SELECT, INSERT, UPDATE"},
        (pg.ENSURE_FUNCTION,),
    )
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
            (pg.ENSURE_FUNCTION, pg.DROP_FUNCTION),
        )
    return statements


def retention_floor_sql(floor_days: int, *, schema: str) -> str:
    """
    Return the statement that changes the retention floor of ``jasil_audit_drop_partitions``.

    The floor is baked into the function when migration ``rev0006`` runs, from
    ``AuditSettings.retention_floor_days``. Changing the setting afterwards does not change the guard;
    run this statement as the role that owns the function. It keeps the function's owner and grants.

    Args:
        floor_days: The youngest age, in days, the function will let a cutoff reach. Between 1 and 36500.
        schema: Schema holding the audit tables, as migration ``rev0006`` found it (``current_schema()``
            when it ran). Required: the function body names its schema, so there is no safe default.

    Returns:
        A ``CREATE OR REPLACE FUNCTION`` statement.

    Raises:
        ValueError: When ``floor_days`` is below 1 or above 36500.
    """
    return pg.drop_partitions_function(schema, floor_days).strip()
