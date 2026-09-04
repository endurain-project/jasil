"""Alembic migrations for JASIL's companion tables.

JASIL maps its tables into the **host's** declarative registry (see
:mod:`jasil.orm`), so its tables and the host's live in one database. To let
JASIL evolve its schema without owning the host's Alembic history, these
migrations run on their **own** version table (``jasil_alembic_version``) and are
scoped to JASIL's own tables — the host's tables are never touched.

Requires the optional ``jasil[migrations]`` extra (Alembic). ``import jasil``
never pulls this in; import it explicitly::

    import jasil.orm as jasil_orm
    from jasil import migrations

    jasil_orm.map_models(Base)                  # the metadata must exist first
    migrations.adopt_existing_schema(engine)   # validate/stamp legacy tables, if any
    migrations.upgrade(engine)                  # create/upgrade JASIL's tables
    migrations.verify_schema_current(engine)   # fail fast if not migrated

Hosts that prefer a single, unified Alembic history can instead point their own
``env.py`` at their ``Base.metadata`` (the base passed to
:func:`jasil.orm.map_models`) and add this package's ``versions`` directory to
their ``version_locations`` — but the self-contained runner here needs no host
wiring.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any

from jasil._core import optional_deps

try:
    import alembic as _alembic
except ImportError:  # pragma: no cover - exercised via the missing-dep guard
    _alembic = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

__all__ = [
    "VERSION_TABLE",
    "SchemaCompatibilityError",
    "adopt_existing_schema",
    "db_revision",
    "downgrade",
    "head_revision",
    "jasil_include_object",
    "upgrade",
    "verify_schema_current",
]

#: Dedicated Alembic version table, kept separate from the host's
#: ``alembic_version`` so the two migration histories never collide in one DB.
VERSION_TABLE = "jasil_alembic_version"

_MIGRATIONS_DIR = Path(__file__).resolve().parent
_POSTGRES_GIN_INDEX = "idx_event_log_metadata_gin"


class SchemaCompatibilityError(RuntimeError):
    """An unversioned JASIL schema cannot safely be adopted."""


def jasil_include_object(obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
    """Alembic ``include_object`` hook scoping operations to JASIL's tables.

    Excludes every host table sharing the registry, so JASIL's migrations and
    autogenerate never add, drop, or diff them.
    """
    from jasil.orm import jasil_table_names

    if type_ == "table":
        return name in jasil_table_names()
    return True


def _require_alembic() -> Any:
    return optional_deps.require(_alembic, package="alembic", extra="migrations", feature="Alembic migrations")


def _config(connection: Any = None) -> Any:
    """Build a programmatic Alembic ``Config`` bound to this package."""
    _require_alembic()
    from alembic.config import Config

    config = Config()
    config.set_main_option("script_location", str(_MIGRATIONS_DIR))
    config.set_main_option("version_locations", str(_MIGRATIONS_DIR / "versions"))
    # Alembic 1.18+ splits version_locations on the OS path separator; setting it
    # explicitly silences the legacy-splitting warning and is ignored by older
    # versions.
    config.set_main_option("path_separator", "os")
    if connection is not None:
        config.attributes["connection"] = connection
    return config


def _run_on_connection(connection: Any, command_name: str, *args: Any) -> None:
    _require_alembic()
    from alembic import command

    config = _config(connection)
    getattr(command, command_name)(config, *args)


def _run(engine: "Engine", command_name: str, *args: Any) -> None:
    with engine.connect() as connection:
        _run_on_connection(connection, command_name, *args)


def upgrade(engine: "Engine", revision: str = "head") -> None:
    """Create or upgrade JASIL's tables to ``revision`` (default ``head``)."""
    _run(engine, "upgrade", revision)


def downgrade(engine: "Engine", revision: str) -> None:
    """Downgrade JASIL's tables to ``revision`` (``"base"`` drops them all)."""
    _run(engine, "downgrade", revision)


def head_revision() -> str | None:
    """Return the newest revision shipped in this package."""
    _require_alembic()
    from alembic.script import ScriptDirectory

    return ScriptDirectory.from_config(_config()).get_current_head()


def db_revision(engine: "Engine") -> str | None:
    """Return the JASIL migration revision currently recorded in ``engine``."""
    _require_alembic()
    from alembic.runtime.migration import MigrationContext

    with engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
        return context.get_current_revision()


def _format_difference(difference: Any) -> str:
    if isinstance(difference, list):
        return "; ".join(_format_difference(item) for item in difference)

    operation = difference[0]
    if operation in {"add_index", "remove_index", "add_constraint", "remove_constraint"}:
        obj = difference[1]
        state = "missing" if operation.startswith("add_") else "unexpected"
        kind = "index" if operation.endswith("index") else "unique constraint"
        return f"{obj.table.name}: {state} {kind} {obj.name!r}"

    table_name = difference[2]
    if operation == "add_column":
        return f"{table_name}: missing column {difference[3].name!r}"
    if operation == "remove_column":
        return f"{table_name}: unexpected column {difference[3].name!r}"
    if operation == "modify_type":
        return f"{table_name}.{difference[3]}: expected type {difference[6]}, found {difference[5]}"
    if operation == "modify_nullable":
        return f"{table_name}.{difference[3]}: expected nullable={difference[6]!r}, found nullable={difference[5]!r}"
    return f"{table_name}: incompatible schema operation {operation!r}"


def _adoption_include_object(obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
    if type_ == "index":
        if name == _POSTGRES_GIN_INDEX:
            return False
        if reflected and compare_to is None:
            # Additional non-unique indexes do not change JASIL's write contract.
            return False
    return jasil_include_object(obj, name, type_, reflected, compare_to)


def _primary_key_differences(inspector: Any, metadata: Any, table_names: frozenset[str]) -> list[str]:
    differences = []
    for table_name in sorted(table_names):
        expected = [column.name for column in metadata.tables[table_name].primary_key.columns]
        actual = inspector.get_pk_constraint(table_name).get("constrained_columns") or []
        if actual != expected:
            differences.append(f"{table_name}: expected primary key {expected!r}, found {actual!r}")
    return differences


def _postgres_gin_differences(connection: Any, inspector: Any) -> list[str]:
    if connection.dialect.name != "postgresql":
        return []

    indexes = {index["name"]: index for index in inspector.get_indexes("event_log")}
    index = indexes.get(_POSTGRES_GIN_INDEX)
    if index is None:
        return [f"event_log: missing index {_POSTGRES_GIN_INDEX!r}"]

    differences = []
    if index.get("column_names") != ["event_metadata"]:
        differences.append(f"event_log: index {_POSTGRES_GIN_INDEX!r} must cover only 'event_metadata'")
    options = index.get("dialect_options", {})
    if options.get("postgresql_using") != "gin":
        differences.append(f"event_log: index {_POSTGRES_GIN_INDEX!r} must use GIN")
    operators = options.get("postgresql_ops")
    if operators is not None and operators.get("event_metadata") != "jsonb_path_ops":
        differences.append(f"event_log: index {_POSTGRES_GIN_INDEX!r} must use jsonb_path_ops")
    return differences


def adopt_existing_schema(engine: "Engine") -> bool:
    """Validate and adopt complete, unversioned JASIL tables.

    Returns ``True`` only when this call records the installed head. An empty
    database or one that already has a JASIL revision is left unchanged and
    returns ``False``.

    Raises:
        SchemaCompatibilityError: If only some JASIL tables exist, or their
            physical schema is incompatible with the installed migration head.
    """
    _require_alembic()
    from alembic import command
    from alembic.autogenerate import compare_metadata
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import inspect

    from jasil.orm import get_active_base, jasil_table_names

    with engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
        if context.get_current_revision() is not None:
            return False

        inspector = inspect(connection)
        expected_tables = jasil_table_names()
        present_tables = set(inspector.get_table_names()) & expected_tables
        if not present_tables:
            return False
        if present_tables != expected_tables:
            missing = sorted(expected_tables - present_tables)
            raise SchemaCompatibilityError("Cannot adopt a partial JASIL schema; missing tables: " + ", ".join(missing))

        metadata = get_active_base().metadata
        comparison_context = MigrationContext.configure(
            connection,
            opts={
                "compare_type": True,
                "include_object": _adoption_include_object,
            },
        )
        raw_differences = compare_metadata(comparison_context, metadata)
        differences = [_format_difference(difference) for difference in raw_differences]
        differences.extend(_primary_key_differences(inspector, metadata, expected_tables))
        differences.extend(_postgres_gin_differences(connection, inspector))
        if differences:
            raise SchemaCompatibilityError("Cannot adopt an incompatible JASIL schema: " + "; ".join(differences))

        head = head_revision()
        if head is None:
            raise RuntimeError("JASIL has no packaged migration head")
        config = _config(connection)
        command.stamp(config, head)
        connection.commit()
        return True


def verify_schema_current(engine: "Engine") -> None:
    """Raise if the database is not migrated to the packaged head revision.

    A fail-fast startup check: call it after configuring the engine to catch a
    forgotten upgrade before the first query does.

    Raises:
        RuntimeError: If the recorded revision differs from the packaged head.
    """
    head = head_revision()
    current = db_revision(engine)
    if current != head:
        raise RuntimeError(
            "JASIL database schema is out of date "
            f"(database revision={current!r}, expected head={head!r}). "
            "Run jasil.migrations.adopt_existing_schema(engine) for legacy unversioned tables, "
            "then jasil.migrations.upgrade(engine) at deploy time."
        )
