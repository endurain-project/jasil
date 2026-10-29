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
    migrations.adopt_existing_schema(engine)   # validate/record legacy tables, if any
    migrations.upgrade(engine)                  # create/upgrade JASIL's tables
    migrations.verify_schema_current(engine)   # fail fast if not migrated

Hosts that prefer a single, unified Alembic history can instead point their own
``env.py`` at their ``Base.metadata`` (the base passed to
:func:`jasil.orm.map_models`) and add this package's ``versions`` directory to
their ``version_locations`` — but the self-contained runner here needs no host
wiring. Only :func:`upgrade` creates a fresh database with the ``jasil_`` names
directly; from a host's own history ``rev0001`` first creates the unprefixed names
and ``rev0003`` renames them.
"""

import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from jasil._core import optional_deps
from jasil.migrations import _names

try:
    import alembic as _alembic
except ImportError:  # pragma: no cover - exercised via the missing-dep guard
    _alembic = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

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

#: The first revision after which every table carries the ``jasil_`` prefix.
_PREFIXED_FROM = "rev0003"
#: The PostgreSQL-only GIN index on the event log's metadata, by the table it is on:
#: the prefixed name used from rev0003 on, and the unprefixed one before it.
_POSTGRES_GIN_INDEXES = {
    "jasil_event_log": "idx_jasil_event_log_metadata_gin",
    "event_log": "idx_event_log_metadata_gin",
}

#: What an unversioned pre-0.9 schema always holds. A lone ``event_log`` table is
#: far more likely the host's own than JASIL's, so two of the three together mark
#: one - a damaged schema missing a table is still recognised and refused.
_LEGACY_BASELINE_TABLES = frozenset({"event_log", "event_outbox", "processing_jobs"})


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


#: Name of the database lock a migration run holds; see :func:`_one_run_at_a_time`.
_RUN_LOCK = "jasil_migrations"


@contextmanager
def _one_run_at_a_time(connection: Any) -> Generator[None]:
    """Hold a database lock for one migration run, so processes upgrading at once queue instead of colliding.

    The run that waited then reads the version table after the first one committed and finds nothing left
    to do. Each revision commits on its own, so PostgreSQL and MySQL hold a session lock for the whole run
    and give it back afterwards, a failed run included. SQLite is not locked: it serves a single process,
    and JASIL refuses a multi-process SQLite deployment.

    Raises:
        RuntimeError: When MySQL does not grant the lock.
    """
    from sqlalchemy import text

    dialect = connection.dialect.name
    if dialect == "postgresql":
        connection.execute(text("SELECT pg_advisory_lock(hashtext(:name))"), {"name": _RUN_LOCK})
        try:
            yield
        except BaseException:
            # A failure can leave the connection in an aborted transaction, where the unlock would fail too. On
            # success a transaction still open is the caller's (adoption stamps inside one) and is left to it.
            if connection.in_transaction():
                connection.rollback()
            raise
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(hashtext(:name))"), {"name": _RUN_LOCK})
    elif dialect in ("mysql", "mariadb"):
        if connection.execute(text("SELECT GET_LOCK(:name, -1)"), {"name": _RUN_LOCK}).scalar() != 1:
            raise RuntimeError("MySQL did not grant the JASIL migration lock")
        try:
            yield
        finally:
            connection.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": _RUN_LOCK})
    else:
        yield


def _run_on_connection(connection: Any, command_name: str, *args: Any) -> None:
    _require_alembic()
    from alembic import command

    config = _config(connection)
    getattr(command, command_name)(config, *args)


def _run(engine: "Engine", command_name: str, *args: Any) -> None:
    with engine.connect() as connection:
        _run_on_connection(connection, command_name, *args)


def _fresh_install_prefix(connection: Any, revision: str) -> str:
    """Return the table prefix a run to ``revision`` should create tables with.

    ``jasil_`` for a fresh database heading to :data:`_PREFIXED_FROM` or later, so
    JASIL never creates an unprefixed table that a host's own could already
    occupy; empty everywhere else (see :mod:`jasil.migrations._names`). Fresh means
    no recorded revision and no prefixed table. A single unprefixed table of the
    same name proves nothing, since it may be the host's; two of the baseline
    tables together are an unversioned old schema, which is refused.
    """
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory
    from sqlalchemy import inspect

    from jasil.orm import jasil_table_names

    context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
    if context.get_current_revision() is not None:
        return ""
    present = set(inspect(connection).get_table_names())
    if present & jasil_table_names():
        # Creating the unprefixed revisions beside prefixed tables would leave
        # duplicates, and rev0003 would then skip its renames.
        raise SchemaCompatibilityError(
            "The database holds unversioned prefixed JASIL tables; "
            "call adopt_existing_schema(engine) before upgrade(engine)"
        )
    if len(_LEGACY_BASELINE_TABLES & present) >= 2:
        # An unversioned pre-0.9 schema: creating the prefixed tables beside it
        # would leave the data behind and make rev0003 skip the rename.
        raise SchemaCompatibilityError(
            "The database holds unversioned, unprefixed JASIL tables from 0.8 or earlier; "
            "call adopt_existing_schema(engine) before upgrade(engine)"
        )
    steps = {script.revision for script in ScriptDirectory.from_config(_config()).iterate_revisions(revision, "base")}
    return _names.PREFIX if _PREFIXED_FROM in steps else ""


def upgrade(engine: "Engine", revision: str = "head") -> None:
    """Create or upgrade JASIL's tables to ``revision`` (default ``head``).

    On a fresh database the tables are created with their final ``jasil_`` names
    directly. A 0.8 database (unprefixed names) is renamed in place by ``rev0003``,
    so **stop every process running 0.8 or earlier before running this**.

    Raises:
        SchemaCompatibilityError: When the mapped base's ``MetaData`` names a schema.
    """
    _require_alembic()
    _refuse_schema_qualified_base()
    # On its own connection: inspecting begins a transaction, and Alembic only
    # commits one it began itself.
    with engine.connect() as connection:
        prefix = _fresh_install_prefix(connection, revision)
    with _names.use_prefix(prefix):
        _run(engine, "upgrade", revision)


def downgrade(engine: "Engine", revision: str) -> None:
    """Downgrade JASIL's tables to ``revision`` (``"base"`` drops them all).

    Raises:
        SchemaCompatibilityError: When the mapped base's ``MetaData`` names a schema.
    """
    _refuse_schema_qualified_base()
    _run(engine, "downgrade", revision)


def _refuse_schema_qualified_base() -> None:
    """Refuse a base whose ``MetaData`` names a schema.

    The migrations create JASIL's tables, and the audit trail's PostgreSQL functions, in the connection's
    default schema; models mapped into another one would query tables that are not there.

    Raises:
        SchemaCompatibilityError: When the mapped base's ``MetaData`` names a schema.
    """
    from jasil.orm import get_active_base

    schema = get_active_base().metadata.schema
    if schema is not None:
        raise SchemaCompatibilityError(
            f"JASIL's migrations create its tables in the connection's default schema, but the mapped base's "
            f"MetaData names schema {schema!r}; map JASIL onto a base without one, and point the connection's "
            "search_path (or default database) at the schema instead"
        )


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


def _adoption_include_object(tables: frozenset[str]) -> Any:
    """Build the ``include_object`` hook for comparing one candidate schema.

    Only the candidate's own tables are compared: under the unprefixed naming a
    host's unrelated ``event_log`` can sit in the same database.
    """

    def include_object(obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any) -> bool:
        if type_ == "table":
            return name in tables
        if type_ == "index":
            if name in _POSTGRES_GIN_INDEXES.values():
                return False
            if reflected and compare_to is None:
                # An additional non-unique index does not change JASIL's write contract; a unique one refuses
                # writes JASIL makes, so it is reported.
                return bool(getattr(obj, "unique", False))
        return True

    return include_object


def _primary_key_differences(inspector: Any, metadata: Any, table_names: frozenset[str]) -> list[str]:
    differences = []
    for table_name in sorted(table_names):
        expected = [column.name for column in metadata.tables[table_name].primary_key.columns]
        actual = inspector.get_pk_constraint(table_name).get("constrained_columns") or []
        if actual != expected:
            differences.append(f"{table_name}: expected primary key {expected!r}, found {actual!r}")
    return differences


def _server_default_differences(inspector: Any, metadata: Any, table_names: frozenset[str]) -> list[str]:
    """Name each NOT NULL column whose database default is missing.

    A writer of the previous release omits the columns a newer revision added and relies on that default, so
    its absence breaks the rolling deploy. Only presence is compared: how a database spells a default (quoting,
    casts, ``now()`` or ``CURRENT_TIMESTAMP``) differs by dialect and driver.
    """
    differences = []
    for table_name in sorted(table_names):
        reflected = {column["name"]: column for column in inspector.get_columns(table_name)}
        for column in metadata.tables[table_name].columns:
            if column.server_default is None or column.nullable:
                continue
            found = reflected.get(column.name)
            if found is not None and found.get("default") is None:
                differences.append(f"{table_name}.{column.name}: missing server default")
    return differences


def _check_constraint_differences(inspector: Any, metadata: Any, table_names: frozenset[str]) -> list[str]:
    """Name each CHECK constraint the schema lacks, by name; their expressions differ by dialect too."""
    from sqlalchemy import CheckConstraint

    differences: list[str] = []
    for table_name in sorted(table_names):
        expected = {
            constraint.name
            for constraint in metadata.tables[table_name].constraints
            if isinstance(constraint, CheckConstraint) and isinstance(constraint.name, str)
        }
        if not expected:
            continue
        present = {constraint["name"] for constraint in inspector.get_check_constraints(table_name)}
        differences.extend(f"{table_name}: missing check constraint {name!r}" for name in sorted(expected - present))
    return differences


def _postgres_audit_differences(connection: Any) -> list[str]:
    """Name what a PostgreSQL audit log built by ``rev0006`` has and this one lacks.

    ``create_all`` builds a plain table, and ``upgrade`` after adoption would never add the partitions, the
    append-only triggers or the retention functions.
    """
    if connection.dialect.name != "postgresql":
        return []
    from sqlalchemy import text

    from jasil.audit import _postgres as audit_pg

    table = audit_pg.LOG
    differences = []
    kind = connection.execute(text("SELECT relkind FROM pg_class WHERE oid = to_regclass(:name)"), {"name": table})
    if kind.scalar() != "p":
        differences.append(f"{table}: not partitioned")
    triggers = set(
        connection.execute(
            text("SELECT tgname FROM pg_trigger WHERE tgrelid = to_regclass(:name) AND NOT tgisinternal"),
            {"name": table},
        ).scalars()
    )
    differences.extend(
        f"{table}: missing trigger {name!r}"
        for name in (f"{table}_no_update_delete", f"{table}_no_truncate")
        if name not in triggers
    )
    for signature in (f"{audit_pg.ENSURE_FUNCTION}(integer)", f"{audit_pg.DROP_FUNCTION}(timestamptz)"):
        if connection.execute(text("SELECT to_regprocedure(:signature)"), {"signature": signature}).scalar() is None:
            differences.append(f"{table}: missing function {signature!r}")
    return differences


def _postgres_gin_differences(connection: Any, inspector: Any, table_name: str) -> list[str]:
    if connection.dialect.name != "postgresql":
        return []

    gin_index = _POSTGRES_GIN_INDEXES[table_name]
    indexes = {index["name"]: index for index in inspector.get_indexes(table_name)}
    index = indexes.get(gin_index)
    if index is None:
        return [f"{table_name}: missing index {gin_index!r}"]

    differences = []
    if index.get("column_names") != ["event_metadata"]:
        differences.append(f"{table_name}: index {gin_index!r} must cover only 'event_metadata'")
    options = index.get("dialect_options", {})
    if options.get("postgresql_using") != "gin":
        differences.append(f"{table_name}: index {gin_index!r} must use GIN")
    operators = options.get("postgresql_ops")
    if operators is not None and operators.get("event_metadata") != "jsonb_path_ops":
        differences.append(f"{table_name}: index {gin_index!r} must use jsonb_path_ops")
    return differences


class _MetadataOps:
    """Stand-in for ``alembic.op`` that applies a downgrade to a ``MetaData``.

    Lets an older revision's schema be derived from the migration that removes
    it, so nothing is maintained twice. Only the operations JASIL's downgrades
    use are supported; anything else raises, so a new kind of operation cannot
    silently yield a wrong schema.
    """

    def __init__(self, metadata: Any) -> None:
        self._metadata = metadata

    def drop_index(self, index_name: str, table_name: str | None = None, **kwargs: Any) -> None:
        tables = [self._metadata.tables[table_name]] if table_name else list(self._metadata.tables.values())
        for table in tables:
            for index in list(table.indexes):
                if index.name == index_name:
                    table.indexes.discard(index)
                    return
        raise RuntimeError(f"downgrade drops unknown index {index_name!r}")

    def create_index(self, index_name: str, table_name: str, columns: Sequence[str], **kwargs: Any) -> None:
        from sqlalchemy import Index

        table = self._metadata.tables[table_name]
        Index(index_name, *(table.c[column] for column in columns), **kwargs)

    def rename_table(self, old_table_name: str, new_table_name: str, **kwargs: Any) -> None:
        table = self._metadata.tables[old_table_name]
        self._metadata.remove(table)
        table.to_metadata(self._metadata, name=new_table_name)

    def drop_constraint(self, constraint_name: str, table_name: str, type_: str | None = None, **kwargs: Any) -> None:
        table = self._metadata.tables[table_name]
        for constraint in list(table.constraints):
            if constraint.name == constraint_name:
                table.constraints.discard(constraint)
                return
        raise RuntimeError(f"downgrade drops unknown constraint {constraint_name!r}")

    def create_unique_constraint(
        self, constraint_name: str, table_name: str, columns: Sequence[str], **kwargs: Any
    ) -> None:
        from sqlalchemy import UniqueConstraint

        table = self._metadata.tables[table_name]
        table.append_constraint(UniqueConstraint(*(table.c[column] for column in columns), name=constraint_name))

    @contextmanager
    def batch_alter_table(self, table_name: str, **kwargs: Any) -> Generator["_MetadataBatchOps"]:
        yield _MetadataBatchOps(self, table_name)

    def get_bind(self) -> Any:
        """Report a dialect no migration branches on, so the portable path is taken."""
        return SimpleNamespace(dialect=SimpleNamespace(name="metadata"))

    def drop_table(self, table_name: str, **kwargs: Any) -> None:
        self._metadata.remove(self._metadata.tables[table_name])

    def drop_column(self, table_name: str, column_name: str, **kwargs: Any) -> None:
        table = self._metadata.tables[table_name]
        table._columns.remove(table.c[column_name])

    def __getattr__(self, name: str) -> Any:
        raise NotImplementedError(
            f"adoption cannot derive an older schema through op.{name}(); teach jasil.migrations._MetadataOps"
        )


class _MetadataBatchOps:
    """The table-scoped operations of ``op.batch_alter_table`` for :class:`_MetadataOps`."""

    def __init__(self, ops: _MetadataOps, table_name: str) -> None:
        self._ops = ops
        self._table_name = table_name

    def drop_constraint(self, constraint_name: str, type_: str | None = None, **kwargs: Any) -> None:
        self._ops.drop_constraint(constraint_name, self._table_name, type_=type_)

    def create_unique_constraint(self, constraint_name: str, columns: Sequence[str], **kwargs: Any) -> None:
        self._ops.create_unique_constraint(constraint_name, self._table_name, columns)


def _copy_jasil_metadata(metadata: Any, only: frozenset[str] | None = None) -> Any:
    from sqlalchemy import MetaData

    from jasil.orm import jasil_table_names, legacy_table_names

    copied = MetaData()
    known = jasil_table_names() | legacy_table_names()
    for table_name in sorted(known & set(metadata.tables) & (set(metadata.tables) if only is None else only)):
        metadata.tables[table_name].to_metadata(copied)
    return copied


def _revision_schemas() -> list[tuple[str, Any]]:
    """Return ``(revision, metadata)`` for every revision, newest first.

    The head entry is a copy of the active metadata. Each older entry is derived
    by running the newer revision's ``downgrade`` against a copy of the one
    before it. The base state (no tables) is not a candidate.
    """
    from alembic.script import ScriptDirectory

    from jasil.orm import get_active_base

    current = _copy_jasil_metadata(get_active_base().metadata)
    schemas: list[tuple[str, Any]] = []
    for script in ScriptDirectory.from_config(_config()).walk_revisions():
        schemas.append((script.revision, current))
        if script.down_revision is None:
            break
        current = _copy_jasil_metadata(current)
        module: Any = script.module
        original_op = module.op
        module.op = _MetadataOps(current)
        try:
            module.downgrade()
        finally:
            module.op = original_op
    return schemas


def _schema_differences(connection: Any, inspector: Any, metadata: Any) -> tuple[list[str], int]:
    """Describe how the unversioned JASIL tables differ from ``metadata``.

    Returns the readable differences and a count of individual discrepancies,
    used to rank which revision came closest.
    """
    from alembic.autogenerate import compare_metadata
    from alembic.runtime.migration import MigrationContext

    from jasil.orm import jasil_table_names, legacy_table_names

    expected_tables = frozenset(metadata.tables)
    # The names this candidate's scheme can use: a host's unrelated ``event_log``
    # is not "unexpected" when the candidate is the prefixed schema.
    universe = legacy_table_names() if expected_tables & legacy_table_names() else jasil_table_names()
    present_tables = frozenset(inspector.get_table_names()) & universe
    missing = sorted(expected_tables - present_tables)
    unexpected = sorted(present_tables - expected_tables)
    differences = []
    if missing:
        differences.append("missing tables: " + ", ".join(missing))
    if unexpected:
        differences.append("unexpected tables: " + ", ".join(unexpected))
    count = len(missing) + len(unexpected)

    # Compare column-level detail only where both sides have the table.
    common = expected_tables & present_tables
    restricted = _copy_jasil_metadata(metadata, common)
    comparison_context = MigrationContext.configure(
        connection,
        opts={
            "compare_type": True,
            "include_object": _adoption_include_object(common),
        },
    )
    detailed = [
        _format_difference(difference)
        for difference in compare_metadata(comparison_context, restricted)
        if difference[0] != "remove_table"
    ]
    detailed.extend(_primary_key_differences(inspector, restricted, common))
    detailed.extend(_server_default_differences(inspector, restricted, common))
    detailed.extend(_check_constraint_differences(inspector, restricted, common))
    for table_name in common & _POSTGRES_GIN_INDEXES.keys():
        detailed.extend(_postgres_gin_differences(connection, inspector, table_name))
    if "jasil_audit_log" in common:
        detailed.extend(_postgres_audit_differences(connection))
    return differences + detailed, count + len(detailed)


def adopt_existing_schema(engine: "Engine") -> bool:
    """Validate and adopt complete, unversioned JASIL tables.

    The tables are matched exactly against the head schema, then against each
    earlier revision's schema, newest first. The first exact match is recorded
    as the database's revision, so a following ``upgrade()`` applies whatever
    revisions come after it. If two revisions describe the same schema, the
    newest is recorded. An older revision's schema is derived from the
    migrations' own ``downgrade`` functions, never maintained separately.

    Returns ``True`` only when this call records a revision. An empty database
    or one that already has a JASIL revision is left unchanged and returns
    ``False``.

    Raises:
        SchemaCompatibilityError: If no revision matches exactly; the message
            names the closest revision and lists its differences. Also when the
            mapped base's ``MetaData`` names a schema.
    """
    _require_alembic()
    _refuse_schema_qualified_base()
    from alembic import command
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import inspect

    from jasil.orm import jasil_table_names, legacy_table_names

    with engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
        if context.get_current_revision() is not None:
            return False

        inspector = inspect(connection)
        present = set(inspector.get_table_names())
        if present & jasil_table_names():
            candidate_names = jasil_table_names()
        elif len(_LEGACY_BASELINE_TABLES & present) >= 2:
            # Unprefixed tables from before 0.9. Two baseline tables are needed:
            # one alone is the host's own.
            candidate_names = legacy_table_names()
        else:
            return False

        closest: tuple[str, list[str], int] | None = None
        for revision, metadata in _revision_schemas():
            if not set(metadata.tables) <= candidate_names:
                continue
            differences, weight = _schema_differences(connection, inspector, metadata)
            if not differences:
                command.stamp(_config(connection), revision)
                connection.commit()
                return True
            if closest is None or weight < closest[2]:
                closest = (revision, differences, weight)

        if closest is None:
            raise RuntimeError("JASIL has no packaged migration head")
        raise SchemaCompatibilityError(
            f"Cannot adopt an incompatible JASIL schema (closest revision {closest[0]}): " + "; ".join(closest[1])
        )


def verify_schema_current(engine: "Engine") -> None:
    """Raise if the database is not migrated at least to the packaged head revision.

    A fail-fast startup check: call it after configuring the engine to catch a
    forgotten upgrade before the first query does. A revision this package does not
    ship can only have been recorded by a newer JASIL, whose revisions stay
    compatible with the previous release during a rolling deploy, so it passes with
    a warning rather than stopping the previous release from starting.

    Raises:
        RuntimeError: If no revision is recorded, or the recorded one precedes the packaged head.
    """
    _require_alembic()
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(_config())
    head = script.get_current_head()
    current = db_revision(engine)
    if current is not None and current != head and current not in {s.revision for s in script.walk_revisions()}:
        logger.warning(
            "JASIL database revision %r is newer than this release's head %r; running against it as a rolling "
            "deploy does",
            current,
            head,
        )
        return
    if current != head:
        raise RuntimeError(
            "JASIL database schema is out of date "
            f"(database revision={current!r}, expected head={head!r}). "
            "Run jasil.migrations.adopt_existing_schema(engine) for legacy unversioned tables, "
            "then jasil.migrations.upgrade(engine) at deploy time."
        )
