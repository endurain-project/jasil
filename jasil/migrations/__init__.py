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
    migrations.upgrade(engine)                  # create/upgrade JASIL's tables
    # migrations.stamp(engine)                  # existing DB already at head
    # migrations.verify_schema_current(engine)  # fail fast if not migrated

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
    "db_revision",
    "downgrade",
    "head_revision",
    "jasil_include_object",
    "stamp",
    "upgrade",
    "verify_schema_current",
]

#: Dedicated Alembic version table, kept separate from the host's
#: ``alembic_version`` so the two migration histories never collide in one DB.
VERSION_TABLE = "jasil_alembic_version"

_MIGRATIONS_DIR = Path(__file__).resolve().parent


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


def _run(engine: "Engine", command_name: str, *args: Any) -> None:
    _require_alembic()
    from alembic import command

    with engine.connect() as connection:
        config = _config(connection)
        getattr(command, command_name)(config, *args)


def upgrade(engine: "Engine", revision: str = "head") -> None:
    """Create or upgrade JASIL's tables to ``revision`` (default ``head``)."""
    _run(engine, "upgrade", revision)


def downgrade(engine: "Engine", revision: str) -> None:
    """Downgrade JASIL's tables to ``revision`` (``"base"`` drops them all)."""
    _run(engine, "downgrade", revision)


def stamp(engine: "Engine", revision: str = "head") -> None:
    """Record ``revision`` without running migrations.

    Use on an existing deployment whose JASIL tables were created with
    ``Base.metadata.create_all`` (or an older release): stamping marks it as
    being at head so future :func:`upgrade` calls apply only new revisions.
    """
    _run(engine, "stamp", revision)


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
            "Run jasil.migrations.upgrade(engine) at deploy time, or "
            "jasil.migrations.stamp(engine) if the tables already exist."
        )
