"""JASIL's SQLAlchemy registry plumbing and session-factory helpers.

**Option B (the host owns the ``Base``).** JASIL's companion tables —
``event_log``, ``processing_jobs``, ``event_outbox`` — must live in the same
declarative registry as the host's own models so that one ``create_all`` /
migration run and one metadata object cover the whole schema. The host owns that
registry; JASIL maps its models **into** it.

Host applications:

1. Own a declarative base::

       from sqlalchemy.orm import DeclarativeBase

       class Base(DeclarativeBase):
           ...  # the host's own base (naming conventions, schema, ...)

2. Map JASIL's tables into that base's registry, once, at startup::

       import jasil.orm as jasil_orm
       jasil_orm.map_models(Base)

   This must happen **before any JASIL model module is imported**. The model
   modules bind their classes to the active base at import time, so importing one
   first is a configuration error and raises. A host that would rather not own a
   base may call ``map_models()`` with no argument and use JASIL's convenience
   :data:`Base`.

   Two modules reach a model directly and therefore inherit that ordering
   constraint: :mod:`jasil.jobs.crud` and :mod:`jasil.event_log.crud`. Import
   them inside a function, or only after ``map_models`` has run. Every other
   public entry point — :mod:`jasil.publisher`, :mod:`jasil.retention`,
   :mod:`jasil.jobs.service`, :mod:`jasil.container`, :mod:`jasil.deps`,
   :mod:`jasil.testing` — defers its model imports and is safe to import from
   anywhere, at any point in the host's import graph.

3. Register a session factory bound to their own engine::

       from sqlalchemy import create_engine
       from sqlalchemy.orm import sessionmaker

       engine = create_engine(...)
       jasil_orm.configure_sessionmaker(sessionmaker(bind=engine))

   An async host registers an ``async_sessionmaker`` instead — or as well::

       from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

       engine = create_async_engine("postgresql+psycopg://...")
       jasil_orm.configure_async_sessionmaker(async_sessionmaker(bind=engine))

   The two slots are independent, and the same mapped models serve both: an
   ``AsyncSession`` and a ``Session`` are two ways of driving one registry, which
   is why the async face needed no second set of models. Configure whichever the
   process actually uses — or both, when one process runs an async API beside a
   synchronous worker.

JASIL never creates the engine; the host owns the connection.
"""

import importlib
from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from jasil._core.registry import ConfigSlot

# Only for the annotations. ``sqlalchemy.ext.asyncio`` needs greenlet, which
# SQLAlchemy ships separately (the ``async`` extra), so a synchronous deployment
# must be able to import this module without it.
if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

__all__ = [
    "Base",
    "configure_async_sessionmaker",
    "configure_sessionmaker",
    "get_active_base",
    "get_async_engine",
    "get_async_sessionmaker",
    "get_engine",
    "get_sessionmaker",
    "is_async_sessionmaker_configured",
    "is_models_mapped",
    "is_sessionmaker_configured",
    "jasil_table_names",
    "map_models",
    "mapper_registry",
    "reset",
]


class Base(DeclarativeBase):
    """JASIL's convenience declarative base.

    Under Option B the **host** owns the registry: define your own
    :class:`~sqlalchemy.orm.DeclarativeBase` and pass it to :func:`map_models`.
    Use this default only if you would rather not own one.
    """


#: The registry behind :data:`Base`. Exposed so a host may build its own base on
#: the *same* registry (``class Base(DeclarativeBase): registry = mapper_registry``)
#: instead of passing a base to :func:`map_models`.
mapper_registry = Base.registry

# The active declarative base JASIL's models are mapped onto; ``None`` until
# ``map_models`` runs.
_active_base: type[DeclarativeBase] | None = None

# Every module defining a JASIL companion model. ``map_models`` imports each, and
# each binds its classes to the active base via ``get_active_base()``.
_MODEL_MODULES: tuple[str, ...] = (
    "jasil.event_log.models",
    "jasil.jobs.models",
)

#: Tables JASIL owns. The migrations scope every operation to these, so a host
#: sharing the registry never has its own tables added, dropped, or diffed.
_TABLE_NAMES: frozenset[str] = frozenset({"event_log", "processing_jobs", "event_outbox"})


def jasil_table_names() -> frozenset[str]:
    """Return the names of the tables JASIL owns."""
    return _TABLE_NAMES


def get_active_base() -> type[DeclarativeBase]:
    """Return the declarative base JASIL's models are mapped onto.

    Model modules call this at import time to obtain their base, so importing a
    model module before :func:`map_models` is a configuration error.

    Raises:
        RuntimeError: If :func:`map_models` has not been called yet.
    """
    if _active_base is None:
        raise RuntimeError(
            "JASIL's models are not mapped yet. Call jasil.orm.map_models(YourBase) once at startup, "
            "before importing any JASIL model module (omit the base to use jasil.orm.Base). "
            "You are seeing this because something imported jasil.jobs.crud, jasil.event_log.crud, "
            "or a model module directly at import time — move that import inside a function, or map "
            "first. jasil.publisher, jasil.retention, jasil.jobs.service, jasil.container and "
            "jasil.deps are always safe to import at module scope."
        )
    return _active_base


def is_models_mapped() -> bool:
    """Return whether :func:`map_models` has been called."""
    return _active_base is not None


def map_models(base: type[DeclarativeBase] | None = None) -> None:
    """Define and map JASIL's companion tables into ``base``'s registry.

    Call once at startup, before any database use. Calling it again with the same
    base is a no-op, so a host with several entry points need not coordinate.

    Args:
        base: The host's :class:`~sqlalchemy.orm.DeclarativeBase` subclass. Omit
            it to use JASIL's own :data:`Base`.

    Raises:
        RuntimeError: If called again with a different base.
    """
    global _active_base
    target = base if base is not None else Base
    if _active_base is not None:
        if _active_base is not target:
            raise RuntimeError("jasil.orm.map_models() was already called with a different base; call it once.")
        return
    _active_base = target
    try:
        for module_name in _MODEL_MODULES:
            importlib.import_module(module_name)
        # Resolve every mapper now so a misconfiguration fails fast at startup
        # rather than on the first query.
        target.registry.configure()
    except Exception:
        _active_base = None  # let the host fix the problem and retry
        raise


_session_factory: ConfigSlot[sessionmaker[Session]] = ConfigSlot(
    missing_message=(
        "JASIL has no *synchronous* session factory. Call "
        "jasil.orm.configure_sessionmaker(sessionmaker(bind=engine)) at startup. "
        "If this process is async, you are on a synchronous code path (the sync platform, recorder, "
        "job runner or retention prune) — use its async counterpart, or configure both factories."
    )
)

_async_session_factory: ConfigSlot["async_sessionmaker[AsyncSession]"] = ConfigSlot(
    missing_message=(
        "JASIL has no *async* session factory. Call "
        "jasil.orm.configure_async_sessionmaker(async_sessionmaker(bind=engine)) at startup, with an "
        "engine built by sqlalchemy.ext.asyncio.create_async_engine. "
        "If this process is synchronous, you are on an async code path — use its synchronous "
        "counterpart, or configure both factories."
    )
)


def configure_sessionmaker(factory: sessionmaker[Session]) -> None:
    """Install the host's synchronous session factory.

    Call once at startup with a ``sessionmaker`` bound to the application's
    engine. The event-log recorder, the job runner, the relay, and the retention
    sweeps all obtain their sessions from it.

    Independent of :func:`configure_async_sessionmaker`: configure this one for a
    synchronous deployment, the async one for an async deployment, or both when a
    single process runs each.

    Args:
        factory: A configured ``sessionmaker``.
    """
    _session_factory.configure(factory)


def configure_async_sessionmaker(factory: "async_sessionmaker[AsyncSession]") -> None:
    """Install the host's async session factory.

    The counterpart of :func:`configure_sessionmaker` for a host built on
    ``AsyncSession``. The async event-log recorder, the async job runner, the
    async relay, and the async retention sweep obtain their sessions from it.

    The factory must be bound to an engine from
    :func:`sqlalchemy.ext.asyncio.create_async_engine`, using an async driver
    (``postgresql+psycopg``, ``postgresql+asyncpg``, ``sqlite+aiosqlite``, …).
    JASIL's models need no change to work through it: an ``AsyncSession`` drives
    the same declarative registry :func:`map_models` populated.

    Args:
        factory: A configured ``async_sessionmaker``.
    """
    _async_session_factory.configure(factory)


def get_sessionmaker() -> sessionmaker[Session]:
    """Return the installed synchronous session factory.

    Raises:
        RuntimeError: If :func:`configure_sessionmaker` has not been called.
    """
    return _session_factory.get()


def get_async_sessionmaker() -> "async_sessionmaker[AsyncSession]":
    """Return the installed async session factory.

    Raises:
        RuntimeError: If :func:`configure_async_sessionmaker` has not been called.
    """
    return _async_session_factory.get()


def is_sessionmaker_configured() -> bool:
    """Return whether :func:`configure_sessionmaker` has been called."""
    return _session_factory.is_configured()


def is_async_sessionmaker_configured() -> bool:
    """Return whether :func:`configure_async_sessionmaker` has been called."""
    return _async_session_factory.is_configured()


def get_engine() -> Any:
    """Return the engine the synchronous session factory is bound to.

    Needed by the Postgres advisory-lock backend, which holds a dedicated
    connection for the lifetime of a lock and so cannot work through a session.

    Raises:
        RuntimeError: If :func:`configure_sessionmaker` has not been called, or
            its factory is not bound to an engine.
    """
    bind = get_sessionmaker().kw.get("bind")
    if bind is None:
        raise RuntimeError(
            "JASIL's session factory is not bound to an engine. Pass bind= to "
            "sessionmaker(...) so capabilities needing a raw connection (the "
            "postgres-advisory lock) can reach it."
        )
    return bind


def get_async_engine() -> Any:
    """Return the ``AsyncEngine`` the async session factory is bound to.

    Needed by the async Postgres advisory-lock backend for the same reason as
    :func:`get_engine`: a session-level advisory lock lives on one connection for
    the lock's whole lifetime, which no session-scoped API can express. The async
    backend calls ``AsyncEngine.connect()`` on what this returns.

    Raises:
        RuntimeError: If :func:`configure_async_sessionmaker` has not been called,
            or its factory is not bound to an engine.
    """
    bind = get_async_sessionmaker().kw.get("bind")
    if bind is None:
        raise RuntimeError(
            "JASIL's async session factory is not bound to an engine. Pass bind= to "
            "async_sessionmaker(...) so capabilities needing a raw connection (the "
            "postgres-advisory lock) can reach it."
        )
    return bind


def reset() -> None:
    """Clear the mapped base and both session factories.

    For tests that need a clean process-wide state between cases; production code
    configures once at startup and never resets.
    """
    global _active_base
    _active_base = None
    _session_factory.reset()
    _async_session_factory.reset()
