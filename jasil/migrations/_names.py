"""Naming of JASIL's tables, indexes and constraints while a migration runs.

Up to 0.8 the tables had no prefix (``event_log``); from 0.9 every one starts
with ``jasil_`` so none can collide with a host's own table. ``rev0001`` and
``rev0002`` create and alter tables, and they cannot simply be rewritten: a
database stamped at either holds the unprefixed names, and ``rev0003`` renames it
from there. But on a **fresh** database those two revisions would first create
the unprefixed tables and so collide with a host's same-named table before
``rev0003`` could rename anything.

So the prefix they use is chosen by the runner. :func:`use_prefix` is set to
``jasil_`` for a fresh install heading to ``rev0003`` or later, which then never
sees an unprefixed table (``rev0003`` renames only what it finds). Everywhere
else - an existing database, an explicit older target, a downgrade, the replay
that derives an older revision's schema - it is empty, which is what those
revisions always meant.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

__all__ = ["PREFIX", "index", "table", "use_prefix"]

#: The prefix every JASIL table name has from ``rev0003`` on.
PREFIX = "jasil_"

_prefix: ContextVar[str] = ContextVar("jasil_migration_table_prefix", default="")


@contextmanager
def use_prefix(prefix: str) -> Iterator[None]:
    """Make the revisions that follow the prefix convention use ``prefix``."""
    token = _prefix.set(prefix)
    try:
        yield
    finally:
        _prefix.reset(token)


def table(name: str) -> str:
    """Return the table ``name`` as the running migration spells it."""
    return f"{_prefix.get()}{name}"


def index(name: str) -> str:
    """Return an ``idx_...`` / ``uq_...`` name as the running migration spells it.

    The prefix goes after the marker, so ``idx_event_log_created`` becomes
    ``idx_jasil_event_log_created``.
    """
    marker, _, rest = name.partition("_")
    return f"{marker}_{_prefix.get()}{rest}"
