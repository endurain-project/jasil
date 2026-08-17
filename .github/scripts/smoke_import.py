"""Smoke-test a JASIL install: bare import, model mapping, platform assembly.

Run against an environment that has **only** the base runtime dependencies
(``sqlalchemy`` + ``pydantic``; no ``redis`` / ``s3`` / ``geocoding`` / ``jobs``
/ ``fastapi`` extras, no dev groups). It proves four things a packaging mistake
would break:

1. ``import jasil`` works without any optional dependency.
2. The distribution carries its version metadata (i.e. it was installed, not
   merely found on ``sys.path``).
3. The composition root assembles a platform, which transitively imports every
   backend module, model, and schema in the package — and must do so without
   loading an optional client.
4. The packaged Alembic revisions ship in the wheel (data files are the classic
   thing a build config silently drops).

Must be run from **outside** the repository root, otherwise the local ``jasil/``
package directory shadows the installed distribution and this would silently
validate the working tree instead of the built artifact::

    cd /tmp && python /path/to/repo/.github/scripts/smoke_import.py
"""

import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

import jasil
import jasil.orm as jasil_orm
import jasil.settings as jasil_settings
from jasil.container import build_platform

# Distributions behind an optional extra. A core install must load none of them,
# even after the composition root has assembled a full platform.
OPTIONAL_DISTRIBUTIONS = ("redis", "boto3", "botocore", "fastapi", "requests", "apscheduler")

# The tables JASIL maps into the host's registry.
EXPECTED_TABLES = {"event_log", "processing_jobs", "event_outbox"}


class Base(DeclarativeBase):
    """Host-owned declarative base (JASIL maps its tables into this registry)."""


def main() -> int:
    """Run the smoke checks, returning a process exit code."""
    if (Path.cwd() / "jasil" / "__init__.py").exists():
        print("FAIL: run this from outside the repository root, or the source tree shadows the install")
        return 1

    if not jasil.__version__ or jasil.__version__.endswith("+unknown"):
        print(f"FAIL: version metadata missing from the installed distribution: {jasil.__version__!r}")
        return 1

    if "site-packages" not in jasil.__file__:
        print(f"FAIL: imported from the source tree, not an install: {jasil.__file__}")
        return 1

    jasil_orm.map_models(Base)
    mapped = set(Base.metadata.tables) & EXPECTED_TABLES
    if mapped != EXPECTED_TABLES:
        print(f"FAIL: expected {sorted(EXPECTED_TABLES)} mapped, got {sorted(mapped)}")
        return 1

    engine = create_engine("sqlite://")
    jasil_orm.configure_sessionmaker(sessionmaker(bind=engine))
    Base.metadata.create_all(engine)

    jasil_settings.configure(jasil_settings.JasilSettings())
    platform = build_platform()

    missing = [
        name
        for name in ("state", "storage", "events", "lock", "clock", "geocoding")
        if getattr(platform, name, None) is None
    ]
    if missing:
        print(f"FAIL: platform is missing capabilities: {missing}")
        return 1

    # The packaged revisions are data files, not modules, so a build-config
    # mistake drops them without breaking any import.
    revisions = Path(jasil.__file__).parent / "migrations" / "versions"
    if not list(revisions.glob("rev*.py")):
        print(f"FAIL: packaged Alembic revisions missing from the wheel: {revisions}")
        return 1

    leaked = [name for name in OPTIONAL_DISTRIBUTIONS if name in sys.modules]
    if leaked:
        print(f"FAIL: a core install loaded optional dependencies: {leaked}")
        return 1

    print(
        f"smoke OK: jasil {jasil.__version__}, "
        f"{len(EXPECTED_TABLES)} tables mapped, platform assembled on the "
        f"{platform.profile.value!r} profile, from {jasil.__file__}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
