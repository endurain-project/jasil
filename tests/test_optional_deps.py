"""A core install must stay dependency-light.

``pyproject.toml`` promises that only ``sqlalchemy`` and ``pydantic`` are
required, with every backend client behind an extra (``redis``, ``s3``,
``geocoding``, ``jobs``, ``fastapi``). That promise is only real if importing
JASIL — or any of its pure modules — does not pull one of those clients in.

The checks run in a subprocess: the test session itself imports fakeredis and
friends, so ``sys.modules`` in-process cannot answer the question.
"""

import pathlib
import subprocess
import sys
import textwrap

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Distributions behind an optional extra. None may be loaded as a side effect of
# importing the package or any module on its pure surface.
OPTIONAL_DISTRIBUTIONS = ("redis", "boto3", "botocore", "fastapi", "requests", "apscheduler")

# Modules a host may import without opting into any extra.
PURE_MODULES = (
    "jasil",
    "jasil.capabilities",
    "jasil.correlation",
    "jasil.events",
    "jasil.event_versioning",
    "jasil.node",
    "jasil.profile",
    "jasil.providers",
    "jasil.pruning",
    "jasil.settings",
    "jasil._core.network",
    "jasil._core.registry",
)


def _run(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed interpreter, no shell, no user input
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )


@pytest.mark.parametrize("module", PURE_MODULES)
def test_pure_module_imports_without_loading_an_optional_dependency(module: str):
    result = _run(f"""
        import sys
        import {module}
        loaded = [name for name in {OPTIONAL_DISTRIBUTIONS!r} if name in sys.modules]
        print(",".join(loaded))
    """)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"importing {module} loaded optional dependencies: {result.stdout.strip()}"


def test_the_composition_root_imports_without_any_extra():
    """``jasil.container`` must be importable on a core install.

    It references every backend so it can select one, which makes it the module
    most likely to drag an optional client in — as it did until ``requests`` was
    made lazy inside the geocoding backend.
    """
    result = _run(f"""
        import sys
        import jasil.container
        loaded = [name for name in {OPTIONAL_DISTRIBUTIONS!r} if name in sys.modules]
        print(",".join(loaded))
    """)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "", f"importing the container loaded: {result.stdout.strip()}"


def test_redis_module_does_not_import_redis_until_an_attribute_is_used():
    """``jasil.redis`` is the sole owner of the redis client and loads it lazily.

    A ``local`` deployment imports this module transitively (the state and event
    backends reference it at their top level) but must never load ``redis``.
    """
    result = _run("""
        import sys
        import jasil.redis
        print("redis" in sys.modules)
    """)

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False"


def test_redis_module_rejects_an_unknown_attribute():
    """The lazy ``__getattr__`` must not turn a typo into an opaque import error."""
    import jasil.redis as platform_redis

    with pytest.raises(AttributeError, match="no attribute 'Nonsense'"):
        _ = platform_redis.Nonsense
