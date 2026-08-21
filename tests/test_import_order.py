"""Every host-facing module must import before ``map_models`` has run.

JASIL's model modules bind their classes to the host's declarative base at import
time, so anything that reaches one inherits an ordering constraint: it cannot be
imported until ``jasil.orm.map_models`` has run. That is fine for the CRUD layer,
which a host imports deliberately — but it is a trap for :mod:`jasil.publisher`,
which every producer imports at module scope, far below the entry point where
``map_models`` is called. Python executes those imports long before any startup
hook, so a top-level model import there makes the library unusable in a normal
import graph.

These checks run in a subprocess: the test session calls ``map_models()`` at
conftest import time (it has to — the test modules import models at module
scope), so nothing in-process can answer the question.
"""

import pathlib
import subprocess
import sys
import textwrap

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Public entry points a host imports from anywhere in its own import graph, which
# means they must not touch a model until they are called.
ALWAYS_IMPORTABLE = (
    "jasil",
    "jasil.admin",
    "jasil.container",
    "jasil.deps",
    "jasil.jobs.registry",
    "jasil.jobs.service",
    "jasil.lifecycle",
    "jasil.publisher",
    "jasil.retention",
    "jasil.runtime",
    "jasil.subscribers",
    "jasil.testing",
)

# The database layer proper. A host importing one of these is already in ORM
# territory, so the ordering constraint stands — it just has to be legible.
NEEDS_MAPPING_FIRST = (
    "jasil.event_log.crud",
    "jasil.event_log.models",
    "jasil.jobs.crud",
    "jasil.jobs.models",
)


def _run(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed interpreter, no shell, no user input
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )


@pytest.mark.parametrize("module", ALWAYS_IMPORTABLE)
def test_the_entry_point_imports_before_the_models_are_mapped(module: str):
    result = _run(f"import {module}")

    assert result.returncode == 0, f"importing {module} before map_models() failed:\n{result.stderr}"


def test_a_producer_can_import_the_publish_seam_at_module_scope():
    """The case that motivates all of this: ``from jasil.publisher import publish``.

    A domain module writes exactly this at its top, and is imported by a router
    that is imported by the application entry point — so it runs before any
    ``map_models`` call the host makes in that entry point.
    """
    result = _run("""
        from jasil.publisher import publish, publish_committing, publish_many_committing

        import jasil.orm as jasil_orm
        assert not jasil_orm.is_models_mapped()
    """)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("module", NEEDS_MAPPING_FIRST)
def test_the_database_layer_says_what_to_do_about_it(module: str):
    """The error has to name the fix; an unmapped-base traceback explains nothing."""
    result = _run(f"import {module}")
    # The module is identified by the file the traceback points at. Asserting on
    # the dotted name instead would pass only on the Python versions that echo
    # the ``-c`` source line (3.13+), which is not what is being tested.
    offending_file = pathlib.Path(*module.split(".")).with_suffix(".py")

    assert result.returncode != 0
    assert "jasil.orm.map_models" in result.stderr
    assert str(offending_file) in result.stderr


@pytest.mark.parametrize("module", NEEDS_MAPPING_FIRST)
def test_the_database_layer_imports_once_the_models_are_mapped(module: str):
    result = _run(f"""
        import jasil.orm as jasil_orm
        jasil_orm.map_models()
        import {module}
    """)

    assert result.returncode == 0, result.stderr
