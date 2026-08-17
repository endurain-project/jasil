"""JASIL must never depend on the application that hosts it.

The library was extracted from Endurain, where it imported ``core.logger``,
``core.config``, ``core.database``, ``core.network`` and
``core.middleware_request_id``. Those couplings are gone; this module exists so
they cannot come back unnoticed — a re-introduced ``import core.x`` fails the
build rather than being discovered when a second host tries to install JASIL.
"""

import ast
import pathlib

import pytest

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent / "jasil"

# Top-level module names that belong to a *host application* rather than to
# JASIL or to a third-party package. Importing any of these makes the library
# unusable outside the application it was extracted from.
FORBIDDEN_ROOTS = frozenset({"core", "endurain"})


def _source_files() -> list[pathlib.Path]:
    return sorted(PACKAGE_ROOT.rglob("*.py"))


def _imported_roots(tree: ast.AST) -> set[str]:
    """Return the top-level package name of every import in ``tree``."""
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def test_the_package_has_source_files_to_scan():
    """Guard the guard: an empty scan would make every other test here vacuous."""
    assert len(_source_files()) > 10


@pytest.mark.parametrize("path", _source_files(), ids=lambda p: p.name)
def test_module_does_not_import_a_host_application(path: pathlib.Path):
    tree = ast.parse(path.read_text(), str(path))

    offending = _imported_roots(tree) & FORBIDDEN_ROOTS

    assert not offending, f"{path.relative_to(PACKAGE_ROOT.parent)} imports host module(s): {sorted(offending)}"


def test_no_source_file_mentions_the_host_logger_helper():
    """``core_logger.context(...)`` has no stdlib equivalent and must stay gone.

    An aliased import (``import core.logger as core_logger``) would be caught by
    the AST scan above, but a stray call left behind after a partial edit would
    only fail at runtime.
    """
    offenders = [path.name for path in _source_files() if "core_logger" in path.read_text()]

    assert not offenders, f"files still referencing the host logger: {offenders}"
