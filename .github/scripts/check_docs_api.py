"""Fail the build when the documentation names something the package does not have.

Every ``import jasil...`` in ``README.md`` and ``docs/*.md`` is a promise a reader
will copy and run. A rename that misses the prose leaves a snippet that raises
``AttributeError`` on first contact, and nothing else in CI notices: the docs
build only checks that mkdocs can render the page, and the test suite never reads
it. This closes that gap by resolving every documented symbol against the real
package.

It checks two things per code block:

* the module in an ``import jasil.x`` / ``from jasil.x import ...`` line exists
  and imports cleanly, and
* every name imported from it, and every attribute reached through its alias,
  is really defined on it.

Run it from the repository root::

    python .github/scripts/check_docs_api.py
"""

import importlib
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

# ``import jasil.orm as jasil_orm``
_IMPORT_AS = re.compile(r"^\s*import\s+(jasil[\w.]*)\s+as\s+(\w+)\s*$", re.MULTILINE)
# ``from jasil.publisher import publish, publish_committing`` (parenthesised too)
_FROM_IMPORT = re.compile(r"^\s*from\s+(jasil[\w.]*)\s+import\s+(\(?[^\n)]+\)?)", re.MULTILINE)


def _sources() -> list[pathlib.Path]:
    return [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").glob("*.md"))]


def _imported_names(spec: str) -> list[str]:
    """Split the name list of a ``from ... import ...`` line, dropping any alias."""
    return [name.split(" as ")[0].strip() for name in spec.strip("()").split(",") if name.strip()]


def _resolve(module_name: str, failures: list[str], where: str) -> object | None:
    """Import ``module_name``, recording a failure instead of raising."""
    try:
        return importlib.import_module(module_name)
    except Exception as error:
        # Any import failure is a documentation failure: the reader cannot run it.
        failures.append(f"{where}: cannot import {module_name} ({type(error).__name__}: {error})")
        return None


def _check(path: pathlib.Path, failures: list[str]) -> int:
    text = path.read_text(encoding="utf-8")
    where = path.relative_to(REPO_ROOT).as_posix()
    checked = 0

    for module_name, names in _FROM_IMPORT.findall(text):
        module = _resolve(module_name, failures, where)
        if module is None:
            continue
        for name in _imported_names(names):
            checked += 1
            # A submodule is a legitimate target of ``from package import name``
            # and is not an attribute until it has been imported.
            if not hasattr(module, name) and _resolve(f"{module_name}.{name}", [], where) is None:
                failures.append(f"{where}: {module_name}.{name} does not exist")

    for module_name, alias in _IMPORT_AS.findall(text):
        module = _resolve(module_name, failures, where)
        if module is None:
            continue
        for attribute in sorted(set(re.findall(rf"\b{re.escape(alias)}\.(\w+)", text))):
            checked += 1
            if not hasattr(module, attribute):
                failures.append(
                    f"{where}: {module_name}.{attribute} does not exist (documented as {alias}.{attribute})"
                )

    return checked


def main() -> int:
    # The CRUD modules bind to the declarative base at import time, so the
    # mapping has to exist before anything documented can resolve.
    import jasil.orm as jasil_orm

    jasil_orm.map_models()

    failures: list[str] = []
    checked = sum(_check(path, failures) for path in _sources())

    if failures:
        print(f"Documentation references {len(failures)} symbol(s) that do not exist:\n")
        for failure in failures:
            print(f"  {failure}")
        print("\nUpdate the documentation, or restore the symbol it names.")
        return 1

    print(f"OK: all {checked} documented jasil references resolve.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
