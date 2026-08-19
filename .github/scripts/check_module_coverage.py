"""Fail the build when any single module falls below the per-module coverage floor.

``fail_under`` is an average, and an average hides its worst case: this package
once reported 90% overall while the Redis event bus sat at 48%, the geocoding
backend at 43% and the Postgres advisory lock at 52%. Those are the modules that
only run in the ``distributed`` profile — the code least exercised in development
and most exercised in production — and the aggregate said nothing about them.

This enforces a floor on every module instead, so a well-covered majority can no
longer carry an untested one.

Run it after the suite, from the repository root::

    python -m pytest
    python .github/scripts/check_module_coverage.py
"""

import json
import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
COVERAGE_JSON = REPO_ROOT / "coverage.json"

#: No module may fall below this.
FLOOR = 80.0

#: Modules held to a lower floor, each for a reason that is not "we did not get
#: to it". Keep this list short, and justify every entry.
EXEMPTIONS = {
    # Alembic *executes* env.py through its own runner rather than importing it,
    # so the online/offline branches never run under pytest. The revisions it
    # drives are covered by tests/test_migrations.py, which asserts the migrated
    # schema matches the models column-for-column.
    "jasil/migrations/env.py": 60.0,
}


def _coverage_data() -> dict:
    """Return a freshly generated report of the last recorded run.

    Regenerated every time rather than reused: a stale ``coverage.json`` left by
    an earlier run would report on code that no longer exists.
    """
    subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, "-m", "coverage", "json", "-o", str(COVERAGE_JSON), "--quiet"],
        cwd=REPO_ROOT,
        check=True,
    )
    return json.loads(COVERAGE_JSON.read_text(encoding="utf-8"))


def main() -> int:
    files = _coverage_data()["files"]
    if not files:
        print("No coverage data found. Run the test suite first.")
        return 1

    failures = []
    for path, report in sorted(files.items()):
        normalised = pathlib.Path(path).as_posix()
        floor = EXEMPTIONS.get(normalised, FLOOR)
        percent = report["summary"]["percent_covered"]
        if percent < floor:
            failures.append((normalised, percent, floor))

    if failures:
        print(f"{len(failures)} module(s) below the per-module coverage floor:\n")
        width = max(len(path) for path, _, _ in failures)
        for path, percent, floor in failures:
            print(f"  {path.ljust(width)}  {percent:5.1f}%  (floor {floor:.0f}%)")
        print("\nAdd tests, or justify an exemption in this script.")
        return 1

    lowest, report = min(files.items(), key=lambda item: item[1]["summary"]["percent_covered"])
    print(
        f"OK: all {len(files)} modules are at or above their floor "
        f"(lowest: {pathlib.Path(lowest).as_posix()} at {report['summary']['percent_covered']:.1f}%)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
