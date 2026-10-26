# Testing

- Tests live in `tests/`, files `test_*.py`, grouped in `Test*` classes; shared fixtures in `tests/conftest.py`.
- Tests are hermetic: no real network, no real Redis (`fakeredis`), no on-disk database (in-memory SQLite), DNS monkeypatched, time injected (`FixedClock`). Nothing sleeps.
- Pytest runs with `asyncio_mode = "auto"` and `--strict-markers`; a new marker is declared in `pyproject.toml`.
- Cover empty/None, missing entity, errors and malformed input, not only the happy path.
- `with pytest.raises(...) as exc_info` for exceptions.
- Async tests: `async def test_*`.
- Skips need `reason=`.
- Coverage: `fail_under` in `pyproject.toml` is a ratchet; raise it when coverage rises and never lower it. `check_module_coverage.py` also enforces an 80% floor per module (exemptions are listed and justified in the script), so an untested backend cannot hide behind the average.
- Do not mock the session to check a query or a commit; use a real database.

## Real-database tests

| Question | Use |
| --- | --- |
| Does this query, migration or constraint behave? | the default in-memory SQLite |
| Does it hold on PostgreSQL or MySQL (advisory locks, `SKIP LOCKED`, migrations)? | set `JASIL_TEST_DATABASE_URL` to a disposable database; the `*_postgres` tests skip without a real PostgreSQL |

## Architecture tests
- `tests/test_host_agnostic.py` fails on a host-application import; `tests/test_optional_deps.py` and `tests/test_import_order.py` protect lazy backends.
- A new boundary rule that names a module is added as an import-linter contract, with a test that the module still exists.

```bash
uv run pytest tests/test_jobs.py -v
uv run pytest --cov=jasil --cov-report=term-missing
```
