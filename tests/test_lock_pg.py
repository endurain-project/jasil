"""The PostgreSQL advisory-lock backend — the distributed profile's coordination lock.

Session-level advisory locks live on a *connection*, not in a table, so the whole
contract is about connection handling: hold one open for exactly as long as the
lock is held, release the lock on the way out, and never leak the connection —
including when the release itself fails, which would otherwise return a
still-locked backend session to the pool and deadlock every later acquirer.

A fake engine stands in for a server. Advisory locks are a PostgreSQL feature, so
there is no SQLite equivalent to run this against, and what needs pinning is the
sequence of calls rather than the server's behaviour. The CI database matrix
covers the real thing.
"""

import pytest

import jasil.orm as jasil_orm
from jasil.backends.lock_pg import PgAdvisoryLock, advisory_key
from jasil.providers import LockProvider


class FakeConnection:
    """Records the SQL it is asked to run, and whether it was closed or discarded."""

    def __init__(self, *, acquired: bool = True, unlock_error: Exception | None = None) -> None:
        self._acquired = acquired
        self._unlock_error = unlock_error
        self.statements: list[tuple[str, dict]] = []
        self.closed = False
        self.invalidated = False

    def execute(self, statement, parameters=None):
        text = str(statement)
        self.statements.append((text, parameters or {}))
        if "pg_advisory_unlock" in text and self._unlock_error is not None:
            raise self._unlock_error
        return FakeResult(self._acquired)

    def close(self):
        self.closed = True

    def invalidate(self):
        self.invalidated = True


class FakeResult:
    def __init__(self, value) -> None:
        self._value = value

    def scalar(self):
        return self._value


class FakeEngine:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection
        self.connect_calls = 0

    def connect(self) -> FakeConnection:
        self.connect_calls += 1
        return self._connection


def _statements(connection: FakeConnection) -> str:
    return " ".join(text for text, _ in connection.statements)


class TestAdvisoryKey:
    def test_the_same_name_always_yields_the_same_key(self):
        """Every replica has to derive the same key or they contend for nothing."""
        assert advisory_key("nightly-backfill") == advisory_key("nightly-backfill")

    def test_different_names_yield_different_keys(self):
        assert advisory_key("backfill") != advisory_key("prune")

    def test_the_key_fits_a_signed_bigint(self):
        """``pg_advisory_lock`` keys are ``bigint``; an out-of-range value errors."""
        for name in ("a", "nightly-backfill", "x" * 500, ""):
            assert -(2**63) <= advisory_key(name) < 2**63


class TestAcquisition:
    def test_it_satisfies_the_protocol(self):
        assert isinstance(PgAdvisoryLock(FakeEngine(FakeConnection())), LockProvider)

    def test_a_free_lock_is_acquired(self):
        connection = FakeConnection(acquired=True)

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill") as acquired:
            assert acquired is True

    def test_a_held_lock_yields_false_rather_than_waiting(self):
        """Non-blocking: a replica that loses the race skips the work."""
        connection = FakeConnection(acquired=False)

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill") as acquired:
            assert acquired is False

    def test_the_key_is_bound_not_interpolated(self):
        connection = FakeConnection()

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill"):
            pass

        text, parameters = connection.statements[0]
        assert "pg_try_advisory_lock" in text
        assert parameters == {"key": advisory_key("backfill")}

    def test_a_ttl_is_ignored(self):
        """A session advisory lock lives until unlock or disconnect; there is no TTL."""
        connection = FakeConnection()

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill", ttl_seconds=30) as acquired:
            assert acquired is True

    def test_one_connection_is_held_for_the_lock(self):
        engine = FakeEngine(FakeConnection())

        with PgAdvisoryLock(engine).try_acquire("backfill"):
            pass

        assert engine.connect_calls == 1


class TestRelease:
    def test_an_acquired_lock_is_released(self):
        connection = FakeConnection(acquired=True)

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill"):
            pass

        assert "pg_advisory_unlock" in _statements(connection)

    def test_a_lock_that_was_never_acquired_is_not_released(self):
        """Unlocking a lock held by another session would be a no-op at best."""
        connection = FakeConnection(acquired=False)

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill"):
            pass

        assert "pg_advisory_unlock" not in _statements(connection)

    def test_the_connection_is_closed_either_way(self):
        for acquired in (True, False):
            connection = FakeConnection(acquired=acquired)

            with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill"):
                pass

            assert connection.closed is True

    def test_a_failing_body_still_releases_the_lock(self):
        """Otherwise one exception in the guarded work locks the name out forever."""
        connection = FakeConnection(acquired=True)
        lock = PgAdvisoryLock(FakeEngine(connection))

        with pytest.raises(RuntimeError, match="work failed"), lock.try_acquire("backfill"):
            raise RuntimeError("work failed")

        assert "pg_advisory_unlock" in _statements(connection)
        assert connection.closed is True


class TestReleaseFailure:
    """A broken unlock must not return a still-locked session to the pool."""

    def test_the_failure_is_logged_not_raised(self, caplog):
        connection = FakeConnection(unlock_error=RuntimeError("connection reset"))

        with caplog.at_level("ERROR"), PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill"):
            pass

        assert "Failed to release advisory lock" in caplog.text

    def test_the_connection_is_discarded_rather_than_reused(self):
        """Invalidating drops the backend session, which drops the lock with it."""
        connection = FakeConnection(unlock_error=RuntimeError("connection reset"))

        with PgAdvisoryLock(FakeEngine(connection)).try_acquire("backfill"):
            pass

        assert connection.invalidated is True
        assert connection.closed is True

    def test_it_does_not_mask_the_body_exception(self, caplog):
        connection = FakeConnection(unlock_error=RuntimeError("connection reset"))
        lock = PgAdvisoryLock(FakeEngine(connection))

        with (
            caplog.at_level("ERROR"),
            pytest.raises(ValueError, match="the real problem"),
            lock.try_acquire("backfill"),
        ):
            raise ValueError("the real problem")


class TestFromMainDatabase:
    def test_it_borrows_the_engine_behind_the_session_factory(self, monkeypatch):
        """JASIL never creates an engine; the lock needs a raw connection from the host's."""
        engine = FakeEngine(FakeConnection())
        monkeypatch.setattr(jasil_orm, "get_engine", lambda: engine)

        assert PgAdvisoryLock.from_main_database()._engine is engine

    def test_an_unconfigured_session_factory_surfaces(self, monkeypatch):
        def _explode():
            raise RuntimeError("JASIL has no session factory")

        monkeypatch.setattr(jasil_orm, "get_engine", _explode)

        with pytest.raises(RuntimeError, match="no session factory"):
            PgAdvisoryLock.from_main_database()
