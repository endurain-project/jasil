"""The session-bound after-commit hook behind ``publish_on_commit``."""

import pytest
from sqlalchemy.exc import IntegrityError

from jasil._core.after_commit import run_after_commit


def test_a_raising_callback_is_logged_and_does_not_stop_the_rest(db, caplog):
    ran = []

    def _boom():
        raise RuntimeError("callback down")

    run_after_commit(db, _boom)
    run_after_commit(db, lambda: ran.append("second"))

    with caplog.at_level("ERROR"):
        db.commit()

    assert ran == ["second"]
    assert "After-commit callback failed" in caplog.text


def test_a_failed_flush_discards_the_callbacks(db):
    from datetime import UTC, datetime

    from jasil.jobs.models import EventOutbox

    now = datetime(2026, 1, 1, tzinfo=UTC)
    row = {
        "id": "x",
        "event_id": "x",
        "event_type": "t",
        "source": "s",
        "timestamp": now.isoformat(),
        "payload": {},
        "schema_version": 1,
        "created_at": now,
    }
    ran = []
    db.add(EventOutbox(**row))
    db.commit()
    db.add(EventOutbox(**row))  # duplicate primary key: IntegrityError on every dialect
    run_after_commit(db, lambda: ran.append(1))

    with pytest.raises(IntegrityError):
        db.commit()
    db.rollback()
    db.commit()

    assert ran == []


def test_a_failed_listener_attach_leaves_the_session_unmarked(db, monkeypatch):
    import jasil._core.after_commit as after_commit

    def _refuse(*_args, **_kwargs):
        raise RuntimeError("no listeners")

    monkeypatch.setattr(after_commit.event, "listen", _refuse)
    with pytest.raises(RuntimeError):
        run_after_commit(db, lambda: None)

    assert "jasil.after_commit.pending" not in db.info


def test_a_listener_session_without_pending_work_commits(db):
    # A session can carry the listeners without the pending list, e.g. when
    # they were attached at sessionmaker level; its commit must not fail.
    from sqlalchemy import event

    import jasil._core.after_commit as after_commit

    event.listen(db, "after_commit", after_commit._on_commit)
    event.listen(db, "after_transaction_end", after_commit._on_transaction_end)
    db.begin()

    db.commit()


def test_another_raising_after_commit_listener_does_not_drop_committed_callbacks(db):
    from sqlalchemy import event

    def _boom(_session):
        raise RuntimeError("other listener down")

    # Registered first, so it would run (and raise) before ours without insert=True.
    event.listen(db, "after_commit", _boom)
    ran = []
    run_after_commit(db, lambda: ran.append(1))

    with pytest.raises(RuntimeError, match="other listener down"):
        db.commit()
    db.close()

    assert ran == [1]
