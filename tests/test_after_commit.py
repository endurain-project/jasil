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
        "timestamp": now,
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
