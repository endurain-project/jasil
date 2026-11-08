"""A host session factory that routes engines with ``binds=`` rather than one ``bind=``.

A host with several databases maps JASIL into the base of one of them and routes each base to its own engine.
Such a session has no single ``bind``, so every statement JASIL runs, and every dialect check it makes, has to go
through those rules. This module overrides the suite's ``session_factory``: JASIL's base and another base are
each routed to an engine of their own, and there is no default.
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

import jasil._core.pruning as pruning
import jasil.audit.maintenance as audit_maintenance
import jasil.jobs.crud as jobs_crud
import jasil.jobs.outbox as jobs_outbox
import jasil.jobs.relay as jobs_relay
import jasil.orm as orm
from jasil.events import new_event
from jasil.jobs.registry import JobHandlerRegistry

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class _OtherBase(DeclarativeBase):
    """The base of the host's other database."""


class _Clock:
    def now(self) -> datetime:
        return T0


@pytest.fixture
def other_engine():
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(db_engine, mapped_base, other_engine):
    """JASIL's base routed to the test database and another base elsewhere, with no default engine."""
    factory = sessionmaker(binds={mapped_base: db_engine, _OtherBase: other_engine}, expire_on_commit=False)
    orm.configure_sessionmaker(factory)
    return factory


def test_the_engine_is_the_one_jasils_base_is_routed_to(session_factory, db_engine):
    assert orm.get_engine() is db_engine


def test_a_durable_event_is_staged_relayed_claimed_and_pruned(db, session_factory, db_engine, monkeypatch):
    """The locking checks are given the engine the rules route JASIL's tables to, not a missing ``bind``."""
    checked = []

    def recording(check):
        def record(bind):
            checked.append(bind)
            return check(bind)

        return record

    for module in (jobs_crud, jobs_outbox, pruning):
        monkeypatch.setattr(module, "supports_skip_locked", recording(module.supports_skip_locked))
    monkeypatch.setattr(jobs_outbox, "needs_locking_read", recording(jobs_outbox.needs_locking_read))
    registry = JobHandlerRegistry()
    registry.register("activity.created", "s", lambda _event: None)
    event = new_event("activity.created", {"activity_id": 7}, source="test")

    assert jobs_outbox.add_to_outbox(event, now=T0, db=db) is not None
    assert jobs_outbox.add_to_outbox(event, now=T0, db=db) is None
    relayed = jobs_relay.relay_outbox_once(
        registry=registry, clock=_Clock(), session_factory=session_factory, max_attempts=3, batch_size=10
    )
    claimed = jobs_crud.claim_jobs(worker_id="w", limit=10, lease_seconds=60, now=T0, db=db)
    reclaimed = jobs_crud.reclaim_expired_leases(now=T0 + timedelta(minutes=5), db=db)
    pruned = jobs_outbox.delete_relayed_before(T0 + timedelta(days=1), db=db)

    assert (relayed, len(claimed), reclaimed, pruned) == (1, 1, 1, 1)
    assert checked
    assert all(bind is db_engine for bind in checked)


def test_audit_maintenance_runs_every_step(audit_env, session_factory, caplog):
    with caplog.at_level(logging.WARNING):
        audit_maintenance.run_audit_maintenance()

    assert "failed" not in caplog.text
