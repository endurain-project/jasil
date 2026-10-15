"""Keeping a running job's lease alive: the lease keeper and the worker heartbeat.

A lease means "this worker may go this long without a sign of life", not "a
handler may run this long". These tests drive the keeper with an injected clock
and call the beat directly rather than sleeping; the one threaded test uses a
tiny interval to prove the real loop renews while a handler is blocked.
"""

import contextlib
import logging
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

import jasil.container as container
import jasil.jobs._worker_registry as worker_registry
import jasil.jobs.crud as jobs_crud
from jasil._core.job_context import job_lease, lease_lost
from jasil.backends.clock_system import SystemClock
from jasil.backends.mail_memory import MemoryMail
from jasil.events import new_event
from jasil.jobs import lease_lost as public_lease_lost
from jasil.jobs.heartbeat import LeaseKeeper
from jasil.jobs.models import JobWorker, ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.jobs.worker import WorkerHeartbeat, WorkerTelemetry, run_worker
from jasil.providers import MailError, MailMessage
from jasil.settings import JasilSettings, MailSettings
from jasil.testing import FixedClock

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
LEASE = 60
CAP = 600
INTERVAL = 15
WAIT_TIMEOUT = 5.0
NEVER = 3600.0


def _naive(moment: datetime) -> datetime:
    return moment.replace(tzinfo=None)


def _job(db, subscriber: str) -> ProcessingJob:
    db.rollback()  # end the read snapshot so the runner's commits are visible
    return db.query(ProcessingJob).filter_by(subscriber_id=subscriber).one()


@pytest.fixture
def clock():
    return FixedClock(moment=T0)


@pytest.fixture
def registry():
    return JobHandlerRegistry()


@pytest.fixture
def runner(registry, clock, session_factory):
    return JobRunner(
        registry=registry,
        clock=clock,
        session_factory=session_factory,
        worker_id="worker-1",
        lease_seconds=LEASE,
        max_runtime_seconds=CAP,
        heartbeat_interval_seconds=INTERVAL,
        batch_size=10,
        backoff_base_seconds=10,
        backoff_max_seconds=100,
    )


def _enqueue(db, subscriber: str) -> None:
    jobs_crud.enqueue_job(
        new_event("activity.created", {"id": 1}, source="test"), subscriber, max_attempts=3, now=T0, db=db
    )


def _register_both(registry, handler_for):
    for name in ("s", "t"):
        registry.register("activity.created", name, handler_for(name))


class TestLeaseRenewal:
    def test_a_beat_extends_the_running_job_and_the_ones_waiting(self, runner, registry, clock, db):
        leases = []

        def handler_for(_name):
            def handle(_event):
                if leases:
                    return
                clock.advance(30)
                runner.leases.renew()
                db.rollback()
                leases.extend(job.lease_expires_at for job in db.query(ProcessingJob).filter_by(status="claimed"))

            return handle

        _register_both(registry, handler_for)
        _enqueue(db, "s")
        _enqueue(db, "t")

        runner.run_once()

        assert len(leases) == 2
        assert {_naive(lease) for lease in leases} == {_naive(T0 + timedelta(seconds=30 + LEASE))}

    def test_a_handler_longer_than_the_lease_is_not_reclaimed(self, runner, registry, clock, db):
        reclaimed = []

        def handle(_event):
            for _ in range(3):
                clock.advance(50)  # each step is inside the lease; the total is far past it
                runner.leases.renew()
                reclaimed.append(runner.reap_once())

        registry.register("activity.created", "s", handle)
        _enqueue(db, "s")

        runner.run_once()

        assert reclaimed == [0, 0, 0]
        assert _job(db, "s").status == jobs_crud.STATUS_COMPLETED

    def test_without_a_beat_the_same_handler_is_reclaimed(self, runner, registry, clock, db):
        """The control: this is the double run the heartbeat exists to prevent."""
        reclaimed = []

        def handle(_event):
            clock.advance(150)
            reclaimed.append(runner.reap_once())

        registry.register("activity.created", "s", handle)
        _enqueue(db, "s")

        runner.run_once()

        assert reclaimed == [1]

    def test_an_idle_worker_renews_nothing(self, runner):
        assert runner.leases.renew() == set()


class TestALostLease:
    @staticmethod
    def _steal(db, subscriber):
        db.execute(update(ProcessingJob).where(ProcessingJob.subscriber_id == subscriber).values(locked_by="other"))
        db.commit()

    def test_the_handler_is_told_and_its_result_is_discarded(self, runner, registry, db):
        told = []

        def handle(_event):
            self._steal(db, "s")
            runner.leases.renew()
            told.append(lease_lost())

        registry.register("activity.created", "s", handle)
        _enqueue(db, "s")

        runner.run_once()

        job = _job(db, "s")
        assert told == [True]
        assert job.status == jobs_crud.STATUS_CLAIMED
        assert job.locked_by == "other"

    def test_the_rest_of_the_batch_goes_back_to_the_queue_unspent(self, runner, registry, db):
        ran = []

        def handler_for(name):
            def handle(_event):
                ran.append(name)
                self._steal(db, name)
                runner.leases.renew()

            return handle

        _register_both(registry, handler_for)
        _enqueue(db, "s")
        _enqueue(db, "t")

        assert runner.run_once() == 2

        (first,) = ran
        other = "t" if first == "s" else "s"
        released = _job(db, other)
        assert released.status == jobs_crud.STATUS_PENDING
        assert released.attempts == 0, "the claim of a job that never started is refunded"
        assert released.locked_by is None
        assert released.lease_expires_at is None

    def test_a_released_job_is_run_by_the_next_batch(self, runner, registry, db):
        ran = []

        def handler_for(name):
            def handle(_event):
                ran.append(name)
                if len(ran) == 1:
                    self._steal(db, name)
                    runner.leases.renew()

            return handle

        _register_both(registry, handler_for)
        _enqueue(db, "s")
        _enqueue(db, "t")

        runner.run_once()
        runner.run_once()

        assert sorted(ran) == ["s", "t"]
        other = "t" if ran[0] == "s" else "s"
        assert _job(db, other).status == jobs_crud.STATUS_COMPLETED
        assert _job(db, other).attempts == 1


class TestMaxRuntime:
    def test_an_overrunning_handler_is_signalled_and_no_longer_renewed(self, runner, registry, clock, db, caplog):
        told = []
        leases = []

        def handle(_event):
            clock.advance(CAP + 1)
            with caplog.at_level(logging.ERROR, logger="jasil.jobs.heartbeat"):
                runner.leases.renew()
            told.append(lease_lost())
            db.rollback()
            leases.append(db.query(ProcessingJob).filter_by(subscriber_id="s").one().lease_expires_at)
            clock.advance(10)
            runner.leases.renew()
            db.rollback()
            leases.append(db.query(ProcessingJob).filter_by(subscriber_id="s").one().lease_expires_at)

        registry.register("activity.created", "s", handle)
        _enqueue(db, "s")

        runner.run_once()

        assert told == [True]
        assert leases[0] == leases[1], "an abandoned job's lease is left to expire"
        assert "exceeded its maximum runtime" in caplog.text

    def test_the_waiting_jobs_are_released_rather_than_held_hostage(self, runner, registry, clock, db):
        seen_during = []

        def handler_for(name):
            def handle(_event):
                if seen_during:
                    return
                clock.advance(CAP + 1)
                runner.leases.renew()
                other = "t" if name == "s" else "s"
                seen_during.append(_job(db, other))

            return handle

        _register_both(registry, handler_for)
        _enqueue(db, "s")
        _enqueue(db, "t")

        runner.run_once()

        (waiting,) = seen_during
        assert waiting.status == jobs_crud.STATUS_PENDING
        assert waiting.attempts == 0


class TestWhenReleasingFails:
    def test_the_waiting_jobs_are_dropped_and_left_to_the_reaper(
        self, runner, registry, clock, db, monkeypatch, caplog
    ):
        """Released or not, a job this worker will not run must stop being renewed."""

        def fail(*args, **kwargs):
            raise RuntimeError("database unreachable")

        monkeypatch.setattr(jobs_crud, "release_leases", fail)

        def handler_for(name):
            def handle(_event):
                with caplog.at_level(logging.WARNING, logger="jasil.jobs.heartbeat"):
                    clock.advance(CAP + 1)
                    runner.leases.renew()

            return handle

        _register_both(registry, handler_for)
        _enqueue(db, "s")
        _enqueue(db, "t")

        runner.run_once()

        assert "could not be released; leaving them to the reaper" in caplog.text
        assert runner.leases._held == {}
        states = {_job(db, name).status for name in ("s", "t")}
        assert jobs_crud.STATUS_CLAIMED in states, "left held until the lease expires"


class TestWhenRenewalFails:
    @pytest.fixture
    def broken_renewal(self, monkeypatch):
        def fail(*args, **kwargs):
            raise RuntimeError("database unreachable")

        monkeypatch.setattr(jobs_crud, "renew_leases", fail)

    def test_the_job_is_fenced_before_its_lease_can_have_been_reclaimed(
        self, runner, registry, clock, db, broken_renewal
    ):
        told = []

        def handle(_event):
            for step in (LEASE - INTERVAL - 1, 2):  # just inside the silence window, then past it
                clock.advance(step)
                with pytest.raises(RuntimeError, match="database unreachable"):
                    runner.leases.renew()
                told.append(lease_lost())

        registry.register("activity.created", "s", handle)
        _enqueue(db, "s")

        runner.run_once()

        assert told == [False, True]

    def test_a_job_it_could_not_confirm_is_skipped_not_run(self, runner, registry, db, caplog, monkeypatch):
        ran = []

        def handler_for(name):
            def handle(_event):
                ran.append(name)

            return handle

        _register_both(registry, handler_for)
        _enqueue(db, "s")
        _enqueue(db, "t")

        def fail():
            raise RuntimeError("database unreachable")

        monkeypatch.setattr(runner.leases, "renew", fail)

        with caplog.at_level(logging.ERROR, logger="jasil.jobs.runner"):
            assert runner.run_once() == 2

        assert len(ran) == 1
        assert "lease could not be renewed; skipping" in caplog.text
        skipped = "t" if ran == ["s"] else "s"
        assert _job(db, skipped).status == jobs_crud.STATUS_CLAIMED, "its lease is left to expire, not run blind"


class TestWorkerHeartbeat:
    class _Leases:
        def __init__(self, error=None):
            self.calls = 0
            self.error = error

        def renew(self):
            self.calls += 1
            if self.error is not None:
                raise self.error
            return set()

    class _Telemetry:
        def __init__(self):
            self.events = []

        def record_start(self):
            self.events.append("start")

        def record_heartbeat(self):
            self.events.append("heartbeat")

        def record_stop(self):
            self.events.append("stop")

    def test_one_beat_renews_leases_then_records_liveness(self):
        leases, telemetry = self._Leases(), self._Telemetry()
        heartbeat = WorkerHeartbeat(leases=leases, interval_seconds=1, telemetry=telemetry)

        heartbeat.beat()

        assert leases.calls == 1
        assert telemetry.events == ["heartbeat"]

    def test_a_renewal_failure_does_not_skip_the_registry_write(self, caplog):
        leases, telemetry = self._Leases(RuntimeError("down")), self._Telemetry()
        heartbeat = WorkerHeartbeat(leases=leases, interval_seconds=1, telemetry=telemetry)

        with caplog.at_level(logging.WARNING, logger="jasil.jobs.worker"):
            heartbeat.beat()

        assert telemetry.events == ["heartbeat"]
        assert "lease renewal failed" in caplog.text

    def test_a_registry_failure_does_not_skip_the_renewal(self, session_factory, monkeypatch, caplog):
        def fail(*args, **kwargs):
            raise RuntimeError("telemetry database unavailable")

        monkeypatch.setattr(worker_registry, "record_worker_heartbeat", fail)
        telemetry = WorkerTelemetry(
            instance_id="w", clock=FixedClock(moment=T0), session_factory=session_factory, queues=None
        )
        telemetry.record_start()
        leases = self._Leases()
        heartbeat = WorkerHeartbeat(leases=leases, interval_seconds=1, telemetry=telemetry)

        with caplog.at_level(logging.WARNING, logger="jasil.jobs.worker"):
            heartbeat.beat()

        assert leases.calls == 1
        assert "Durable worker heartbeat failed" in caplog.text

    def test_telemetry_start_and_stop_failures_are_logged_not_raised(self, session_factory, monkeypatch, caplog):
        def fail(*args, **kwargs):
            raise RuntimeError("telemetry database unavailable")

        monkeypatch.setattr(worker_registry, "record_worker_start", fail)
        monkeypatch.setattr(worker_registry, "record_worker_stop", fail)
        telemetry = WorkerTelemetry(
            instance_id="w", clock=FixedClock(moment=T0), session_factory=session_factory, queues=None
        )
        heartbeat = WorkerHeartbeat(leases=self._Leases(), interval_seconds=NEVER, telemetry=telemetry)

        with caplog.at_level(logging.WARNING, logger="jasil.jobs.worker"):
            heartbeat.start()
            assert heartbeat.stop() is True

        assert "start telemetry failed" in caplog.text
        assert "stop telemetry failed" in caplog.text

    def test_a_heartbeat_before_the_start_record_writes_nothing(self, session_factory):
        telemetry = WorkerTelemetry(
            instance_id="w", clock=FixedClock(moment=T0), session_factory=session_factory, queues=None
        )

        telemetry.record_heartbeat()

        with session_factory() as db:
            assert db.get(JobWorker, "w") is None

    def test_a_restart_after_the_thread_died_starts_a_new_one(self):
        heartbeat = WorkerHeartbeat(leases=self._Leases(), interval_seconds=NEVER)
        heartbeat.start()
        first = heartbeat._thread
        heartbeat._stop.set()
        first.join(timeout=WAIT_TIMEOUT)
        assert not first.is_alive()

        heartbeat.start()

        assert heartbeat._thread is not first
        assert heartbeat.stop() is True

    def test_it_renews_without_any_telemetry(self):
        leases = self._Leases()

        WorkerHeartbeat(leases=leases, interval_seconds=1).beat()

        assert leases.calls == 1

    @pytest.mark.parametrize("interval", [0, -1])
    def test_a_non_positive_interval_is_refused(self, interval):
        with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
            WorkerHeartbeat(leases=self._Leases(), interval_seconds=interval)

    def test_starting_twice_leaves_one_thread_and_one_start_record(self):
        telemetry = self._Telemetry()
        heartbeat = WorkerHeartbeat(leases=self._Leases(), interval_seconds=NEVER, telemetry=telemetry)

        heartbeat.start()
        thread = heartbeat._thread
        heartbeat.start()

        assert heartbeat._thread is thread
        assert telemetry.events == ["start"]
        assert heartbeat.stop() is True
        assert telemetry.events == ["start", "stop"]

    def test_the_thread_is_a_daemon_and_beats_on_its_interval(self):
        leases = self._Leases()
        heartbeat = WorkerHeartbeat(leases=leases, interval_seconds=0.01)

        heartbeat.start()
        try:
            assert heartbeat._thread.daemon is True
            deadline = time.monotonic() + WAIT_TIMEOUT
            while leases.calls < 2 and time.monotonic() < deadline:
                time.sleep(0.005)
        finally:
            heartbeat.stop()

        assert leases.calls >= 2
        assert heartbeat._thread is None


class TestTheRealLoop:
    """A handler blocked for many intervals keeps its lease, on real threads."""

    @pytest.fixture
    def file_database(self, tmp_path, mapped_base):
        engine = create_engine(f"sqlite:///{tmp_path / 'heartbeat.db'}")
        mapped_base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        yield factory
        mapped_base.metadata.drop_all(engine)
        engine.dispose()

    def test_leases_are_renewed_while_the_handler_is_blocked(self, file_database, monkeypatch):
        renewals = []
        original = jobs_crud.renew_leases

        def counting(*args, **kwargs):
            result = original(*args, **kwargs)
            renewals.append(set(result))
            return result

        monkeypatch.setattr(jobs_crud, "renew_leases", counting)
        started, release = threading.Event(), threading.Event()
        registry = JobHandlerRegistry()

        def handle(_event):
            started.set()
            assert release.wait(timeout=WAIT_TIMEOUT)

        registry.register("activity.created", "s", handle)
        with file_database() as db:
            _enqueue(db, "s")
        clock = SystemClock()
        runner = JobRunner(
            registry=registry,
            clock=clock,
            session_factory=file_database,
            worker_id="worker-1",
            lease_seconds=LEASE,
            max_runtime_seconds=CAP,
            heartbeat_interval_seconds=0.01,
            batch_size=1,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
        )
        telemetry = WorkerTelemetry(instance_id="worker-1", clock=clock, session_factory=file_database, queues=None)
        heartbeat = WorkerHeartbeat(leases=runner.leases, interval_seconds=0.01, telemetry=telemetry)
        stop = threading.Event()
        thread = threading.Thread(
            target=run_worker,
            args=(runner,),
            kwargs={"poll_interval_seconds": NEVER, "stop": stop, "heartbeat": heartbeat},
        )

        thread.start()
        assert started.wait(timeout=WAIT_TIMEOUT)
        deadline = time.monotonic() + WAIT_TIMEOUT
        while sum(1 for ids in renewals if ids) < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
        stop.set()
        release.set()
        thread.join(timeout=WAIT_TIMEOUT)

        assert not thread.is_alive()
        assert sum(1 for ids in renewals if ids) >= 2, "the running job was renewed more than once"
        with file_database() as db:
            assert db.query(ProcessingJob).one().status == jobs_crud.STATUS_COMPLETED
            assert db.get(JobWorker, "worker-1").stopped_at is not None


class TestRenewalIsSerialised:
    def test_two_threads_never_renew_at_once(self, monkeypatch):
        """The heartbeat thread and the runner's before-turn renewal share one keeper."""
        active = peak = 0
        guard = threading.Lock()

        def slow_renew(job_attempts, **_kwargs):
            nonlocal active, peak
            with guard:
                active += 1
                peak = max(peak, active)
            time.sleep(0.02)
            with guard:
                active -= 1
            return set(job_attempts)

        monkeypatch.setattr(jobs_crud, "renew_leases", slow_renew)
        keeper = LeaseKeeper(
            session_factory=contextlib.nullcontext,
            clock=FixedClock(moment=T0),
            worker_id="w",
            lease_seconds=LEASE,
            max_runtime_seconds=CAP,
            interval_seconds=INTERVAL,
        )
        keeper.hold({"a": 1, "b": 1})

        threads = [threading.Thread(target=keeper.renew) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=WAIT_TIMEOUT)

        assert peak == 1


class TestReleasingLeases:
    def _claim(self, db, worker, *subscribers):
        for name in subscribers:
            _enqueue(db, name)
        return {
            job.subscriber_id: job
            for job in jobs_crud.claim_jobs(worker_id=worker, limit=10, lease_seconds=LEASE, now=T0, db=db)
        }

    def test_a_held_job_returns_to_pending_with_its_attempt_refunded(self, db):
        claimed = self._claim(db, "w1", "s", "t")
        release = {claimed["s"].id: claimed["s"].attempts}

        released = jobs_crud.release_leases(release, worker_id="w1", now=T0 + timedelta(seconds=5), db=db)

        assert released == 1
        db.rollback()
        job = jobs_crud.get_job(claimed["s"].id, db)
        assert (job.status, job.attempts, job.locked_by, job.lease_expires_at) == ("pending", 0, None, None)
        assert _naive(job.available_at) == _naive(T0 + timedelta(seconds=5))
        assert jobs_crud.get_job(claimed["t"].id, db).status == jobs_crud.STATUS_CLAIMED

    def test_a_job_another_worker_holds_is_left_alone(self, db):
        claimed = self._claim(db, "w1", "s")

        released = jobs_crud.release_leases({claimed["s"].id: 1}, worker_id="w2", now=T0, db=db)

        assert released == 0
        db.rollback()
        assert jobs_crud.get_job(claimed["s"].id, db).locked_by == "w1"

    def test_a_later_claim_of_the_same_job_is_left_alone(self, db):
        """The snapshot's attempt is part of the claim: attempt 2 is not attempt 1's to release."""
        claimed = self._claim(db, "w1", "s")
        job_id = claimed["s"].id

        released = jobs_crud.release_leases({job_id: 0}, worker_id="w1", now=T0, db=db)

        assert released == 0
        db.rollback()
        assert jobs_crud.get_job(job_id, db).attempts == 1

    def test_nothing_to_release_is_a_no_op(self, db):
        assert jobs_crud.release_leases({}, worker_id="w1", now=T0, db=db) == 0


class _RenewsAfterTheSelect:
    """A session that renews the lease right after the reaper has selected its ids.

    This is the race the reaper's expiry guard exists for, without SKIP LOCKED:
    the reaper selects an expired lease, the worker's heartbeat renews it, and the
    reaper's UPDATE then runs against a lease that is no longer expired.
    """

    def __init__(self, session, renew):
        self._session = session
        self._renew = renew
        self._calls = 0

    def execute(self, *args, **kwargs):
        result = self._session.execute(*args, **kwargs)
        self._calls += 1
        if self._calls == 1:
            ids = list(result.scalars().all()) if hasattr(result, "scalars") else []
            self._renew()
            return _Rows(ids)
        return result

    def __getattr__(self, name):
        return getattr(self._session, name)


class _Rows:
    def __init__(self, ids):
        self._ids = ids

    def scalars(self):
        return self

    def all(self):
        return self._ids


class TestTheReaperDoesNotRequeueARenewedLease:
    def test_a_lease_renewed_after_the_select_survives(self, db):
        _enqueue(db, "s")
        (job,) = jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        later = T0 + timedelta(seconds=LEASE + 1)

        def renew():
            jobs_crud.renew_leases({job.id: job.attempts}, worker_id="w1", lease_seconds=LEASE, now=later, db=db)

        reclaimed = jobs_crud.reclaim_expired_leases(now=later, db=_RenewsAfterTheSelect(db, renew))

        assert reclaimed == 0
        db.rollback()
        kept = jobs_crud.get_job(job.id, db)
        assert (kept.status, kept.locked_by) == (jobs_crud.STATUS_CLAIMED, "w1")

    def test_an_expired_lease_is_still_reclaimed(self, db):
        _enqueue(db, "s")
        (job,) = jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        reclaimed = jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=LEASE + 1), db=db)

        assert reclaimed == 1
        assert jobs_crud.get_job(job.id, db).status == jobs_crud.STATUS_PENDING


class TestLeaseLostSignal:
    def test_it_is_false_outside_a_job(self):
        assert lease_lost() is False

    def test_it_follows_the_event_inside_a_job(self):
        lost = threading.Event()

        with job_lease(lost):
            assert lease_lost() is False
            lost.set()
            assert lease_lost() is True

        assert lease_lost() is False

    def test_the_binding_is_restored_after_nesting(self):
        outer, inner = threading.Event(), threading.Event()
        inner.set()

        with job_lease(outer):
            with job_lease(inner):
                assert lease_lost() is True
            assert lease_lost() is False

    def test_it_is_part_of_the_public_jobs_namespace(self):
        assert public_lease_lost is lease_lost

    def test_a_handler_sees_the_signal_the_runner_bound(self, runner, registry, db):
        seen = []

        def handle(_event):
            seen.append(lease_lost())

        registry.register("activity.created", "s", handle)
        _enqueue(db, "s")

        runner.run_once()

        assert seen == [False]
        assert lease_lost() is False, "the binding does not leak past the handler"


class TestMailHonoursTheSignal:
    @staticmethod
    def _message():
        return MailMessage(to=["a@example.com"], subject="s", text="b")

    def test_memory_mail_records_nothing_once_the_lease_is_lost(self):
        mail = MemoryMail(clock=FixedClock(moment=T0), from_address="x@example.com")
        lost = threading.Event()
        lost.set()

        with job_lease(lost), pytest.raises(MailError, match="lease lost"):
            mail.send(self._message())

        assert mail.outbox == []

    def test_memory_mail_sends_while_the_lease_is_held(self):
        mail = MemoryMail(clock=FixedClock(moment=T0), from_address="x@example.com")

        with job_lease(threading.Event()):
            mail.send(self._message())

        assert len(mail.outbox) == 1

    def test_smtp_mail_does_not_connect_once_the_lease_is_lost(self, monkeypatch):
        settings = JasilSettings(mail=MailSettings(uri="smtps://relay.example.com", from_address="a@example.com"))
        mail = container._build_mail(settings, SystemClock())
        connections = []
        monkeypatch.setattr("socket.getaddrinfo", lambda *a, **k: connections.append(a) or [])
        lost = threading.Event()
        lost.set()

        with job_lease(lost), pytest.raises(MailError, match="lease lost"):
            mail.send(self._message())

        assert connections == []
