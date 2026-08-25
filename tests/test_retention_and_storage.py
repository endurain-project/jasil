"""Scheduled retention and the local storage backend.

Retention is single-runner across replicas and inert unless configured; the
storage backend's contract is mostly about refusing to write outside its root.
"""

import contextlib
import io
from datetime import UTC, datetime, timedelta

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import jasil.jobs.outbox as jobs_outbox
import jasil.retention as retention
import jasil.runtime as runtime
import jasil.settings as settings
from jasil.backends.lock_noop import NoopLock
from jasil.backends.storage_local import LocalStorage
from jasil.event_log.models import EventLog
from jasil.events import new_event
from jasil.jobs.models import EventOutbox
from jasil.providers import StorageBackendUnavailableError

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
OLD = T0 - timedelta(days=90)


class FixedClock:
    def now(self) -> datetime:
        return T0


class UnavailableLock:
    """Stands in for another replica already holding the prune lock."""

    @contextlib.contextmanager
    def try_acquire(self, name, ttl_seconds=None):
        yield False


class FakePlatform:
    def __init__(self, lock=None) -> None:
        self.lock = lock if lock is not None else NoopLock()
        self.clock = FixedClock()


@pytest.fixture
def platform(monkeypatch):
    installed = FakePlatform()
    monkeypatch.setattr(runtime, "_active_platform", installed)
    return installed


def _configure_retention(*, event_log_days: int, jobs_days: int) -> None:
    settings.configure(
        settings.JasilSettings(
            event_log=settings.EventLogSettings(retention_days=event_log_days),
            jobs=settings.JobSettings(retention_days=jobs_days),
        )
    )


def _old_event_log_row(db) -> None:
    db.add(
        EventLog(
            id="old",
            event_type="activity.created",
            event_source="test",
            status="completed",
            event_payload={},
            event_metadata={},
            created_at=OLD,
        )
    )
    db.commit()


class TestRetention:
    def test_it_is_inert_when_both_windows_are_disabled(self, platform, session_factory, db):
        """Retention off means keep every row forever."""
        _configure_retention(event_log_days=0, jobs_days=0)
        _old_event_log_row(db)

        retention.prune_expired_records()

        assert db.query(EventLog).count() == 1

    def test_it_prunes_the_event_log_past_its_window(self, platform, session_factory, db):
        _configure_retention(event_log_days=30, jobs_days=0)
        _old_event_log_row(db)

        retention.prune_expired_records()

        assert db.query(EventLog).count() == 0

    def test_the_two_windows_are_independent(self, platform, session_factory, db):
        """Enabling job retention alone must not prune the event_log."""
        _configure_retention(event_log_days=0, jobs_days=30)
        _old_event_log_row(db)
        outbox_id = jobs_outbox.add_to_outbox(new_event("a.b", {}, source="t"), now=OLD, db=db)
        jobs_outbox.mark_relayed(outbox_id, now=OLD, db=db)

        retention.prune_expired_records()

        assert db.query(EventLog).count() == 1
        assert db.query(EventOutbox).count() == 0

    def test_it_skips_when_another_replica_holds_the_lock(self, monkeypatch, session_factory, db):
        """Single-runner: the deletes are idempotent, but duplicating the work
        across replicas is pointless load."""
        monkeypatch.setattr(runtime, "_active_platform", FakePlatform(lock=UnavailableLock()))
        _configure_retention(event_log_days=30, jobs_days=30)
        _old_event_log_row(db)

        retention.prune_expired_records()

        assert db.query(EventLog).count() == 1

    def test_a_pass_that_deletes_nothing_is_quiet(self, platform, session_factory, db, caplog):
        _configure_retention(event_log_days=30, jobs_days=30)

        with caplog.at_level("INFO"):
            retention.prune_expired_records()

        assert "Retention prune: deleted" not in caplog.text

    def test_a_pass_that_deletes_reports_what_it_removed(self, platform, session_factory, db, caplog):
        _configure_retention(event_log_days=30, jobs_days=0)
        _old_event_log_row(db)

        with caplog.at_level("INFO"):
            retention.prune_expired_records()

        assert "1 event_log" in caplog.text


class TestScheduleRetentionMaintenance:
    """Retention is scheduled separately from durable jobs, because it also
    prunes the event_log — a deployment that never enabled jobs still needs it."""

    @pytest.fixture
    async def scheduler(self):
        """A started-but-paused scheduler.

        It has to be started for jobs to reach the jobstore — a pending scheduler
        just queues ``add_job`` calls, where ``replace_existing`` has no meaning.
        Async, because ``AsyncIOScheduler.start`` binds the running loop, and
        paused so nothing fires while the test inspects it.
        """
        scheduler = AsyncIOScheduler()
        scheduler.start(paused=True)
        yield scheduler
        scheduler.shutdown(wait=False)

    async def test_it_registers_the_prune(self, scheduler):
        retention.schedule_retention_maintenance(scheduler)

        assert [job.id for job in scheduler.get_jobs()] == [retention._PRUNE_JOB_ID]

    async def test_the_registered_job_is_the_prune(self, scheduler):
        retention.schedule_retention_maintenance(scheduler)

        assert scheduler.get_jobs()[0].func is retention.prune_expired_records

    async def test_registering_twice_replaces_rather_than_duplicates(self, scheduler):
        """Otherwise a re-entrant startup would prune twice per interval."""
        retention.schedule_retention_maintenance(scheduler)

        retention.schedule_retention_maintenance(scheduler)

        assert len(scheduler.get_jobs()) == 1

    async def test_it_runs_once_at_startup_by_default(self, scheduler):
        """A daily interval alone means a process redeployed daily never prunes."""
        retention.schedule_retention_maintenance(scheduler)

        assert scheduler.get_jobs()[0].next_run_time <= datetime.now(UTC)

    async def test_the_startup_run_can_be_declined(self, scheduler):
        retention.schedule_retention_maintenance(scheduler, run_at_startup=False)

        assert scheduler.get_jobs()[0].next_run_time > datetime.now(UTC)


class TestLocalStorage:
    @pytest.fixture
    def storage(self, tmp_path):
        return LocalStorage(str(tmp_path), url_prefix="/media")

    def test_a_blob_round_trips(self, storage):
        storage.save("thumbnails", "1.webp", b"bytes")

        assert storage.get("thumbnails", "1.webp") == b"bytes"

    def test_saving_returns_the_key(self, storage):
        assert storage.save("thumbnails", "1.webp", b"x") == "1.webp"

    def test_streaming_and_byte_writes_use_the_same_file_permissions(self, storage, tmp_path):
        storage.save("thumbnails", "bytes.webp", b"x")
        storage.save_stream("thumbnails", "stream.webp", io.BytesIO(b"x"))

        byte_mode = (tmp_path / "thumbnails" / "bytes.webp").stat().st_mode & 0o777
        stream_mode = (tmp_path / "thumbnails" / "stream.webp").stat().st_mode & 0o777
        assert stream_mode == byte_mode

    def test_a_missing_blob_reads_as_none(self, storage):
        assert storage.get("thumbnails", "absent.webp") is None

    def test_existence_is_reported(self, storage):
        assert storage.exists("thumbnails", "1.webp") is False

        storage.save("thumbnails", "1.webp", b"x")

        assert storage.exists("thumbnails", "1.webp") is True

    def test_deleting_removes_the_blob(self, storage):
        storage.save("thumbnails", "1.webp", b"x")

        storage.delete("thumbnails", "1.webp")

        assert storage.exists("thumbnails", "1.webp") is False

    def test_deleting_a_missing_blob_is_a_no_op(self, storage):
        storage.delete("thumbnails", "absent.webp")

    def test_areas_are_isolated(self, storage):
        storage.save("thumbnails", "1.webp", b"thumb")
        storage.save("media", "1.webp", b"media")

        assert storage.get("thumbnails", "1.webp") == b"thumb"
        assert storage.get("media", "1.webp") == b"media"

    def test_keys_are_listed_sorted(self, storage):
        for key in ("c.webp", "a.webp", "b.webp"):
            storage.save("thumbnails", key, b"x")

        assert storage.list_keys("thumbnails") == ["a.webp", "b.webp", "c.webp"]

    def test_keys_can_be_filtered_by_prefix(self, storage):
        storage.save("thumbnails", "user-1.webp", b"x")
        storage.save("thumbnails", "other.webp", b"x")

        assert storage.list_keys("thumbnails", prefix="user-") == ["user-1.webp"]

    def test_listing_an_unknown_area_is_empty(self, storage):
        assert storage.list_keys("nothing") == []

    def test_a_url_is_built_from_the_prefix(self, storage):
        assert storage.url("thumbnails", "1.webp") == "/media/thumbnails/1.webp"

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("a b.webp", "/media/thumbnails/a%20b.webp"),
            ("a?b.webp", "/media/thumbnails/a%3Fb.webp"),
            ("a#b.webp", "/media/thumbnails/a%23b.webp"),
            ("100%.webp", "/media/thumbnails/100%25.webp"),
        ],
    )
    def test_a_key_is_percent_encoded(self, storage, key, expected):
        """``?`` would end the path early and ``%`` would change it; only traversal is validated."""
        assert storage.url("thumbnails", key) == expected

    def test_a_nested_key_keeps_its_separators(self, storage):
        """``save`` accepts a nested key, so ``/`` is structure rather than data."""
        assert storage.url("thumbnails", "2026/01/1.webp") == "/media/thumbnails/2026/01/1.webp"

    def test_a_requested_expiry_is_reported_as_ignored(self, storage, caplog):
        """The one place the two storage backends genuinely differ.

        S3 returns a presigned URL that stops working; this backend returns a
        path for the host's own web server and cannot expire it. A caller who
        asked for a lifetime and is silently not getting one would otherwise be
        relying on an authorization control that does not exist.
        """
        with caplog.at_level("WARNING"):
            url = storage.url("thumbnails", "1.webp", expires_in=60)

        assert url == "/media/thumbnails/1.webp"
        assert "expires_in=60 was ignored" in caplog.text
        assert "permanent" in caplog.text

    def test_the_expiry_warning_is_logged_once_per_backend(self, storage, caplog):
        """``url`` is called per serialized record; this must not flood the log."""
        with caplog.at_level("WARNING"):
            for index in range(5):
                storage.url("thumbnails", f"{index}.webp", expires_in=60)

        assert caplog.text.count("was ignored") == 1

    def test_not_asking_for_an_expiry_is_quiet(self, storage, caplog):
        """The default means "no opinion", so there is nothing to warn about."""
        with caplog.at_level("WARNING"):
            storage.url("thumbnails", "1.webp")

        assert caplog.text == ""

    def test_response_header_controls_are_reported_as_ignored(self, storage, caplog):
        with caplog.at_level("WARNING"):
            url = storage.url(
                "thumbnails",
                "1.webp",
                download_as="photo.webp",
                content_type="application/octet-stream",
            )

        assert url == "/media/thumbnails/1.webp"
        assert "download_as, content_type were ignored" in caplog.text
        assert "permanent" in caplog.text

    def test_a_missing_storage_root_is_not_reported_writable(self, tmp_path):
        storage = LocalStorage(str(tmp_path / "detached-volume"))

        with pytest.raises(StorageBackendUnavailableError):
            storage.check_writable()

    @pytest.mark.parametrize("bad", ["../escape", "/etc/passwd", "a/../../b"])
    @pytest.mark.parametrize("field", ["area", "key"])
    def test_path_traversal_is_refused(self, storage, bad, field):
        """Keys are server-generated, but a stray value must never escape the root."""
        args = {"area": "thumbnails", "key": "1.webp", field: bad}

        with pytest.raises(ValueError, match="escapes base directory"):
            storage.save(args["area"], args["key"], b"x")

    def test_traversal_is_refused_before_any_filesystem_access(self, storage, tmp_path):
        with pytest.raises(ValueError, match="escapes base directory"):
            storage.get("../..", "passwd")

    def test_listing_refuses_a_traversing_area(self, storage):
        with pytest.raises(ValueError, match="escapes base directory"):
            storage.list_keys("../etc")

    @pytest.mark.parametrize("field", ["area", "key"])
    def test_an_empty_segment_is_refused(self, storage, field):
        """An empty segment collapses the path onto the storage root itself."""
        args = {"area": "thumbnails", "key": "1.webp", field: ""}

        with pytest.raises(ValueError, match="must not be empty"):
            storage.save(args["area"], args["key"], b"x")

    def test_an_empty_key_prefix_still_lists_the_area(self, storage):
        """The prefix is optional, unlike the area and the key."""
        storage.save("thumbnails", "1.webp", b"x")

        assert storage.list_keys("thumbnails", "") == ["1.webp"]

    def test_a_nested_key_round_trips(self, storage):
        storage.save("thumbnails", "2026/01/1.webp", b"x")

        assert storage.get("thumbnails", "2026/01/1.webp") == b"x"

    def test_a_nested_key_is_listed_by_its_full_path(self, storage):
        """``save`` creates the directories, so listing only the top level would
        hide the blob here while S3, whose listing is a flat prefix scan,
        returned it."""
        storage.save("thumbnails", "2026/01/1.webp", b"x")
        storage.save("thumbnails", "flat.webp", b"x")

        assert storage.list_keys("thumbnails") == ["2026/01/1.webp", "flat.webp"]

    def test_a_nested_key_can_be_filtered_by_its_leading_path(self, storage):
        storage.save("thumbnails", "2026/01/1.webp", b"x")
        storage.save("thumbnails", "2025/12/9.webp", b"x")

        assert storage.list_keys("thumbnails", prefix="2026/") == ["2026/01/1.webp"]

    def test_an_empty_directory_contributes_no_key(self, storage, tmp_path):
        (tmp_path / "thumbnails" / "empty").mkdir(parents=True)

        assert storage.list_keys("thumbnails") == []

    def test_a_symlink_out_of_the_area_is_not_listed(self, storage, tmp_path):
        """Following one would leak the existence of files outside the root."""
        outside = tmp_path.parent / "secret.txt"
        outside.write_text("secret")
        area = tmp_path / "thumbnails"
        area.mkdir(parents=True)
        (area / "link.webp").symlink_to(outside)

        assert storage.list_keys("thumbnails") == []


class TestRuntimeHandle:
    def test_reading_the_platform_before_startup_explains_the_fix(self, monkeypatch):
        monkeypatch.setattr(runtime, "_active_platform", None)

        with pytest.raises(RuntimeError, match="build_platform must run at startup"):
            runtime.get_active_platform()

    def test_the_published_platform_is_returned(self, monkeypatch):
        installed = FakePlatform()
        monkeypatch.setattr(runtime, "_active_platform", None)

        runtime.set_active_platform(installed)

        assert runtime.get_active_platform() is installed
        monkeypatch.setattr(runtime, "_active_platform", None)
