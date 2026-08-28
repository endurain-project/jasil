"""Scheduled retention and the local storage backend.

Retention is single-runner across replicas and inert unless configured; the
storage backend's contract is mostly about refusing to write outside its root.
"""

import contextlib
import io
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import jasil.backends.storage_local as storage_local
import jasil.jobs.outbox as jobs_outbox
import jasil.retention as retention
import jasil.runtime as runtime
import jasil.settings as settings
from jasil.backends.lock_noop import NoopLock
from jasil.backends.storage_local import LocalStorage
from jasil.event_log.models import EventLog
from jasil.events import new_event
from jasil.jobs.models import EventOutbox
from jasil.providers import (
    PartRef,
    ServeFile,
    StorageBackendUnavailableError,
    StorageSizeLimitError,
    StorageUploadSessionError,
)

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


def _local_object_url(storage: LocalStorage, area: str, key: str) -> str:
    relative_path = storage._resolve(area, key).relative_to(storage._base.resolve()).as_posix()
    return f"/media/{relative_path}"


def _upload_part(storage, session, part_number, data, *, size=None):
    declared_size = len(data) if size is None else size
    return storage.upload_part(session, part_number, io.BytesIO(data), size=declared_size)


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

        byte_mode = storage._resolve("thumbnails", "bytes.webp").stat().st_mode & 0o777
        stream_mode = storage._resolve("thumbnails", "stream.webp").stat().st_mode & 0o777
        assert stream_mode == byte_mode

    def test_a_negative_stream_limit_creates_no_private_object_state(self, storage, tmp_path):
        with pytest.raises(ValueError, match="must not be negative"):
            storage.save_stream("thumbnails", "stream.webp", io.BytesIO(b"x"), max_bytes=-1)

        assert (tmp_path / storage_local._OBJECTS_DIRECTORY).exists() is False

    def test_a_symlinked_key_identity_is_not_followed_or_listed(self, storage, tmp_path):
        storage.save("thumbnails", "original.webp", b"original")
        payload = storage._resolve("thumbnails", "original.webp")
        key_file = payload.parent / storage_local._OBJECT_KEY_FILE
        key_file.unlink()
        outside = tmp_path / "outside-key"
        outside.write_text("original.webp")
        key_file.symlink_to(outside)

        with pytest.raises(StorageBackendUnavailableError, match="unsafe"):
            storage.get("thumbnails", "original.webp")
        assert storage.list_keys("thumbnails") == []

    def test_a_symlinked_area_identity_blocks_new_writes(self, storage, tmp_path):
        storage.save("thumbnails", "original.webp", b"original")
        area_file = storage._resolve_area("thumbnails").parent / storage_local._OBJECT_AREA_FILE
        area_file.unlink()
        outside = tmp_path / "outside-area"
        outside.write_text("thumbnails")
        area_file.symlink_to(outside)

        with pytest.raises(StorageBackendUnavailableError, match="unsafe"):
            storage.save("thumbnails", "other.webp", b"other")

    def test_a_tampered_key_identity_blocks_reads_and_overwrites(self, storage):
        storage.save("thumbnails", "original.webp", b"original")
        payload = storage._resolve("thumbnails", "original.webp")
        (payload.parent / storage_local._OBJECT_KEY_FILE).write_text("different.webp")

        with pytest.raises(StorageBackendUnavailableError, match="does not match"):
            storage.get("thumbnails", "original.webp")
        with pytest.raises(StorageBackendUnavailableError, match="does not match"):
            storage.save("thumbnails", "original.webp", b"replacement")

    @pytest.mark.parametrize("component", ["foreign", "c-not-base32!", "c-MZXW6"])
    def test_invalid_private_layout_components_are_not_listed(self, storage, component):
        invalid_object = storage._resolve_area("thumbnails") / component / storage_local._OBJECT_PAYLOAD_FILE
        invalid_object.parent.mkdir(parents=True)
        invalid_object.write_bytes(b"private")
        storage.save("thumbnails", "valid.webp", b"valid")

        assert storage.list_keys("thumbnails") == ["valid.webp"]

    def test_local_stream_errors_are_provider_neutral(self):
        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage_local._translate_local_stream_error(OSError("storage unavailable"))

        assert isinstance(excinfo.value.__cause__, OSError)
        assert storage_local._translate_local_stream_error(ValueError("not an I/O failure")) is None

    @pytest.mark.parametrize(
        ("failure", "error"),
        [(RuntimeError("loop"), ValueError), (OSError("storage unavailable"), StorageBackendUnavailableError)],
    )
    def test_object_root_resolution_failures_are_translated(self, storage, monkeypatch, failure, error):
        original_resolve = Path.resolve

        def resolve(path, *args, **kwargs):
            if path == storage._base:
                raise failure
            return original_resolve(path, *args, **kwargs)

        monkeypatch.setattr(Path, "resolve", resolve)

        with pytest.raises(error):
            storage.get("thumbnails", "1.webp")

    def test_a_resolved_path_outside_the_root_is_rejected(self, storage, monkeypatch, tmp_path):
        original_resolve = Path.resolve
        calls = 0

        def resolve(path, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                return tmp_path.parent / "outside"
            return original_resolve(path, *args, **kwargs)

        monkeypatch.setattr(Path, "resolve", resolve)

        with pytest.raises(ValueError, match="escapes base directory"):
            storage.get("thumbnails", "1.webp")

    def test_regular_file_metadata_failures_are_provider_neutral(self, storage):
        unavailable_path = Mock()
        unavailable_path.stat.side_effect = OSError("storage unavailable")

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage._is_regular_file(unavailable_path)

        assert isinstance(excinfo.value.__cause__, OSError)

    def test_a_legacy_object_remains_readable_and_visible(self, storage, tmp_path):
        legacy_path = tmp_path / "thumbnails" / "legacy.webp"
        legacy_path.parent.mkdir()
        legacy_path.write_bytes(b"legacy")

        metadata = storage.stat("thumbnails", "legacy.webp")
        plan = storage.serve("thumbnails", "legacy.webp")

        assert storage.get("thumbnails", "legacy.webp") == b"legacy"
        assert storage.exists("thumbnails", "legacy.webp") is True
        assert metadata is not None
        assert metadata.size == 6
        assert plan == ServeFile(legacy_path)
        assert storage.url("thumbnails", "legacy.webp") == "/media/thumbnails/legacy.webp"
        assert storage.list_keys("thumbnails") == ["legacy.webp"]

    def test_overwriting_a_legacy_object_migrates_it_to_the_versioned_layout(self, storage, tmp_path):
        legacy_path = tmp_path / "thumbnails" / "legacy.webp"
        legacy_path.parent.mkdir()
        legacy_path.write_bytes(b"legacy")

        storage.save("thumbnails", "legacy.webp", b"current")

        assert legacy_path.exists() is False
        assert storage._resolve("thumbnails", "legacy.webp").read_bytes() == b"current"
        assert storage.get("thumbnails", "legacy.webp") == b"current"

    def test_a_legacy_cleanup_failure_does_not_hide_the_committed_replacement(
        self,
        storage,
        tmp_path,
        monkeypatch,
        caplog,
    ):
        legacy_path = tmp_path / "thumbnails" / "legacy.webp"
        legacy_path.parent.mkdir()
        legacy_path.write_bytes(b"legacy")
        original_unlink = Path.unlink

        def fail_legacy_unlink(path, *args, **kwargs):
            if path == legacy_path:
                raise OSError("storage unavailable")
            return original_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", fail_legacy_unlink)

        with caplog.at_level("WARNING"):
            storage.save("thumbnails", "legacy.webp", b"current")

        assert storage.get("thumbnails", "legacy.webp") == b"current"
        assert legacy_path.read_bytes() == b"legacy"
        assert "Failed to remove" in caplog.text

    def test_pruning_tolerates_an_unresolvable_storage_root(self, storage):
        unavailable_base = Mock()
        unavailable_base.resolve.side_effect = OSError("storage unavailable")
        storage._base = unavailable_base

        storage._prune_empty_directories(Path("unused"))

    def test_prefix_deletion_counts_duplicate_legacy_and_current_objects_once(self, storage, tmp_path):
        storage.save("thumbnails", "release/a.webp", b"current")
        legacy_path = tmp_path / "thumbnails" / "release" / "a.webp"
        legacy_path.parent.mkdir(parents=True)
        legacy_path.write_bytes(b"legacy")
        (legacy_path.parent / "b.webp").write_bytes(b"legacy-only")

        assert storage.delete_prefix("thumbnails", "release") == 2
        assert storage.list_keys("thumbnails") == []

    @pytest.mark.parametrize("legacy_parent", [True, False])
    def test_legacy_and_current_objects_can_form_one_logical_tree(self, storage, tmp_path, legacy_parent, caplog):
        legacy_key = "release/1" if legacy_parent else "release/1/archive.bin"
        current_key = "release/1/archive.bin" if legacy_parent else "release/1"
        legacy_path = tmp_path / "packages" / legacy_key
        legacy_path.parent.mkdir(parents=True)
        legacy_path.write_bytes(b"legacy")

        with caplog.at_level("WARNING"):
            storage.save("packages", current_key, b"current")

        assert storage.get("packages", legacy_key) == b"legacy"
        assert storage.get("packages", current_key) == b"current"
        assert storage.list_keys("packages", "release/1") == ["release/1", "release/1/archive.bin"]
        assert caplog.text == ""
        assert storage.delete_prefix("packages", "release/1") == 2

    def test_a_resumable_upload_survives_backend_instances_and_commits_atomically(self, storage, tmp_path):
        storage.save("packages", "release.bin", b"previous")
        session = storage.begin_upload(
            "packages",
            "release.bin",
            max_bytes=5 * 1024 * 1024 + 4,
            content_type="application/octet-stream",
        )
        tail = _upload_part(storage, session, 2, b"tail")
        other_instance = LocalStorage(str(tmp_path), url_prefix="/media")
        first = _upload_part(other_instance, session, 1, b"a" * session.min_part_size)

        assert storage.get("packages", "release.bin") == b"previous"
        assert storage.list_keys("packages") == ["release.bin"]

        stored = other_instance.complete_upload(session, [first, tail])

        assert stored == session.min_part_size + 4
        assert storage.get("packages", "release.bin") == b"a" * session.min_part_size + b"tail"

    def test_reuploading_a_part_replaces_it_and_invalidates_the_old_reference(self, storage):
        session = storage.begin_upload("packages", "release.bin")
        stale = _upload_part(storage, session, 1, b"old")
        current = _upload_part(storage, session, 1, b"new")

        with pytest.raises(StorageUploadSessionError, match="does not match"):
            storage.complete_upload(session, [stale])

        assert storage.complete_upload(session, [current]) == 3
        assert storage.get("packages", "release.bin") == b"new"

    def test_a_session_size_limit_counts_every_current_part(self, storage):
        session = storage.begin_upload("packages", "release.bin", max_bytes=4)
        _upload_part(storage, session, 1, b"1234")

        with pytest.raises(StorageSizeLimitError, match="max_bytes=4"):
            _upload_part(storage, session, 2, b"5")

        replacement = _upload_part(storage, session, 1, b"12")
        assert storage.complete_upload(session, [replacement]) == 2

    def test_completion_requires_every_part_in_strict_order(self, storage):
        session = storage.begin_upload("packages", "release.bin")
        first = _upload_part(storage, session, 1, b"a" * session.min_part_size)
        second = _upload_part(storage, session, 2, b"tail")

        with pytest.raises(ValueError, match="ordered"):
            storage.complete_upload(session, [second, first])
        with pytest.raises(StorageUploadSessionError, match="every uploaded part"):
            storage.complete_upload(session, [first])

        assert storage.complete_upload(session, [first, second]) == first.size + second.size

    def test_every_non_final_part_must_meet_the_portable_minimum(self, storage):
        session = storage.begin_upload("packages", "release.bin")
        first = _upload_part(storage, session, 1, b"small")
        second = _upload_part(storage, session, 2, b"tail")

        with pytest.raises(ValueError, match="except the last"):
            storage.complete_upload(session, [first, second])

    def test_abort_is_idempotent_and_makes_the_session_terminal(self, storage):
        session = storage.begin_upload("packages", "release.bin")
        part = _upload_part(storage, session, 1, b"partial")

        storage.abort_upload(session)
        storage.abort_upload(session)

        with pytest.raises(StorageUploadSessionError, match="not active"):
            storage.complete_upload(session, [part])
        assert storage.exists("packages", "release.bin") is False

    def test_cleanup_removes_only_sessions_older_than_the_cutoff(self, storage, monkeypatch):
        monkeypatch.setattr(storage_local.time, "time", lambda: 10.0)
        old_session = storage.begin_upload("packages", "old.bin")
        monkeypatch.setattr(storage_local.time, "time", lambda: 20.0)
        current_session = storage.begin_upload("packages", "current.bin")

        assert storage.cleanup_uploads(older_than_epoch=15.0) == 1

        with pytest.raises(StorageUploadSessionError, match="not active"):
            _upload_part(storage, old_session, 1, b"old")
        current = _upload_part(storage, current_session, 1, b"current")
        assert storage.complete_upload(current_session, [current]) == 7

    def test_a_symlinked_staged_part_is_rejected(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        outside = tmp_path / "outside"
        outside.write_bytes(b"outside")
        parts = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_PARTS_DIRECTORY
        (parts / "00001.part").symlink_to(outside)

        with pytest.raises(StorageUploadSessionError, match="unsafe"):
            _upload_part(storage, session, 2, b"data")

    def test_a_symlinked_parts_directory_is_rejected(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        session_directory = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id
        parts = session_directory / storage_local._UPLOAD_PARTS_DIRECTORY
        parts.rmdir()
        outside = tmp_path / "outside-parts"
        outside.mkdir()
        parts.symlink_to(outside, target_is_directory=True)

        with pytest.raises(StorageUploadSessionError, match="unsafe"):
            _upload_part(storage, session, 1, b"data")

    def test_the_private_staging_area_is_reserved(self, storage):
        with pytest.raises(ValueError, match="reserved"):
            storage.begin_upload(storage_local._UPLOADS_DIRECTORY, "release.bin")

    def test_malformed_and_mismatched_sessions_are_rejected(self, storage):
        session = storage.begin_upload("packages", "release.bin")

        with pytest.raises(StorageUploadSessionError, match="not valid"):
            _upload_part(storage, replace(session, session_id="not-a-uuid"), 1, b"data")
        with pytest.raises(StorageUploadSessionError, match="durable state"):
            _upload_part(storage, replace(session, key="other.bin"), 1, b"data")
        with pytest.raises(StorageUploadSessionError, match="not valid"):
            _upload_part(storage, replace(session, max_parts=session.max_parts - 1), 1, b"data")

    def test_a_symlinked_upload_root_is_rejected(self, tmp_path):
        base = tmp_path / "storage"
        base.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        (base / storage_local._UPLOADS_DIRECTORY).symlink_to(outside, target_is_directory=True)
        storage = LocalStorage(str(base))

        with pytest.raises(StorageBackendUnavailableError, match="unsafe"):
            storage.begin_upload("packages", "release.bin")

    def test_an_upload_root_resolution_failure_is_provider_neutral(self, storage):
        unavailable_base = Mock()
        unavailable_base.resolve.side_effect = OSError("storage unavailable")
        storage._base = unavailable_base

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage._upload_root()

        assert isinstance(excinfo.value.__cause__, OSError)

    def test_a_symlinked_session_manifest_is_rejected(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        manifest = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_MANIFEST
        manifest.unlink()
        outside = tmp_path / "outside.json"
        outside.write_text("{}")
        manifest.symlink_to(outside)

        with pytest.raises(StorageUploadSessionError, match="unsafe"):
            _upload_part(storage, session, 1, b"data")

    def test_a_corrupt_session_manifest_is_rejected(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        manifest = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_MANIFEST
        manifest.write_text("{")

        with pytest.raises(StorageUploadSessionError, match="manifest is invalid"):
            _upload_part(storage, session, 1, b"data")

    def test_a_non_finite_session_timestamp_is_rejected(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        manifest_path = (
            tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_MANIFEST
        )
        manifest = json.loads(manifest_path.read_text())
        manifest["created_epoch"] = float("inf")
        manifest_path.write_text(json.dumps(manifest))

        with pytest.raises(StorageUploadSessionError, match="durable state"):
            _upload_part(storage, session, 1, b"data")

    def test_a_manifest_read_failure_is_provider_neutral(self, storage, monkeypatch):
        session = storage.begin_upload("packages", "release.bin")
        monkeypatch.setattr(Path, "read_text", Mock(side_effect=OSError("storage unavailable")))

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            _upload_part(storage, session, 1, b"data")

        assert isinstance(excinfo.value.__cause__, OSError)

    @pytest.mark.parametrize("staged_name", ["bad.part", "00001.part"])
    def test_invalid_staged_part_state_is_rejected(self, storage, tmp_path, staged_name):
        session = storage.begin_upload("packages", "release.bin")
        parts = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_PARTS_DIRECTORY
        staged = parts / staged_name
        if staged_name == "00001.part":
            staged.mkdir()
            match = "not a file"
        else:
            staged.write_bytes(b"data")
            match = "name is invalid"

        with pytest.raises(StorageUploadSessionError, match=match):
            _upload_part(storage, session, 2, b"data")

    def test_duplicate_staged_part_names_are_rejected(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        parts = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_PARTS_DIRECTORY
        (parts / "00001.part").write_bytes(b"first")
        (parts / "1.part").write_bytes(b"duplicate")

        with pytest.raises(StorageUploadSessionError, match="duplicate"):
            _upload_part(storage, session, 2, b"data")

    @pytest.mark.parametrize("part_number", [0, 10_001])
    def test_part_numbers_outside_the_portable_range_are_rejected(self, storage, part_number):
        session = storage.begin_upload("packages", "release.bin")

        with pytest.raises(ValueError, match="between 1 and 10000"):
            _upload_part(storage, session, part_number, b"data")

    @pytest.mark.parametrize(
        ("parts", "error", "match"),
        [
            ([], ValueError, "At least one"),
            ([PartRef(1, 0, '"etag"')] * 10_001, ValueError, "at most"),
            ([PartRef(1, -1, '"etag"')], ValueError, "size"),
            ([PartRef(1, 0, "")], ValueError, "validator"),
        ],
    )
    def test_completion_validates_part_references_before_reading_staging(self, storage, parts, error, match):
        session = storage.begin_upload("packages", "release.bin")

        with pytest.raises(error, match=match):
            storage.complete_upload(session, parts)

    def test_completion_rechecks_the_total_limit(self, storage):
        session = storage.begin_upload("packages", "release.bin", max_bytes=1)
        part = _upload_part(storage, session, 1, b"x")

        with pytest.raises(StorageSizeLimitError, match="max_bytes=1"):
            storage.complete_upload(session, [replace(part, size=2)])

    def test_an_oversized_part_is_rejected_before_writing(self, storage, monkeypatch):
        monkeypatch.setattr(storage_local, "_UPLOAD_MAX_PART_SIZE", 3)
        session = storage.begin_upload("packages", "release.bin")

        with pytest.raises(StorageSizeLimitError, match="max_part_size=3"):
            _upload_part(storage, session, 1, b"four")

    def test_a_part_staging_failure_is_provider_neutral(self, storage, monkeypatch):
        session = storage.begin_upload("packages", "release.bin")
        original_open = Path.open

        def fail_part_staging(path, *args, **kwargs):
            if path.name.endswith(".tmp"):
                raise OSError("storage unavailable")
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(Path, "open", fail_part_staging)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            _upload_part(storage, session, 1, b"data")

        assert isinstance(excinfo.value.__cause__, OSError)

    def test_a_part_size_change_is_rejected_before_assembly(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        part = _upload_part(storage, session, 1, b"data")
        part_path = (
            tmp_path
            / storage_local._UPLOADS_DIRECTORY
            / session.session_id
            / storage_local._UPLOAD_PARTS_DIRECTORY
            / "00001.part"
        )
        part_path.write_bytes(b"changed")

        with pytest.raises(StorageUploadSessionError, match="size does not match"):
            storage.complete_upload(session, [part])

    @pytest.mark.parametrize(
        ("failure", "error"),
        [(FileNotFoundError(), StorageUploadSessionError), (OSError(), StorageBackendUnavailableError)],
    )
    def test_part_open_failures_are_translated_during_assembly(self, storage, monkeypatch, failure, error):
        session = storage.begin_upload("packages", "release.bin")
        part = _upload_part(storage, session, 1, b"data")
        original_open = Path.open

        def fail_part_open(path, *args, **kwargs):
            if path.name == "00001.part":
                raise failure
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(Path, "open", fail_part_open)

        with pytest.raises(error):
            storage.complete_upload(session, [part])

    def test_a_completed_object_survives_staging_cleanup_failure(self, storage, monkeypatch, caplog):
        session = storage.begin_upload("packages", "release.bin")
        part = _upload_part(storage, session, 1, b"data")
        monkeypatch.setattr(storage_local.shutil, "rmtree", Mock(side_effect=OSError("storage unavailable")))

        with caplog.at_level("WARNING"):
            assert storage.complete_upload(session, [part]) == 4

        assert storage.get("packages", "release.bin") == b"data"
        assert "Failed to remove" in caplog.text

    def test_an_abort_failure_is_provider_neutral(self, storage, monkeypatch):
        session = storage.begin_upload("packages", "release.bin")
        monkeypatch.setattr(storage_local.shutil, "rmtree", Mock(side_effect=OSError("storage unavailable")))

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.abort_upload(session)

        assert isinstance(excinfo.value.__cause__, OSError)

    def test_cleanup_requires_a_finite_cutoff(self, storage):
        with pytest.raises(ValueError, match="finite"):
            storage.cleanup_uploads(older_than_epoch=float("nan"))

    def test_cleanup_of_an_uninitialized_backend_is_empty(self, storage):
        assert storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0) == 0

    def test_cleanup_listing_failures_are_provider_neutral(self, storage, monkeypatch):
        upload_root = Mock()
        upload_root.iterdir.side_effect = OSError("storage unavailable")
        monkeypatch.setattr(storage, "_upload_root", lambda: upload_root)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0)

        assert isinstance(excinfo.value.__cause__, OSError)

    def test_cleanup_ignores_non_session_entries(self, storage, tmp_path):
        upload_root = tmp_path / storage_local._UPLOADS_DIRECTORY
        upload_root.mkdir()
        (upload_root / "not-a-session").write_bytes(b"data")

        assert storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0) == 0

    def test_cleanup_uses_directory_time_for_a_corrupt_manifest(self, storage, tmp_path):
        session = storage.begin_upload("packages", "release.bin")
        session_directory = tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id
        (session_directory / storage_local._UPLOAD_MANIFEST).write_text("{")
        os.utime(session_directory, (10.0, 10.0))

        assert storage.cleanup_uploads(older_than_epoch=20.0) == 1

    def test_manifest_timestamp_tampering_cannot_postpone_cleanup(self, storage, tmp_path, monkeypatch):
        monkeypatch.setattr(storage_local.time, "time", lambda: 10.0)
        session = storage.begin_upload("packages", "release.bin")
        manifest_path = (
            tmp_path / storage_local._UPLOADS_DIRECTORY / session.session_id / storage_local._UPLOAD_MANIFEST
        )
        manifest = json.loads(manifest_path.read_text())
        manifest["created_epoch"] = 1_000_000_000_000.0
        manifest_path.write_text(json.dumps(manifest))

        assert storage.cleanup_uploads(older_than_epoch=20.0) == 1

    @pytest.mark.parametrize("failure", [FileNotFoundError(), OSError("storage unavailable")])
    def test_cleanup_handles_removal_races_and_failures(self, storage, monkeypatch, failure):
        storage.begin_upload("packages", "release.bin")
        monkeypatch.setattr(storage_local.shutil, "rmtree", Mock(side_effect=failure))

        if isinstance(failure, FileNotFoundError):
            assert storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0) == 0
        else:
            with pytest.raises(StorageBackendUnavailableError) as excinfo:
                storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0)
            assert isinstance(excinfo.value.__cause__, OSError)

    def test_cleanup_attempts_later_sessions_before_reporting_a_failure(self, storage, monkeypatch):
        first = storage.begin_upload("packages", "first.bin")
        second = storage.begin_upload("packages", "second.bin")
        first_path = storage._upload_root() / first.session_id
        second_path = storage._upload_root() / second.session_id
        original_rmtree = storage_local.shutil.rmtree

        def remove(path, *args, **kwargs):
            if path == first_path:
                raise OSError("storage unavailable")
            return original_rmtree(path, *args, **kwargs)

        monkeypatch.setattr(storage_local.shutil, "rmtree", remove)

        with pytest.raises(StorageBackendUnavailableError):
            storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0)

        assert first_path.exists() is True
        assert second_path.exists() is False

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

    def test_deleting_prunes_empty_directories_but_not_the_storage_root(self, storage, tmp_path):
        storage.save("thumbnails", "nested/1.webp", b"x")

        storage.delete("thumbnails", "nested/1.webp")

        assert (tmp_path / "thumbnails").exists() is False
        assert tmp_path.is_dir()

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

    def test_stat_reports_portable_filesystem_metadata(self, storage):
        storage.save("thumbnails", "1.webp", b"bytes", content_type="image/webp")

        metadata = storage.stat("thumbnails", "1.webp")

        assert metadata is not None
        assert metadata.size == 5
        assert metadata.modified_epoch > 0
        assert metadata.content_type is None
        assert metadata.etag is None

    def test_a_directory_has_no_object_metadata(self, storage, tmp_path):
        (tmp_path / "thumbnails" / "directory").mkdir(parents=True)

        assert storage.stat("thumbnails", "directory") is None

    @pytest.mark.parametrize("operation", ["stat", "serve", "delete_prefix"])
    def test_object_inspection_failures_are_provider_neutral(self, storage, monkeypatch, operation):
        unavailable_path = Mock()
        unavailable_path.stat.side_effect = OSError("storage unavailable")
        unavailable_path.open.side_effect = OSError("storage unavailable")
        if operation == "delete_prefix":

            def fail_to_resolve_legacy(_area, _key):
                error = OSError("storage unavailable")
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

            monkeypatch.setattr(storage, "_resolve_legacy", fail_to_resolve_legacy)
        else:
            monkeypatch.setattr(storage, "_resolve", lambda _area, _key: unavailable_path)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            getattr(storage, operation)("thumbnails", "1.webp")

        assert isinstance(excinfo.value.__cause__, OSError)

    def test_serve_returns_a_readable_absolute_file(self, storage):
        storage.save("thumbnails", "1.webp", b"bytes")

        plan = storage.serve("thumbnails", "1.webp")

        assert isinstance(plan, ServeFile)
        assert plan.path.is_absolute()
        assert plan.path.read_bytes() == b"bytes"

    def test_a_url_is_built_from_the_prefix(self, storage):
        assert storage.url("thumbnails", "1.webp") == _local_object_url(storage, "thumbnails", "1.webp")

    @pytest.mark.parametrize(
        "key",
        [
            "a b.webp",
            "a?b.webp",
            "a#b.webp",
            "100%.webp",
        ],
    )
    def test_a_key_with_url_metacharacters_maps_to_a_safe_physical_url(self, storage, key):
        url = storage.url("thumbnails", key)

        assert url == _local_object_url(storage, "thumbnails", key)
        assert not any(character in url for character in (" ", "?", "#", "%"))

    def test_a_nested_key_maps_to_its_physical_url(self, storage):
        assert storage.url("thumbnails", "2026/01/1.webp") == _local_object_url(
            storage,
            "thumbnails",
            "2026/01/1.webp",
        )

    def test_a_requested_expiry_is_reported_as_ignored(self, storage, caplog):
        """The one place the two storage backends genuinely differ.

        S3 returns a presigned URL that stops working; this backend returns a
        path for the host's own web server and cannot expire it. A caller who
        asked for a lifetime and is silently not getting one would otherwise be
        relying on an authorization control that does not exist.
        """
        with caplog.at_level("WARNING"):
            url = storage.url("thumbnails", "1.webp", expires_in=60)

        assert url == _local_object_url(storage, "thumbnails", "1.webp")
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

        assert url == _local_object_url(storage, "thumbnails", "1.webp")
        assert "download_as, content_type were ignored" in caplog.text
        assert "permanent" in caplog.text

    def test_serve_and_url_share_one_ignored_control_warning(self, storage, caplog):
        storage.save("thumbnails", "1.webp", b"bytes")

        with caplog.at_level("WARNING"):
            storage.serve("thumbnails", "1.webp", download_as="photo.webp")
            storage.url("thumbnails", "1.webp", download_as="photo.webp")

        assert caplog.text.count("was ignored") == 1

    def test_delete_prefix_prunes_its_empty_parent_directories(self, storage, tmp_path):
        storage.save("thumbnails", "packages/1/a.webp", b"a")

        assert storage.delete_prefix("thumbnails", "packages/1") == 1
        assert (tmp_path / "thumbnails").exists() is False
        assert tmp_path.is_dir()

    def test_delete_prefix_refuses_a_symlink_into_another_area(self, storage, tmp_path):
        storage.save("archive", "keep.webp", b"keep")
        source_area = tmp_path / "thumbnails"
        source_area.mkdir()
        (source_area / "linked").symlink_to(tmp_path / "archive", target_is_directory=True)

        with pytest.raises(ValueError, match="symbolic link"):
            storage.delete_prefix("thumbnails", "linked")

        assert storage.get("archive", "keep.webp") == b"keep"

    def test_copy_failures_are_provider_neutral(self, storage, monkeypatch):
        unavailable_path = Mock()
        unavailable_path.stat.side_effect = OSError("storage unavailable")
        monkeypatch.setattr(storage, "_resolve", lambda _area, _key: unavailable_path)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.copy("thumbnails", "source.webp", "archive", "destination.webp")

        assert isinstance(excinfo.value.__cause__, OSError)

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

    def test_an_in_area_symlink_alias_is_not_listed(self, storage, tmp_path):
        storage.save("thumbnails", "original.webp", b"original")
        original = storage._resolve("thumbnails", "original.webp")
        alias = storage._resolve("thumbnails", "alias.webp")
        alias.parent.mkdir(parents=True)
        alias.symlink_to(original)

        assert storage.list_keys("thumbnails") == ["original.webp"]


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
