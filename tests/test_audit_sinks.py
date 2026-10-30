"""The external checkpoint sink: write-once copies kept outside the database."""

import base64
import json
import os
import stat
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

import jasil.audit as jasil_audit
import jasil.audit.checkpoints as checkpoints
import jasil.container as container
import jasil.settings as jasil_settings
from jasil.audit import AuditAction, Category, Outcome, Severity
from jasil.audit.models import AuditCheckpoint, Instance
from jasil.backends.audit_sink_file import FileAuditSink
from jasil.backends.audit_sink_storage import AUDIT_SINK_AREA, StorageAuditSink
from jasil.providers import AuditSink, AuditSinkConflictError

T0 = datetime(2026, 1, 1, 12, tzinfo=UTC)


class TestFileSink:
    def test_it_stores_the_bytes_under_the_name(self, tmp_path):
        FileAuditSink(tmp_path).write_once("a.json", b"one")

        assert (tmp_path / "a.json").read_bytes() == b"one"

    def test_the_file_is_read_only(self, tmp_path):
        FileAuditSink(tmp_path).write_once("a.json", b"one")

        mode = stat.S_IMODE(os.stat(tmp_path / "a.json").st_mode)
        assert not mode & 0o222  # no write bit (Windows can only express read-only)

    def test_a_copy_left_writable_by_a_crash_is_made_read_only_by_the_retry(self, tmp_path):
        left = tmp_path / "a.json"
        left.write_bytes(b"one")  # published, then the process stopped before making it read-only
        left.chmod(0o600)

        FileAuditSink(tmp_path).write_once("a.json", b"one")

        assert not stat.S_IMODE(os.stat(left).st_mode) & 0o222

    def test_writing_the_same_bytes_again_is_a_no_op(self, tmp_path):
        sink = FileAuditSink(tmp_path)
        sink.write_once("a.json", b"one")

        sink.write_once("a.json", b"one")

        assert (tmp_path / "a.json").read_bytes() == b"one"

    def test_different_bytes_under_a_taken_name_are_refused_and_the_file_is_untouched(self, tmp_path):
        sink = FileAuditSink(tmp_path)
        sink.write_once("a.json", b"one")

        with pytest.raises(AuditSinkConflictError, match="different content"):
            sink.write_once("a.json", b"two")

        assert (tmp_path / "a.json").read_bytes() == b"one"

    @pytest.mark.parametrize("name", ["", "../a.json", "a/b.json", ".hidden", "a\0b", "/etc/passwd", "a b"])
    def test_a_name_that_is_not_a_plain_file_name_is_refused(self, tmp_path, name):
        with pytest.raises(ValueError, match="invalid audit sink object name"):
            FileAuditSink(tmp_path).write_once(name, b"x")

    def test_a_failed_write_leaves_nothing_under_the_final_name_and_a_retry_succeeds(self, tmp_path, monkeypatch):
        sink = FileAuditSink(tmp_path)
        monkeypatch.setattr(os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError("disk full")))

        with pytest.raises(OSError, match="disk full"):
            sink.write_once("a.json", b"one")
        assert list(tmp_path.iterdir()) == []  # no truncated file, no temporary leftover

        monkeypatch.undo()
        sink.write_once("a.json", b"one")
        assert (tmp_path / "a.json").read_bytes() == b"one"

    @pytest.mark.skipif(os.name == "nt", reason="Windows cannot open a directory for fsync")
    def test_the_directory_entry_is_synced_after_publishing(self, tmp_path, monkeypatch):
        synced = []
        real = os.fsync

        def recording(descriptor):
            synced.append((stat.S_ISDIR(os.fstat(descriptor).st_mode), (tmp_path / "a.json").exists()))
            real(descriptor)

        monkeypatch.setattr(os, "fsync", recording)

        FileAuditSink(tmp_path).write_once("a.json", b"one")

        assert synced == [(False, False), (True, True)]  # the file before the name exists, the directory after

    @pytest.mark.skipif(os.name == "nt", reason="Windows cannot open a directory for fsync")
    def test_a_failed_directory_sync_propagates_and_the_retry_succeeds(self, tmp_path, monkeypatch):
        sink = FileAuditSink(tmp_path)
        real = os.fsync

        def failing_on_directories(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError("directory sync failed")
            real(descriptor)

        monkeypatch.setattr(os, "fsync", failing_on_directories)
        with pytest.raises(OSError, match="directory sync failed"):
            sink.write_once("a.json", b"one")

        monkeypatch.undo()
        sink.write_once("a.json", b"one")  # identical bytes are already there: succeeds, and syncs the name

        assert (tmp_path / "a.json").read_bytes() == b"one"

    def test_no_temporary_file_is_left_behind(self, tmp_path):
        sink = FileAuditSink(tmp_path)
        sink.write_once("a.json", b"one")
        sink.write_once("a.json", b"one")

        assert [path.name for path in tmp_path.iterdir()] == ["a.json"]

    def test_it_reads_back_what_it_stored_and_none_for_a_missing_name(self, tmp_path):
        sink = FileAuditSink(tmp_path)
        sink.write_once("a.json", b"one")

        assert (sink.read("a.json"), sink.read("b.json")) == (b"one", None)

    def test_it_creates_its_directory(self, tmp_path):
        FileAuditSink(tmp_path / "deep" / "er").write_once("a.json", b"x")

        assert (tmp_path / "deep" / "er" / "a.json").exists()

    def test_it_is_built_from_a_file_uri(self, tmp_path):
        sink = FileAuditSink.from_uri((tmp_path / "sink").as_uri())

        sink.write_once("a.json", b"x")

        assert (tmp_path / "sink" / "a.json").exists()

    @pytest.mark.parametrize("uri", ["file://relative/dir", "file://host/abs", "file:relative"])
    def test_a_uri_without_an_absolute_path_is_refused(self, uri):
        with pytest.raises(ValueError, match="file:///absolute/directory"):
            FileAuditSink.from_uri(uri)

    @pytest.mark.parametrize("suffix", ["?mode=append", "#copies"])
    def test_a_uri_with_a_query_or_fragment_is_refused(self, tmp_path, suffix):
        with pytest.raises(ValueError, match="file:///absolute/directory"):
            FileAuditSink.from_uri((tmp_path / "sink").as_uri() + suffix)


class CreateOnlyStorage:
    """The two calls the storage sink makes, with S3's conditional ``PutObject`` semantics."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.creates: list[tuple[str, str, str | None]] = []

    def create(self, area: str, key: str, data: bytes, content_type: str | None = None) -> bool:
        self.creates.append((area, key, content_type))
        if (area, key) in self.objects:
            return False
        self.objects[area, key] = data
        return True

    def get(self, area: str, key: str) -> bytes | None:
        return self.objects.get((area, key))


class TestStorageSink:
    @pytest.fixture
    def storage(self):
        return CreateOnlyStorage()

    def test_it_stores_the_bytes_in_its_area(self, storage):
        StorageAuditSink(storage).write_once("a.json", b"one")

        assert storage.get(AUDIT_SINK_AREA, "a.json") == b"one"
        assert storage.creates == [(AUDIT_SINK_AREA, "a.json", "application/json")]

    def test_writing_the_same_bytes_again_is_a_no_op(self, storage):
        sink = StorageAuditSink(storage)
        sink.write_once("a.json", b"one")

        sink.write_once("a.json", b"one")

        assert storage.get(AUDIT_SINK_AREA, "a.json") == b"one"

    def test_the_write_is_conditional_rather_than_checked_first(self, storage, monkeypatch):
        """A read before an unconditional write would let a racing writer replace the copy in between."""
        monkeypatch.setattr(storage, "get", lambda *args: pytest.fail("read before writing"))

        StorageAuditSink(storage).write_once("a.json", b"one")

    def test_different_bytes_under_a_taken_name_are_refused(self, storage):
        sink = StorageAuditSink(storage)
        sink.write_once("a.json", b"one")

        with pytest.raises(AuditSinkConflictError):
            sink.write_once("a.json", b"two")

        assert storage.get(AUDIT_SINK_AREA, "a.json") == b"one"

    def test_it_reads_back_what_it_stored_and_none_for_a_missing_name(self, storage):
        sink = StorageAuditSink(storage)
        sink.write_once("a.json", b"one")

        assert (sink.read("a.json"), sink.read("b.json")) == (b"one", None)

    def test_both_sinks_satisfy_the_provider_protocol(self, storage, tmp_path):
        assert isinstance(StorageAuditSink(storage), AuditSink)
        assert isinstance(FileAuditSink(tmp_path), AuditSink)


class TestPlatformWiring:
    def _build(self, **audit):
        return container.build_platform(
            jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(**audit), data_dir="data-unused")
        )

    def test_no_sink_by_default(self):
        assert self._build().audit_sink is None

    def test_a_file_uri_selects_the_file_sink(self, tmp_path):
        platform = self._build(external_sink_uri=(tmp_path / "sink").as_uri())

        assert isinstance(platform.audit_sink, FileAuditSink)

    def test_an_s3_uri_selects_the_storage_sink_over_an_s3_backend(self, monkeypatch):
        import jasil.backends.storage_s3 as storage_s3

        built = []
        monkeypatch.setattr(
            storage_s3.S3Storage, "from_uri", classmethod(lambda cls, uri: built.append(uri) or object())
        )

        platform = self._build(external_sink_uri="s3://evidence-bucket/audit?region=eu-west-1")

        assert isinstance(platform.audit_sink, StorageAuditSink)
        assert built == ["s3://evidence-bucket/audit?region=eu-west-1"]

    def test_an_unknown_scheme_fails_the_build(self):
        with pytest.raises(ValueError, match="external_sink_uri scheme"):
            self._build(external_sink_uri="ftp://example.test/x")

    def test_a_disabled_audit_builds_no_sink(self, tmp_path):
        assert self._build(enabled=False, external_sink_uri=(tmp_path / "sink").as_uri()).audit_sink is None
        assert not (tmp_path / "sink").exists()


class TestSealingThroughTheSink:
    @pytest.fixture
    def trail(self, audit_env, tmp_path, session_factory):
        action = jasil_audit.get_catalog().register(
            AuditAction("shop.order.place", Category.SYSTEM, Severity.LOW, "k", "d")
        )
        self.directory = tmp_path / "sink"
        audit_env.configure(external_sink_uri=self.directory.as_uri())
        audit_env.clock.moment = T0 + timedelta(minutes=3)
        jasil_audit.record_detached(action, outcome=Outcome.SUCCESS, target_id="a")
        audit_env.clock.moment = T0 + timedelta(minutes=60)
        self.env, self.session_factory = audit_env, session_factory
        return audit_env

    def _stored(self):
        with self.session_factory() as db:
            return list(db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start)))

    def _name(self, window: str) -> str:
        """The sink object of the window starting at ``window``: named after the deployment, then the window."""
        with self.session_factory() as db:
            instance_id = db.scalar(select(Instance.instance_id))
        return f"checkpoint-{instance_id}-{window}.json"

    def test_every_checkpoint_is_copied_to_the_sink(self, trail):
        assert checkpoints.seal_pending_windows() == 3

        files = sorted(path.name for path in self.directory.iterdir())
        assert files == [self._name("20260101T120000Z"), self._name("20260101T121500Z"), self._name("20260101T123000Z")]

    def test_the_copy_matches_the_stored_checkpoint(self, trail):
        checkpoints.seal_pending_windows()

        stored = self._stored()[0]
        copy = json.loads((self.directory / self._name("20260101T120000Z")).read_text())
        assert copy["hash"] == stored.hash
        assert (copy["row_count"], copy["prev_hash"], copy["signature"], copy["key_id"]) == (1, None, None, None)
        assert (copy["retention_days"], copy["legal_hold"]) == (stored.retention_days, stored.legal_hold)
        assert copy["window_start"] == "2026-01-01T12:00:00.000000+00:00"
        assert copy["instance_id"]

    def test_a_signed_copy_carries_the_signature(self, trail):
        trail.configure(signing_key=base64.b64encode(bytes(range(32))).decode())

        checkpoints.seal_pending_windows()

        copy = json.loads((self.directory / self._name("20260101T120000Z")).read_text())
        assert copy["signature"] == self._stored()[0].signature
        assert copy["key_id"]

    def test_an_unreachable_sink_stores_no_checkpoint_and_the_next_run_catches_up(self, trail, monkeypatch):
        sink = trail.platform.audit_sink
        monkeypatch.setattr(sink, "write_once", lambda name, content: (_ for _ in ()).throw(OSError("sink down")))

        with pytest.raises(OSError, match="sink down"):
            checkpoints.seal_pending_windows()
        assert self._stored() == []

        monkeypatch.undo()
        assert checkpoints.seal_pending_windows() == 3
        assert len(list(self.directory.iterdir())) == 3

    def test_a_crash_between_the_sink_and_the_database_is_retried_identically(self, trail):
        checkpoints.seal_pending_windows()
        with self.session_factory() as db:
            db.delete(db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start.desc())).first())
            db.commit()

        assert checkpoints.seal_pending_windows() == 1  # the sink already holds identical bytes

    def _lose_the_newest_database_write(self) -> None:
        """What a crash between the sink write and the database commit leaves: the copy, and no row."""
        with self.session_factory() as db:
            db.delete(db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start.desc())).first())
            db.commit()

    @pytest.mark.parametrize("change", [{"legal_hold": True}, {"retention_days": 45}])
    @pytest.mark.parametrize("signed", [False, True])
    def test_a_retry_under_changed_settings_adopts_the_copy_the_sink_holds(self, trail, change, signed):
        """Retention or the hold changed before the retry; the immutable copy must not stop sealing for good."""
        if signed:
            trail.configure(signing_key=base64.b64encode(bytes(range(32))).decode())
        checkpoints.seal_pending_windows()
        self._lose_the_newest_database_write()
        trail.configure(**change)

        assert checkpoints.seal_pending_windows() == 1

        newest = self._stored()[-1]
        copy = json.loads((self.directory / self._name("20260101T123000Z")).read_text())
        assert (newest.retention_days, newest.legal_hold) == (90, False)
        assert newest.signature == copy["signature"]
        assert jasil_audit.verify_checkpoints(None, T0, T0 + timedelta(hours=1)).ok

    def test_a_copy_of_other_rows_is_still_a_conflict(self, trail):
        checkpoints.seal_pending_windows()
        self._lose_the_newest_database_write()
        target = self.directory / self._name("20260101T123000Z")
        copy = json.loads(target.read_text())
        target.chmod(0o600)
        target.write_text(json.dumps({**copy, "hash": "f" * 64}, sort_keys=True, separators=(",", ":")))

        with pytest.raises(AuditSinkConflictError):
            checkpoints.seal_pending_windows()
        assert len(self._stored()) == 2

    def test_a_tampered_sink_object_stops_sealing_loudly(self, trail):
        checkpoints.seal_pending_windows()
        with self.session_factory() as db:
            db.delete(db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start.desc())).first())
            db.commit()
        target = self.directory / self._name("20260101T123000Z")
        target.chmod(0o600)
        target.write_text("{}")

        with pytest.raises(AuditSinkConflictError):
            checkpoints.seal_pending_windows()

    def test_no_sink_means_no_copy(self, audit_env, trail, tmp_path):
        audit_env.configure(external_sink_uri=None)

        checkpoints.seal_pending_windows()

        assert list(self.directory.iterdir()) == []
