"""The external checkpoint sink: write-once copies kept outside the database."""

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
from jasil.audit.models import AuditCheckpoint
from jasil.backends.audit_sink_file import FileAuditSink
from jasil.backends.audit_sink_storage import AUDIT_SINK_AREA, StorageAuditSink
from jasil.backends.storage_local import LocalStorage
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

    def test_no_temporary_file_is_left_behind(self, tmp_path):
        sink = FileAuditSink(tmp_path)
        sink.write_once("a.json", b"one")
        sink.write_once("a.json", b"one")

        assert [path.name for path in tmp_path.iterdir()] == ["a.json"]

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


class TestStorageSink:
    @pytest.fixture
    def storage(self, tmp_path):
        return LocalStorage(str(tmp_path))

    def test_it_stores_the_bytes_in_its_area(self, storage):
        StorageAuditSink(storage).write_once("a.json", b"one")

        assert storage.get(AUDIT_SINK_AREA, "a.json") == b"one"

    def test_writing_the_same_bytes_again_does_not_write(self, storage, monkeypatch):
        sink = StorageAuditSink(storage)
        sink.write_once("a.json", b"one")
        monkeypatch.setattr(storage, "save", lambda *args, **kwargs: pytest.fail("rewrote an existing object"))

        sink.write_once("a.json", b"one")

    def test_different_bytes_under_a_taken_name_are_refused(self, storage):
        sink = StorageAuditSink(storage)
        sink.write_once("a.json", b"one")

        with pytest.raises(AuditSinkConflictError):
            sink.write_once("a.json", b"two")

        assert storage.get(AUDIT_SINK_AREA, "a.json") == b"one"

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

    def test_every_checkpoint_is_copied_to_the_sink(self, trail):
        assert checkpoints.seal_pending_windows() == 3

        files = sorted(path.name for path in self.directory.iterdir())
        assert files == [
            "checkpoint-20260101T120000Z.json",
            "checkpoint-20260101T121500Z.json",
            "checkpoint-20260101T123000Z.json",
        ]

    def test_the_copy_matches_the_stored_checkpoint(self, trail):
        checkpoints.seal_pending_windows()

        stored = self._stored()[0]
        copy = json.loads((self.directory / "checkpoint-20260101T120000Z.json").read_text())
        assert copy["hash"] == stored.hash
        assert (copy["row_count"], copy["prev_hash"], copy["signature"], copy["key_id"]) == (1, None, None, None)
        assert copy["window_start"] == "2026-01-01T12:00:00.000000+00:00"
        assert copy["instance_id"]

    def test_a_signed_copy_carries_the_signature(self, trail):
        import base64

        trail.configure(signing_key=base64.b64encode(bytes(range(32))).decode())

        checkpoints.seal_pending_windows()

        copy = json.loads((self.directory / "checkpoint-20260101T120000Z.json").read_text())
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

    def test_a_tampered_sink_object_stops_sealing_loudly(self, trail):
        checkpoints.seal_pending_windows()
        with self.session_factory() as db:
            db.delete(db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start.desc())).first())
            db.commit()
        target = self.directory / "checkpoint-20260101T123000Z.json"
        target.chmod(0o600)
        target.write_text("{}")

        with pytest.raises(AuditSinkConflictError):
            checkpoints.seal_pending_windows()

    def test_no_sink_means_no_copy(self, audit_env, trail, tmp_path):
        audit_env.configure(external_sink_uri=None)

        checkpoints.seal_pending_windows()

        assert list(self.directory.iterdir()) == []
