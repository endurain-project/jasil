"""Signed, hash-chained checkpoints: sealing, the grace period, and detecting each kind of tampering.

The point of a checkpoint is what it catches, so most of this file changes a sealed log the way an
attacker with database access would (these tests run on tables without the PostgreSQL triggers) and
asserts that verification names the damage.
"""

import base64
import itertools
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, insert, select, update

import jasil.audit as jasil_audit
import jasil.audit.checkpoints as checkpoints
from jasil._core.timestamps import as_utc
from jasil._core.uuid7 import uuid7
from jasil.audit import AuditAction, Category, Outcome, Severity
from jasil.audit.models import AuditCheckpoint, AuditLog, Instance

T0 = datetime(2026, 1, 1, 12, tzinfo=UTC)
WINDOW = timedelta(minutes=15)


def at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


@pytest.fixture
def action(audit_env):
    return jasil_audit.get_catalog().register(
        AuditAction("shop.order.place", Category.DATA_CHANGE, Severity.LOW, "k", "d")
    )


@pytest.fixture
def keys():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    def make() -> tuple[str, str]:
        key = Ed25519PrivateKey.generate()
        raw = serialization.NoEncryption()
        private = key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, raw)
        public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        return base64.b64encode(private).decode(), base64.b64encode(public).decode()

    return make


class Trail:
    """Writes rows at chosen minutes and seals the windows behind a chosen 'now'."""

    def __init__(self, audit_env, action, session_factory) -> None:
        self.env, self.action, self.session_factory = audit_env, action, session_factory

    def write(self, minute: int, **fields) -> None:
        self.env.clock.moment = at(minute)
        jasil_audit.record_detached(self.action, outcome=fields.pop("outcome", Outcome.SUCCESS), **fields)

    def seal(self, minute: int) -> int:
        self.env.clock.moment = at(minute)
        return checkpoints.seal_pending_windows()

    def all(self) -> list[AuditCheckpoint]:
        with self.session_factory() as db:
            return list(db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start)))

    def verify(self, public_key=None, start=-60, end=600):
        return jasil_audit.verify_checkpoints(public_key, at(start), at(end))

    def statuses(self, public_key=None, **kwargs) -> list[str]:
        return [window.status.value for window in self.verify(public_key, **kwargs).windows]

    def reseal(self, minute: int, *, prev_hash: str) -> None:
        """Rewrite a checkpoint the way an attacker holding the database would: consistent, but re-linked."""
        with self.session_factory() as db:
            checkpoint = db.get(AuditCheckpoint, at(minute))
            digest = checkpoints._digest(db, at(minute), at(minute) + WINDOW, prev_hash)
            checkpoint.prev_hash, checkpoint.hash = prev_hash, digest.hash
            db.commit()

    def execute(self, statement) -> None:
        with self.session_factory() as db:
            db.execute(statement)
            db.commit()


@pytest.fixture
def trail(audit_env, action, session_factory):
    return Trail(audit_env, action, session_factory)


@pytest.fixture
def sealed(trail):
    """Three windows with rows in the first and third, all sealed."""
    trail.write(3, target_id="a")
    trail.write(4, target_id="b")
    trail.write(35, target_id="c")
    assert trail.seal(60) == 3
    return trail


class TestSealing:
    def test_a_window_is_not_sealed_until_its_grace_period_has_passed(self, trail):
        trail.write(3)

        assert trail.seal(15) == 0  # the window just ended
        assert trail.seal(19) == 0  # inside the grace period
        assert trail.seal(20) == 1  # grace (5 minutes) elapsed

    def test_the_first_window_is_the_one_holding_the_oldest_row(self, trail):
        trail.write(19)

        trail.seal(60)

        assert as_utc(trail.all()[0].window_start) == at(15)

    def test_an_empty_log_seals_nothing(self, trail):
        assert trail.seal(600) == 0

    def test_empty_windows_between_rows_are_sealed_too(self, sealed):
        counts = [cp.row_count for cp in sealed.all()]

        assert counts == [2, 0, 1]

    def test_windows_are_contiguous_and_chained(self, sealed):
        rows = sealed.all()

        assert rows[0].prev_hash is None
        assert [cp.prev_hash for cp in rows[1:]] == [cp.hash for cp in rows[:-1]]
        assert all(later.window_start == earlier.window_end for earlier, later in itertools.pairwise(rows))
        assert all(cp.window_end - cp.window_start == WINDOW for cp in rows)

    def test_sealing_again_writes_nothing(self, sealed):
        assert sealed.seal(60) == 0
        assert len(sealed.all()) == 3

    def test_later_windows_continue_the_chain(self, sealed):
        sealed.write(62, target_id="d")

        assert sealed.seal(90) == 2
        rows = sealed.all()
        assert rows[3].prev_hash == rows[2].hash
        assert sealed.statuses() == ["ok"] * 5

    def test_a_backlog_is_worked_off_in_bounded_runs(self, trail, monkeypatch):
        monkeypatch.setattr(checkpoints, "MAX_WINDOWS_PER_RUN", 2)
        trail.write(0)

        assert trail.seal(120) == 2
        assert trail.seal(120) == 2

    def test_the_health_counters_follow_the_latest_seal(self, sealed):
        assert jasil_audit.get_audit_health()["last_checkpoint_at"] == at(45)

    def test_the_hash_depends_on_the_previous_one(self, trail):
        trail.write(3, target_id="a")
        trail.write(20, target_id="a")
        trail.seal(60)
        first, second = trail.all()[:2]

        assert first.hash != second.hash

    def test_a_different_window_size_is_honoured(self, audit_env, trail):
        audit_env.configure(checkpoint_window_minutes=5, checkpoint_grace_minutes=0)
        trail.write(0)

        assert trail.seal(10) == 2

    def test_a_changed_window_size_rejoins_its_grid(self, audit_env, trail):
        trail.write(3)
        assert trail.seal(20) == 1  # 12:00-12:15
        audit_env.configure(checkpoint_window_minutes=60, checkpoint_grace_minutes=0)
        trail.write(200)

        trail.seal(300)

        bounds = [(as_utc(cp.window_start), as_utc(cp.window_end)) for cp in trail.all()]
        assert bounds[1] == (at(15), at(60))  # shortened to the next hour, so no window straddles midnight
        assert all(start.minute == 0 and end - start == timedelta(hours=1) for start, end in bounds[2:])
        assert trail.verify(end=300).ok

    def test_a_checkpoint_ending_after_now_stops_sealing_loudly(self, trail):
        trail.write(3)
        later = at(10 * 365 * 24 * 60)
        trail.execute(
            insert(AuditCheckpoint).values(
                window_start=later,
                window_end=later + WINDOW,
                row_count=0,
                hash="0" * 64,
                retention_days=90,
                legal_hold=False,
            )
        )

        with pytest.raises(RuntimeError, match="after now"):
            trail.seal(60)

    def test_a_checkpoint_records_the_retention_it_was_sealed_under(self, audit_env, trail):
        audit_env.configure(retention_days=45, legal_hold=True)
        trail.write(3)

        trail.seal(60)

        assert {(cp.retention_days, cp.legal_hold) for cp in trail.all()} == {(45, True)}

    def test_unsigned_checkpoints_warn_once(self, trail, caplog):
        checkpoints._unsigned_warned = False
        trail.write(3)

        with caplog.at_level("WARNING"):
            trail.seal(60)
            trail.write(63)
            trail.seal(100)

        assert caplog.text.count("not signed") == 1
        assert {(cp.signature, cp.key_id) for cp in trail.all()} == {(None, None)}


class TestVerification:
    def test_an_untouched_trail_verifies(self, sealed):
        report = sealed.verify()

        assert report.ok is True
        assert report.signed is False
        assert [(w.expected_rows, w.actual_rows) for w in report.windows] == [(2, 2), (0, 0), (1, 1)]

    def test_a_reversed_range_is_refused(self, sealed):
        with pytest.raises(ValueError, match="after to_ts"):
            sealed.verify(start=60, end=0)

    def test_a_range_longer_than_a_year_is_refused(self, sealed):
        """Every row in the range is read and hashed in one session; a longer audit is verified in parts."""
        with pytest.raises(ValueError, match="at most 366 days"):
            jasil_audit.verify_checkpoints(None, at(0), at(0) + timedelta(days=366, minutes=1))

    def test_a_modified_row_is_reported(self, sealed):
        sealed.execute(update(AuditLog).where(AuditLog.target_id == "b").values(outcome="failure"))

        assert sealed.statuses() == ["row_mismatch", "ok", "ok"]
        assert sealed.verify().ok is False

    def test_a_modified_detail_is_reported(self, sealed):
        sealed.execute(update(AuditLog).where(AuditLog.target_id == "c").values(details={"x": 1}))

        assert sealed.statuses() == ["ok", "ok", "row_mismatch"]

    def test_a_deleted_row_is_reported(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.target_id == "a"))

        assert sealed.statuses() == ["row_mismatch", "ok", "ok"]

    def test_a_row_inserted_into_a_sealed_window_is_reported(self, sealed):
        sealed.execute(
            insert(AuditLog).values(
                id=str(uuid7()),
                occurred_at=at(20),  # the empty window
                action_id="shop.order.place",
                category="data_change",
                severity="low",
                outcome="success",
                actor_type="anonymous",
                source="api",
                details_version=1,
            )
        )

        assert sealed.statuses() == ["ok", "row_mismatch", "ok"]

    def test_a_row_moved_to_another_window_is_reported_in_both(self, sealed):
        sealed.execute(update(AuditLog).where(AuditLog.target_id == "a").values(occurred_at=at(20)))

        assert sealed.statuses() == ["row_mismatch", "row_mismatch", "ok"]

    def test_a_deleted_checkpoint_is_a_missing_window_and_a_broken_chain(self, sealed):
        sealed.execute(delete(AuditCheckpoint).where(AuditCheckpoint.window_start == at(15)))

        report = sealed.verify()

        assert [(w.window_start, w.status.value) for w in report.windows] == [
            (at(0), "ok"),
            (at(15), "missing_window"),
            (at(30), "broken_chain"),
        ]
        assert report.ok is False

    def test_an_altered_checkpoint_hash_is_reported(self, sealed):
        sealed.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(15)).values(hash="0" * 64))

        assert sealed.statuses()[1] == "row_mismatch"

    def test_a_checkpoint_resealed_onto_a_different_predecessor_is_a_broken_chain(self, sealed):
        sealed.reseal(30, prev_hash="f" * 64)

        assert sealed.statuses() == ["ok", "ok", "broken_chain"]

    def test_a_stored_link_that_no_longer_matches_the_rows_is_a_row_mismatch(self, sealed):
        sealed.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(30)).values(prev_hash="f" * 64))

        assert sealed.statuses() == ["ok", "ok", "row_mismatch"]

    def test_a_chain_whose_head_was_removed_is_reported(self, sealed):
        sealed.execute(delete(AuditCheckpoint).where(AuditCheckpoint.window_start == at(0)))

        assert sealed.statuses(start=14)[0] == "broken_chain"

    def test_the_range_limits_which_windows_are_checked(self, sealed):
        sealed.execute(update(AuditLog).where(AuditLog.target_id == "a").values(outcome="failure"))

        assert sealed.statuses(start=15) == ["ok", "ok"]
        assert sealed.statuses(start=0, end=15) == ["row_mismatch"]

    def test_a_window_partly_outside_the_range_is_not_checked(self, sealed):
        assert sealed.statuses(start=5, end=44) == ["ok"]

    def test_an_empty_range_is_ok(self, sealed):
        report = sealed.verify(start=1000, end=2000)

        assert (report.ok, report.windows) == (True, [])

    def test_checkpoints_deleted_from_the_whole_range_are_a_missing_window(self, sealed):
        sealed.seal(200)
        sealed.execute(
            delete(AuditCheckpoint).where(AuditCheckpoint.window_start >= at(15), AuditCheckpoint.window_start < at(90))
        )

        report = sealed.verify(start=15, end=90)

        assert [(w.window_start, w.window_end, w.status.value) for w in report.windows] == [
            (at(15), at(90), "missing_window")
        ]
        assert report.ok is False

    def test_checkpoints_deleted_from_the_end_of_the_range_are_a_missing_window(self, sealed):
        sealed.seal(200)
        sealed.execute(delete(AuditCheckpoint).where(AuditCheckpoint.window_start.in_([at(30), at(45)])))

        assert sealed.statuses(start=0, end=60) == ["ok", "ok", "missing_window"]

    def test_a_row_dated_before_the_first_checkpoint_is_reported(self, sealed):
        sealed.write(-30, target_id="forged")

        report = sealed.verify(start=-60, end=45)

        first = report.windows[0]
        assert (first.window_end, first.status.value, first.expected_rows, first.actual_rows) == (
            at(0),
            "row_mismatch",
            0,
            1,
        )
        assert report.ok is False

    def test_rows_after_the_newest_checkpoint_are_not_yet_sealed(self, sealed):
        sealed.write(50)

        assert sealed.verify(start=0, end=120).ok is True

    def test_a_trail_with_no_checkpoints_verifies_vacuously(self, trail):
        assert trail.verify().ok is True

    def test_the_link_to_the_window_before_the_range_is_checked(self, sealed):
        sealed.reseal(15, prev_hash="f" * 64)

        assert sealed.statuses(start=15) == ["broken_chain", "broken_chain"]  # and the next link no longer fits


class TestSignatures:
    @pytest.fixture
    def signed(self, trail, audit_env, keys):
        private, public = keys()
        audit_env.configure(signing_key=private)
        trail.write(3, target_id="a")
        trail.write(35, target_id="c")
        trail.seal(60)
        return trail, public, keys

    def test_checkpoints_carry_a_signature_and_a_key_id(self, signed):
        trail, _, _ = signed

        assert all(cp.signature and len(cp.key_id) == 16 for cp in trail.all())
        assert len({cp.key_id for cp in trail.all()}) == 1

    def test_the_signatures_verify_with_the_public_key(self, signed):
        trail, public, _ = signed

        report = trail.verify(public)

        assert (report.ok, report.signed) == (True, True)

    def test_another_key_is_a_bad_signature(self, signed):
        trail, _, keys = signed

        assert trail.statuses(keys()[1]) == ["bad_signature"] * 3

    def test_a_removed_signature_is_a_bad_signature(self, signed):
        trail, public, _ = signed
        trail.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(15)).values(signature=None))

        assert trail.statuses(public) == ["ok", "bad_signature", "ok"]

    def test_a_signature_does_not_cover_a_changed_row_count(self, signed):
        trail, public, _ = signed
        trail.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(0)).values(row_count=9))

        # The recomputed rows no longer match the stored count, and the signature no longer matches either.
        assert trail.statuses(public)[0] == "row_mismatch"

    def test_a_forged_hash_with_a_valid_looking_chain_still_fails_the_signature(self, signed):
        trail, public, _ = signed
        rows = trail.all()
        # An attacker who rewrites a row and re-seals its window cannot sign the new hash.
        trail.execute(update(AuditLog).where(AuditLog.target_id == "c").values(outcome="failure"))
        with trail.session_factory() as db:
            forged = checkpoints._digest(db, rows[2].window_start, rows[2].window_end, rows[2].prev_hash)
        trail.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(30)).values(hash=forged.hash))

        assert trail.statuses(public)[2] == "bad_signature"

    @pytest.mark.parametrize("change", [{"retention_days": 1}, {"legal_hold": True}])
    def test_the_signature_covers_the_recorded_retention(self, signed, change):
        trail, public, _ = signed
        trail.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(0)).values(**change))

        assert trail.statuses(public)[0] == "bad_signature"

    def test_the_signature_binds_the_checkpoint_to_its_deployment(self, signed):
        trail, public, _ = signed
        # Replayed into another deployment signed with the same key, or the instance id rewritten.
        trail.execute(update(Instance).values(instance_id=str(uuid7())))

        assert trail.statuses(public) == ["bad_signature"] * 3

    def test_several_keys_verify_a_trail_signed_across_a_rotation(self, signed, audit_env):
        trail, old, keys = signed
        new_private, new = keys()
        audit_env.configure(signing_key=new_private)
        trail.write(65, target_id="d")
        trail.seal(120)

        assert trail.statuses(old, end=120) == ["ok"] * 3 + ["bad_signature"] * 4
        assert trail.verify([old, new], end=120).ok is True

    def test_a_key_list_without_the_signing_key_is_a_bad_signature(self, signed):
        trail, _, keys = signed

        assert trail.statuses([keys()[1], keys()[1]]) == ["bad_signature"] * 3

    def test_an_empty_key_list_is_refused(self, signed):
        trail, _, _ = signed

        with pytest.raises(ValueError, match="key"):
            trail.verify([])

    def test_without_a_public_key_signatures_are_not_checked(self, signed):
        trail, _, _ = signed

        assert trail.verify(None).signed is False

    def test_a_malformed_signing_key_is_refused_before_anything_is_written_to_seal(self, trail, audit_env):
        """It used to be found at the first seal, which then stopped sealing and retention without a word."""
        with pytest.raises(ValueError, match="signing_key"):
            audit_env.configure(signing_key="not base64!")
        assert trail.all() == []

    def test_a_malformed_public_key_is_refused(self, sealed):
        with pytest.raises(ValueError, match="public key"):
            sealed.verify("%%%")

    def test_signing_without_the_extra_installed_explains_what_to_install(self, trail, audit_env, monkeypatch):
        audit_env.configure(signing_key=base64.b64encode(bytes(32)).decode())
        checkpoints._signer.cache_clear()
        monkeypatch.setattr(
            checkpoints.importlib, "import_module", lambda name: (_ for _ in ()).throw(ImportError(name))
        )
        trail.write(3)

        with pytest.raises(ImportError, match=r"jasil\[audit\]"):
            trail.seal(60)


class TestPrunedWindows:
    """An emptied window is expected only where a checkpoint's own signed retention allowed the prune."""

    def _authorise(self, trail, minute: int, **settings) -> None:
        """Seal on until ``minute`` under a one-day retention, as the checkpoints retention relies on would be."""
        trail.env.configure(retention_days=1, retention_floor_days=1, **settings)
        trail.seal(minute)

    def test_windows_emptied_within_a_signed_retention_are_expected(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.occurred_at < at(30)))
        self._authorise(sealed, 2 * 24 * 60)

        report = sealed.verify(end=45)

        assert [(w.status.value, w.pruned) for w in report.windows] == [("ok", True), ("ok", False), ("ok", False)]
        assert report.windows[0].actual_rows is None

    def test_emptied_windows_no_checkpoint_allows_are_a_mismatch(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.occurred_at < at(30)))

        assert sealed.statuses() == ["row_mismatch", "ok", "ok"]

    def test_a_forged_prune_row_excuses_nothing(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.target_id == "c"))
        # Anyone who can insert into the log can write a prune row with any timestamp and cutoff.
        forged_at = T0 + timedelta(days=365)
        sealed.execute(
            insert(AuditLog).values(
                id=str(uuid7()),
                occurred_at=forged_at,
                action_id="audit.retention.prune",
                category="system",
                severity="medium",
                outcome="success",
                actor_type="system",
                source="worker",
                details={"cutoff": (forged_at - timedelta(days=60)).isoformat(), "partitions_dropped": 1},
                details_version=1,
            )
        )

        assert sealed.statuses(end=100)[2] == "row_mismatch"

    def test_a_window_younger_than_the_signed_retention_is_a_mismatch(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.occurred_at < at(45)))
        self._authorise(sealed, 24 * 60 + 40)  # the newest checkpoint ends at 24h30, so a day back is 00:30

        report = sealed.verify(end=45)

        assert [(w.status.value, w.pruned) for w in report.windows] == [
            ("ok", True),
            ("ok", False),
            ("row_mismatch", False),
        ]

    def test_a_retention_too_long_to_represent_allows_no_prune_instead_of_failing(self, sealed):
        """Whoever can write the table must not be able to make verification crash rather than report."""
        sealed.execute(update(AuditCheckpoint).values(retention_days=2_000_000_000))

        assert sealed.verify().ok

    def test_a_checkpoint_sealed_under_legal_hold_allows_no_prune(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.occurred_at < at(30)))
        self._authorise(sealed, 2 * 24 * 60, legal_hold=True)

        assert sealed.statuses(end=100)[0] == "row_mismatch"

    def test_a_rewritten_retention_allows_no_prune_when_signatures_are_checked(self, trail, audit_env, keys):
        private, public = keys()
        audit_env.configure(signing_key=private)
        trail.write(3, target_id="a")
        trail.seal(2 * 24 * 60)  # signed under the default 90-day retention
        trail.execute(delete(AuditLog).where(AuditLog.occurred_at < at(15)))
        trail.execute(update(AuditCheckpoint).values(retention_days=1))

        assert trail.statuses(public, end=100)[0] == "row_mismatch"
        assert trail.statuses(public, start=100, end=2 * 24 * 60)[0] == "bad_signature"
        assert trail.statuses(None, end=100)[0] == "ok"  # unsigned, the chain proves only consistency

    def test_windows_pruned_after_a_key_rotation_are_expected_with_both_keys(self, trail, audit_env, keys):
        old_private, old = keys()
        new_private, new = keys()
        audit_env.configure(signing_key=old_private)
        trail.write(3, target_id="a")
        trail.write(35, target_id="c")
        trail.seal(60)
        audit_env.configure(signing_key=new_private)
        self._authorise(trail, 2 * 24 * 60)  # only checkpoints signed with the new key allow the prune
        trail.execute(delete(AuditLog).where(AuditLog.occurred_at < at(30)))

        report = trail.verify([old, new], end=45)

        assert [(w.status.value, w.pruned) for w in report.windows] == [("ok", True), ("ok", False), ("ok", False)]

    def test_a_pruned_window_still_has_its_chain_and_signature_checked(self, sealed):
        sealed.execute(delete(AuditLog).where(AuditLog.occurred_at < at(30)))
        self._authorise(sealed, 2 * 24 * 60)
        sealed.execute(update(AuditCheckpoint).where(AuditCheckpoint.window_start == at(0)).values(prev_hash="f" * 64))

        assert sealed.statuses(end=100)[0] == "broken_chain"


class TestCanonicalForm:
    def _row(self, **overrides):
        values = {
            "id": "i",
            "occurred_at": T0,
            "action_id": "a.b.c",
            "category": "system",
            "severity": "info",
            "outcome": "success",
            "actor_type": "system",
            "actor_id": None,
            "credential_type": None,
            "credential_id": None,
            "on_behalf_of_id": None,
            "target_type": None,
            "target_id": None,
            "target_owner_id": None,
            "request_id": None,
            "event_id": None,
            "source": "api",
            "ip_address": None,
            "user_agent": None,
            "details": None,
            "details_version": 1,
            "instance_id": None,
            "app_name": None,
            "app_version": None,
        }
        return type("Row", (), {**values, **overrides})

    def test_key_order_inside_details_does_not_matter(self):
        one = checkpoints.canonical_row(self._row(details={"a": 1, "b": {"c": 2, "d": 3}}))
        two = checkpoints.canonical_row(self._row(details={"b": {"d": 3, "c": 2}, "a": 1}))

        assert one == two

    def test_a_naive_timestamp_is_taken_as_utc(self):
        naive = checkpoints.canonical_row(self._row(occurred_at=T0.replace(tzinfo=None)))

        assert naive == checkpoints.canonical_row(self._row())

    def test_an_offset_timestamp_is_the_same_instant_in_utc(self):
        """A driver hands ``timestamptz`` back in the session's TimeZone; the bytes must not depend on it."""
        berlin = T0.astimezone(timezone(timedelta(hours=1)))

        assert checkpoints.canonical_row(self._row(occurred_at=berlin)) == checkpoints.canonical_row(self._row())
        assert (
            checkpoints._sink_name(berlin, "i")
            == checkpoints._sink_name(T0, "i")
            == "checkpoint-i-20260101T120000Z.json"
        )

    def test_every_column_is_part_of_the_bytes(self):
        base = checkpoints.canonical_row(self._row())

        for column in checkpoints._COLUMNS:
            if column in {"occurred_at", "details_version"}:
                continue
            assert checkpoints.canonical_row(self._row(**{column: "changed"})) != base, column
        assert checkpoints.canonical_row(self._row(details_version=2)) != base
        assert checkpoints.canonical_row(self._row(occurred_at=T0 + timedelta(microseconds=1))) != base

    def test_the_columns_are_exactly_the_model_columns(self):
        assert set(checkpoints._COLUMNS) == {column.name for column in AuditLog.__table__.columns}

    def test_rows_sharing_a_timestamp_are_ordered_by_id_whatever_the_database_returns(self, trail, session_factory):
        trail.write(3, target_id="x")
        with session_factory() as db:
            template = db.scalars(select(AuditLog)).one()
            for identifier in ("b", "a", "c"):
                db.execute(
                    insert(AuditLog).values(
                        id=identifier,
                        occurred_at=template.occurred_at,
                        action_id=template.action_id,
                        category="system",
                        severity="info",
                        outcome="success",
                        actor_type="system",
                        source="api",
                        details_version=1,
                    )
                )
            db.commit()
            ordered = [row.id for row in checkpoints._window_rows(db, at(0), at(15))]

        assert len(ordered) == 4
        assert ordered == sorted(ordered)
