"""MySQL and MariaDB conformance for the audit log: partitions, append-only triggers, the retention guard, roles.

Skipped on the SQLite developer default and the PostgreSQL job. Nothing is mocked: the migration builds the
real partitioned table and the real procedures, and the assertions are what the server does when asked to
break them.
"""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, func, select, table, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

import jasil.audit as jasil_audit
import jasil.orm as jasil_orm
import jasil.settings as jasil_settings
import jasil.testing as jasil_testing
from jasil import migrations
from jasil._core.timestamps import as_utc
from jasil.audit import AuditAction, Category, Outcome, Severity
from jasil.audit import _mysql as audit_mysql
from jasil.audit.models import AuditCheckpoint, AuditLog
from jasil.backends.clock_system import SystemClock

pytest.importorskip("alembic", reason="migrations require the 'migrations' extra")

ACCOUNTS = ("jasil_t_runtime", "jasil_t_reader", "jasil_t_maintenance")
PASSWORD = "jasil-test-only"
# Retention prunes only what a sealed checkpoint allows; day-long windows seal months of history in one run.
DAY_WINDOWS = jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(checkpoint_window_minutes=1440))


def _database(connection) -> str:
    return connection.execute(text("SELECT DATABASE()")).scalar_one()


def _clean(engine, mapped_base) -> None:
    mapped_base.metadata.drop_all(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {migrations.VERSION_TABLE}")
        for statement in audit_mysql.uninstall_statements(_database(connection)):
            connection.exec_driver_sql(statement)


@pytest.fixture
def engine(mapped_base, database_url):
    engine = create_engine(database_url)
    if engine.dialect.name not in ("mysql", "mariadb"):
        engine.dispose()
        pytest.skip("requires the real MySQL or MariaDB test service")
    _clean(engine, mapped_base)
    yield engine
    _clean(engine, mapped_base)
    engine.dispose()


@pytest.fixture
def migrated(engine):
    migrations.upgrade(engine)
    jasil_orm.configure_sessionmaker(sessionmaker(bind=engine, expire_on_commit=False))
    return engine


@pytest.fixture
def action():
    return jasil_audit.get_catalog().register(
        AuditAction("shop.order.place", Category.DATA_CHANGE, Severity.LOW, "k", "d")
    )


def _scalar(engine, sql: str, **params):
    with engine.connect() as connection:
        return connection.execute(text(sql), params).scalar()


def _partitions(engine) -> list[str]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                text(
                    "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'jasil_audit_log' AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_NAME"
                )
            ).scalars()
        )


def _rows_in(engine, day: date) -> int:
    """Count the rows in the partition of ``day``, read through MySQL's partition selection."""
    log = table("jasil_audit_log")
    with engine.connect() as connection:
        return connection.execute(
            select(func.count()).select_from(log).with_hint(log, f"PARTITION (p{day:%Y%m%d})")
        ).scalar_one()


def _insert_row(connection, when: datetime, identifier: str = "00000000-0000-7000-8000-000000000001") -> None:
    connection.execute(
        text(
            "INSERT INTO jasil_audit_log (id, occurred_at, action_id, category, severity, outcome, actor_type, source) "
            "VALUES (:id, :at, 'shop.order.place', 'data_change', 'low', 'success', 'system', 'api')"
        ),
        {"id": identifier, "at": when.astimezone(UTC).replace(tzinfo=None)},
    )


def _at_one_pm(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=13)


def _drop(engine, cutoff: datetime, prune_at: datetime | None = None) -> int:
    with engine.connect() as connection:
        dropped = connection.execute(text(audit_mysql.DROP_CALL), {"cutoff": cutoff, "prune_at": prune_at}).scalar()
        connection.commit()
        return dropped


def _prune_rows(engine) -> int:
    return _scalar(engine, "SELECT COUNT(*) FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'")


class TestMigration:
    def test_the_log_is_partitioned_by_range_of_occurred_at(self, migrated):
        method, expression = (
            migrated.connect()
            .execute(
                text(
                    "SELECT PARTITION_METHOD, PARTITION_EXPRESSION FROM information_schema.PARTITIONS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'jasil_audit_log' LIMIT 1"
                )
            )
            .one()
        )

        assert method == "RANGE COLUMNS"
        assert expression.strip("`") == "occurred_at"

    def test_partitions_exist_from_yesterday_to_the_configured_horizon(self, migrated):
        today = datetime.now(UTC).date()
        wanted = {f"p{today + timedelta(days=offset):%Y%m%d}" for offset in range(-1, 8)}

        assert wanted <= set(_partitions(migrated))

    def test_a_row_lands_in_the_partition_of_its_day(self, migrated):
        moment = datetime.now(UTC)  # read once: the insert and the expected partition must agree on the day
        with migrated.begin() as connection:
            _insert_row(connection, moment.replace(microsecond=0))

        assert _rows_in(migrated, moment.date()) == 1

    def test_ensuring_partitions_is_idempotent_and_returns_the_last_day(self, migrated):
        with migrated.connect() as connection:
            first = connection.execute(text(audit_mysql.ENSURE_CALL), {"days": 10}).scalar()
        before = _partitions(migrated)
        with migrated.connect() as connection:
            second = connection.execute(text(audit_mysql.ENSURE_CALL), {"days": 10}).scalar()
            expected = connection.execute(text("SELECT UTC_DATE() + INTERVAL 10 DAY")).scalar()

        assert first == second == expected
        assert _partitions(migrated) == before

    @pytest.mark.parametrize("days", [-1, 367])
    def test_the_horizon_is_bounded(self, migrated, days):
        with pytest.raises(DBAPIError, match="between 0 and 366"), migrated.connect() as connection:
            connection.execute(text(audit_mysql.ENSURE_CALL), {"days": days})

    def test_the_procedures_run_with_their_definer_rights(self, migrated):
        security = _scalar(
            migrated,
            "SELECT GROUP_CONCAT(DISTINCT SECURITY_TYPE) FROM information_schema.ROUTINES "
            "WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME LIKE 'jasil_audit_%'",
        )

        assert security == "DEFINER"

    @pytest.mark.parametrize("call", [audit_mysql.ENSURE_CALL, audit_mysql.DROP_CALL])
    def test_the_procedures_restore_the_session_lock_wait(self, migrated, call):
        with migrated.connect() as connection:
            connection.execute(text("SET SESSION lock_wait_timeout = 1234"))
            connection.execute(
                text(call), {"days": 1, "cutoff": datetime.now(UTC) - timedelta(days=400), "prune_at": None}
            )

            assert connection.execute(text("SELECT @@SESSION.lock_wait_timeout")).scalar() == 1234

    def test_a_failing_procedure_restores_the_session_lock_wait_too(self, migrated):
        with migrated.connect() as connection:
            connection.execute(text("SET SESSION lock_wait_timeout = 1234"))
            with pytest.raises(DBAPIError, match="retention floor"):
                connection.execute(text(audit_mysql.DROP_CALL), {"cutoff": datetime.now(UTC), "prune_at": None})

            assert connection.execute(text("SELECT @@SESSION.lock_wait_timeout")).scalar() == 1234

    def test_the_floor_is_baked_in_from_the_settings_at_migration_time(self, engine):
        jasil_settings.configure(
            jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(retention_floor_days=45))
        )
        migrations.upgrade(engine)

        with pytest.raises(DBAPIError, match="floor of 45 days"):
            _drop(engine, datetime.now(UTC) - timedelta(days=40))

    def test_rows_already_in_the_log_keep_their_day_and_are_protected(self, engine):
        migrations.upgrade(engine, "rev0007")
        today = datetime.now(UTC).date()
        with engine.begin() as connection:
            for age in (200, 100, 3):
                _insert_row(connection, _at_one_pm(today - timedelta(days=age)), f"00000000-0000-7000-8000-{age:012d}")

        migrations.upgrade(engine)

        for age in (200, 100, 3):
            assert _rows_in(engine, today - timedelta(days=age)) == 1
        with pytest.raises(DBAPIError, match="append-only"), engine.begin() as connection:
            connection.execute(text("DELETE FROM jasil_audit_log"))

    def test_a_run_that_failed_after_creating_a_trigger_can_run_again(self, engine):
        """DDL commits on these servers, so a failed run leaves what it created behind."""
        migrations.upgrade(engine, "rev0007")
        with engine.begin() as connection:
            connection.execute(text(audit_mysql.trigger_ddl()[0]))

        migrations.upgrade(engine)

        assert migrations.db_revision(engine) == migrations.head_revision()
        migrations.verify_schema_current(engine)

    def test_downgrading_removes_the_partitions_triggers_and_procedures_and_keeps_the_rows(self, migrated):
        with migrated.begin() as connection:
            _insert_row(connection, datetime.now(UTC).replace(microsecond=0))

        migrations.downgrade(migrated, "rev0007")

        assert _partitions(migrated) == []
        assert (
            _scalar(
                migrated,
                "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() "
                "AND ROUTINE_NAME LIKE 'jasil_audit_%'",
            )
            == 0
        )
        with migrated.begin() as connection:
            connection.execute(text("UPDATE jasil_audit_log SET outcome = 'failure'"))
        assert _scalar(migrated, "SELECT outcome FROM jasil_audit_log") == "failure"
        assert migrations.db_revision(migrated) == "rev0007"

    def test_downgrading_past_the_audit_trail_removes_everything(self, migrated):
        migrations.downgrade(migrated, "rev0005")

        assert (
            _scalar(
                migrated,
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = 'jasil_audit_log'",
            )
            == 0
        )
        assert migrations.db_revision(migrated) == "rev0005"

    def test_the_migrated_schema_matches_the_models(self, migrated):
        migrations.verify_schema_current(migrated)


class TestAppendOnly:
    @pytest.fixture
    def row(self, migrated):
        with migrated.begin() as connection:
            _insert_row(connection, datetime.now(UTC).replace(microsecond=0))
        return migrated

    @pytest.mark.parametrize(
        "statement",
        [
            "UPDATE jasil_audit_log SET outcome = 'failure'",
            "DELETE FROM jasil_audit_log",
            "UPDATE jasil_audit_log PARTITION (p{day}) SET outcome = 'failure'",
            "DELETE FROM jasil_audit_log PARTITION (p{day})",
        ],
    )
    def test_changing_a_row_is_rejected(self, row, statement):
        with pytest.raises(DBAPIError, match="append-only"), row.begin() as connection:
            connection.execute(text(statement.format(day=f"{datetime.now(UTC):%Y%m%d}")))

        assert _scalar(row, "SELECT COUNT(*) FROM jasil_audit_log") == 1

    def test_inserting_still_works(self, row):
        with row.begin() as connection:
            _insert_row(connection, datetime.now(UTC).replace(microsecond=0), "00000000-0000-7000-8000-000000000002")

        assert _scalar(row, "SELECT COUNT(*) FROM jasil_audit_log") == 2

    def test_a_row_for_a_day_without_a_partition_is_refused(self, migrated):
        with pytest.raises(DBAPIError, match="no partition"), migrated.begin() as connection:
            _insert_row(connection, datetime.now(UTC) + timedelta(days=60))


class TestDropPartitions:
    @pytest.fixture
    def aged(self, engine):
        """Rows 100, 99 and 2 days old, each in its own partition, beside today's."""
        migrations.upgrade(engine, "rev0007")
        today = datetime.now(UTC).date()
        with engine.begin() as connection:
            for age in (100, 99, 2):
                _insert_row(connection, _at_one_pm(today - timedelta(days=age)), f"00000000-0000-7000-8000-{age:012d}")
        migrations.upgrade(engine)
        jasil_orm.configure_sessionmaker(sessionmaker(bind=engine, expire_on_commit=False))
        return engine, today

    def test_a_cutoff_inside_the_floor_is_refused_and_nothing_is_dropped(self, aged):
        engine, _ = aged
        before = _partitions(engine)

        for cutoff in (datetime.now(UTC), datetime.now(UTC) - timedelta(days=29)):
            with pytest.raises(DBAPIError, match="inside the retention floor of 30 days"):
                _drop(engine, cutoff)

        assert _partitions(engine) == before

    def test_the_floor_ignores_the_session_time_zone(self, aged):
        engine, _ = aged
        with engine.connect() as connection:
            connection.execute(text("SET time_zone = '+13:00'"))
            utc_now = as_utc(connection.execute(text(audit_mysql.NOW_SQL)).scalar())
            dropped = connection.execute(
                text(audit_mysql.DROP_CALL), {"cutoff": utc_now - timedelta(days=30, minutes=1), "prune_at": None}
            ).scalar()
            connection.execute(text("SET time_zone = '-12:00'"))
            with pytest.raises(DBAPIError, match="inside the retention floor"):
                connection.execute(
                    text(audit_mysql.DROP_CALL), {"cutoff": utc_now - timedelta(days=29, hours=23), "prune_at": None}
                )

        assert dropped == 2

    def test_it_drops_whole_partitions_older_than_the_cutoff_only(self, aged):
        engine, today = aged

        dropped = _drop(engine, datetime.now(UTC) - timedelta(days=90))

        assert dropped == 2
        names = _partitions(engine)
        assert f"p{today - timedelta(days=100):%Y%m%d}" not in names
        assert f"p{today - timedelta(days=99):%Y%m%d}" not in names
        assert f"p{today - timedelta(days=2):%Y%m%d}" in names

    def test_a_partition_that_straddles_the_cutoff_is_kept_whole(self, aged):
        engine, today = aged
        # The cutoff falls in the middle of the day 99 days ago: that day's partition ends after it.
        cutoff = datetime.combine(today - timedelta(days=99), datetime.min.time(), UTC) + timedelta(hours=12)

        dropped = _drop(engine, cutoff)

        assert dropped == 1
        assert _rows_in(engine, today - timedelta(days=99)) == 1

    def test_the_rows_go_with_their_partition_and_the_others_are_untouched(self, aged):
        engine, _ = aged

        _drop(engine, datetime.now(UTC) - timedelta(days=90))

        assert _scalar(engine, "SELECT COUNT(*) FROM jasil_audit_log WHERE action_id = 'shop.order.place'") == 1

    def test_it_records_its_own_prune_row(self, aged):
        engine, today = aged

        _drop(engine, datetime.now(UTC) - timedelta(days=90))

        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT category, severity, outcome, actor_type, source, details, details_version, target_type "
                    "FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'"
                )
            ).one()
        details = jasil_audit.RetentionPruneDetails.model_validate_json(row[5]) if isinstance(row[5], str) else None
        assert row[:5] == ("system", "medium", "success", "system", "worker")
        assert row[6] == 1
        assert row[7] == "jasil_audit_log"
        # Valid against the model JASIL validates every other prune row with, in strict JSON types.
        assert details is not None
        assert details.partitions_dropped == 2
        assert details.first_day == today - timedelta(days=100)
        assert details.last_day == today - timedelta(days=99)
        assert '"partitions_dropped": 2' in row[5]

    def test_the_prune_row_carries_the_instant_the_caller_gives(self, aged):
        """Maintenance passes the platform clock, so the row belongs to the window that clock is sealing."""
        engine, _ = aged
        stamped = datetime.now(UTC).replace(microsecond=0) + timedelta(days=3)

        _drop(engine, datetime.now(UTC) - timedelta(days=90), stamped)

        assert (
            as_utc(_scalar(engine, "SELECT occurred_at FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'"))
            == stamped
        )

    def test_without_an_instant_the_prune_row_is_stamped_by_the_server_in_utc(self, aged):
        engine, _ = aged
        with engine.connect() as connection:
            connection.execute(text("SET time_zone = '+05:00'"))
            connection.execute(
                text(audit_mysql.DROP_CALL), {"cutoff": datetime.now(UTC) - timedelta(days=90), "prune_at": None}
            )
            connection.commit()

        stamped = _scalar(engine, "SELECT occurred_at FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'")

        assert abs(as_utc(stamped) - datetime.now(UTC)) < timedelta(minutes=5)

    def test_the_prune_row_id_is_a_uuid_v7_of_the_current_millisecond(self, aged):
        engine, _ = aged
        _drop(engine, datetime.now(UTC) - timedelta(days=90))

        identifier = _scalar(engine, "SELECT id FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'")

        assert identifier[14] == "7" and identifier[19] in "89ab"
        milliseconds = int(identifier.replace("-", "")[:12], 16)
        assert abs(datetime.fromtimestamp(milliseconds / 1000, UTC) - datetime.now(UTC)) < timedelta(minutes=5)

    def test_dropping_nothing_writes_no_row(self, aged):
        engine, _ = aged

        assert _drop(engine, datetime.now(UTC) - timedelta(days=400)) == 0
        assert _prune_rows(engine) == 0

    def test_the_floor_can_be_changed_with_the_documented_statements(self, aged):
        engine, _ = aged
        with engine.begin() as connection:
            for statement in jasil_audit.retention_floor_sql(120, schema=_database(connection), dialect="mysql"):
                connection.execute(text(statement))

        with pytest.raises(DBAPIError, match="floor of 120 days"):
            _drop(engine, datetime.now(UTC) - timedelta(days=100))

    def test_a_running_maintenance_prunes_through_the_procedure_never_row_by_row(self, aged, action):
        engine, today = aged
        jasil_testing.install_test_platform(".", clock=SystemClock(), settings=DAY_WINDOWS)

        jasil_audit.run_audit_maintenance()

        assert f"p{today - timedelta(days=100):%Y%m%d}" not in _partitions(engine)
        assert _prune_rows(engine) == 1

    def test_an_application_clock_ahead_of_the_server_still_prunes_at_the_floor(self, aged, action):
        engine, today = aged
        clock = jasil_testing.FixedClock()
        clock.moment = datetime.now(UTC).replace(microsecond=0) + timedelta(hours=2)
        retention_at_the_floor = jasil_settings.AuditSettings(retention_days=30, retention_floor_days=30)
        jasil_testing.install_test_platform(
            ".", clock=clock, settings=jasil_settings.JasilSettings(audit=retention_at_the_floor)
        )
        # A checkpoint sealed just behind the fast clock, so the newest one allows a prune up to 30 days back.
        start = datetime.fromtimestamp(clock.moment.timestamp() // 900 * 900 - 1800, UTC)
        with sessionmaker(bind=engine)() as db:
            db.add(
                AuditCheckpoint(
                    window_start=start,
                    window_end=start + timedelta(minutes=15),
                    row_count=0,
                    hash="0" * 64,
                    retention_days=30,
                    legal_hold=False,
                )
            )
            db.commit()

        jasil_audit.run_audit_maintenance()

        assert f"p{today - timedelta(days=99):%Y%m%d}" not in _partitions(engine)
        # Stamped by the platform clock, like every other row, so it cannot fall into a window already sealed.
        prune_at = _scalar(engine, "SELECT occurred_at FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'")
        assert as_utc(prune_at) == clock.moment

    def test_legal_hold_keeps_the_partitions(self, aged, action):
        engine, today = aged
        jasil_testing.install_test_platform(
            ".",
            clock=SystemClock(),
            settings=jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(legal_hold=True)),
        )

        jasil_audit.run_audit_maintenance()

        assert f"p{today - timedelta(days=100):%Y%m%d}" in _partitions(engine)


class TestAccounts:
    """The grants ``grants_sql(dialect="mysql")`` writes, exercised by accounts that hold only them."""

    @pytest.fixture
    def accounts(self, migrated):
        # Only a missing privilege skips; a failing GRANT is a failure.
        if not _scalar(
            migrated, "SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES WHERE PRIVILEGE_TYPE = 'CREATE USER'"
        ):
            pytest.skip("creating accounts needs the CREATE USER privilege")
        with migrated.begin() as connection:
            for account in ACCOUNTS:
                connection.exec_driver_sql(f"DROP USER IF EXISTS '{account}'@'%%'")
                connection.exec_driver_sql(f"CREATE USER '{account}'@'%%' IDENTIFIED BY '{PASSWORD}'")
            for statement in jasil_audit.grants_sql(*ACCOUNTS[:2], maintenance_role=ACCOUNTS[2], dialect="mysql"):
                connection.execute(text(statement))
        engines = {
            account: create_engine(migrated.url.set(username=account, password=PASSWORD)) for account in ACCOUNTS
        }
        yield engines
        for engine in engines.values():
            engine.dispose()
        with migrated.begin() as connection:
            for account in ACCOUNTS:
                connection.exec_driver_sql(f"DROP USER IF EXISTS '{account}'@'%%'")

    def _as(self, accounts, account: str, sql: str, **params):
        with accounts[account].connect() as connection:
            result = connection.execute(text(sql), params).all()
            connection.commit()
            return result

    def test_the_runtime_account_can_stage_through_the_orm(self, accounts, action):
        runtime = accounts[ACCOUNTS[0]]
        jasil_orm.configure_sessionmaker(sessionmaker(bind=runtime, expire_on_commit=False))
        jasil_testing.install_test_platform(".", clock=SystemClock())
        with sessionmaker(bind=runtime)() as session:
            jasil_audit.stage(session, action, target_id="7")
            session.commit()

    def test_the_runtime_account_cannot_read_the_log(self, accounts):
        with pytest.raises(DBAPIError, match="denied"):
            self._as(accounts, ACCOUNTS[0], "SELECT COUNT(*) FROM jasil_audit_log")

    @pytest.mark.parametrize(
        "statement",
        [
            "UPDATE jasil_audit_log SET outcome = 'failure'",
            "DELETE FROM jasil_audit_log",
            "TRUNCATE jasil_audit_log",
            "ALTER TABLE jasil_audit_log DROP PARTITION p20000101",
            "CALL jasil_audit_drop_partitions(UTC_TIMESTAMP() - INTERVAL 90 DAY, NULL)",
        ],
    )
    def test_the_runtime_account_cannot_modify_or_prune(self, accounts, statement):
        with pytest.raises(DBAPIError, match="denied"):
            self._as(accounts, ACCOUNTS[0], statement)

    def test_the_runtime_account_can_insert_and_create_partitions(self, accounts):
        with accounts[ACCOUNTS[0]].connect() as connection:
            _insert_row(connection, datetime.now(UTC).replace(microsecond=0), "00000000-0000-7000-8000-000000000009")
            connection.commit()
            assert connection.execute(text(audit_mysql.ENSURE_CALL), {"days": 9}).scalar() is not None

    def test_maintenance_as_the_runtime_account_still_creates_partitions(self, accounts, action, caplog):
        """The runtime account sees only the routine it may execute; that must not read as a plain table."""
        import jasil.audit.maintenance as maintenance

        maintenance._warned_plain = False
        jasil_orm.configure_sessionmaker(sessionmaker(bind=accounts[ACCOUNTS[0]], expire_on_commit=False))
        jasil_testing.install_test_platform(
            ".",
            clock=SystemClock(),
            settings=jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(partition_days_ahead=12)),
        )

        with caplog.at_level("WARNING"):
            jasil_audit.run_audit_maintenance()

        assert "is a plain table" not in caplog.text
        assert f"p{datetime.now(UTC).date() + timedelta(days=12):%Y%m%d}" in _partitions(accounts[ACCOUNTS[1]])

    def test_the_reader_account_can_read_but_not_write(self, accounts):
        assert self._as(accounts, ACCOUNTS[1], "SELECT COUNT(*) FROM jasil_audit_log") == [(0,)]
        with pytest.raises(DBAPIError, match="denied"):
            self._as(accounts, ACCOUNTS[1], "INSERT INTO jasil_instance (id, instance_id) VALUES (1, 'x')")
        with pytest.raises(DBAPIError, match="denied"):
            self._as(accounts, ACCOUNTS[1], "CALL jasil_audit_ensure_partitions(8)")

    def test_the_maintenance_account_can_read_and_prune_but_not_edit(self, accounts):
        assert self._as(accounts, ACCOUNTS[2], "SELECT COUNT(*) FROM jasil_audit_log") == [(0,)]
        with pytest.raises(DBAPIError, match="denied"):
            self._as(accounts, ACCOUNTS[2], "UPDATE jasil_audit_log SET outcome = 'failure'")
        with pytest.raises(DBAPIError, match="inside the retention floor"):
            self._as(accounts, ACCOUNTS[2], "CALL jasil_audit_drop_partitions(UTC_TIMESTAMP(), NULL)")

    def test_changing_the_floor_takes_the_maintenance_grant_until_the_grants_run_again(self, accounts, migrated):
        with migrated.begin() as connection:
            for statement in jasil_audit.retention_floor_sql(60, schema=_database(connection), dialect="mysql"):
                connection.execute(text(statement))
        with pytest.raises(DBAPIError, match="denied"):
            self._as(accounts, ACCOUNTS[2], "CALL jasil_audit_drop_partitions(UTC_TIMESTAMP(), NULL)")

        with migrated.begin() as connection:
            for statement in jasil_audit.grants_sql(*ACCOUNTS[:2], maintenance_role=ACCOUNTS[2], dialect="mysql"):
                connection.execute(text(statement))

        with pytest.raises(DBAPIError, match="floor of 60 days"):
            self._as(accounts, ACCOUNTS[2], "CALL jasil_audit_drop_partitions(UTC_TIMESTAMP(), NULL)")


class TestEndToEnd:
    def test_the_python_paths_work_on_the_partitioned_table(self, migrated, action):
        # DATETIME rounds to whole seconds, so a row staged on the system clock can be stored after "now".
        platform = jasil_testing.install_test_platform(".", clock=jasil_testing.FixedClock())
        now = datetime.now(UTC).replace(microsecond=0)
        platform.clock.moment = now - timedelta(minutes=1)
        jasil_audit.get_catalog()  # the built-ins
        with sessionmaker(bind=migrated)() as db:
            jasil_audit.stage(db, action, outcome=Outcome.DENIED, target_id="7")
            db.commit()
        jasil_audit.record_detached(action, outcome=Outcome.FAILURE, target_id="8")
        platform.clock.moment = now

        page = jasil_audit.list_entries()

        assert sorted((entry.target_id, entry.outcome) for entry in page.entries) == [("7", "denied"), ("8", "failure")]
        assert jasil_audit.summary().total == 2

    def test_maintenance_creates_partitions_and_reports_the_horizon(self, migrated, action):
        jasil_testing.install_test_platform(".", clock=SystemClock())

        jasil_audit.run_audit_maintenance()

        assert jasil_audit.get_audit_health()["furthest_partition_date"] == datetime.now(UTC).date() + timedelta(days=7)

    def test_checkpoints_seal_and_verify_across_partitions(self, migrated, action):
        platform = jasil_testing.install_test_platform(".", clock=jasil_testing.FixedClock())
        clock = platform.clock
        now = datetime.now(UTC).replace(microsecond=0)
        clock.moment = now - timedelta(minutes=50)
        jasil_audit.record_detached(action, outcome=Outcome.SUCCESS, target_id="a", details=None)
        clock.moment = now

        from jasil.audit import checkpoints

        assert checkpoints.seal_pending_windows() >= 1
        report = jasil_audit.verify_checkpoints(None, now - timedelta(hours=2), now + timedelta(hours=1))

        assert report.ok and report.windows
        with sessionmaker(bind=migrated)() as db:
            assert db.query(AuditCheckpoint).count() == len(report.windows)
            assert db.query(AuditLog).filter(AuditLog.target_id == "a").count() == 1

    def test_a_missing_partition_fails_the_staged_write_and_counts_the_detached_one(self, migrated, action):
        clock = jasil_testing.FixedClock()
        clock.moment = datetime.now(UTC) + timedelta(days=60)
        jasil_testing.install_test_platform(".", clock=clock)

        with pytest.raises(DBAPIError, match="no partition"), sessionmaker(bind=migrated)() as db:
            jasil_audit.stage(db, action)
            db.commit()
        jasil_audit.record_detached(action, outcome=Outcome.SUCCESS)

        assert jasil_audit.get_audit_health()["write_failures"] == 1


class TestPlainSchema:
    """A schema from ``create_all`` is a plain table on MySQL too; maintenance must still work on it."""

    @pytest.fixture
    def plain(self, engine, mapped_base):
        mapped_base.metadata.create_all(engine)
        jasil_orm.configure_sessionmaker(sessionmaker(bind=engine, expire_on_commit=False))
        return engine

    def test_maintenance_falls_back_to_a_delete_and_says_so_once(self, plain, action, caplog):
        import jasil.audit.maintenance as maintenance

        maintenance._warned_plain = False
        platform = jasil_testing.install_test_platform(".", clock=jasil_testing.FixedClock(), settings=DAY_WINDOWS)
        platform.clock.moment = datetime.now(UTC) - timedelta(days=100)
        jasil_audit.record_detached(action, outcome=Outcome.SUCCESS, target_id="old")
        platform.clock.moment = datetime.now(UTC)

        with caplog.at_level("WARNING"):
            jasil_audit.run_audit_maintenance()
            jasil_audit.run_audit_maintenance()

        assert caplog.text.count("is a plain table") == 1
        assert _scalar(plain, "SELECT COUNT(*) FROM jasil_audit_log WHERE target_id = 'old'") == 0
        assert _prune_rows(plain) == 1
        assert jasil_audit.get_audit_health()["furthest_partition_date"] is None
