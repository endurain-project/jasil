"""PostgreSQL conformance for the audit log: partitions, append-only triggers, the retention guard, roles.

Skipped on the SQLite developer default and the MySQL jobs. Nothing is mocked: the migration builds the
real partitioned table and the real functions, and the assertions are what the database does when asked
to break them.
"""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, func, select, table, text
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.orm import sessionmaker

import jasil.audit as jasil_audit
import jasil.orm as jasil_orm
import jasil.settings as jasil_settings
import jasil.testing as jasil_testing
from jasil import migrations
from jasil.audit import AuditAction, Category, Outcome, Severity
from jasil.audit.models import AuditCheckpoint, AuditLog
from jasil.backends.clock_system import SystemClock

pytest.importorskip("alembic", reason="migrations require the 'migrations' extra")

ROLES = ("jasil_t_runtime", "jasil_t_reader", "jasil_t_maintenance")
TABLES = ("jasil_audit_log", "jasil_audit_actions", "jasil_audit_checkpoints", "jasil_instance")
# Retention prunes only what a sealed checkpoint allows; day-long windows seal months of history in one run.
DAY_WINDOWS = jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(checkpoint_window_minutes=1440))
FUNCTIONS = (
    "jasil_audit_drop_partitions(timestamptz)",
    "jasil_audit_ensure_partitions(integer)",
    "jasil_audit_forbid_mutation()",
)


def _clean(engine, mapped_base) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {', '.join(TABLES)} CASCADE")
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {migrations.VERSION_TABLE}")
        for function in FUNCTIONS:
            connection.exec_driver_sql(f"DROP FUNCTION IF EXISTS {function}")
        for role in ROLES:
            connection.exec_driver_sql(f"DROP OWNED BY {role}") if _role_exists(connection, role) else None
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {role}")
    mapped_base.metadata.drop_all(engine)


def _role_exists(connection, role: str) -> bool:
    return connection.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}).first() is not None


@pytest.fixture
def engine(mapped_base, database_url):
    engine = create_engine(database_url)
    if engine.dialect.name != "postgresql":
        engine.dispose()
        pytest.skip("requires the real PostgreSQL test service")
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


def _count(engine, name: str) -> int:
    with engine.connect() as connection:
        return connection.execute(select(func.count()).select_from(table(name))).scalar_one()


def _partitions(engine) -> list[str]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                text(
                    "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                    "WHERE i.inhparent = 'jasil_audit_log'::regclass ORDER BY c.relname"
                )
            ).scalars()
        )


def _insert_row(connection, when: datetime, identifier: str = "00000000-0000-7000-8000-000000000001") -> None:
    connection.execute(
        text(
            "INSERT INTO jasil_audit_log (id, occurred_at, action_id, category, severity, outcome, actor_type, source) "
            "VALUES (:id, :at, 'shop.order.place', 'data_change', 'low', 'success', 'system', 'api')"
        ),
        {"id": identifier, "at": when},
    )


def _old_partition(connection, day: date) -> str:
    name = f"jasil_audit_log_p{day:%Y%m%d}"
    connection.execute(
        text(
            f"CREATE TABLE {name} PARTITION OF jasil_audit_log "
            f"FOR VALUES FROM ('{day} 00:00:00+00') TO ('{day + timedelta(days=1)} 00:00:00+00')"
        )
    )
    return name


def _shadow_catalog(connection, child: str) -> None:
    """Shadow the partition catalog with session temporary tables that list ``child`` as the only partition.

    Any role may create temporary tables, and an unqualified name is looked up in ``pg_temp`` first unless
    the ``search_path`` names it.
    """
    connection.execute(text("CREATE TEMP TABLE pg_inherits (inhrelid oid, inhparent oid)"))
    connection.execute(text("CREATE TEMP TABLE pg_class (oid oid, relname name)"))
    connection.execute(text("INSERT INTO pg_temp.pg_inherits VALUES (1, 'jasil_audit_log'::regclass)"))
    connection.execute(text("INSERT INTO pg_temp.pg_class VALUES (1, :child)"), {"child": child})


class TestMigration:
    def test_the_log_is_a_range_partitioned_table(self, migrated):
        assert _scalar(migrated, "SELECT relkind FROM pg_class WHERE relname = 'jasil_audit_log'") == "p"
        assert "RANGE (occurred_at)" in _scalar(
            migrated, "SELECT pg_get_partkeydef('jasil_audit_log'::regclass)"
        ).replace("(occurred_at)", " (occurred_at)").replace("  ", " ")

    def test_partitions_exist_from_yesterday_to_the_configured_horizon(self, migrated):
        today = datetime.now(UTC).date()
        wanted = {f"jasil_audit_log_p{today + timedelta(days=offset):%Y%m%d}" for offset in range(-1, 8)}

        assert wanted <= set(_partitions(migrated))

    def test_a_row_lands_in_the_partition_of_its_day(self, migrated):
        with migrated.begin() as connection:
            _insert_row(connection, datetime.now(UTC))

        assert _scalar(migrated, "SELECT count(*) FROM ONLY jasil_audit_log") == 0
        assert _count(migrated, f"jasil_audit_log_p{datetime.now(UTC):%Y%m%d}") == 1

    def test_ensuring_partitions_is_idempotent_and_returns_the_last_day(self, migrated):
        with migrated.begin() as connection:
            first = connection.execute(text("SELECT jasil_audit_ensure_partitions(10)")).scalar()
        before = _partitions(migrated)

        with migrated.begin() as connection:
            second = connection.execute(text("SELECT jasil_audit_ensure_partitions(10)")).scalar()

        assert first == second == datetime.now(UTC).date() + timedelta(days=10)
        assert _partitions(migrated) == before

    @pytest.mark.parametrize("days", [-1, 367])
    def test_the_horizon_is_bounded(self, migrated, days):
        with pytest.raises(DBAPIError, match="between 0 and 366"), migrated.begin() as connection:
            connection.execute(text("SELECT jasil_audit_ensure_partitions(:d)"), {"d": days})

    def test_a_table_squatting_a_partition_name_is_refused(self, migrated):
        squatted = f"jasil_audit_log_p{datetime.now(UTC).date() + timedelta(days=8):%Y%m%d}"
        with migrated.begin() as connection:
            connection.execute(text(f"CREATE TABLE {squatted} (id integer)"))
        try:
            with pytest.raises(DBAPIError, match="is not a partition"), migrated.begin() as connection:
                connection.execute(text("SELECT jasil_audit_ensure_partitions(8)"))
        finally:
            with migrated.begin() as connection:
                connection.execute(text(f"DROP TABLE {squatted}"))

    def test_a_temporary_table_cannot_shadow_the_catalog_for_the_partition_horizon(self, migrated):
        with migrated.begin() as connection:
            _shadow_catalog(connection, "jasil_audit_log_p29991231")
            furthest = connection.execute(text("SELECT jasil_audit_ensure_partitions(0)")).scalar()

        assert furthest == datetime.now(UTC).date() + timedelta(days=7)

    def test_the_functions_are_closed_to_public(self, migrated):
        acl = _scalar(migrated, "SELECT proacl::text FROM pg_proc WHERE proname = 'jasil_audit_drop_partitions'")

        grantees = [entry.split("=")[0] for entry in acl.strip("{}").split(",")]
        assert "" not in grantees  # an empty grantee is PUBLIC

    def test_the_floor_is_baked_in_from_the_settings_at_migration_time(self, engine, mapped_base):
        jasil_settings.configure(
            jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(retention_floor_days=45))
        )
        migrations.upgrade(engine)

        with pytest.raises(DBAPIError, match="floor of 45 days"), engine.begin() as connection:
            connection.execute(text("SELECT jasil_audit_drop_partitions(now() - interval '40 days')"))

    def test_downgrading_removes_the_tables_partitions_and_functions(self, migrated):
        migrations.downgrade(migrated, "rev0005")

        assert _scalar(migrated, "SELECT to_regclass('jasil_audit_log')") is None
        assert _scalar(migrated, "SELECT count(*) FROM pg_proc WHERE proname LIKE 'jasil_audit_%'") == 0
        assert migrations.db_revision(migrated) == "rev0005"

    def test_the_migrated_schema_matches_the_models(self, migrated):
        migrations.verify_schema_current(migrated)


class TestAppendOnly:
    @pytest.fixture
    def row(self, migrated):
        with migrated.begin() as connection:
            _insert_row(connection, datetime.now(UTC))
        return migrated

    @pytest.mark.parametrize(
        "statement",
        [
            "UPDATE jasil_audit_log SET outcome = 'failure'",
            "DELETE FROM jasil_audit_log",
            "TRUNCATE jasil_audit_log",
            "TRUNCATE jasil_audit_log, jasil_audit_actions",
        ],
    )
    def test_the_parent_rejects_mutation(self, row, statement):
        with pytest.raises(DBAPIError, match="append-only"), row.begin() as connection:
            connection.execute(text(statement))

        assert _scalar(row, "SELECT count(*) FROM jasil_audit_log") == 1

    @pytest.mark.parametrize("verb", ["UPDATE {t} SET outcome = 'failure'", "DELETE FROM {t}", "TRUNCATE {t}"])
    def test_a_partition_rejects_mutation_when_addressed_directly(self, row, verb):
        partition = f"jasil_audit_log_p{datetime.now(UTC):%Y%m%d}"

        with pytest.raises(DBAPIError, match="append-only"), row.begin() as connection:
            connection.execute(text(verb.format(t=partition)))

        assert _scalar(row, "SELECT count(*) FROM jasil_audit_log") == 1

    def test_a_new_partition_is_protected_too(self, row):
        with row.begin() as connection:
            connection.execute(text("SELECT jasil_audit_ensure_partitions(20)"))
        far = f"jasil_audit_log_p{datetime.now(UTC) + timedelta(days=20):%Y%m%d}"

        with pytest.raises(DBAPIError, match="append-only"), row.begin() as connection:
            connection.execute(text(f"TRUNCATE {far}"))

    def test_inserting_still_works(self, row):
        with row.begin() as connection:
            _insert_row(connection, datetime.now(UTC), "00000000-0000-7000-8000-000000000002")

        assert _scalar(row, "SELECT count(*) FROM jasil_audit_log") == 2

    def test_a_row_for_a_day_without_a_partition_is_refused(self, migrated):
        with pytest.raises(DBAPIError, match="no partition"), migrated.begin() as connection:
            _insert_row(connection, datetime.now(UTC) + timedelta(days=60))


class TestDropPartitions:
    @pytest.fixture
    def aged(self, migrated):
        """Rows in partitions 100, 99 and 2 days old, beside today's."""
        today = datetime.now(UTC).date()
        with migrated.begin() as connection:
            for age in (100, 99, 2):
                _old_partition(connection, today - timedelta(days=age))
                _insert_row(
                    connection,
                    datetime.combine(today - timedelta(days=age), datetime.min.time(), UTC) + timedelta(hours=13),
                    f"00000000-0000-7000-8000-{age:012d}",
                )
        return migrated, today

    def _drop(self, engine, cutoff: datetime) -> int:
        with engine.begin() as connection:
            return connection.execute(text("SELECT jasil_audit_drop_partitions(:c)"), {"c": cutoff}).scalar()

    def test_a_cutoff_inside_the_floor_is_refused_and_nothing_is_dropped(self, aged):
        engine, _ = aged
        before = _partitions(engine)

        for cutoff in (datetime.now(UTC), datetime.now(UTC) - timedelta(days=29)):
            with pytest.raises(DBAPIError, match="inside the retention floor of 30 days"), engine.begin() as connection:
                connection.execute(text("SELECT jasil_audit_drop_partitions(:c)"), {"c": cutoff})

        assert _partitions(engine) == before

    def test_it_drops_whole_partitions_older_than_the_cutoff_only(self, aged):
        engine, today = aged

        dropped = self._drop(engine, datetime.now(UTC) - timedelta(days=90))

        assert dropped == 2
        names = _partitions(engine)
        assert f"jasil_audit_log_p{today - timedelta(days=100):%Y%m%d}" not in names
        assert f"jasil_audit_log_p{today - timedelta(days=99):%Y%m%d}" not in names
        assert f"jasil_audit_log_p{today - timedelta(days=2):%Y%m%d}" in names

    def test_a_partition_that_straddles_the_cutoff_is_kept_whole(self, aged):
        engine, today = aged
        # The cutoff falls in the middle of the day 99 days ago: that day's partition ends after it.
        cutoff = datetime.combine(today - timedelta(days=99), datetime.min.time(), UTC) + timedelta(hours=12)

        dropped = self._drop(engine, cutoff)

        assert dropped == 1
        assert f"jasil_audit_log_p{today - timedelta(days=99):%Y%m%d}" in _partitions(engine)
        assert _count(engine, f"jasil_audit_log_p{today - timedelta(days=99):%Y%m%d}") == 1

    def test_the_rows_go_with_their_partition_and_the_others_are_untouched(self, aged):
        engine, _ = aged

        self._drop(engine, datetime.now(UTC) - timedelta(days=90))

        remaining = _scalar(engine, "SELECT count(*) FROM jasil_audit_log WHERE action_id = 'shop.order.place'")
        assert remaining == 1

    def test_it_records_its_own_prune_row(self, aged):
        engine, today = aged
        cutoff = datetime.now(UTC) - timedelta(days=90)

        self._drop(engine, cutoff)

        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT category, severity, outcome, actor_type, source, details, details_version FROM jasil_audit_log "
                    "WHERE action_id = 'audit.retention.prune'"
                )
            ).one()
        assert row[:5] == ("system", "medium", "success", "system", "worker")
        assert row[5]["partitions_dropped"] == 2
        assert row[5]["first_day"] == str(today - timedelta(days=100))
        assert row[5]["last_day"] == str(today - timedelta(days=99))
        assert row[6] == 1
        # The row is valid against the model JASIL validates every other prune row with.
        jasil_audit.RetentionPruneDetails.model_validate(row[5])

    def test_the_prune_row_id_is_a_uuid_v7(self, aged):
        engine, _ = aged
        self._drop(engine, datetime.now(UTC) - timedelta(days=90))

        identifier = _scalar(engine, "SELECT id FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'")

        assert identifier[14] == "7" and identifier[19] in "89ab"

    def test_a_temporary_table_cannot_make_it_drop_another_table(self, aged):
        engine, _ = aged
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE decoy_p20000101 (id integer)"))
        try:
            with engine.begin() as connection:
                _shadow_catalog(connection, "decoy_p20000101")
                dropped = connection.execute(
                    text("SELECT jasil_audit_drop_partitions(:c)"), {"c": datetime.now(UTC) - timedelta(days=400)}
                ).scalar()

            assert dropped == 0
            assert _scalar(engine, "SELECT to_regclass('decoy_p20000101')::text") == "decoy_p20000101"
        finally:
            with engine.begin() as connection:
                connection.execute(text("DROP TABLE IF EXISTS decoy_p20000101"))

    def test_dropping_nothing_writes_no_row(self, aged):
        engine, _ = aged

        assert self._drop(engine, datetime.now(UTC) - timedelta(days=400)) == 0
        assert _scalar(engine, "SELECT count(*) FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'") == 0

    def test_the_floor_can_be_changed_with_the_documented_statement(self, aged):
        engine, _ = aged
        with engine.begin() as connection:
            connection.execute(text(jasil_audit.retention_floor_sql(120, schema="public")))

        with pytest.raises(DBAPIError, match="floor of 120 days"), engine.begin() as connection:
            connection.execute(text("SELECT jasil_audit_drop_partitions(now() - interval '100 days')"))

    def test_a_running_maintenance_prunes_through_the_function_never_row_by_row(self, aged, action):
        engine, today = aged
        jasil_testing.install_test_platform(".", clock=SystemClock(), settings=DAY_WINDOWS)

        jasil_audit.run_audit_maintenance()

        assert f"jasil_audit_log_p{today - timedelta(days=100):%Y%m%d}" not in _partitions(engine)
        assert _scalar(engine, "SELECT count(*) FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'") == 1

    def test_an_application_clock_ahead_of_the_database_still_prunes_at_the_floor(self, aged, action):
        engine, today = aged
        clock = jasil_testing.FixedClock()
        clock.moment = datetime.now(UTC) + timedelta(hours=2)
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

        assert f"jasil_audit_log_p{today - timedelta(days=99):%Y%m%d}" not in _partitions(engine)

    def test_legal_hold_keeps_the_partitions(self, aged, action):
        engine, today = aged
        jasil_testing.install_test_platform(
            ".",
            clock=SystemClock(),
            settings=jasil_settings.JasilSettings(audit=jasil_settings.AuditSettings(legal_hold=True)),
        )

        jasil_audit.run_audit_maintenance()

        assert f"jasil_audit_log_p{today - timedelta(days=100):%Y%m%d}" in _partitions(engine)


class TestRoles:
    @pytest.fixture
    def roles(self, migrated):
        try:
            with migrated.begin() as connection:
                for role in ROLES:
                    connection.exec_driver_sql(f"CREATE ROLE {role} NOLOGIN")
                for statement in jasil_audit.grants_sql(ROLES[0], ROLES[1], maintenance_role=ROLES[2]):
                    connection.exec_driver_sql(statement)
        except ProgrammingError:
            pytest.skip("creating roles needs a superuser")
        return migrated

    def _as(self, engine, role: str, sql: str, **params):
        with engine.connect() as connection:
            connection.exec_driver_sql(f"SET ROLE {role}")
            try:
                return connection.execute(text(sql), params).all()
            finally:
                connection.rollback()

    def test_the_runtime_role_can_stage_through_the_orm(self, roles, action):
        jasil_orm.configure_sessionmaker(sessionmaker(bind=roles, expire_on_commit=False))
        jasil_testing.install_test_platform(".", clock=SystemClock())
        with roles.connect() as connection:
            connection.exec_driver_sql("INSERT INTO jasil_instance (id, instance_id) VALUES (1, 'x')")
            connection.commit()
        with roles.connect() as connection:
            connection.exec_driver_sql(f"SET ROLE {ROLES[0]}")
            session = sessionmaker(bind=connection, join_transaction_mode="create_savepoint")()
            jasil_audit.stage(session, action, target_id="7")
            session.commit()
            connection.rollback()

    def test_the_runtime_role_cannot_read_the_log(self, roles):
        with pytest.raises(DBAPIError, match="permission denied"):
            self._as(roles, ROLES[0], "SELECT count(*) FROM jasil_audit_log")

    @pytest.mark.parametrize(
        "statement",
        [
            "UPDATE jasil_audit_log SET outcome = 'failure'",
            "DELETE FROM jasil_audit_log",
            "TRUNCATE jasil_audit_log",
            "SELECT jasil_audit_drop_partitions(now() - interval '90 days')",
        ],
    )
    def test_the_runtime_role_cannot_modify_or_prune(self, roles, statement):
        with pytest.raises(DBAPIError, match="permission denied"):
            self._as(roles, ROLES[0], statement)

    def test_the_runtime_role_can_insert_and_create_partitions(self, roles):
        with roles.connect() as connection:
            connection.exec_driver_sql(f"SET ROLE {ROLES[0]}")
            _insert_row(connection, datetime.now(UTC), "00000000-0000-7000-8000-000000000009")
            connection.execute(text("SELECT jasil_audit_ensure_partitions(8)"))
            connection.rollback()

    def test_the_reader_role_can_read_but_not_write(self, roles):
        assert self._as(roles, ROLES[1], "SELECT count(*) FROM jasil_audit_log") == [(0,)]
        with pytest.raises(DBAPIError, match="permission denied"):
            self._as(roles, ROLES[1], "INSERT INTO jasil_instance (id, instance_id) VALUES (1, 'x')")
        with pytest.raises(DBAPIError, match="permission denied"):
            self._as(roles, ROLES[1], "SELECT jasil_audit_ensure_partitions(8)")

    def test_a_role_with_no_grants_can_do_nothing(self, roles):
        with roles.begin() as connection:
            connection.exec_driver_sql("CREATE ROLE jasil_t_reader_x NOLOGIN")
        try:
            for statement in ("SELECT count(*) FROM jasil_audit_log", "SELECT jasil_audit_ensure_partitions(8)"):
                with pytest.raises(DBAPIError, match="permission denied"):
                    self._as(roles, "jasil_t_reader_x", statement)
        finally:
            with roles.begin() as connection:
                connection.exec_driver_sql("DROP ROLE jasil_t_reader_x")

    def test_the_maintenance_role_can_read_checkpoint_and_prune_but_not_edit(self, roles):
        assert self._as(roles, ROLES[2], "SELECT count(*) FROM jasil_audit_log") == [(0,)]
        with pytest.raises(DBAPIError, match="permission denied"):
            self._as(roles, ROLES[2], "UPDATE jasil_audit_log SET outcome = 'failure'")
        with pytest.raises(DBAPIError, match="inside the retention floor"):
            self._as(roles, ROLES[2], "SELECT jasil_audit_drop_partitions(now())")


class TestEndToEnd:
    def test_the_python_paths_work_on_the_partitioned_table(self, migrated, action):
        jasil_testing.install_test_platform(".", clock=SystemClock())
        jasil_audit.get_catalog()  # the built-ins
        with sessionmaker(bind=migrated)() as db:
            jasil_audit.stage(db, action, outcome=Outcome.DENIED, target_id="7")
            db.commit()
        jasil_audit.record_detached(action, outcome=Outcome.FAILURE, target_id="8")

        page = jasil_audit.list_entries()

        assert sorted((entry.target_id, entry.outcome) for entry in page.entries) == [("7", "denied"), ("8", "failure")]
        assert jasil_audit.summary().total == 2

    def test_maintenance_creates_partitions_syncs_and_reports_the_horizon(self, migrated, action):
        jasil_testing.install_test_platform(".", clock=SystemClock())
        with migrated.begin() as connection:
            connection.execute(text("SELECT jasil_audit_ensure_partitions(0)"))

        jasil_audit.run_audit_maintenance()

        assert jasil_audit.get_audit_health()["furthest_partition_date"] == datetime.now(UTC).date() + timedelta(days=7)
        assert {entry.id for entry in jasil_audit.list_catalog()} >= {"shop.order.place", "audit.catalog.sync"}

    def test_checkpoints_seal_and_verify_across_partitions(self, migrated, action):
        platform = jasil_testing.install_test_platform(".", clock=jasil_testing.FixedClock())
        clock = platform.clock
        now = datetime.now(UTC).replace(microsecond=0)
        clock.moment = now - timedelta(minutes=50)
        with migrated.begin() as connection:
            connection.execute(text("SELECT jasil_audit_ensure_partitions(1)"))
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
    """A schema from ``create_all`` is a plain table on PostgreSQL too; maintenance must still work on it."""

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
        assert _scalar(plain, "SELECT count(*) FROM jasil_audit_log WHERE target_id = 'old'") == 0
        assert _scalar(plain, "SELECT count(*) FROM jasil_audit_log WHERE action_id = 'audit.retention.prune'") == 1
        assert jasil_audit.get_audit_health()["furthest_partition_date"] is None
