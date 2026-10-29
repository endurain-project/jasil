"""Packaged Alembic migrations.

The failure that matters here is **drift**: the migration creating a schema that
differs from what the models declare. Hosts using ``create_all`` in tests and
migrations in production would then run against two different schemas, and the
difference would only surface in production. ``test_the_migration_matches_the_models``
is the guard for that.
"""

import logging
import threading
import time
from types import SimpleNamespace
from typing import ClassVar

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, UniqueConstraint, create_engine, inspect, text

from jasil import migrations
from jasil.audit import _postgres as audit_pg
from jasil.orm import jasil_table_names, legacy_table_names

pytest.importorskip("alembic", reason="migrations require the 'migrations' extra")


@pytest.fixture
def engine(mapped_base, database_url):
    engine = create_engine(database_url)
    _drop_everything(engine, mapped_base)
    yield engine
    # A real server is shared across the run, so leave nothing behind.
    _drop_everything(engine, mapped_base)
    engine.dispose()


def _drop_everything(engine, mapped_base) -> None:
    """Remove JASIL's tables, its Alembic version table and the audit functions, if present."""
    mapped_base.metadata.drop_all(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {migrations.VERSION_TABLE}")
        # Tables from before the ``jasil_`` prefix, left by a test that migrated
        # only to an older revision.
        for name in sorted(legacy_table_names()):
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
        # rev0006's PostgreSQL functions outlive the tables: left behind, they make a later
        # test's plain ``create_all`` schema look partitioned to audit maintenance.
        if engine.dialect.name == "postgresql":
            schema = connection.execute(text("SELECT current_schema()")).scalar_one()
            for statement in audit_pg.uninstall_statements(schema):
                connection.exec_driver_sql(statement)


def _drop_version_table(engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE {migrations.VERSION_TABLE}")


def _copied_jasil_metadata(mapped_base) -> MetaData:
    metadata = MetaData()
    for table_name in jasil_table_names():
        mapped_base.metadata.tables[table_name].to_metadata(metadata)
    return metadata


def _revision_metadata(revision: str) -> MetaData:
    """The schema ``revision`` builds, derived from the migrations' own downgrades."""
    return next(metadata for rev, metadata in migrations._revision_schemas() if rev == revision)


def _schema(engine) -> dict[str, dict]:
    """Snapshot JASIL's tables as {table: {column: type_string}}."""
    inspector = inspect(engine)
    present = set(inspector.get_table_names()) & jasil_table_names()
    return {
        table: {column["name"]: str(column["type"]) for column in inspector.get_columns(table)} for table in present
    }


def _indexes(engine) -> dict[str, set[str]]:
    inspector = inspect(engine)
    present = set(inspector.get_table_names()) & jasil_table_names()
    return {table: {index["name"] for index in inspector.get_indexes(table)} for table in present}


class TestUpgrade:
    def test_it_creates_every_jasil_table(self, engine):
        migrations.upgrade(engine)

        assert set(inspect(engine).get_table_names()) >= jasil_table_names()

    def test_it_records_the_head_revision(self, engine):
        migrations.upgrade(engine)

        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_it_uses_a_dedicated_version_table(self, engine):
        """The host owns ``alembic_version``; colliding with it would make one
        project's history overwrite the other's."""
        migrations.upgrade(engine)

        tables = set(inspect(engine).get_table_names())
        assert migrations.VERSION_TABLE in tables
        assert "alembic_version" not in tables

    def test_it_is_idempotent(self, engine):
        migrations.upgrade(engine)

        migrations.upgrade(engine)

        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_existing_jobs_become_default_queue_jobs(self, engine):
        migrations.upgrade(engine, "rev0001")
        with engine.begin() as connection:
            connection.execute(
                text(
                    """INSERT INTO processing_jobs (
                        id, event_id, event_type, subscriber_id, source, payload,
                        max_attempts, available_at, created_at, updated_at
                    ) VALUES (
                        'job-1', 'event-1', 'event.created', 'subscriber', 'test', '{}',
                        3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )"""
                )
            )

        migrations.upgrade(engine)

        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT queue FROM jasil_processing_jobs WHERE id = 'job-1'")).scalar_one()
                == "default"
            )

    def test_old_writers_receive_the_database_default_queue(self, engine):
        migrations.upgrade(engine)

        with engine.begin() as connection:
            connection.execute(
                text(
                    """INSERT INTO jasil_processing_jobs (
                        id, event_id, event_type, subscriber_id, source, payload,
                        max_attempts, available_at, created_at, updated_at
                    ) VALUES (
                        'job-1', 'event-1', 'event.created', 'subscriber', 'test', '{}',
                        3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )"""
                )
            )

        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT queue FROM jasil_processing_jobs WHERE id = 'job-1'")).scalar_one()
                == "default"
            )


class TestNoDrift:
    # The migration adds one index the ORM cannot declare: a PostgreSQL-only GIN
    # index on ``event_metadata`` (jsonb_path_ops) for ``@>`` correlation
    # queries. SQLite cannot build it, so it is created in the migration rather
    # than in ``__table_args__`` — an intentional difference, not drift.
    DIALECT_ONLY_INDEXES: ClassVar[frozenset[str]] = frozenset({"idx_jasil_event_log_metadata_gin"})

    def test_the_migration_matches_the_models(self, engine, mapped_base):
        """A migrated database and a ``create_all`` database must be identical.

        Drift here means tests (create_all) and production (migrations) run
        against different schemas. Both halves run against the *same* database in
        turn, so this holds on whichever backend the suite is pointed at.
        """
        migrations.upgrade(engine)
        migrated = _schema(engine)
        _drop_everything(engine, mapped_base)

        mapped_base.metadata.create_all(engine)
        created = _schema(engine)

        assert migrated == created

    def test_the_indexes_match_the_models(self, engine, mapped_base):
        migrations.upgrade(engine)
        migrated = _indexes(engine)
        _drop_everything(engine, mapped_base)

        mapped_base.metadata.create_all(engine)
        created = _indexes(engine)

        assert {table: names - self.DIALECT_ONLY_INDEXES for table, names in migrated.items()} == created

    def test_postgres_gets_the_metadata_gin_index(self, engine):
        """It is what makes ``event_metadata @> '{...}'`` usable on a large trail."""
        if engine.dialect.name != "postgresql":
            pytest.skip("GIN indexes are PostgreSQL-only")

        migrations.upgrade(engine)

        assert "idx_jasil_event_log_metadata_gin" in _indexes(engine)["jasil_event_log"]

    def test_the_unique_constraint_survives_the_migration(self, engine):
        """``(event_id, subscriber_id)`` uniqueness is the idempotent-consumer
        guarantee; losing it in the migration would let a subscriber run twice."""
        migrations.upgrade(engine)

        constraints = inspect(engine).get_unique_constraints("jasil_processing_jobs")

        assert any(set(c["column_names"]) == {"event_id", "subscriber_id"} for c in constraints)


class TestDowngrade:
    def test_it_removes_every_jasil_table(self, engine):
        migrations.upgrade(engine)

        migrations.downgrade(engine, "base")

        assert not set(inspect(engine).get_table_names()) & jasil_table_names()

    def test_the_durable_job_additions_can_downgrade_and_upgrade_again(self, engine):
        migrations.upgrade(engine)

        migrations.downgrade(engine, "rev0001")

        tables = set(inspect(engine).get_table_names())
        assert "queue" not in {column["name"] for column in inspect(engine).get_columns("processing_jobs")}
        assert not tables & {"job_workers", "job_attempts", "jasil_job_workers", "jasil_job_attempts"}

        migrations.upgrade(engine)

        tables = set(inspect(engine).get_table_names())
        assert "queue" in {column["name"] for column in inspect(engine).get_columns("jasil_processing_jobs")}
        assert {"jasil_job_workers", "jasil_job_attempts"} <= tables
        columns = {column["name"] for column in inspect(engine).get_columns("jasil_job_attempts")}
        assert {"job_id", "worker_id", "attempt", "outcome", "duration_ms", "claimed_at"} <= columns
        indexes = {index["name"] for index in inspect(engine).get_indexes("jasil_job_attempts")}
        assert {
            "idx_jasil_job_attempts_job",
            "idx_jasil_job_attempts_worker",
            "idx_jasil_job_attempts_event",
        } <= indexes
        claim = next(
            index
            for index in inspect(engine).get_indexes("jasil_processing_jobs")
            if index["name"] == "idx_jasil_processing_jobs_queue_claim"
        )
        assert claim["column_names"] == ["status", "queue", "available_at"]


class TestPrefixRename:
    def test_rows_survive_the_rename(self, engine):
        migrations.upgrade(engine, "rev0002")
        with engine.begin() as connection:
            connection.execute(
                text(
                    """INSERT INTO processing_jobs (
                        id, event_id, event_type, subscriber_id, source, payload,
                        max_attempts, available_at, created_at, updated_at
                    ) VALUES (
                        'job-1', 'event-1', 'event.created', 'subscriber', 'test', '{}',
                        3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )"""
                )
            )

        migrations.upgrade(engine)

        with engine.connect() as connection:
            assert connection.execute(text("SELECT event_id FROM jasil_processing_jobs")).scalar_one() == "event-1"

    def test_no_unprefixed_table_or_index_is_left_behind(self, engine):
        migrations.upgrade(engine)

        inspector = inspect(engine)
        assert not set(inspector.get_table_names()) & legacy_table_names()
        names = {index["name"] for table in jasil_table_names() for index in inspector.get_indexes(table)}
        assert names
        assert all(name.startswith(("idx_jasil_", "sqlite_autoindex_", "uq_jasil_")) for name in names), names

    def test_the_unique_constraint_is_renamed(self, engine):
        migrations.upgrade(engine)

        constraints = inspect(engine).get_unique_constraints("jasil_processing_jobs")

        assert [c["name"] for c in constraints if set(c["column_names"]) == {"event_id", "subscriber_id"}] == [
            "uq_jasil_processing_jobs_event_subscriber"
        ]

    def test_the_rename_downgrades_to_the_unprefixed_schema_and_back(self, engine):
        migrations.upgrade(engine)

        migrations.downgrade(engine, "rev0002")

        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        assert legacy_table_names() <= tables
        assert not tables & jasil_table_names()
        assert any(
            set(c["column_names"]) == {"event_id", "subscriber_id"}
            and c["name"] == "uq_processing_jobs_event_subscriber"
            for c in inspector.get_unique_constraints("processing_jobs")
        )
        assert {"idx_event_log_created", "idx_event_log_type_status"} <= {
            index["name"] for index in inspector.get_indexes("event_log")
        }

        migrations.upgrade(engine)

        assert jasil_table_names() <= set(inspect(engine).get_table_names())


class TestDeadLetterNotified:
    def test_existing_dead_letters_are_not_called_back_after_the_upgrade(self, engine):
        migrations.upgrade(engine, "rev0003")
        with engine.begin() as connection:
            for status in ("dead_letter", "pending"):
                connection.execute(
                    text(
                        """INSERT INTO jasil_processing_jobs (
                            id, event_id, event_type, subscriber_id, source, payload, status,
                            max_attempts, available_at, created_at, updated_at
                        ) VALUES (
                            :status, :event_id, 'a.b', 's', 't', '{}', :status,
                            1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )"""
                    ),
                    {"status": status, "event_id": f"e-{status}"},
                )

        migrations.upgrade(engine)

        with engine.connect() as connection:
            rows = dict(connection.execute(text("SELECT id, dead_letter_notified_at FROM jasil_processing_jobs")).all())
        assert rows["dead_letter"] is not None
        assert rows["pending"] is None

    def test_it_downgrades_and_upgrades_again(self, engine):
        migrations.upgrade(engine)

        migrations.downgrade(engine, "rev0003")
        columns = {c["name"] for c in inspect(engine).get_columns("jasil_processing_jobs")}
        assert not columns & {"dead_letter_notified_at", "dead_letter_notify_after", "dead_letter_attempts"}

        migrations.upgrade(engine)
        columns = {c["name"] for c in inspect(engine).get_columns("jasil_processing_jobs")}
        assert {"dead_letter_notified_at", "dead_letter_notify_after", "dead_letter_attempts"} <= columns


class TestRevisionTransactions:
    """Each revision commits on its own, so no lock one takes is held through the next."""

    @pytest.fixture
    def failing_build(self, monkeypatch):
        """Make the named index builds fail, as an interrupted ``CREATE INDEX CONCURRENTLY`` would."""
        from jasil.migrations import _online

        def fail(names, where="create_index"):
            original = getattr(_online, where)

            def build(name, *args, **kwargs):
                if name in names:
                    raise RuntimeError(f"build of {name} interrupted")
                return original(name, *args, **kwargs)

            monkeypatch.setattr(_online, where, build)
            return lambda: monkeypatch.setattr(_online, where, original)

        return fail

    def test_a_failed_revision_keeps_the_ones_before_it(self, engine, failing_build):
        failing_build({"idx_jasil_processing_jobs_dead_letter"})

        with pytest.raises(RuntimeError, match="interrupted"):
            migrations.upgrade(engine)

        assert migrations.db_revision(engine) == "rev0003"

    def test_an_upgrade_that_failed_in_an_index_build_is_resumed(self, engine, failing_build):
        if engine.dialect.name != "postgresql":
            pytest.skip("only PostgreSQL commits a revision's changes before building its index")
        restore = failing_build({"idx_jasil_processing_jobs_dead_letter"}, where="_create_concurrently")
        with pytest.raises(RuntimeError, match="interrupted"):
            migrations.upgrade(engine)
        columns = {c["name"] for c in inspect(engine).get_columns("jasil_processing_jobs")}
        assert "dead_letter_notified_at" in columns  # committed before the build
        assert migrations.db_revision(engine) == "rev0003"
        restore()

        migrations.upgrade(engine)

        assert migrations.db_revision(engine) == migrations.head_revision()
        assert _index_validity(engine, "idx_jasil_processing_jobs_dead_letter") is True

    def test_an_index_a_failed_build_left_invalid_is_rebuilt(self, engine, failing_build):
        if engine.dialect.name != "postgresql":
            pytest.skip("only PostgreSQL leaves an invalid index behind")
        restore = failing_build({"idx_jasil_processing_jobs_dead_letter"}, where="_create_concurrently")
        with pytest.raises(RuntimeError, match="interrupted"):
            migrations.upgrade(engine)
        restore()
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE INDEX idx_jasil_processing_jobs_dead_letter ON jasil_processing_jobs "
                    "(status, dead_letter_notified_at, dead_letter_notify_after)"
                )
            )
            connection.execute(
                text(
                    "UPDATE pg_index SET indisvalid = false "
                    "WHERE indexrelid = 'idx_jasil_processing_jobs_dead_letter'::regclass"
                )
            )

        migrations.upgrade(engine)

        assert _index_validity(engine, "idx_jasil_processing_jobs_dead_letter") is True

    def test_the_jobs_table_indexes_are_built_concurrently_on_postgresql(self, engine):
        if engine.dialect.name != "postgresql":
            pytest.skip("CONCURRENTLY is PostgreSQL's")

        migrations.upgrade(engine)

        for name in (
            "idx_jasil_processing_jobs_queue_claim",
            "idx_jasil_processing_jobs_worker_claim",
            "idx_jasil_processing_jobs_dead_letter",
        ):
            assert _index_validity(engine, name) is True


def _index_validity(engine, name: str) -> bool | None:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"), {"name": name}
        ).scalar()


class TestOfflineSql:
    @pytest.mark.parametrize("span", ["rev0002:rev0003", "base:rev0003"])
    def test_the_prefix_rename_can_be_rendered_as_sql(self, span):
        """``alembic upgrade --sql`` has no database to inspect."""
        import io

        from alembic import command

        config = migrations._config()
        config.set_main_option("sqlalchemy.url", "postgresql://offline/none")
        config.output_buffer = io.StringIO()

        command.upgrade(config, span, sql=True)

        sql = config.output_buffer.getvalue()
        assert "RENAME TO jasil_event_log" in sql
        assert 'RENAME CONSTRAINT "uq_processing_jobs_event_subscriber"' in sql

    def test_the_jobs_table_indexes_render_outside_the_revision_transaction(self):
        import io

        from alembic import command

        config = migrations._config()
        config.set_main_option("sqlalchemy.url", "postgresql://offline/none")
        config.output_buffer = io.StringIO()

        command.upgrade(config, "rev0003:rev0005", sql=True)

        sql = config.output_buffer.getvalue()
        build = sql.index("CREATE INDEX CONCURRENTLY idx_jasil_processing_jobs_dead_letter")
        assert sql.rindex("COMMIT", 0, build) > sql.rindex("ADD COLUMN dead_letter_notified_at", 0, build)


class TestAdoptExistingSchema:
    def test_an_empty_database_is_not_stamped_and_can_be_upgraded(self, engine):
        assert migrations.adopt_existing_schema(engine) is False
        assert migrations.db_revision(engine) is None

        migrations.upgrade(engine)

        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_a_complete_unversioned_schema_is_adopted_idempotently(self, engine):
        migrations.upgrade(engine)
        _drop_version_table(engine)

        assert migrations.adopt_existing_schema(engine) is True
        assert migrations.db_revision(engine) == migrations.head_revision()
        assert migrations.adopt_existing_schema(engine) is False

        migrations.upgrade(engine)
        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_a_partial_schema_fails_without_recording_a_revision(self, engine, mapped_base):
        mapped_base.metadata.create_all(engine)
        mapped_base.metadata.tables["jasil_event_outbox"].drop(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match=r"missing tables: jasil_event_outbox"):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_missing_column_is_identified_without_recording_a_revision(self, engine, mapped_base):
        migrations.upgrade(engine)
        _drop_version_table(engine)
        expected_index = next(
            index
            for index in mapped_base.metadata.tables["jasil_event_outbox"].indexes
            if index.name == "idx_jasil_event_outbox_relayed"
        )
        expected_index.drop(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE jasil_event_outbox DROP COLUMN relayed_at")

        with pytest.raises(
            migrations.SchemaCompatibilityError, match=r"jasil_event_outbox.*missing column 'relayed_at'"
        ):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_an_unexpected_column_is_rejected(self, engine):
        migrations.upgrade(engine)
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("ALTER TABLE jasil_event_outbox ADD COLUMN legacy_value VARCHAR(20)")

        with pytest.raises(
            migrations.SchemaCompatibilityError, match=r"jasil_event_outbox.*unexpected column 'legacy_value'"
        ):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_missing_required_index_is_rejected(self, engine, mapped_base):
        migrations.upgrade(engine)
        _drop_version_table(engine)
        expected_index = next(
            index
            for index in mapped_base.metadata.tables["jasil_event_outbox"].indexes
            if index.name == "idx_jasil_event_outbox_relayed"
        )
        expected_index.drop(engine)

        with pytest.raises(
            migrations.SchemaCompatibilityError,
            match=r"jasil_event_outbox.*missing index 'idx_jasil_event_outbox_relayed'",
        ):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_wrong_column_type_is_rejected(self, engine, mapped_base):
        metadata = _copied_jasil_metadata(mapped_base)
        metadata.tables["jasil_event_outbox"].c.event_type.type = Integer()
        metadata.create_all(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match=r"jasil_event_outbox.event_type.*expected type"):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_wrong_nullability_is_rejected(self, engine, mapped_base):
        metadata = _copied_jasil_metadata(mapped_base)
        metadata.tables["jasil_event_outbox"].c.event_type.nullable = True
        metadata.create_all(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match=r"jasil_event_outbox.event_type.*nullable=False"):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_missing_primary_key_is_rejected(self, engine, mapped_base):
        metadata = _copied_jasil_metadata(mapped_base)
        copied_table = metadata.tables["jasil_event_outbox"]
        columns = [
            Column(
                column.name,
                column.type,
                nullable=column.nullable,
                server_default=column.server_default,
                comment=column.comment,
            )
            for column in copied_table.columns
        ]
        metadata.remove(copied_table)
        table = Table("jasil_event_outbox", metadata, *columns)
        Index("idx_jasil_event_outbox_relayed", table.c.relayed_at, table.c.created_at)
        metadata.create_all(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match=r"jasil_event_outbox.*expected primary key"):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_missing_unique_constraint_is_rejected(self, engine, mapped_base):
        metadata = _copied_jasil_metadata(mapped_base)
        table = metadata.tables["jasil_processing_jobs"]
        constraint = next(item for item in table.constraints if isinstance(item, UniqueConstraint))
        table.constraints.remove(constraint)
        metadata.create_all(engine)

        with pytest.raises(
            migrations.SchemaCompatibilityError,
            match=r"jasil_processing_jobs.*missing unique constraint 'uq_jasil_processing_jobs_event_subscriber'",
        ):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_an_additional_non_unique_index_is_allowed(self, engine, mapped_base):
        migrations.upgrade(engine)
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE INDEX idx_host_event_outbox_event_id ON jasil_event_outbox (event_id)")

        assert migrations.adopt_existing_schema(engine) is True
        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_an_existing_head_revision_is_not_restamped(self, engine):
        migrations.upgrade(engine)

        assert migrations.adopt_existing_schema(engine) is False
        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_an_existing_older_revision_is_preserved(self, engine):
        metadata = MetaData()
        version_table = Table(
            migrations.VERSION_TABLE,
            metadata,
            Column("version_num", String(32), primary_key=True),
        )
        metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(version_table.insert().values(version_num="older_revision"))

        assert migrations.adopt_existing_schema(engine) is False
        assert migrations.db_revision(engine) == "older_revision"

    def test_a_baseline_schema_is_adopted_at_its_revision_then_upgraded(self, engine):
        migrations.upgrade(engine, "rev0001")
        _drop_version_table(engine)
        assert "job_workers" not in inspect(engine).get_table_names()

        assert migrations.adopt_existing_schema(engine) is True
        assert migrations.db_revision(engine) == "rev0001"
        assert migrations.adopt_existing_schema(engine) is False

        migrations.upgrade(engine)
        assert migrations.db_revision(engine) == migrations.head_revision()
        migrations.verify_schema_current(engine)
        assert _schema(engine)["jasil_processing_jobs"].keys() >= {"queue"}

    def test_an_unversioned_pre_prefix_schema_is_adopted_then_renamed(self, engine):
        """Adoption is how a 0.8 database created without migrations reaches 0.9."""
        migrations.upgrade(engine, "rev0002")
        _drop_version_table(engine)

        assert migrations.adopt_existing_schema(engine) is True
        assert migrations.db_revision(engine) == "rev0002"

        migrations.upgrade(engine)

        tables = set(inspect(engine).get_table_names())
        assert jasil_table_names() <= tables
        assert not tables & legacy_table_names()

    def test_upgrade_refuses_an_unversioned_pre_prefix_schema(self, engine):
        """Creating empty prefixed tables beside the old ones would strand the data."""
        migrations.upgrade(engine, "rev0002")
        _drop_version_table(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match="adopt_existing_schema"):
            migrations.upgrade(engine)

        tables = set(inspect(engine).get_table_names())
        assert not tables & jasil_table_names()

    def test_upgrade_refuses_an_unversioned_prefixed_schema(self, engine):
        """Running the unprefixed revisions beside prefixed tables would duplicate them."""
        migrations.upgrade(engine)
        _drop_version_table(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match="adopt_existing_schema"):
            migrations.upgrade(engine)

        assert not set(inspect(engine).get_table_names()) & legacy_table_names()

    def test_a_baseline_schema_with_a_wrong_column_type_fails_closed(self, engine):
        migrations.upgrade(engine, "rev0001")
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE event_outbox")
        metadata = _revision_metadata("rev0001")
        metadata.tables["event_outbox"].c.event_type.type = Integer()
        metadata.tables["event_outbox"].create(engine)

        with pytest.raises(migrations.SchemaCompatibilityError, match=r"closest revision rev0001.*event_type"):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_baseline_schema_missing_a_table_fails_closed(self, engine):
        migrations.upgrade(engine, "rev0001")
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE event_outbox")

        with pytest.raises(migrations.SchemaCompatibilityError, match=r"closest revision rev0001.*missing tables"):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_baseline_schema_with_a_later_revisions_table_is_not_adopted_as_the_baseline(self, engine):
        migrations.upgrade(engine, "rev0001")
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE job_workers (instance_id VARCHAR(36) PRIMARY KEY)")

        with pytest.raises(migrations.SchemaCompatibilityError):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None

    def test_a_hosts_own_event_log_table_is_not_mistaken_for_jaisls(self, engine):
        """The collision the prefix exists to avoid: a host with its own ``event_log``
        must be able to adopt and migrate without JASIL touching it."""
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE event_log (host_id INTEGER PRIMARY KEY, note VARCHAR(20))")
            connection.exec_driver_sql("INSERT INTO event_log (host_id, note) VALUES (1, 'mine')")

        assert migrations.adopt_existing_schema(engine) is False
        migrations.upgrade(engine)

        assert jasil_table_names() <= set(inspect(engine).get_table_names())
        with engine.connect() as connection:
            assert connection.execute(text("SELECT note FROM event_log")).scalar_one() == "mine"

    def test_a_hosts_own_event_log_table_does_not_break_adopting_the_prefixed_schema(self, engine):
        migrations.upgrade(engine)
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE event_log (host_id INTEGER PRIMARY KEY)")

        assert migrations.adopt_existing_schema(engine) is True
        assert migrations.db_revision(engine) == migrations.head_revision()

    def test_postgres_requires_the_metadata_gin_index(self, engine):
        if engine.dialect.name != "postgresql":
            pytest.skip("GIN indexes are PostgreSQL-only")
        migrations.upgrade(engine)
        _drop_version_table(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP INDEX idx_jasil_event_log_metadata_gin")

        with pytest.raises(
            migrations.SchemaCompatibilityError,
            match=r"jasil_event_log.*missing index 'idx_jasil_event_log_metadata_gin'",
        ):
            migrations.adopt_existing_schema(engine)

        assert migrations.db_revision(engine) is None


class TestSchemaVerification:
    def test_an_unmigrated_database_is_reported(self, engine):
        with pytest.raises(RuntimeError, match="out of date"):
            migrations.verify_schema_current(engine)

    def test_the_error_names_the_remedy(self, engine):
        with pytest.raises(RuntimeError, match=r"jasil\.migrations\.upgrade"):
            migrations.verify_schema_current(engine)

    def test_a_migrated_database_passes(self, engine):
        migrations.upgrade(engine)

        migrations.verify_schema_current(engine)

    def test_a_database_behind_head_is_reported(self, engine):
        migrations.upgrade(engine, "rev0005")

        with pytest.raises(RuntimeError, match="'rev0005'"):
            migrations.verify_schema_current(engine)

    def test_a_database_a_newer_release_migrated_passes_with_a_warning(self, engine, caplog):
        """During a rolling deploy the previous release restarts against the schema the next one migrated."""
        migrations.upgrade(engine)
        with engine.begin() as connection:
            connection.execute(text("UPDATE jasil_alembic_version SET version_num = 'rev9999'"))

        with caplog.at_level(logging.WARNING, logger="jasil.migrations"):
            migrations.verify_schema_current(engine)

        assert "rev9999" in caplog.text


class TestHostIsolation:
    def test_host_tables_are_out_of_scope(self):
        """The scoping hook is what stops JASIL's autogenerate from dropping the
        host's tables, which share the same registry."""
        assert migrations.jasil_include_object(None, "users", "table", True, None) is False
        assert migrations.jasil_include_object(None, "jasil_event_log", "table", True, None) is True

    def test_an_unprefixed_name_is_the_hosts(self):
        """The prefix exists so a host's own ``event_log`` is never JASIL's to diff or drop."""
        assert migrations.jasil_include_object(None, "event_log", "table", True, None) is False

    def test_non_table_objects_are_always_included(self):
        assert migrations.jasil_include_object(None, "some_index", "index", True, None) is True


class TestRevisionSchemas:
    def test_every_derived_schema_matches_the_real_migration(self, engine, mapped_base):
        """The downgrade-derived schema of each revision equals what upgrading to it builds."""
        derived = migrations._revision_schemas()
        assert derived[0][0] == migrations.head_revision()
        for revision, metadata in derived:
            _drop_everything(engine, mapped_base)
            migrations.upgrade(engine, revision)
            _drop_version_table(engine)
            with engine.connect() as connection:
                differences, _ = migrations._schema_differences(connection, inspect(connection), metadata)
            assert differences == [], revision

    def test_an_unsupported_downgrade_operation_is_refused(self, mapped_base):
        ops = migrations._MetadataOps(_copied_jasil_metadata(mapped_base))

        with pytest.raises(NotImplementedError, match=r"op\.alter_column"):
            ops.alter_column("jasil_event_log", "status")

    def test_the_metadata_ops_reject_an_unknown_index(self, mapped_base):
        ops = migrations._MetadataOps(_copied_jasil_metadata(mapped_base))

        with pytest.raises(RuntimeError, match="unknown index"):
            ops.drop_index("idx_missing", table_name="jasil_event_log")


class TestASchemaQualifiedBaseIsRefused:
    """The migrations create JASIL's tables in the default schema; models in another one would never find them."""

    @pytest.fixture
    def host_schema(self, monkeypatch):
        import jasil.orm

        monkeypatch.setattr(jasil.orm, "get_active_base", lambda: SimpleNamespace(metadata=MetaData(schema="host")))

    @pytest.mark.parametrize(
        "run",
        [migrations.upgrade, lambda engine: migrations.downgrade(engine, "base"), migrations.adopt_existing_schema],
        ids=["upgrade", "downgrade", "adopt"],
    )
    def test_every_entry_point_refuses_it(self, engine, host_schema, run):
        with pytest.raises(migrations.SchemaCompatibilityError, match="'host'"):
            run(engine)

        assert migrations.db_revision(engine) is None


class _Connection:
    """Records the statements a lock helper runs; ``acquired`` is what ``GET_LOCK`` answers."""

    def __init__(self, dialect: str, acquired: int | None = 1) -> None:
        self.dialect = type("Dialect", (), {"name": dialect})()
        self.statements: list[str] = []
        self._acquired = acquired

    def execute(self, statement, parameters=None):
        self.statements.append(str(statement))
        return type("Result", (), {"scalar": lambda _self: self._acquired})()

    def in_transaction(self) -> bool:
        return True

    def rollback(self) -> None:
        self.statements.append("ROLLBACK")


class TestOneRunAtATime:
    """Processes that upgrade at once (replicas starting together) must queue, not collide."""

    def test_postgresql_holds_a_session_lock_across_the_revision_commits(self):
        """Each revision commits on its own, so a transaction lock would be gone after the first one."""
        connection = _Connection("postgresql")

        with migrations._one_run_at_a_time(connection):
            assert connection.statements == ["SELECT pg_advisory_lock(hashtext(:name))"]

        # A transaction still open is the caller's: adoption stamps inside one and commits it itself.
        assert connection.statements[-1] == "SELECT pg_advisory_unlock(hashtext(:name))"
        assert "ROLLBACK" not in connection.statements

    def test_postgresql_releases_the_lock_after_a_failed_run(self):
        connection = _Connection("postgresql")

        with pytest.raises(RuntimeError, match="boom"), migrations._one_run_at_a_time(connection):
            raise RuntimeError("boom")

        # The failed transaction is ended first, or the unlock could not run in it.
        assert connection.statements[-2:] == ["ROLLBACK", "SELECT pg_advisory_unlock(hashtext(:name))"]

    @pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
    def test_mysql_holds_a_session_lock_and_releases_it_even_when_the_run_fails(self, dialect):
        connection = _Connection(dialect)

        with pytest.raises(RuntimeError, match="boom"), migrations._one_run_at_a_time(connection):
            raise RuntimeError("boom")

        assert connection.statements == ["SELECT GET_LOCK(:name, -1)", "SELECT RELEASE_LOCK(:name)"]

    @pytest.mark.parametrize("answer", [0, None])
    def test_mysql_refuses_to_run_without_the_lock(self, answer):
        connection = _Connection("mysql", acquired=answer)

        with pytest.raises(RuntimeError, match="migration lock"), migrations._one_run_at_a_time(connection):
            pytest.fail("ran without the lock")

    def test_sqlite_is_not_locked(self):
        connection = _Connection("sqlite")

        with migrations._one_run_at_a_time(connection):
            pass

        assert connection.statements == []

    def test_an_upgrade_waits_for_one_already_running(self, engine):
        if engine.dialect.name == "sqlite":
            pytest.skip("SQLite takes a single process: JASIL refuses a multi-process SQLite deployment")
        hold, release, waiting = _LOCK_PROBES[engine.dialect.name]
        finished = threading.Event()
        with engine.connect() as holder:
            holder.execute(text(hold))
            upgrading = threading.Thread(target=lambda: (migrations.upgrade(engine), finished.set()))
            upgrading.start()
            try:
                assert _eventually(lambda: _scalar(engine, waiting) > 0), "the upgrade never waited for the lock"
                assert not finished.is_set()
            finally:
                holder.execute(text(release))
                holder.commit()
                upgrading.join(timeout=30)

        assert finished.is_set()
        assert migrations.db_revision(engine) == migrations.head_revision()


#: Per dialect: take the migration lock in another session, give it back, count the sessions waiting for it.
_LOCK_PROBES = {
    "postgresql": (
        "SELECT pg_advisory_lock(hashtext('jasil_migrations'))",
        "SELECT pg_advisory_unlock(hashtext('jasil_migrations'))",
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
    ),
    "mysql": (
        "SELECT GET_LOCK('jasil_migrations', 0)",
        "SELECT RELEASE_LOCK('jasil_migrations')",
        "SELECT count(*) FROM information_schema.processlist WHERE state = 'User lock'",
    ),
}


def _scalar(engine, sql: str):
    with engine.connect() as connection:
        return connection.execute(text(sql)).scalar()


def _eventually(condition, timeout: float = 10.0) -> bool:
    """Poll ``condition`` (a query, so the loop is paced by the database) until it holds or time runs out."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
    return True
