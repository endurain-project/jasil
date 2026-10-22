"""Packaged Alembic migrations.

The failure that matters here is **drift**: the migration creating a schema that
differs from what the models declare. Hosts using ``create_all`` in tests and
migrations in production would then run against two different schemas, and the
difference would only surface in production. ``test_the_migration_matches_the_models``
is the guard for that.
"""

from typing import ClassVar

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, UniqueConstraint, create_engine, inspect, text

from jasil import migrations
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
    """Remove JASIL's tables and its Alembic version table, if present."""
    mapped_base.metadata.drop_all(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {migrations.VERSION_TABLE}")
        # Tables from before the ``jasil_`` prefix, left by a test that migrated
        # only to an older revision.
        for name in sorted(legacy_table_names()):
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")


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
