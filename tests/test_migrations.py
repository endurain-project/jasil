"""Packaged Alembic migrations.

The failure that matters here is **drift**: the migration creating a schema that
differs from what the models declare. Hosts using ``create_all`` in tests and
migrations in production would then run against two different schemas, and the
difference would only surface in production. ``test_the_migration_matches_the_models``
is the guard for that.
"""

import pytest
from sqlalchemy import create_engine, inspect

from jasil import migrations
from jasil.orm import jasil_table_names

pytest.importorskip("alembic", reason="migrations require the 'migrations' extra")


@pytest.fixture
def engine():
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


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


class TestNoDrift:
    def test_the_migration_matches_the_models(self, engine, mapped_base):
        """A migrated database and a ``create_all`` database must be identical.

        Drift here means tests (create_all) and production (migrations) run
        against different schemas.
        """
        migrations.upgrade(engine)
        migrated = _schema(engine)

        created_engine = create_engine("sqlite://")
        mapped_base.metadata.create_all(created_engine)
        created = _schema(created_engine)
        created_engine.dispose()

        assert migrated == created

    def test_the_indexes_match_the_models(self, engine, mapped_base):
        migrations.upgrade(engine)
        migrated = _indexes(engine)

        created_engine = create_engine("sqlite://")
        mapped_base.metadata.create_all(created_engine)
        created = _indexes(created_engine)
        created_engine.dispose()

        assert migrated == created

    def test_the_unique_constraint_survives_the_migration(self, engine):
        """``(event_id, subscriber_id)`` uniqueness is the idempotent-consumer
        guarantee; losing it in the migration would let a subscriber run twice."""
        migrations.upgrade(engine)

        constraints = inspect(engine).get_unique_constraints("processing_jobs")

        assert any(set(c["column_names"]) == {"event_id", "subscriber_id"} for c in constraints)


class TestDowngrade:
    def test_it_removes_every_jasil_table(self, engine):
        migrations.upgrade(engine)

        migrations.downgrade(engine, "base")

        assert not set(inspect(engine).get_table_names()) & jasil_table_names()


class TestStamp:
    def test_stamping_records_head_without_creating_tables(self, engine):
        """For a deployment whose tables already exist from ``create_all``."""
        migrations.stamp(engine)

        assert migrations.db_revision(engine) == migrations.head_revision()
        assert not set(inspect(engine).get_table_names()) & jasil_table_names()


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
        assert migrations.jasil_include_object(None, "event_log", "table", True, None) is True

    def test_non_table_objects_are_always_included(self):
        assert migrations.jasil_include_object(None, "some_index", "index", True, None) is True
