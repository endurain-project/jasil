"""PostgreSQL conformance for adopting a baseline schema.

Skipped on the SQLite developer default and the MySQL/MariaDB matrix jobs; no SQL
is mocked here.
"""

import pytest
from sqlalchemy import create_engine, inspect, text

from jasil import migrations
from jasil.audit import _postgres as audit_pg
from jasil.orm import legacy_table_names

pytest.importorskip("alembic", reason="migrations require the 'migrations' extra")


@pytest.fixture
def engine(mapped_base, database_url):
    engine = create_engine(database_url)
    if engine.dialect.name != "postgresql":
        engine.dispose()
        pytest.skip("requires the real PostgreSQL test service")

    def clean() -> None:
        mapped_base.metadata.drop_all(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {migrations.VERSION_TABLE}")
            for name in sorted(legacy_table_names()):
                connection.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
            # rev0006's functions outlive the tables; see ``test_migrations._drop_everything``.
            schema = connection.execute(text("SELECT current_schema()")).scalar_one()
            for statement in audit_pg.uninstall_statements(schema):
                connection.exec_driver_sql(statement)

    clean()
    yield engine
    clean()
    engine.dispose()


def test_a_rev0001_database_is_adopted_then_upgraded_to_head(engine):
    migrations.upgrade(engine, "rev0001")
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE {migrations.VERSION_TABLE}")

    assert migrations.adopt_existing_schema(engine) is True
    assert migrations.db_revision(engine) == "rev0001"

    migrations.upgrade(engine)
    migrations.verify_schema_current(engine)
    inspector = inspect(engine)
    assert {"jasil_job_workers", "jasil_job_attempts"} <= set(inspector.get_table_names())
    assert "queue" in {column["name"] for column in inspector.get_columns("jasil_processing_jobs")}


def test_a_rev0001_database_without_the_gin_index_fails_closed(engine):
    migrations.upgrade(engine, "rev0001")
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE {migrations.VERSION_TABLE}")
        connection.exec_driver_sql("DROP INDEX idx_event_log_metadata_gin")

    with pytest.raises(migrations.SchemaCompatibilityError, match=r"closest revision.*idx_event_log_metadata_gin"):
        migrations.adopt_existing_schema(engine)

    assert migrations.db_revision(engine) is None


def test_the_rename_keeps_the_gin_index_and_the_unique_constraint(engine):
    """PostgreSQL renames in place; the Postgres-only GIN index must follow its table."""
    migrations.upgrade(engine, "rev0002")

    migrations.upgrade(engine)

    inspector = inspect(engine)
    assert "idx_jasil_event_log_metadata_gin" in {i["name"] for i in inspector.get_indexes("jasil_event_log")}
    assert "idx_event_log_metadata_gin" not in {i["name"] for i in inspector.get_indexes("jasil_event_log")}
    assert [c["name"] for c in inspector.get_unique_constraints("jasil_processing_jobs")] == [
        "uq_jasil_processing_jobs_event_subscriber"
    ]
    assert not set(inspector.get_table_names()) & legacy_table_names()


def test_a_fresh_install_is_prefixed_and_has_the_gin_index(engine):
    migrations.upgrade(engine)

    assert "idx_jasil_event_log_metadata_gin" in {i["name"] for i in inspect(engine).get_indexes("jasil_event_log")}


def test_the_renamed_schema_is_adopted_and_requires_the_prefixed_gin_index(engine):
    migrations.upgrade(engine, "rev0002")
    migrations.upgrade(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE {migrations.VERSION_TABLE}")
        connection.exec_driver_sql("DROP INDEX idx_jasil_event_log_metadata_gin")

    with pytest.raises(migrations.SchemaCompatibilityError, match=r"jasil_event_log.*missing index"):
        migrations.adopt_existing_schema(engine)


def test_the_rename_downgrades_in_place(engine):
    migrations.upgrade(engine)

    migrations.downgrade(engine, "rev0002")

    inspector = inspect(engine)
    assert legacy_table_names() <= set(inspector.get_table_names())
    assert "idx_event_log_metadata_gin" in {i["name"] for i in inspector.get_indexes("event_log")}
