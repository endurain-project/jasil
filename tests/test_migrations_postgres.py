"""PostgreSQL conformance for adopting a baseline schema.

Skipped on the SQLite developer default and the MySQL/MariaDB matrix jobs; no SQL
is mocked here.
"""

import pytest
from sqlalchemy import create_engine, inspect

from jasil import migrations

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
    assert {"job_workers", "job_attempts"} <= set(inspector.get_table_names())
    assert "queue" in {column["name"] for column in inspector.get_columns("processing_jobs")}


def test_a_rev0001_database_without_the_gin_index_fails_closed(engine):
    migrations.upgrade(engine, "rev0001")
    with engine.begin() as connection:
        connection.exec_driver_sql(f"DROP TABLE {migrations.VERSION_TABLE}")
        connection.exec_driver_sql("DROP INDEX idx_event_log_metadata_gin")

    with pytest.raises(migrations.SchemaCompatibilityError, match=r"closest revision.*idx_event_log_metadata_gin"):
        migrations.adopt_existing_schema(engine)

    assert migrations.db_revision(engine) is None
