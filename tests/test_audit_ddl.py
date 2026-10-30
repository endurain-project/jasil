"""The SQL helpers a host runs itself: role grants and the retention floor."""

import pytest

import jasil.audit as jasil_audit
from jasil.audit import _postgres as pg


def _grants(**kwargs) -> list[str]:
    return jasil_audit.grants_sql("app_rt", "app_ro", **kwargs)


class TestGrants:
    def test_the_runtime_role_can_only_insert_into_the_log(self):
        statements = _grants()

        assert 'GRANT INSERT ON TABLE jasil_audit_log TO "app_rt"' in statements
        granted_on_log = [
            s for s in statements if s.startswith("GRANT") and "TABLE jasil_audit_log " in s and '"app_rt"' in s
        ]
        assert granted_on_log == ['GRANT INSERT ON TABLE jasil_audit_log TO "app_rt"']

    def test_the_runtime_role_cannot_touch_checkpoints_and_can_sync_the_catalog(self):
        statements = _grants()

        assert not any(s.startswith("GRANT") and "jasil_audit_checkpoints" in s and '"app_rt"' in s for s in statements)
        assert 'GRANT SELECT, INSERT, UPDATE ON TABLE jasil_audit_actions TO "app_rt"' in statements
        assert 'GRANT SELECT, INSERT ON TABLE jasil_instance TO "app_rt"' in statements

    def test_the_runtime_role_may_create_partitions_but_not_drop_them(self):
        statements = _grants()

        assert 'GRANT EXECUTE ON FUNCTION jasil_audit_ensure_partitions(integer) TO "app_rt"' in statements
        assert not any(
            "jasil_audit_drop_partitions" in s and s.startswith("GRANT") and "app_rt" in s for s in statements
        )

    def test_the_reader_role_can_only_select(self):
        granted = [s for s in _grants() if s.startswith("GRANT") and '"app_ro"' in s]

        assert sorted(granted) == sorted(
            f'GRANT SELECT ON TABLE {table} TO "app_ro"'
            for table in ("jasil_audit_log", "jasil_audit_actions", "jasil_audit_checkpoints", "jasil_instance")
        )

    def test_a_maintenance_role_gets_what_sealing_and_pruning_need(self):
        granted = [s for s in _grants(maintenance_role="app_mx") if s.startswith("GRANT") and '"app_mx"' in s]

        assert 'GRANT SELECT, INSERT ON TABLE jasil_audit_log TO "app_mx"' in granted
        assert 'GRANT SELECT, INSERT ON TABLE jasil_audit_checkpoints TO "app_mx"' in granted
        assert 'GRANT EXECUTE ON FUNCTION jasil_audit_drop_partitions(timestamptz, timestamptz) TO "app_mx"' in granted
        assert 'GRANT EXECUTE ON FUNCTION jasil_audit_ensure_partitions(integer) TO "app_mx"' in granted
        assert not any("UPDATE ON TABLE jasil_audit_log" in s or "DELETE" in s for s in granted)

    def test_there_is_no_maintenance_grant_unless_asked_for(self):
        assert not any("app_mx" in s for s in _grants())

    def test_nothing_ever_grants_update_delete_or_truncate_on_the_log(self):
        for statement in _grants(maintenance_role="app_mx"):
            if statement.startswith("GRANT") and "TABLE jasil_audit_log " in statement:
                assert not {"UPDATE", "DELETE", "TRUNCATE", "ALL"} & set(statement.replace(",", " ").split()[1:4])

    def test_each_role_is_reset_before_it_is_granted_anything(self):
        statements = _grants()

        for role in ('"app_rt"', '"app_ro"'):
            mine = [s for s in statements if role in s]
            assert mine[0].startswith("REVOKE ALL ON TABLE")
            first_grant = next(i for i, s in enumerate(mine) if s.startswith("GRANT"))
            assert all(s.startswith("REVOKE") for s in mine[:first_grant])

    def test_a_schema_qualifies_every_object(self):
        statements = _grants(schema="audit")

        assert 'GRANT INSERT ON TABLE "audit".jasil_audit_log TO "app_rt"' in statements
        assert 'GRANT EXECUTE ON FUNCTION "audit".jasil_audit_ensure_partitions(integer) TO "app_rt"' in statements

    def test_role_names_are_quoted(self):
        statements = jasil_audit.grants_sql('a"; DROP TABLE x; --', "ro")

        assert 'TO "a""; DROP TABLE x; --"' in " ".join(statements)

    def test_the_runtime_and_reader_roles_must_differ(self):
        with pytest.raises(ValueError, match="must differ"):
            jasil_audit.grants_sql("same", "same")

    def test_the_maintenance_and_reader_roles_must_differ(self):
        with pytest.raises(ValueError, match="must differ"):
            jasil_audit.grants_sql("app_rt", "app_ro", maintenance_role="app_ro")

    def test_an_empty_role_is_refused(self):
        with pytest.raises(ValueError, match="must not be empty"):
            jasil_audit.grants_sql("", "ro")


class TestRetentionFloor:
    def test_it_bakes_the_floor_into_the_function(self):
        statement = jasil_audit.retention_floor_sql(45, schema="public")

        assert statement.startswith("CREATE OR REPLACE FUNCTION")
        assert "floor_days constant integer := 45;" in statement
        assert '"public".jasil_audit_drop_partitions' in statement

    def test_the_schema_has_no_default(self):
        with pytest.raises(TypeError, match="schema"):
            jasil_audit.retention_floor_sql(30)

    def test_it_follows_the_schema(self):
        assert '"audit".jasil_audit_drop_partitions' in jasil_audit.retention_floor_sql(30, schema="audit")

    def test_a_floor_beyond_the_longest_retention_is_refused(self):
        with pytest.raises(ValueError, match="at most 36500"):
            jasil_audit.retention_floor_sql(36_501, schema="public")

    @pytest.mark.parametrize("floor_days", [0, -3])
    def test_a_floor_below_one_day_is_refused(self, floor_days):
        with pytest.raises(ValueError, match="at least 1"):
            jasil_audit.retention_floor_sql(floor_days, schema="public")

    def test_the_function_is_security_definer_with_a_pinned_search_path(self):
        statement = jasil_audit.retention_floor_sql(30, schema="public")

        assert "SECURITY DEFINER" in statement
        assert "SET search_path = pg_catalog" in statement

    @pytest.mark.parametrize(
        "statement", [pg.ensure_partitions_function("public"), pg.drop_partitions_function("public", 30)]
    )
    def test_a_lock_another_session_holds_cannot_stall_the_functions(self, statement):
        """Any role that can connect can take their advisory lock; waiting forever would hang maintenance."""
        assert "SET lock_timeout = '10s'" in statement

    def test_the_partition_function_is_pinned_too(self):
        statement = pg.ensure_partitions_function("public")

        assert "SECURITY DEFINER" in statement
        assert "SET search_path = pg_catalog" in statement
        assert "@" not in statement  # every placeholder was filled


class TestQuoting:
    def test_identifiers_double_their_quotes(self):
        assert pg.quote_ident('we"ird') == '"we""ird"'

    def test_literals_double_their_quotes(self):
        assert pg.quote_literal("it's") == "'it''s'"

    def test_a_hostile_schema_cannot_break_out_of_the_function_definition(self):
        statement = pg.drop_partitions_function("x'; DROP TABLE y; --", 30)

        assert "'x''; DROP TABLE y; --'" in statement
        assert '"x\'; DROP TABLE y; --"' in statement

    @pytest.mark.parametrize("schema", ['"@SCHEMA_LIT@', "p@SCHEMA_LIT@", "@LOG@"])
    def test_a_schema_naming_a_placeholder_is_quoted_and_not_substituted_again(self, schema):
        statement = pg.drop_partitions_function(schema, 30)

        assert pg.quote_ident(schema) + "." in statement
        assert pg.quote_literal(schema) in statement

    def test_a_schema_that_could_end_the_dollar_quoted_body_is_refused(self):
        with pytest.raises(ValueError, match=r"\$"):
            jasil_audit.retention_floor_sql(30, schema="a$body$; DROP TABLE y; --")

    def test_the_trigger_statements_target_the_parent_table(self):
        statements = pg.trigger_ddl("public")

        assert any("BEFORE UPDATE OR DELETE" in s and "FOR EACH ROW" in s for s in statements)
        assert any("BEFORE TRUNCATE" in s and "FOR EACH STATEMENT" in s for s in statements)


class TestMigrationStatements:
    def test_installing_defines_the_functions_before_the_triggers_that_use_them(self):
        statements = pg.install_statements("public", 30, 7)

        function = next(i for i, s in enumerate(statements) if "FUNCTION" in s and pg.FORBID_FUNCTION in s)
        trigger = next(i for i, s in enumerate(statements) if s.startswith("CREATE TRIGGER"))
        assert function < trigger

    def test_installing_closes_the_definer_functions_to_public_before_creating_partitions(self):
        statements = pg.install_statements("public", 30, 7)

        revoked = [i for i, s in enumerate(statements) if s.startswith("REVOKE ALL ON FUNCTION")]
        ensured = next(i for i, s in enumerate(statements) if s.startswith("SELECT"))
        assert len(revoked) == 2
        assert max(revoked) < ensured
        assert statements[ensured] == 'SELECT "public".jasil_audit_ensure_partitions(7)'

    def test_installing_bakes_in_the_floor(self):
        assert any("floor_days constant integer := 45;" in s for s in pg.install_statements("public", 45, 7))

    def test_uninstalling_drops_every_function_it_installed(self):
        installed = {
            name
            for s in pg.install_statements("public", 30, 7)
            for name in ("drop_partitions", "ensure_partitions", "forbid_mutation")
            if f"jasil_audit_{name}" in s
        }
        dropped = " ".join(pg.uninstall_statements("public"))

        assert installed == {"drop_partitions", "ensure_partitions", "forbid_mutation"}
        assert all(f"jasil_audit_{name}" in dropped for name in installed)

    def test_a_floor_below_one_day_cannot_be_installed(self):
        with pytest.raises(ValueError, match="at least 1"):
            pg.install_statements("public", 0, 7)
