"""The audit catalog, the ambient context and the audit settings.

Ids are stored in rows that outlive the code, so the catalog's job is to refuse the mistakes that cannot
be undone later: a duplicate id, a malformed one, an unregistered one.
"""

import asyncio
import threading
from dataclasses import FrozenInstanceError, replace

import pytest
from pydantic import BaseModel

import jasil.audit as jasil_audit
import jasil.settings as jasil_settings
from jasil.audit.catalog import AuditAction, AuditCatalog, Category, NoDetails, Severity, UnknownAuditActionError
from jasil.audit.context import MAX_USER_AGENT_LENGTH, ActorType, AuditContext, Source
from jasil.event_versioning import VersionedPayload


def _action(action_id: str = "billing.invoice.create", **overrides) -> AuditAction:
    fields = {
        "id": action_id,
        "category": Category.DATA_CHANGE,
        "severity": Severity.LOW,
        "description_key": "audit.test",
        "description_default": "A test action",
    }
    return AuditAction(**{**fields, **overrides})


class TestAction:
    @pytest.mark.parametrize("action_id", ["a.b.c", "session.token.refresh", "audit_x.res_1.verb2"])
    def test_a_three_segment_snake_case_id_is_accepted(self, action_id):
        assert _action(action_id).id == action_id

    @pytest.mark.parametrize(
        "action_id",
        ["", "a.b", "a.b.c.d", "A.b.c", "a.B.c", "1a.b.c", "a..c", "a.b.c ", "a-b.c.d", "a.b.c\n", "." * 3],
    )
    def test_a_malformed_id_is_rejected(self, action_id):
        with pytest.raises(ValueError, match=r"<module>\.<resource>\.<verb>"):
            _action(action_id)

    def test_an_over_long_id_is_rejected(self):
        with pytest.raises(ValueError, match="at most 100"):
            _action("a." + "b" * 98 + ".c")

    def test_a_description_is_required(self):
        with pytest.raises(ValueError, match="description"):
            _action(description_key="")

    def test_the_details_model_must_be_versioned(self):
        class Plain(BaseModel):
            pass

        with pytest.raises(ValueError, match="VersionedPayload"):
            _action(details_model=Plain)

    def test_it_defaults_to_no_details_and_not_deprecated(self):
        action = _action()

        assert action.details_model is NoDetails
        assert action.deprecated is False

    def test_an_action_is_immutable(self):
        with pytest.raises(FrozenInstanceError):
            _action().deprecated = True


class TestCatalog:
    def test_it_registers_and_returns_an_action(self):
        catalog = AuditCatalog()

        assert catalog.register(_action()) is catalog.get("billing.invoice.create")
        assert "billing.invoice.create" in catalog
        assert len(catalog) == 1

    def test_a_duplicate_id_is_refused(self):
        catalog = AuditCatalog()
        catalog.register(_action())

        with pytest.raises(ValueError, match="already registered"):
            catalog.register(_action(severity=Severity.HIGH))

    def test_an_unknown_id_is_refused(self):
        with pytest.raises(UnknownAuditActionError, match="not registered"):
            AuditCatalog().get("billing.invoice.create")

    def test_resolving_a_value_that_differs_from_the_registered_one_is_refused(self):
        catalog = AuditCatalog()
        catalog.register(_action())

        with pytest.raises(UnknownAuditActionError, match="differs"):
            catalog.resolve(_action(severity=Severity.HIGH))

    def test_resolving_accepts_an_id_or_the_registered_value(self):
        catalog = AuditCatalog()
        registered = catalog.register(_action())

        assert catalog.resolve("billing.invoice.create") is registered
        assert catalog.resolve(_action()) is registered

    def test_it_iterates_in_id_order(self):
        catalog = AuditCatalog()
        for action_id in ("b.b.b", "a.a.a", "c.c.c"):
            catalog.register(_action(action_id))

        assert [action.id for action in catalog] == ["a.a.a", "b.b.b", "c.c.c"]

    def test_a_deprecated_action_stays_registered(self):
        catalog = AuditCatalog()
        catalog.register(_action(deprecated=True))

        assert catalog.get("billing.invoice.create").deprecated is True

    def test_the_process_catalog_starts_with_the_substrate_actions_and_resets_to_them(self):
        catalog = jasil_audit.get_catalog()
        assert {action.id for action in catalog} == {
            "audit.retention.prune",
            "audit.retention.change",
            "audit.integrity.violation",
            "audit.catalog.sync",
        }
        catalog.register(_action())

        jasil_audit.reset()

        assert "billing.invoice.create" not in jasil_audit.get_catalog()

    def test_the_details_model_carries_a_schema_version(self):
        class V2(VersionedPayload):
            SCHEMA_VERSION = 2

        assert _action(details_model=V2).details_model.SCHEMA_VERSION == 2


class TestContext:
    def test_the_default_is_an_anonymous_api_call(self):
        context = jasil_audit.get_context()

        assert (context.actor_type, context.source, context.actor_id) == (ActorType.ANONYMOUS, Source.API, None)

    def test_set_context_is_read_back_and_reset_clears_it(self):
        jasil_audit.set_context(AuditContext(actor_type="user", actor_id="7"))

        assert jasil_audit.get_context().actor_id == "7"
        assert jasil_audit.get_context().actor_type is ActorType.USER

        jasil_audit.reset_context()

        assert jasil_audit.get_context().actor_id is None

    def test_a_provider_wins_over_the_contextvar(self):
        jasil_audit.set_context(AuditContext(actor_id="from-contextvar"))
        jasil_audit.configure_provider(lambda: AuditContext(actor_id="from-provider"))

        assert jasil_audit.get_context().actor_id == "from-provider"

    def test_a_provider_that_fails_or_returns_nothing_yields_the_default(self):
        def broken() -> AuditContext:
            raise RuntimeError("middleware gone")

        jasil_audit.configure_provider(broken)
        assert jasil_audit.get_context().actor_id is None

        jasil_audit.configure_provider(lambda: None)
        assert jasil_audit.get_context().actor_id is None

    def test_removing_the_provider_restores_the_contextvar(self):
        jasil_audit.set_context(AuditContext(actor_id="7"))
        jasil_audit.configure_provider(lambda: AuditContext(actor_id="p"))
        jasil_audit.configure_provider(None)

        assert jasil_audit.get_context().actor_id == "7"

    def test_the_block_form_layers_on_the_current_context_and_restores_it(self):
        jasil_audit.set_context(AuditContext(actor_type="user", actor_id="7", ip_address="10.0.0.1"))

        with jasil_audit.audit_context(actor_type="system", source="worker") as inside:
            assert (inside.actor_type, inside.source, inside.actor_id) == (ActorType.SYSTEM, Source.WORKER, "7")
            assert jasil_audit.get_context() == inside

        assert jasil_audit.get_context().actor_type is ActorType.USER

    def test_the_block_form_restores_the_context_when_the_block_raises(self):
        with pytest.raises(RuntimeError), jasil_audit.audit_context(actor_id="9"):
            raise RuntimeError

        assert jasil_audit.get_context().actor_id is None

    def test_an_unknown_actor_type_is_rejected(self):
        with pytest.raises(ValueError, match="ActorType"):
            AuditContext(actor_type="robot")

    def test_a_long_user_agent_is_clipped_with_a_marker(self):
        context = AuditContext(user_agent="x" * (MAX_USER_AGENT_LENGTH + 100))

        assert len(context.user_agent) == MAX_USER_AGENT_LENGTH
        assert context.user_agent.endswith("...")

    def test_a_short_user_agent_is_kept(self):
        assert AuditContext(user_agent="curl/8").user_agent == "curl/8"

    def test_an_over_long_actor_id_is_rejected_rather_than_clipped(self):
        with pytest.raises(ValueError, match="actor_id"):
            AuditContext(actor_id="x" * 65)

    def test_the_request_id_is_not_part_of_the_context(self):
        assert "request_id" not in {field for field in AuditContext.__dataclass_fields__}

    def test_threads_do_not_share_a_context(self):
        seen: dict[str, str | None] = {}
        jasil_audit.set_context(AuditContext(actor_id="main"))

        def worker(name: str) -> None:
            seen[name + "-before"] = jasil_audit.get_context().actor_id
            jasil_audit.set_context(AuditContext(actor_id=name))
            seen[name + "-after"] = jasil_audit.get_context().actor_id

        threads = [threading.Thread(target=worker, args=(name,)) for name in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert seen == {"a-before": None, "a-after": "a", "b-before": None, "b-after": "b"}
        assert jasil_audit.get_context().actor_id == "main"

    async def test_asyncio_tasks_do_not_share_a_context(self):
        async def task(name: str) -> str | None:
            jasil_audit.set_context(AuditContext(actor_id=name))
            await asyncio.sleep(0)
            return jasil_audit.get_context().actor_id

        assert await asyncio.gather(task("a"), task("b"), task("c")) == ["a", "b", "c"]
        assert jasil_audit.get_context().actor_id is None

    def test_a_context_replaces_cleanly(self):
        assert replace(AuditContext(actor_id="1"), actor_id="2").actor_id == "2"


class TestSettings:
    def test_the_defaults_are_the_neutral_ones(self):
        audit = jasil_settings.AuditSettings()

        assert (audit.enabled, audit.retention_days, audit.retention_floor_days) == (True, 90, 30)
        assert (audit.legal_hold, audit.partition_days_ahead) == (False, 7)
        assert (audit.checkpoint_window_minutes, audit.checkpoint_grace_minutes) == (15, 5)
        assert (audit.signing_key, audit.external_sink_uri, audit.max_details_bytes) == (None, None, 4096)

    def test_it_is_part_of_the_jasil_settings(self):
        assert jasil_settings.JasilSettings().audit == jasil_settings.AuditSettings()

    def test_the_signing_key_is_not_in_the_repr(self):
        assert "secret" not in repr(jasil_settings.AuditSettings(signing_key="secret"))

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"retention_floor_days": 0}, "retention_floor_days must be at least 1"),
            ({"partition_days_ahead": 0}, "partition_days_ahead must be at least 1"),
            ({"checkpoint_window_minutes": 0}, "checkpoint_window_minutes must be at least 1"),
            ({"checkpoint_window_minutes": 7}, "must divide"),
            ({"checkpoint_grace_minutes": -1}, "checkpoint_grace_minutes must be at least 0"),
            ({"max_details_bytes": 0}, "max_details_bytes must be at least 1"),
            ({"retention_days": 10, "retention_floor_days": 30}, "retention_days"),
        ],
    )
    def test_an_out_of_range_value_is_rejected(self, overrides, message):
        with pytest.raises(ValueError, match=message):
            jasil_settings.AuditSettings(**overrides)

    @pytest.mark.parametrize("retention_days", [0, -1])
    def test_retention_can_be_disabled_below_the_floor(self, retention_days):
        assert jasil_settings.AuditSettings(retention_days=retention_days).retention_days == retention_days

    def test_retention_equal_to_the_floor_is_accepted(self):
        assert jasil_settings.AuditSettings(retention_days=30, retention_floor_days=30).retention_days == 30
