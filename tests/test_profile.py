"""Deployment profile, topology, the capability report, and the consistency checks."""

import pytest

from jasil.capabilities import (
    StateSource,
    build_capability_report,
    check_deployment_consistency,
    check_mail_consistency,
)
from jasil.profile import (
    DeploymentProfile,
    DeploymentTopology,
    StateBackendKind,
    classify_state_uri,
    parse_profile,
    resolve_topology,
)
from jasil.settings import JasilSettings, MailSettings


class TestParseProfile:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("local", DeploymentProfile.LOCAL),
            ("distributed", DeploymentProfile.DISTRIBUTED),
            ("custom", DeploymentProfile.CUSTOM),
            ("  DISTRIBUTED  ", DeploymentProfile.DISTRIBUTED),
        ],
    )
    def test_a_recognised_name_parses(self, value, expected):
        assert parse_profile(value) is expected

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_an_unset_value_defaults_to_local(self, value):
        assert parse_profile(value) is DeploymentProfile.LOCAL

    def test_an_existing_profile_passes_through(self):
        assert parse_profile(DeploymentProfile.CUSTOM) is DeploymentProfile.CUSTOM

    def test_a_typo_raises_rather_than_defaulting(self):
        """Defaulting a typo to ``local`` would silently disable the shared-state
        fail-fast on a distributed deployment."""
        with pytest.raises(ValueError, match="Invalid DEPLOYMENT_PROFILE 'distributd'"):
            parse_profile("distributd")

    def test_the_error_lists_the_valid_values(self):
        with pytest.raises(ValueError, match="local, distributed, custom"):
            parse_profile("nonsense")


class TestClassifyStateUri:
    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            ("memory://", StateBackendKind.MEMORY),
            ("redis://cache:6379/0", StateBackendKind.REDIS),
            ("rediss://cache:6379/0", StateBackendKind.REDIS),
            ("unix:///var/run/redis.sock", StateBackendKind.REDIS),
            ("REDIS://CACHE:6379", StateBackendKind.REDIS),
            ("postgres://db/0", StateBackendKind.UNKNOWN),
            ("", StateBackendKind.UNKNOWN),
            (None, StateBackendKind.UNKNOWN),
        ],
    )
    def test_classification(self, uri, expected):
        assert classify_state_uri(uri) is expected


class TestTopology:
    def test_worker_count_is_clamped_to_at_least_one(self):
        assert resolve_topology(DeploymentProfile.LOCAL, 0).web_workers == 1
        assert resolve_topology(DeploymentProfile.LOCAL, -3).web_workers == 1

    @pytest.mark.parametrize(
        ("profile", "workers", "expected"),
        [
            pytest.param(DeploymentProfile.LOCAL, 1, False, id="single-process-local"),
            pytest.param(DeploymentProfile.LOCAL, 4, True, id="multi-worker-local"),
            pytest.param(DeploymentProfile.DISTRIBUTED, 1, True, id="distributed-single-worker"),
            pytest.param(DeploymentProfile.CUSTOM, 1, False, id="custom-single-worker"),
        ],
    )
    def test_shared_state_requirement(self, profile, workers, expected):
        """Process-local memory cannot be shared, so more than one process — by
        replica *or* by worker — requires a shared backend."""
        topology = DeploymentTopology(profile=profile, web_workers=workers)

        assert topology.requires_shared_state is expected


class TestStateSource:
    def test_the_backend_is_classified_from_the_uri(self):
        assert StateSource(label="state_uri", uri="redis://c:6379").backend is StateBackendKind.REDIS

    def test_a_source_applies_by_default(self):
        assert StateSource(label="state_uri", uri="memory://").applies is True


class TestCapabilityReport:
    @pytest.fixture
    def report(self):
        return build_capability_report(JasilSettings())

    def test_every_capability_appears_exactly_once(self, report):
        names = [row.name for row in report.rows]

        assert sorted(names) == sorted(set(names))
        assert {"state", "storage", "events", "lock", "mail", "clock"} == set(names)

    def test_mail_reports_disabled_when_unset(self, report):
        row = next(row for row in report.rows if row.name == "mail")

        assert (row.backend, row.source) == ("disabled", "not configured")

    def test_mail_reports_its_scheme_without_the_host_or_credentials(self):
        settings = JasilSettings(mail=MailSettings(uri="smtps://relay.test", from_address="a@b.c"))

        row = next(row for row in build_capability_report(settings).rows if row.name == "mail")

        assert (row.backend, row.source) == ("smtps", "mail.uri")

    def test_a_cleartext_relay_says_so(self):
        settings = JasilSettings(mail=MailSettings(uri="smtp://127.0.0.1", from_address="a@b.c", secure=False))

        row = next(row for row in build_capability_report(settings).rows if row.name == "mail")

        assert row.backend == "smtp (cleartext)"

    @pytest.mark.parametrize("uri", ["user:hunter2@relay.example.com", "smtp:u:hunter2@relay.example.com:587"])
    def test_a_malformed_mail_uri_is_never_echoed(self, uri):
        row = next(
            row for row in build_capability_report(JasilSettings(mail=MailSettings(uri=uri))).rows if row.name == "mail"
        )

        assert row.backend == "invalid"
        assert "hunter2" not in row.backend + row.source

    def test_the_clock_row_is_always_present(self, report):
        """The clock is the one capability with no alternative backend."""
        clock_rows = [row for row in report.rows if row.name == "clock"]

        assert len(clock_rows) == 1

    def test_it_reports_the_backend_each_uri_resolved_to(self, report):
        backends = {row.name: row.backend for row in report.rows}

        assert backends["state"] == "memory"
        assert backends["storage"] == "local"
        assert backends["lock"] == "noop"

    def test_an_unset_uri_is_attributed_to_the_profile(self, report):
        """An operator needs to see which values they chose and which they inherited."""
        sources = {row.name: row.source for row in report.rows}

        assert sources["state"] == "profile default"

    def test_an_explicit_uri_is_attributed_to_its_setting(self):
        report = build_capability_report(JasilSettings(state_uri="memory://"))

        sources = {row.name: row.source for row in report.rows}

        assert sources["state"] == "state_uri"

    def test_rendering_includes_the_topology_header(self, report):
        rendered = report.render()

        assert "Deployment profile: local" in rendered
        assert "web_workers=1" in rendered
        assert "requires_shared_state=False" in rendered

    def test_rendering_lists_every_row(self, report):
        rendered = report.render()

        assert len(rendered.splitlines()) == len(report.rows) + 1
        for row in report.rows:
            assert row.backend in rendered
            assert row.source in rendered


class TestDeploymentConsistency:
    """The combination has to be checked, not just each URI on its own.

    Each of these wirings is individually legal and starts fine; what makes it
    fatal is the topology it is paired with, and the symptom (state that exists on
    one replica but not another, a scheduled job running four times) never points
    back at the setting that caused it.
    """

    def test_the_default_local_profile_is_consistent(self):
        assert check_deployment_consistency(JasilSettings()) == []

    def test_multiple_workers_on_memory_state_are_refused(self):
        issues = check_deployment_consistency(JasilSettings(web_workers=4))

        assert any("state_uri resolves to process-local memory" in issue for issue in issues)

    def test_multiple_workers_on_an_in_process_bus_are_refused(self):
        issues = check_deployment_consistency(JasilSettings(web_workers=4))

        assert any("events_uri resolves to process-local memory" in issue for issue in issues)

    def test_multiple_workers_on_memory_mail_are_refused(self):
        """Each process would keep its own outbox and nothing would be delivered."""
        issues = check_deployment_consistency(JasilSettings(web_workers=4, mail=MailSettings(uri="memory://")))

        assert any("mail.uri resolves to the in-memory mail recorder" in issue for issue in issues)

    def test_the_distributed_profile_on_memory_mail_is_refused(self):
        issues = check_mail_consistency(
            profile=DeploymentProfile.DISTRIBUTED, web_workers=1, mail_uri="MEMORY://", mail_label="mail.uri"
        )

        assert len(issues) == 1 and "profile=distributed" in issues[0]

    @pytest.mark.parametrize(
        ("profile", "web_workers", "uri"),
        [
            pytest.param(DeploymentProfile.LOCAL, 1, "memory://", id="single-process"),
            pytest.param(DeploymentProfile.CUSTOM, 4, "memory://", id="custom"),
            pytest.param(DeploymentProfile.LOCAL, 4, "smtp://relay.test", id="relay"),
            pytest.param(DeploymentProfile.LOCAL, 4, None, id="unset"),
            pytest.param(DeploymentProfile.LOCAL, 4, "", id="empty"),
        ],
    )
    def test_mail_that_cannot_silently_go_nowhere_is_accepted(self, profile, web_workers, uri):
        assert check_mail_consistency(profile=profile, web_workers=web_workers, mail_uri=uri, mail_label="m") == []

    def test_multiple_workers_on_a_no_op_lock_are_refused(self):
        """Otherwise every worker runs every scheduled job."""
        issues = check_deployment_consistency(JasilSettings(web_workers=4))

        assert any("lock_uri resolves to an in-process no-op lock" in issue for issue in issues)

    def test_multiple_workers_on_local_disk_are_allowed(self):
        """Workers share one host's disk; only separate nodes do not."""
        issues = check_deployment_consistency(JasilSettings(web_workers=4))

        assert not any("storage_uri" in issue for issue in issues)

    def test_distributed_on_local_disk_is_refused(self):
        issues = check_deployment_consistency(
            JasilSettings(
                profile=DeploymentProfile.DISTRIBUTED,
                state_uri="redis://c:6379/0",
                events_uri="redis://c:6379/1",
                storage_uri="local://",
                lock_uri="postgres-advisory://",
            )
        )

        assert any("storage_uri resolves to the local filesystem" in issue for issue in issues)

    def test_a_fully_wired_distributed_deployment_is_consistent(self):
        issues = check_deployment_consistency(
            JasilSettings(
                profile=DeploymentProfile.DISTRIBUTED,
                state_uri="redis://c:6379/0",
                events_uri="redis://c:6379/1",
                storage_uri="s3://bucket",
                lock_uri="postgres-advisory://",
            )
        )

        assert issues == []

    def test_the_custom_profile_opts_out(self):
        """``custom`` promises no defaults, so nothing here can contradict one."""
        issues = check_deployment_consistency(
            JasilSettings(
                profile=DeploymentProfile.CUSTOM,
                web_workers=8,
                state_uri="memory://",
                events_uri="memory://",
                storage_uri="local://",
                lock_uri="noop://",
            )
        )

        assert issues == []
