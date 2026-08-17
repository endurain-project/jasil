"""Deployment profile, topology, and the capability report."""

import pytest

from jasil.capabilities import StateSource, build_capability_report
from jasil.profile import (
    DeploymentProfile,
    DeploymentTopology,
    StateBackendKind,
    classify_state_uri,
    parse_profile,
    resolve_topology,
)


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
        assert StateSource(label="STATE_URI", uri="redis://c:6379").backend is StateBackendKind.REDIS

    def test_a_source_applies_by_default(self):
        assert StateSource(label="STATE_URI", uri="memory://").applies is True


class TestCapabilityReport:
    @pytest.fixture
    def report(self):
        return build_capability_report(
            profile=DeploymentProfile.LOCAL,
            web_workers=1,
            primary_state=StateSource(label="STATE_URI", uri="memory://"),
            storage_backend="local",
            storage_source="STORAGE_URI",
            events_backend="in-process",
            events_source="EVENTS_URI",
            lock_backend="none",
            lock_source="LOCK_URI",
        )

    def test_every_capability_appears_exactly_once(self, report):
        names = [row.name for row in report.rows]

        assert sorted(names) == sorted(set(names))
        assert {"storage", "events", "lock", "clock"} <= set(names)

    def test_the_clock_row_is_always_present(self, report):
        """The clock is the one capability with no alternative backend."""
        clock_rows = [row for row in report.rows if row.name == "clock"]

        assert len(clock_rows) == 1

    def test_rendering_includes_the_topology_header(self, report):
        rendered = report.render()

        assert "Deployment profile: local" in rendered
        assert "WEB_WORKERS=1" in rendered
        assert "requires_shared_state=False" in rendered

    def test_rendering_lists_every_row(self, report):
        rendered = report.render()

        assert len(rendered.splitlines()) == len(report.rows) + 1
        for row in report.rows:
            assert row.backend in rendered
            assert row.source in rendered
