"""Deployment profile, topology, the capability report, and the consistency checks."""

import pytest

from jasil.capabilities import (
    StateSource,
    build_capability_report,
    check_deployment_consistency,
    check_job_worker_consistency,
    check_lock_consistency,
    check_mail_consistency,
    check_state_consistency,
    check_storage_consistency,
    topology_of,
)
from jasil.profile import (
    DeploymentProfile,
    JobWorkerMode,
    StateBackendKind,
    classify_state_uri,
    parse_job_worker_mode,
    parse_profile,
    resolve_topology,
)
from jasil.settings import JasilSettings, JobSettings, MailSettings

LOCAL = DeploymentProfile.LOCAL
DISTRIBUTED = DeploymentProfile.DISTRIBUTED
CUSTOM = DeploymentProfile.CUSTOM
IN_PROCESS = JobWorkerMode.IN_PROCESS
STANDALONE = JobWorkerMode.STANDALONE


def _topology(profile=LOCAL, web_workers=1, job_worker_mode=IN_PROCESS):
    return resolve_topology(profile=profile, web_workers=web_workers, job_worker_mode=job_worker_mode)


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


class TestParseJobWorkerMode:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("in_process", IN_PROCESS),
            ("standalone", STANDALONE),
            ("  STANDALONE  ", STANDALONE),
        ],
    )
    def test_a_recognised_name_parses(self, value, expected):
        assert parse_job_worker_mode(value) is expected

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_an_unset_value_defaults_to_in_process(self, value):
        assert parse_job_worker_mode(value) is IN_PROCESS

    def test_an_existing_mode_passes_through(self):
        assert parse_job_worker_mode(STANDALONE) is STANDALONE

    def test_a_typo_raises_rather_than_defaulting(self):
        """Defaulting a typo to ``in_process`` would hide every worker process
        from the consistency checks."""
        with pytest.raises(ValueError, match="Invalid job worker mode 'standalon'"):
            parse_job_worker_mode("standalon")

    def test_the_error_lists_the_valid_values(self):
        with pytest.raises(ValueError, match="in_process, standalone"):
            parse_job_worker_mode("nonsense")


class TestTopology:
    def test_worker_count_is_clamped_to_at_least_one(self):
        assert _topology(web_workers=0).web_workers == 1
        assert _topology(web_workers=-3).web_workers == 1

    @pytest.mark.parametrize(
        ("profile", "workers", "mode", "multi_process", "multi_node"),
        [
            pytest.param(LOCAL, 1, IN_PROCESS, False, False, id="single-process-local"),
            pytest.param(LOCAL, 4, IN_PROCESS, True, False, id="multi-worker-local"),
            pytest.param(LOCAL, 1, STANDALONE, True, False, id="standalone-workers-local"),
            pytest.param(LOCAL, 4, STANDALONE, True, False, id="multi-worker-and-standalone"),
            pytest.param(DISTRIBUTED, 1, IN_PROCESS, True, True, id="distributed-single-worker"),
            pytest.param(DISTRIBUTED, 1, STANDALONE, True, True, id="distributed-standalone"),
            pytest.param(CUSTOM, 1, IN_PROCESS, False, False, id="custom-single-worker"),
            pytest.param(CUSTOM, 1, STANDALONE, True, False, id="custom-standalone"),
        ],
    )
    def test_what_the_shape_implies(self, profile, workers, mode, multi_process, multi_node):
        """Process-local memory cannot be shared by more than one process — whether
        it is a replica, a web worker or a job worker — and a disk cannot be shared
        by more than one machine."""
        topology = _topology(profile, workers, mode)

        assert topology.multi_process is multi_process
        assert topology.multi_node is multi_node

    def test_a_single_process_has_no_causes(self):
        assert _topology().multi_process_causes == ()

    def test_every_input_that_makes_it_multi_process_is_named(self):
        causes = _topology(DISTRIBUTED, 3, STANDALONE).multi_process_causes

        assert len(causes) == 3
        assert any("profile=distributed" in cause for cause in causes)
        assert any("web_workers=3" in cause for cause in causes)
        assert any("job_worker_mode=standalone" in cause for cause in causes)

    def test_the_settings_resolve_to_a_topology(self):
        settings = JasilSettings(profile=DISTRIBUTED, web_workers=2, job_worker_mode=STANDALONE)

        topology = topology_of(settings)

        assert (topology.profile, topology.web_workers, topology.job_worker_mode) == (DISTRIBUTED, 2, STANDALONE)

    def test_the_default_mode_is_in_process(self):
        assert JasilSettings().job_worker_mode is IN_PROCESS


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
        assert {"state", "storage", "events", "realtime", "lock", "mail", "clock"} == set(names)

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

    def test_realtime_is_reported_as_following_the_event_bus_when_unset(self, report):
        row = next(row for row in report.rows if row.name == "realtime")

        assert (row.backend, row.source) == ("memory", "follows events_uri")

    def test_an_explicit_realtime_uri_is_reported(self):
        settings = JasilSettings(realtime_uri="redis://hints:6379/0")

        row = next(row for row in build_capability_report(settings).rows if row.name == "realtime")

        assert (row.backend, row.source) == ("redis", "realtime_uri")

    def test_an_explicit_uri_is_attributed_to_its_setting(self):
        report = build_capability_report(JasilSettings(state_uri="memory://"))

        sources = {row.name: row.source for row in report.rows}

        assert sources["state"] == "state_uri"

    def test_rendering_includes_the_topology_header(self, report):
        rendered = report.render()

        assert "Deployment profile: local" in rendered
        assert "web_workers=1" in rendered
        assert "job_worker_mode=in_process" in rendered
        assert "multi_process=False" in rendered
        assert "multi_node=False" in rendered

    def test_rendering_shows_standalone_workers(self):
        rendered = build_capability_report(
            JasilSettings(job_worker_mode=STANDALONE, jobs=JobSettings(enabled=True))
        ).render()

        assert "job_worker_mode=standalone" in rendered
        assert "multi_process=True" in rendered

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

    def test_multiple_workers_on_in_process_realtime_are_refused(self):
        """A hint published by one process would never reach the one holding the connection."""
        issues = check_deployment_consistency(
            JasilSettings(
                web_workers=4,
                state_uri="redis://c:6379/0",
                events_uri="redis://c:6379/1",
                lock_uri="postgres-advisory://",
                realtime_uri="memory://",
            )
        )

        assert len(issues) == 1
        assert "realtime_uri resolves to process-local memory" in issues[0]

    @pytest.mark.parametrize(
        "topology_settings",
        [
            pytest.param({"profile": DeploymentProfile.DISTRIBUTED, "storage_uri": "s3://b"}, id="distributed"),
            pytest.param({"web_workers": 4}, id="web-workers"),
            pytest.param({"job_worker_mode": STANDALONE, "jobs": JobSettings(enabled=True)}, id="standalone"),
        ],
    )
    def test_every_multi_process_topology_refuses_in_process_realtime(self, topology_settings):
        settings = JasilSettings(
            state_uri="redis://c:6379/0",
            events_uri="redis://c:6379/1",
            lock_uri="postgres-advisory://",
            realtime_uri="memory://",
            **topology_settings,
        )

        assert [issue for issue in check_deployment_consistency(settings) if "realtime_uri" in issue]

    @pytest.mark.parametrize("uri", ["redis://c:6379/2", "rediss://c:6379/2", "unix:///run/redis.sock"])
    def test_multiple_workers_on_redis_realtime_are_accepted(self, uri):
        settings = JasilSettings(
            web_workers=4,
            state_uri="redis://c:6379/0",
            events_uri="redis://c:6379/1",
            lock_uri="postgres-advisory://",
            realtime_uri=uri,
        )

        assert check_deployment_consistency(settings) == []

    def test_an_unset_realtime_uri_is_judged_through_the_event_bus(self):
        """It *is* the events URI, so the refusal names the setting the operator wrote."""
        issues = check_deployment_consistency(JasilSettings(web_workers=4))

        assert any("events_uri" in issue for issue in issues)
        assert not any("realtime_uri" in issue for issue in issues)

    def test_multiple_workers_on_memory_mail_are_refused(self):
        """Each process would keep its own outbox and nothing would be delivered."""
        issues = check_deployment_consistency(JasilSettings(web_workers=4, mail=MailSettings(uri="memory://")))

        assert any("mail.uri resolves to the in-memory mail recorder" in issue for issue in issues)

    def test_the_distributed_profile_on_memory_mail_is_refused(self):
        issues = check_mail_consistency(topology=_topology(DISTRIBUTED), mail_uri="MEMORY://", mail_label="mail.uri")

        assert len(issues) == 1 and "profile=distributed" in issues[0]

    @pytest.mark.parametrize(
        ("profile", "web_workers", "uri"),
        [
            pytest.param(LOCAL, 1, "memory://", id="single-process"),
            pytest.param(CUSTOM, 4, "memory://", id="custom"),
            pytest.param(LOCAL, 4, "smtp://relay.test", id="relay"),
            pytest.param(LOCAL, 4, None, id="unset"),
            pytest.param(LOCAL, 4, "", id="empty"),
        ],
    )
    def test_mail_that_cannot_silently_go_nowhere_is_accepted(self, profile, web_workers, uri):
        topology = _topology(profile, web_workers)

        assert check_mail_consistency(topology=topology, mail_uri=uri, mail_label="m") == []

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


class TestStandaloneJobWorkers:
    """A standalone worker is one more process reaching every capability.

    The default ``local`` profile with one API process looks single-process to
    every rule unless the deployment declares its workers; these are the wirings
    that would otherwise start happily and lose data or work silently.
    """

    @pytest.fixture
    def standalone(self):
        return {"job_worker_mode": STANDALONE, "jobs": JobSettings(enabled=True)}

    def test_memory_state_and_events_are_refused(self, standalone):
        issues = check_deployment_consistency(JasilSettings(**standalone))

        assert any(issue.startswith("state_uri resolves to process-local memory") for issue in issues)
        assert any(issue.startswith("events_uri resolves to process-local memory") for issue in issues)

    def test_the_noop_lock_is_refused(self, standalone):
        issues = check_deployment_consistency(JasilSettings(**standalone))

        assert any(issue.startswith("lock_uri resolves to an in-process no-op lock") for issue in issues)

    def test_memory_mail_is_refused(self, standalone):
        """The handler would 'send' into the worker's memory and the job would complete."""
        issues = check_deployment_consistency(JasilSettings(mail=MailSettings(uri="memory://"), **standalone))

        assert any(issue.startswith("mail.uri resolves to the in-memory mail recorder") for issue in issues)

    def test_the_refusal_names_the_cause(self, standalone):
        issues = check_deployment_consistency(JasilSettings(**standalone))

        assert all("job_worker_mode=standalone" in issue for issue in issues)

    def test_local_disk_is_allowed_because_both_processes_share_a_host(self, standalone):
        issues = check_deployment_consistency(JasilSettings(**standalone))

        assert not any("storage_uri" in issue for issue in issues)

    def test_a_fully_shared_local_deployment_is_consistent(self, standalone):
        issues = check_deployment_consistency(
            JasilSettings(
                state_uri="redis://c:6379/0",
                events_uri="redis://c:6379/1",
                lock_uri="postgres-advisory://",
                mail=MailSettings(uri="smtp://relay.test", from_address="a@b.c"),
                **standalone,
            )
        )

        assert issues == []

    def test_an_in_process_deployment_is_unchanged(self):
        assert check_deployment_consistency(JasilSettings(jobs=JobSettings(enabled=True))) == []

    def test_standalone_workers_with_jobs_disabled_are_refused(self):
        issues = check_job_worker_consistency(topology=_topology(job_worker_mode=STANDALONE), jobs_enabled=False)

        assert len(issues) == 1 and "jobs.enabled is false" in issues[0]

    @pytest.mark.parametrize(
        ("mode", "jobs_enabled"),
        [(STANDALONE, True), (IN_PROCESS, True), (IN_PROCESS, False)],
    )
    def test_every_other_combination_is_accepted(self, mode, jobs_enabled):
        topology = _topology(job_worker_mode=mode)

        assert check_job_worker_consistency(topology=topology, jobs_enabled=jobs_enabled) == []

    def test_the_contradiction_is_reported_by_the_aggregate_check(self):
        issues = check_deployment_consistency(
            JasilSettings(
                job_worker_mode=STANDALONE,
                state_uri="redis://c:6379/0",
                events_uri="redis://c:6379/1",
                lock_uri="postgres-advisory://",
            )
        )

        assert len(issues) == 1 and "jobs.enabled is false" in issues[0]

    def test_the_custom_profile_opts_out_of_wiring_rules_but_not_the_contradiction(self):
        custom = _topology(CUSTOM, job_worker_mode=STANDALONE)
        sources = (StateSource(label="state_uri", uri="memory://"),)

        assert check_state_consistency(topology=custom, state_sources=sources) == []
        assert check_lock_consistency(topology=custom, lock_uri="noop://", lock_label="l") == []
        assert check_mail_consistency(topology=custom, mail_uri="memory://", mail_label="m") == []
        assert len(check_job_worker_consistency(topology=custom, jobs_enabled=False)) == 1


class TestRulesTakeATopology:
    """Each rule reads the one fact it needs from the topology."""

    def test_state_follows_multi_process(self):
        sources = (StateSource(label="state_uri", uri="memory://"),)

        assert check_state_consistency(topology=_topology(), state_sources=sources) == []
        assert len(check_state_consistency(topology=_topology(web_workers=2), state_sources=sources)) == 1

    def test_state_ignores_a_source_that_does_not_apply(self):
        sources = (StateSource(label="events_uri", uri="memory://", applies=False),)

        assert check_state_consistency(topology=_topology(DISTRIBUTED), state_sources=sources) == []

    def test_state_flags_an_unrecognised_scheme(self):
        sources = (StateSource(label="state_uri", uri="postgres://db"),)

        issues = check_state_consistency(topology=_topology(DISTRIBUTED), state_sources=sources)

        assert len(issues) == 1 and "unrecognized storage scheme" in issues[0]

    @pytest.mark.parametrize(
        ("topology", "refused"),
        [
            pytest.param(_topology(), False, id="single-process"),
            pytest.param(_topology(web_workers=4), False, id="several-processes-share-a-disk"),
            pytest.param(_topology(job_worker_mode=STANDALONE), False, id="standalone-workers-share-a-disk"),
            pytest.param(_topology(DISTRIBUTED), True, id="separate-nodes"),
        ],
    )
    def test_storage_follows_multi_node(self, topology, refused):
        issues = check_storage_consistency(topology=topology, storage_uri="LOCAL://", storage_label="storage_uri")

        assert bool(issues) is refused

    @pytest.mark.parametrize(
        ("topology", "refused"),
        [
            pytest.param(_topology(), False, id="single-process"),
            pytest.param(_topology(web_workers=2), True, id="web-workers"),
            pytest.param(_topology(job_worker_mode=STANDALONE), True, id="standalone-workers"),
            pytest.param(_topology(CUSTOM, 4), False, id="custom"),
        ],
    )
    def test_lock_follows_multi_process(self, topology, refused):
        issues = check_lock_consistency(topology=topology, lock_uri=" NOOP:// ", lock_label="lock_uri")

        assert bool(issues) is refused

    def test_a_shared_lock_is_always_accepted(self):
        topology = _topology(DISTRIBUTED, 4, STANDALONE)

        assert check_lock_consistency(topology=topology, lock_uri="postgres-advisory://", lock_label="l") == []
