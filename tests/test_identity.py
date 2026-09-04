"""Process identity for Redis competing-consumer coordination.

Two live Redis consumers sharing one identity would collide in pending-entry
state. The identity is also persisted in the event log and has to fit a
fixed-width column, while a hostname is not something the deployment can
shorten — which is the tension every test here is about.
"""

import socket

import pytest

from jasil._core.identity import process_identity
from jasil._core.limits import MAX_WORKER_ID_LENGTH

LONG_HOSTNAME = "worker-" + "n" * 200


@pytest.fixture
def hostname(monkeypatch):
    """Set the hostname ``process_identity`` derives from."""

    def _install(value: str) -> None:
        monkeypatch.setattr(socket, "gethostname", lambda: value)

    return _install


class TestProcessIdentity:
    def test_it_is_the_hostname_and_the_pid(self, hostname):
        hostname("box")

        assert process_identity().startswith("box-")

    def test_it_is_stable_within_a_process(self, hostname):
        hostname("box")

        assert process_identity() == process_identity()

    def test_a_short_hostname_is_left_alone(self, hostname):
        hostname("box")

        assert "-" in process_identity()
        assert len(process_identity()) < MAX_WORKER_ID_LENGTH


class TestItFitsPersistedWorkerColumns:
    """A long hostname must fit wherever a consumer identity is persisted."""

    def test_a_long_hostname_is_bounded(self, hostname):
        hostname(LONG_HOSTNAME)

        assert len(process_identity()) == MAX_WORKER_ID_LENGTH

    def test_the_lease_column_is_declared_at_that_width(self, mapped_base):
        column = mapped_base.metadata.tables["processing_jobs"].c.locked_by

        assert column.type.length == MAX_WORKER_ID_LENGTH

    def test_the_event_log_column_is_declared_at_that_width(self, mapped_base):
        column = mapped_base.metadata.tables["event_log"].c.worker_id

        assert column.type.length == MAX_WORKER_ID_LENGTH


class TestBoundingPreservesUniqueness:
    """Why the overflow is not simply clipped.

    Kubernetes and cloud hostnames share long prefixes, so a plain truncation
    collapses distinct machines onto one Redis consumer identity.
    """

    def test_two_hosts_sharing_a_prefix_stay_distinct(self, hostname):
        hostname(LONG_HOSTNAME + "-alpha")
        first = process_identity()
        hostname(LONG_HOSTNAME + "-beta")
        second = process_identity()

        assert first != second
        assert len(first) == len(second) == MAX_WORKER_ID_LENGTH

    def test_the_same_host_derives_the_same_identity(self, hostname):
        hostname(LONG_HOSTNAME)
        first = process_identity()
        hostname(LONG_HOSTNAME)

        assert process_identity() == first
