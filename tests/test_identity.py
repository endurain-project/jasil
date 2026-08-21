"""Process identity — the value that decides which worker won a job.

``claim_jobs`` re-selects on ``locked_by == worker_id``, so this string is not
just a label: two live processes sharing one identity would each be handed the
other's rows and run the same subscriber twice. It also has to fit a fixed-width
column, and a hostname is not something the deployment can shorten — which is
the tension every test here is about.
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


class TestItFitsTheLeaseColumn:
    """A long hostname used to overflow ``locked_by`` and fail the claim UPDATE
    on PostgreSQL — from inside the worker loop, far from the cause.
    """

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
    collapses distinct machines onto one identity — and the claim would then hand
    a worker rows another worker had leased.
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
