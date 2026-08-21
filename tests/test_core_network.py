"""The SSRF guard — the library's only security control.

Every case fakes ``socket.getaddrinfo``: the guard's whole job is deciding what
to do with a resolver's answer, so the resolver is exactly what has to be
controlled. It also keeps the suite offline and deterministic — a test that
really resolved ``localhost`` would be at the mercy of the host's ``/etc/hosts``.
"""

import socket

import pytest

import jasil._core.network as network

PUBLIC_IP = "93.184.216.34"
PRIVATE_IP = "10.0.0.5"
LOOPBACK_IP = "127.0.0.1"
LINK_LOCAL_IP = "169.254.169.254"  # cloud metadata service
IPV6_LOOPBACK = "::1"


def _addrinfo(*ips: str) -> list[tuple]:
    """Build a ``getaddrinfo`` result for ``ips`` (IPv6 entries get family 10)."""
    return [
        (
            socket.AF_INET6 if ":" in ip else socket.AF_INET,
            socket.SOCK_STREAM,
            6,
            "",
            (ip, 0),
        )
        for ip in ips
    ]


@pytest.fixture
def resolve(monkeypatch):
    """Return a callable installing a fake DNS answer for every lookup."""

    def _install(*ips: str):
        monkeypatch.setattr(network.socket, "getaddrinfo", lambda *a, **kw: _addrinfo(*ips))

    return _install


@pytest.fixture
def unresolvable(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise socket.gaierror("name or service not known")

    monkeypatch.setattr(network.socket, "getaddrinfo", _boom)


class TestPublicHosts:
    def test_a_public_address_is_allowed(self, resolve):
        resolve(PUBLIC_IP)

        assert network.host_rejection_reason("example.com") is None

    def test_a_port_is_accepted(self, resolve):
        resolve(PUBLIC_IP)

        assert network.host_rejection_reason("example.com:8080") is None

    def test_every_resolved_address_must_be_public(self, resolve):
        """A DNS-rebinding answer mixes a public and a private record.

        Accepting the host because *one* answer was public is the bug this
        guards: the connection could still be made to the private address.
        """
        resolve(PUBLIC_IP, PRIVATE_IP)

        assert network.host_rejection_reason("rebind.example.com") == network._NON_PUBLIC


class TestNonPublicAddresses:
    @pytest.mark.parametrize(
        "ip",
        [
            pytest.param(PRIVATE_IP, id="rfc1918"),
            pytest.param(LOOPBACK_IP, id="loopback"),
            pytest.param(LINK_LOCAL_IP, id="link-local-metadata"),
            pytest.param("0.0.0.0", id="unspecified"),  # noqa: S104 - a test input, not a bind address
            pytest.param("240.0.0.1", id="reserved"),
            pytest.param(IPV6_LOOPBACK, id="ipv6-loopback"),
            pytest.param("fc00::1", id="ipv6-unique-local"),
        ],
    )
    def test_a_non_public_address_is_refused(self, resolve, ip):
        resolve(ip)

        assert network.host_rejection_reason("internal.example.com") == network._NON_PUBLIC

    def test_an_unparseable_resolver_answer_is_refused(self, resolve):
        resolve("not-an-ip")

        assert network.host_rejection_reason("weird.example.com") == network._UNPARSEABLE

    def test_an_unresolvable_host_is_refused(self, unresolvable):
        assert network.host_rejection_reason("nx.example.com") == network._UNRESOLVABLE


class TestAuthorityValidation:
    """A configured value must be a bare ``host[:port]``.

    A value carrying a scheme or path would be interpolated into a URL by the
    caller — ``"evil.example.com/x"`` becomes ``https://evil.example.com/x/reverse``
    — and the hostname check would pass while the request went elsewhere.
    """

    @pytest.mark.parametrize(
        "host",
        [
            pytest.param(None, id="none"),
            pytest.param("", id="empty"),
            pytest.param("http://example.com", id="scheme"),
            pytest.param("example.com/path", id="path"),
            pytest.param("user:pass@example.com", id="credentials"),
            pytest.param("example.com:notaport", id="non-numeric-port"),
            pytest.param("example.com:123456", id="over-long-port"),
            pytest.param("exa mple.com", id="space"),
            pytest.param("-example.com", id="leading-hyphen"),
            pytest.param("a" * 254, id="over-long-hostname"),
        ],
    )
    def test_a_value_that_is_not_a_bare_authority_is_refused(self, host):
        assert network.host_rejection_reason(host) == network._NOT_AN_AUTHORITY

    def test_authority_validation_happens_before_dns(self, monkeypatch):
        """A malformed authority must be rejected without a lookup."""

        def _fail(*_args, **_kwargs):
            raise AssertionError("getaddrinfo must not be called for a malformed authority")

        monkeypatch.setattr(network.socket, "getaddrinfo", _fail)

        assert network.host_rejection_reason("http://example.com") == network._NOT_AN_AUTHORITY


class TestAllowlist:
    def test_a_private_address_is_allowed_when_its_hostname_is_listed(self, resolve):
        resolve(PRIVATE_IP)

        assert network.host_rejection_reason("nominatim.lan", allowed_hosts=["nominatim.lan"]) is None

    def test_hostname_matching_is_case_insensitive(self, resolve):
        resolve(PRIVATE_IP)

        assert network.host_rejection_reason("Nominatim.LAN", allowed_hosts=["nominatim.lan"]) is None

    def test_a_private_address_is_allowed_when_covered_by_a_cidr(self, resolve):
        resolve(PRIVATE_IP)

        assert network.host_rejection_reason("nominatim.lan", allowed_hosts=["10.0.0.0/8"]) is None

    def test_every_address_must_be_covered_not_just_one(self, resolve):
        """A host resolving to both IPv4 and IPv6 needs both allow-listed.

        This is why an IPv4-only CIDR does not by itself unlock ``localhost``.
        """
        resolve(LOOPBACK_IP, IPV6_LOOPBACK)

        assert network.host_rejection_reason("local.test", allowed_hosts=["127.0.0.0/8"]) == network._NON_PUBLIC
        assert network.host_rejection_reason("local.test", allowed_hosts=["127.0.0.0/8", "::1"]) is None

    def test_an_unrelated_allowlist_entry_does_not_help(self, resolve):
        resolve(PRIVATE_IP)

        assert network.host_rejection_reason("evil.lan", allowed_hosts=["safe.lan", "192.168.0.0/16"]) is not None

    def test_the_allowlist_is_empty_by_default(self, resolve):
        resolve(PRIVATE_IP)

        assert network.host_rejection_reason("nominatim.lan") == network._NON_PUBLIC

    def test_an_allowlist_hit_is_logged_for_audit(self, resolve, caplog):
        """Operators must be able to review what the exception is being used for."""
        resolve(PRIVATE_IP)

        with caplog.at_level("INFO", logger=network.__name__):
            network.host_rejection_reason("nominatim.lan", allowed_hosts=["nominatim.lan"], purpose="geocoding")

        assert "SSRF allowlist hit" in caplog.text
        assert "geocoding" in caplog.text

    def test_a_hostname_entry_warns_that_it_is_the_broader_form(self, resolve, caplog):
        """It exempts whatever the name resolves to — a rebind, or a metadata endpoint.

        A CIDR entry cannot widen that way, so the two are not equivalent and the
        operator who chose the broader one should be told.
        """
        resolve(PRIVATE_IP)

        with caplog.at_level("INFO", logger=network.__name__):
            network.host_rejection_reason("nominatim.lan", allowed_hosts=["nominatim.lan"])

        assert [record.levelname for record in caplog.records] == ["WARNING"]
        assert "Prefer a CIDR" in caplog.text

    def test_a_cidr_entry_does_not_warn(self, resolve, caplog):
        """The precise form is the one being recommended; it must not nag."""
        resolve(PRIVATE_IP)

        with caplog.at_level("INFO", logger=network.__name__):
            network.host_rejection_reason("nominatim.lan", allowed_hosts=["10.0.0.0/8"])

        assert [record.levelname for record in caplog.records] == ["INFO"]

    def test_a_rejected_host_logs_nothing(self, resolve, caplog):
        """No exemption was taken, so there is nothing to audit."""
        resolve(PRIVATE_IP)

        with caplog.at_level("INFO", logger=network.__name__):
            assert network.host_rejection_reason("evil.lan", allowed_hosts=["safe.lan"]) == network._NON_PUBLIC

        assert caplog.records == []
