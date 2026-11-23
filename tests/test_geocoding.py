"""The HTTP reverse-geocoding backend and the SSRF gate in front of it.

Geocoding is the only capability that dials an operator-supplied host, so it is
the only one carrying an SSRF guard — and the guard is what most of this module
is about. `resolve_reverse_endpoint` is the gate: it validates the host *once*, at
composition time, and a host it rejects produces no endpoint at all, so the
backend is never built.

The other property worth pinning is that `reverse` never raises. It is
best-effort enrichment hanging off an import or a backfill, and an upstream that
is down, slow, rate-limiting, or returning nonsense must degrade to "no location"
rather than fail the work that asked for it.

No DNS and no HTTP happen here: `getaddrinfo` is monkeypatched and `responses`
intercepts the requests.
"""

import contextlib
import itertools
import math
import os
import socket
import ssl
import threading
from datetime import timedelta

import pytest
import requests
import responses

import jasil._core.network as network
import jasil.backends.geocoding_http as geocoding_http
from jasil.backends.geocoding_http import (
    _MAX_RESPONSE_BYTES,
    HttpGeocoding,
    NullGeocoding,
    _failure_detail,
    resolve_reverse_endpoint,
)
from jasil.backends.state_memory import MemoryState
from jasil.providers import GeocodedPlace, GeocodingOutcome, GeocodingProvider, StateBackendUnavailableError
from jasil.testing import FixedClock

# Windows: shutdown() does not wake a thread blocked in recv(), so the watchdog cannot interrupt a read there; only the per-read timeout bounds one.
CANNOT_INTERRUPT_A_READ = pytest.mark.skipif(
    os.name == "nt", reason="Windows: shutdown() does not wake a thread blocked in recv()"
)

PUBLIC_IP = "93.184.216.34"
PRIVATE_IP = "10.0.0.5"
URL = "https://geo.test/reverse"


def _geocoder(service="nominatim", base_url=URL, *, state=None, clock=None, **kwargs) -> HttpGeocoding:
    """An ``HttpGeocoding`` on a gate of its own, unless the test shares one."""
    return HttpGeocoding(
        service, base_url, state=state if state is not None else MemoryState(), clock=clock or FixedClock(), **kwargs
    )


@pytest.fixture
def resolves_to(monkeypatch):
    """Point every hostname at a chosen address, so no test does real DNS."""

    def _install(address: str) -> None:
        family = socket.AF_INET6 if ":" in address else socket.AF_INET

        def _fake_getaddrinfo(host, port, *args, **kwargs):
            return [(family, socket.SOCK_STREAM, 6, "", (address, 0))]

        monkeypatch.setattr(network.socket, "getaddrinfo", _fake_getaddrinfo)

    return _install


class TestNullGeocoding:
    def test_it_satisfies_the_protocol(self):
        assert isinstance(NullGeocoding(), GeocodingProvider)

    def test_it_resolves_nothing(self):
        """'Disabled' is an explicit backend so callers never branch on availability."""
        assert NullGeocoding().reverse(38.7, -9.1) is None

    def test_a_lookup_is_a_final_answer_of_nothing(self):
        assert NullGeocoding().lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, None)

    @pytest.mark.parametrize("max_wait", [-1.0, math.inf, math.nan])
    def test_a_lookup_refuses_a_max_wait_out_of_range(self, max_wait):
        with pytest.raises(ValueError, match="max_wait_seconds"):
            NullGeocoding().lookup(38.7, -9.1, max_wait_seconds=max_wait)

    def test_a_lookup_takes_a_reservation_key(self):
        assert NullGeocoding().lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="job-1") == GeocodingOutcome()

    def test_a_lookup_refuses_an_empty_reservation_key(self):
        with pytest.raises(ValueError, match="reservation_key"):
            NullGeocoding().lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="")


class TestEndpointConstruction:
    def test_a_public_host_yields_its_reverse_endpoint(self, resolves_to):
        resolves_to(PUBLIC_IP)

        assert resolve_reverse_endpoint("nominatim.example.com", use_https=True)[0] == (
            "https://nominatim.example.com/reverse"
        )

    def test_http_is_available_for_a_self_hosted_instance(self, resolves_to):
        resolves_to(PRIVATE_IP)

        endpoint = resolve_reverse_endpoint("geo.internal", use_https=False, allowed_hosts=["10.0.0.0/8"])[0]

        assert endpoint == "http://geo.internal/reverse"

    def test_a_port_is_preserved(self, resolves_to):
        resolves_to(PUBLIC_IP)

        assert resolve_reverse_endpoint("geo.example.com:8080", use_https=True)[0] == (
            "https://geo.example.com:8080/reverse"
        )

    def test_http_to_a_public_host_is_refused(self, resolves_to, caplog):
        """Cleartext would expose the coordinates on the internet; it is for a private instance only."""
        resolves_to(PUBLIC_IP)

        with caplog.at_level("WARNING"):
            endpoint, reason = resolve_reverse_endpoint("geo.example.com:8080", use_https=False)

        assert endpoint is None
        assert reason == "resolves to a public address"
        assert "cleartext" in caplog.text


class TestEgressValidation:
    """OWASP A10. The host comes from configuration, so it is checked before use."""

    def test_a_private_address_is_refused(self, resolves_to, caplog):
        resolves_to(PRIVATE_IP)

        with caplog.at_level("WARNING"):
            assert resolve_reverse_endpoint("internal.example.com", use_https=True)[0] is None

        assert "non-public address" in caplog.text

    def test_the_refusal_says_how_to_allow_it(self, resolves_to, caplog):
        """An operator with a legitimate private instance needs the next step."""
        resolves_to(PRIVATE_IP)

        with caplog.at_level("WARNING"):
            resolve_reverse_endpoint("internal.example.com", use_https=True)[0]

        assert "allow-listed" in caplog.text

    def test_an_unresolvable_host_is_refused(self, monkeypatch, caplog):
        def _fail(*args, **kwargs):
            raise socket.gaierror("nope")

        monkeypatch.setattr(network.socket, "getaddrinfo", _fail)

        with caplog.at_level("WARNING"):
            assert resolve_reverse_endpoint("nowhere.invalid", use_https=True)[0] is None

        assert "could not be resolved" in caplog.text

    @pytest.mark.parametrize(
        "not_an_authority",
        ["http://geo.example.com", "geo.example.com/reverse", "geo.example.com:notaport", "user@geo.example.com"],
    )
    def test_a_value_that_is_not_a_bare_authority_is_refused(self, not_an_authority, resolves_to):
        """Otherwise it is interpolated into the URL and redirects the request."""
        resolves_to(PUBLIC_IP)

        assert resolve_reverse_endpoint(not_an_authority, use_https=True)[0] is None

    def test_an_allow_listed_cidr_permits_a_private_instance(self, resolves_to):
        resolves_to(PRIVATE_IP)

        endpoint = resolve_reverse_endpoint("internal.example.com", use_https=False, allowed_hosts=["10.0.0.0/8"])[0]

        assert endpoint == "http://internal.example.com/reverse"

    def test_an_allow_listed_hostname_permits_it_too(self, resolves_to):
        resolves_to(PRIVATE_IP)

        endpoint = resolve_reverse_endpoint(
            "internal.example.com", use_https=False, allowed_hosts=["internal.example.com"]
        )[0]

        assert endpoint == "http://internal.example.com/reverse"

    def test_an_unrelated_allowlist_entry_does_not_help(self, resolves_to):
        resolves_to(PRIVATE_IP)

        assert (
            resolve_reverse_endpoint("internal.example.com", use_https=False, allowed_hosts=["10.1.0.0/16"])[0] is None
        )

    def test_every_allowlist_hit_is_logged(self, resolves_to, caplog):
        """Operators must be able to review what the exception is being used for."""
        resolves_to(PRIVATE_IP)

        with caplog.at_level("INFO"):
            resolve_reverse_endpoint("internal.example.com", use_https=False, allowed_hosts=["10.0.0.0/8"])[0]

        assert "SSRF allowlist hit" in caplog.text
        assert "reverse_geocoding" in caplog.text


class TestUrlBuilding:
    @pytest.mark.parametrize(
        ("service", "expected"),
        [
            ("nominatim", "format=jsonv2&lat=38.7&lon=-9.1"),
            ("photon", "lat=38.7&lon=-9.1"),
        ],
    )
    def test_each_service_gets_its_own_query(self, service, expected):
        backend = _geocoder(service, "https://geo.test/reverse")

        assert backend._build_url(38.7, -9.1) == f"https://geo.test/reverse?{expected}"

    def test_the_api_key_is_sent_where_one_is_required(self):
        backend = _geocoder("geocode", "https://geo.test/reverse", api_key="secret")

        assert "api_key=secret" in backend._build_url(38.7, -9.1)

    def test_a_missing_api_key_still_produces_a_url(self):
        """The container refuses this combination; the backend must not crash on it."""
        backend = _geocoder("geocode", "https://geo.test/reverse")

        assert "api_key=" in backend._build_url(38.7, -9.1)


class TestResponseParsing:
    @pytest.fixture
    def backend(self):
        return _geocoder("nominatim", "https://geo.test/reverse")

    @responses.activate
    def test_a_nominatim_answer_is_mapped(self, backend):
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={
                "display_name": "Alvalade, Lisboa, Portugal",
                "address": {
                    "city": "Lisboa",
                    "suburb": "Alvalade",
                    "state": "Lisboa",
                    "country": "Portugal",
                    "country_code": "pt",
                },
            },
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(
            locality="Lisboa",
            sublocality="Alvalade",
            region="Lisboa",
            country="Portugal",
            country_code="PT",
            display_name="Alvalade, Lisboa, Portugal",
        )

    @responses.activate
    def test_a_name_that_is_oversized_or_carries_control_characters_is_treated_as_absent(self, backend):
        """The upstream is untrusted: its strings end up in a host's columns and logs."""
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={
                "display_name": "x" * 5000,
                "address": {
                    "city": "L" * 5000,
                    "town": "Lis\x1b[31mboa\n",
                    "village": "Alvalade",
                    "country": "Portugal\x00",
                },
            },
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(locality="Alvalade")

    @responses.activate
    def test_a_photon_answer_is_mapped(self):
        """Photon names things differently, but lands on the same levels."""
        backend = _geocoder("photon", "https://geo.test/reverse")
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={
                "features": [
                    {
                        "properties": {
                            "city": "Lisboa",
                            "district": "Alvalade",
                            "state": "Lisboa",
                            "country": "Portugal",
                            "countrycode": "PT",
                        }
                    }
                ]
            },
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(
            locality="Lisboa", sublocality="Alvalade", region="Lisboa", country="Portugal", country_code="PT"
        )

    @responses.activate
    def test_an_empty_photon_feature_list_resolves_to_nothing(self):
        backend = _geocoder("photon", "https://geo.test/reverse")
        responses.add(responses.GET, "https://geo.test/reverse", json={"features": []})

        assert backend.reverse(38.7, -9.1) is None

    @responses.activate
    def test_a_partial_answer_is_kept(self, backend):
        """A country alone is still worth recording."""
        responses.add(responses.GET, "https://geo.test/reverse", json={"address": {"country": "Portugal"}})

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(country="Portugal")

    @responses.activate
    @pytest.mark.parametrize(
        ("address", "locality"),
        [
            ({"city": "A", "town": "B"}, "A"),
            ({"town": "B", "village": "C"}, "B"),
            ({"village": "C"}, "C"),
            ({"hamlet": "D"}, "D"),
        ],
    )
    def test_the_locality_is_the_most_specific_settlement_type_present(self, backend, address, locality):
        responses.add(responses.GET, "https://geo.test/reverse", json={"address": address})

        assert backend.reverse(38.7, -9.1).locality == locality

    @responses.activate
    def test_a_malformed_country_code_is_dropped(self, backend):
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={"address": {"country": "Portugal", "country_code": "portugal"}},
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(country="Portugal")

    @responses.activate
    def test_a_non_ascii_country_code_is_dropped(self, backend):
        # "ßa" is two letters but upper-cases to three.
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={"address": {"country": "Portugal", "country_code": "ßa"}},
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(country="Portugal")

    @responses.activate
    def test_an_answer_with_nothing_useful_resolves_to_none(self, backend):
        responses.add(responses.GET, "https://geo.test/reverse", json={"address": {"postcode": "1700"}})

        assert backend.reverse(38.7, -9.1) is None


class TestRequestBehaviour:
    @pytest.fixture
    def backend(self):
        return _geocoder("nominatim", "https://geo.test/reverse", user_agent="my-app/1.0")

    @responses.activate
    def test_the_user_agent_identifies_the_caller(self, backend):
        """Nominatim's usage policy requires it."""
        responses.add(responses.GET, "https://geo.test/reverse", json={"address": {"country": "Portugal"}})

        backend.reverse(38.7, -9.1)

        assert responses.calls[0].request.headers["User-Agent"] == "my-app/1.0"

    @responses.activate
    def test_a_redirect_is_refused(self, backend):
        """SSRF defence in depth: a permitted host must not 3xx onto an internal target."""
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            status=302,
            headers={"Location": "http://169.254.169.254/latest/meta-data/"},
        )

        assert backend.reverse(38.7, -9.1) is None
        assert len(responses.calls) == 1


class TestEnvironmentProxies:
    """An egress proxy from the environment is honoured over TLS, which it cannot read; a cleartext ``+http``
    request never goes through one, since the coordinates would cross to a host the address check never saw."""

    @pytest.fixture(autouse=True)
    def proxy_environment(self, monkeypatch):
        for variable in ("NO_PROXY", "no_proxy", "ALL_PROXY", "all_proxy"):
            monkeypatch.delenv(variable, raising=False)
        for variable in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
            monkeypatch.setenv(variable, "http://proxy.test:3128")

    @responses.activate
    def test_a_cleartext_request_ignores_an_environment_proxy(self):
        responses.add(responses.GET, "http://10.0.0.5/reverse", json={"address": {"country": "Portugal"}})

        _geocoder("nominatim", "http://10.0.0.5/reverse").reverse(38.7, -9.1)

        assert not responses.calls[0].request.req_kwargs["proxies"]

    @responses.activate
    def test_an_https_request_goes_through_an_environment_proxy(self):
        responses.add(responses.GET, "https://geo.test/reverse", json={"address": {"country": "Portugal"}})

        _geocoder("nominatim", "https://geo.test/reverse").reverse(38.7, -9.1)

        assert responses.calls[0].request.req_kwargs["proxies"]["https"] == "http://proxy.test:3128"


class TestFailuresNeverPropagate:
    """Enrichment must not fail the import or backfill that triggered it."""

    @pytest.fixture
    def backend(self):
        return _geocoder("nominatim", "https://geo.test/reverse")

    @responses.activate
    @pytest.mark.parametrize("status", [400, 401, 429, 500, 503])
    def test_an_error_status_resolves_to_none(self, backend, status, caplog):
        responses.add(responses.GET, "https://geo.test/reverse", status=status, json={})

        with caplog.at_level("ERROR"):
            assert backend.reverse(38.7, -9.1) is None

        assert "failed" in caplog.text

    @responses.activate
    def test_an_unparseable_body_resolves_to_none(self, backend):
        responses.add(responses.GET, "https://geo.test/reverse", body="<html>not json</html>", status=200)

        assert backend.reverse(38.7, -9.1) is None

    @responses.activate
    def test_a_connection_failure_resolves_to_none(self, backend):
        responses.add(responses.GET, "https://geo.test/reverse", body=OSError("connection refused"))

        assert backend.reverse(38.7, -9.1) is None

    def test_a_missing_requests_install_resolves_to_none(self, backend, monkeypatch):
        """``requests`` is the optional geocoding extra and is imported lazily."""
        monkeypatch.setitem(__import__("sys").modules, "requests", None)

        assert backend.reverse(38.7, -9.1) is None


class TestTheFailureLogIsRedacted:
    """``requests`` puts the request URL in its error messages, and one service
    carries the API key in that URL's query string. So neither the message nor a
    traceback may reach the log, however useful they would be.
    """

    @responses.activate
    @pytest.mark.parametrize("status", [401, 403, 429, 500])
    def test_the_api_key_never_reaches_the_log(self, status, caplog):
        backend = _geocoder("geocode", "https://geo.test/reverse", api_key="super-secret-key")
        responses.add(responses.GET, "https://geo.test/reverse", status=status, json={})

        with caplog.at_level("ERROR"):
            assert backend.reverse(38.7, -9.1) is None

        assert "super-secret-key" not in caplog.text
        assert "api_key" not in caplog.text

    @responses.activate
    def test_the_status_code_survives_the_redaction(self, caplog):
        """An operator still has to be able to tell a 401 from a 429."""
        backend = _geocoder("nominatim", "https://geo.test/reverse")
        responses.add(responses.GET, "https://geo.test/reverse", status=429, json={})

        with caplog.at_level("ERROR"):
            backend.reverse(38.7, -9.1)

        assert "HTTP 429" in caplog.text

    def test_a_failure_carrying_no_response_is_named_by_its_type(self):
        assert _failure_detail(TimeoutError("connect timed out")) == "TimeoutError"

    def test_a_failure_message_is_never_interpolated(self):
        assert "connect timed out" not in _failure_detail(TimeoutError("connect timed out"))


class TestTheResponseBodyIsCapped:
    """The upstream is exactly the party the SSRF guard assumes may be hostile.

    ``response.json()`` buffers whatever it is sent, and the request timeout
    bounds each read rather than the whole transfer, so the size limit is the
    only thing standing between a bad upstream and the process's memory.
    """

    @pytest.fixture
    def backend(self):
        return _geocoder("nominatim", "https://geo.test/reverse")

    @responses.activate
    def test_an_oversized_body_resolves_to_none(self, backend, caplog):
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            body=b"x" * (_MAX_RESPONSE_BYTES + 1),
            status=200,
        )

        with caplog.at_level("ERROR"):
            assert backend.reverse(38.7, -9.1) is None

        assert "ValueError" in caplog.text

    @responses.activate
    def test_a_large_but_permitted_body_is_still_parsed(self, backend):
        padding = "P" * (_MAX_RESPONSE_BYTES // 2)
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={"address": {"country": "Portugal", "note": padding}},
            status=200,
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(country="Portugal")


class TestTheTransferHasADeadline:
    """The request timeout bounds each read; an upstream dripping bytes must not hold the caller indefinitely."""

    @responses.activate
    def test_a_transfer_past_the_deadline_resolves_to_none(self, monkeypatch, caplog):
        clock = itertools.count(1000, 10)  # every read of the clock is ten seconds after the last
        monkeypatch.setattr("jasil.backends.geocoding_http.time.monotonic", lambda: next(clock))
        monkeypatch.setattr("jasil.backends.geocoding_http.time.sleep", lambda _seconds: None)
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={"address": {"country": "Portugal", "note": "P" * (_MAX_RESPONSE_BYTES // 2)}},
            status=200,
        )

        with caplog.at_level("ERROR"):
            assert _geocoder("nominatim", "https://geo.test/reverse").reverse(38.7, -9.1) is None

        assert "TimeoutError" in caplog.text


class TestTheWatchdog:
    """A read timeout restarts on every byte, so only shutting the socket down bounds a dripping upstream."""

    @pytest.fixture
    def blocked_read(self):
        """A socket with a thread blocked reading it, and the event set when that read returns."""
        ours, theirs = socket.socketpair()
        released = threading.Event()

        def read():
            with contextlib.suppress(OSError):
                ours.recv(1)
            released.set()

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        yield ours, released
        ours.close()
        theirs.close()
        reader.join(timeout=5)

    @CANNOT_INTERRUPT_A_READ
    def test_expiring_releases_a_blocked_read(self, blocked_read):
        sock, released = blocked_read
        deadline = geocoding_http._Deadline()
        deadline.watch(sock)

        deadline.expire()

        assert released.wait(5)

    @CANNOT_INTERRUPT_A_READ
    def test_a_socket_opened_after_the_deadline_is_shut_down_at_once(self, blocked_read):
        """A slow connect hands its socket over only after the deadline has passed."""
        sock, released = blocked_read
        deadline = geocoding_http._Deadline()
        deadline.expire()

        deadline.watch(sock)

        assert released.wait(5)

    @CANNOT_INTERRUPT_A_READ
    def test_expiring_releases_a_tls_handshake_that_detached_the_watched_socket(self):
        """Wrapping a socket in TLS detaches the original, so the deadline must hold a duplicate of it."""
        ours, theirs = socket.socketpair()
        deadline = geocoding_http._Deadline()
        deadline.watch(ours)
        released = threading.Event()

        def handshake():
            with contextlib.suppress(OSError):
                ssl.create_default_context().wrap_socket(ours, server_hostname="geo.test")
            released.set()

        threading.Thread(target=handshake, daemon=True).start()
        try:
            theirs.recv(1)  # the ClientHello is out, so the handshake is now blocked reading

            deadline.expire()

            assert released.wait(5)
        finally:
            theirs.close()
            deadline.close()

    def test_closing_releases_the_duplicates_and_leaves_the_original_open(self):
        ours, theirs = socket.socketpair()
        deadline = geocoding_http._Deadline()
        deadline.watch(ours)
        watched = deadline._sockets[0]

        deadline.close()

        assert watched.fileno() == -1
        assert ours.fileno() != -1
        ours.close()
        theirs.close()

    @CANNOT_INTERRUPT_A_READ
    def test_an_upstream_that_never_answers_is_cut_off_at_the_deadline(self, monkeypatch, caplog):
        """Headers come before the body loop, so a deadline checked between body reads never sees them."""
        ours, theirs = socket.socketpair()
        monkeypatch.setattr(geocoding_http, "_DEADLINE_SECONDS", 0)
        # Without the watchdog the read would only end at this timeout, as a ReadTimeout.
        monkeypatch.setattr(geocoding_http, "_TIMEOUT_SECONDS", 2)
        monkeypatch.setattr("urllib3.util.connection.create_connection", lambda *args, **kwargs: ours)
        try:
            with caplog.at_level("ERROR"):
                assert _geocoder("nominatim", "http://geo.test/reverse").reverse(38.7, -9.1) is None
        finally:
            ours.close()
            theirs.close()

        assert "Reverse-geocoding via nominatim failed" in caplog.text
        assert "Timeout" not in caplog.text


class TestTheWatchedAdapter:
    def test_a_proxied_connection_is_watched_too(self):
        """``requests`` builds a separate manager per proxy, and it must not fall back to unwatched pools."""
        adapter = geocoding_http._watched_adapter()()

        proxy_manager = adapter.proxy_manager_for("http://proxy.test:3128")

        assert proxy_manager.pool_classes_by_scheme == adapter.poolmanager.pool_classes_by_scheme


PORTUGAL = {"address": {"country": "Portugal"}}


@pytest.fixture
def slept(monkeypatch):
    """Record each wait instead of sleeping, on a monotonic clock that stands still."""
    waits = []
    monkeypatch.setattr(geocoding_http.time, "sleep", waits.append)
    monkeypatch.setattr(geocoding_http.time, "monotonic", lambda: 1000.0)
    return waits


class TestTheRateGate:
    """One budget per upstream, shared by every backend on the same state, so N processes never send N times the
    rate and a settings reload does not reset it."""

    @responses.activate
    def test_no_rate_limit_means_no_wait(self, slept):
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend = _geocoder(min_interval_seconds=0)

        backend.reverse(38.7, -9.1)
        backend.reverse(38.7, -9.1)

        assert slept == []
        assert len(responses.calls) == 2

    @responses.activate
    def test_a_second_call_waits_out_the_interval(self, slept):
        """Nominatim's policy is one request per second; exceeding it gets you banned."""
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend = _geocoder(min_interval_seconds=1.0)

        backend.reverse(38.7, -9.1)
        backend.reverse(38.7, -9.1)

        assert slept == [pytest.approx(1.0)]

    @responses.activate
    def test_two_backends_on_one_state_share_the_budget(self, slept):
        """What two processes on one Redis, or a backend and its rebuild after a reload, look like."""
        responses.add(responses.GET, URL, json=PORTUGAL)
        state = MemoryState()
        first = _geocoder(state=state, min_interval_seconds=1.0)
        second = _geocoder(state=state, min_interval_seconds=1.0)

        first.reverse(38.7, -9.1)
        second.reverse(38.7, -9.1)

        assert slept == [pytest.approx(1.0)]
        assert state.get("jasil:geocoding:gate:nominatim:geo.test") is not None

    def test_the_host_is_matched_without_regard_to_case(self):
        """``GEO.test`` and ``geo.test`` are one upstream, so a spelling must not split its budget or back-off."""
        upper, lower = _geocoder(base_url="https://GEO.test/reverse"), _geocoder()

        assert (upper._gate_key, upper._back_off_key) == (lower._gate_key, lower._back_off_key)

    @responses.activate
    def test_a_back_off_recorded_while_waiting_for_a_slot_stops_the_request(self, monkeypatch):
        """A peer's 429 can arrive while this request sleeps for its slot; it must not then send anyway."""
        monkeypatch.setattr(geocoding_http.time, "monotonic", lambda: 1000.0)
        responses.add(responses.GET, URL, json=PORTUGAL)
        clock, state = FixedClock(), MemoryState()
        backend = _geocoder(clock=clock, state=state, min_interval_seconds=1.0)
        peer = _geocoder(clock=clock, state=state, min_interval_seconds=1.0)
        state.reserve_slot(backend._gate_key, 1.0, max_wait_seconds=0)  # the next slot is a second away
        monkeypatch.setattr(geocoding_http.time, "sleep", lambda _seconds: peer._back_off("120"))

        outcome = backend.lookup(38.7, -9.1, max_wait_seconds=5)

        assert outcome == GeocodingOutcome(None, clock.now() + timedelta(seconds=120))
        assert len(responses.calls) == 0

    @responses.activate
    def test_backends_for_different_hosts_do_not_share_a_budget(self, slept):
        responses.add(responses.GET, "https://a.test/reverse", json=PORTUGAL)
        responses.add(responses.GET, "https://b.test/reverse", json=PORTUGAL)
        state = MemoryState()

        _geocoder(base_url="https://a.test/reverse", state=state, min_interval_seconds=1.0).reverse(38.7, -9.1)
        _geocoder(base_url="https://b.test/reverse", state=state, min_interval_seconds=1.0).reverse(38.7, -9.1)

        assert slept == []

    @responses.activate
    def test_a_wait_past_the_bound_says_when_to_retry_without_sending(self, slept):
        responses.add(responses.GET, URL, json=PORTUGAL)
        clock = FixedClock()
        backend = _geocoder(clock=clock, min_interval_seconds=1.0)
        backend.lookup(38.7, -9.1, max_wait_seconds=0)

        outcome = backend.lookup(38.7, -9.1, max_wait_seconds=0.5)

        assert outcome == GeocodingOutcome(None, clock.now() + timedelta(seconds=1))
        assert len(responses.calls) == 1
        assert slept == []

    @responses.activate
    def test_reverse_gives_up_on_a_wait_past_its_bound(self, slept):
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend = _geocoder(min_interval_seconds=geocoding_http._REVERSE_MAX_WAIT_SECONDS + 1)
        backend.reverse(38.7, -9.1)

        assert backend.reverse(38.7, -9.1) is None
        assert len(responses.calls) == 1
        assert slept == []

    @responses.activate
    def test_an_unreachable_state_defers_without_sending(self, caplog):
        class _Down(MemoryState):
            def reserve_send(self, key, interval_seconds, *, max_wait_seconds, booking=None):
                raise StateBackendUnavailableError("down")

        clock = FixedClock()
        backend = _geocoder(state=_Down(), clock=clock, min_interval_seconds=1.0)

        with caplog.at_level("ERROR"):
            outcome = backend.lookup(38.7, -9.1, max_wait_seconds=5)

        assert outcome == GeocodingOutcome(None, clock.now() + timedelta(seconds=geocoding_http._RETRY_BACKOFF_SECONDS))
        assert len(responses.calls) == 0
        assert "StateBackendUnavailableError" in caplog.text

    @pytest.mark.parametrize("max_wait", [-1.0, math.inf, math.nan])
    def test_a_max_wait_out_of_range_is_refused(self, max_wait):
        with pytest.raises(ValueError, match="max_wait_seconds"):
            _geocoder().lookup(38.7, -9.1, max_wait_seconds=max_wait)


@pytest.fixture
def gate_clock(monkeypatch):
    """One instant for the geocoder's clock and the memory gate's monotonic clock, advanced together.

    Each wait is recorded instead of slept.
    """
    clock = FixedClock()
    start = clock.now()
    clock.waits = []
    monkeypatch.setattr(geocoding_http.time, "sleep", clock.waits.append)
    monkeypatch.setattr(geocoding_http.time, "monotonic", lambda: 1000.0 + (clock.now() - start).total_seconds())
    return clock


def _advance_to(clock, moment):
    clock.advance((moment - clock.now()).total_seconds())


class TestBookedLookups:
    """A lookup that names its caller books the next slot instead of sleeping for it, and runs at that slot when it
    comes back, so a backlog of deferred lookups neither holds workers asleep nor wakes together."""

    @responses.activate
    def test_deferred_lookups_each_get_a_slot_of_their_own_and_run_once_there(self, gate_clock):
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend = _geocoder(clock=gate_clock, min_interval_seconds=1.0)
        start = gate_clock.now()

        outcomes = [backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key=f"job-{n}") for n in range(100)]

        assert outcomes[0].place == GeocodedPlace(country="Portugal")
        assert [outcome.retry_at for outcome in outcomes[1:]] == [start + timedelta(seconds=n) for n in range(1, 100)]
        for n, outcome in enumerate(outcomes[1:], start=1):
            _advance_to(gate_clock, outcome.retry_at)
            again = backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key=f"job-{n}")
            assert again.place == GeocodedPlace(country="Portugal"), n
        assert len(responses.calls) == 100
        assert gate_clock.waits == []

    @responses.activate
    def test_a_lookup_back_before_its_slot_keeps_it_and_sends_nothing(self, gate_clock):
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend = _geocoder(clock=gate_clock, min_interval_seconds=1.0)
        backend.lookup(38.7, -9.1, max_wait_seconds=0)
        booked = backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="job-1")
        gate_clock.advance(0.5)

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="job-1") == booked
        assert len(responses.calls) == 1

    @responses.activate
    def test_lookups_back_late_together_send_an_interval_apart(self, gate_clock):
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend = _geocoder(clock=gate_clock, min_interval_seconds=1.0)
        backend.lookup(38.7, -9.1, max_wait_seconds=0)
        for name in ("a", "b"):
            backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key=name)
        gate_clock.advance(30)

        backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="a")
        backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="b")

        assert len(responses.calls) == 3
        assert gate_clock.waits == [pytest.approx(1.0)]  # never more than one interval

    @responses.activate
    def test_an_upstream_back_off_holds_a_booked_lookup_until_it_ends(self, gate_clock):
        responses.add(responses.GET, URL, json=PORTUGAL)
        state = MemoryState()
        backend = _geocoder(clock=gate_clock, state=state, min_interval_seconds=1.0)
        peer = _geocoder(clock=gate_clock, state=state, min_interval_seconds=1.0)
        backend.lookup(38.7, -9.1, max_wait_seconds=0)
        booked = backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="job-1")
        peer._back_off("120")
        backed_off_until = gate_clock.now() + timedelta(seconds=120)
        _advance_to(gate_clock, booked.retry_at)

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="job-1") == GeocodingOutcome(
            None, backed_off_until
        )
        assert len(responses.calls) == 1

        _advance_to(gate_clock, backed_off_until)

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="job-1").place is not None

    def test_a_reservation_key_out_of_range_is_refused(self):
        with pytest.raises(ValueError, match="reservation_key"):
            _geocoder().lookup(38.7, -9.1, max_wait_seconds=0, reservation_key="x" * 201)


class TestLookupOutcomes:
    """``retry_at`` separates "ask again later" from "there is nothing here", so a host never records a throttled
    or failed request as a permanent miss."""

    @pytest.fixture
    def clock(self):
        return FixedClock()

    @pytest.fixture
    def state(self):
        return MemoryState()

    @pytest.fixture
    def backend(self, clock, state):
        return _geocoder(clock=clock, state=state)

    def _after(self, clock, seconds):
        return clock.now() + timedelta(seconds=seconds)

    @responses.activate
    def test_a_resolved_place_is_final(self, backend):
        responses.add(responses.GET, URL, json=PORTUGAL)

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(GeocodedPlace(country="Portugal"))

    @responses.activate
    def test_an_answer_with_nothing_is_final(self, backend):
        responses.add(responses.GET, URL, json={})

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, None)

    @responses.activate
    @pytest.mark.parametrize("status", [429, 503])
    @pytest.mark.parametrize(
        ("retry_after", "seconds"),
        [
            ("120", 120),
            ("Thu, 01 Jan 2026 12:05:00 GMT", 300),
            (None, geocoding_http._RETRY_BACKOFF_SECONDS),
            ("soon", geocoding_http._RETRY_BACKOFF_SECONDS),
            ("999999999", geocoding_http._MAX_RETRY_AFTER_SECONDS),
            ("Thu, 01 Jan 2026 11:00:00 GMT", 0),
        ],
        ids=["delta-seconds", "http-date", "absent", "unreadable", "clamped", "past"],
    )
    def test_a_throttled_answer_says_when_to_retry(self, backend, clock, status, retry_after, seconds):
        headers = {"Retry-After": retry_after} if retry_after is not None else {}
        responses.add(responses.GET, URL, status=status, headers=headers, json={})

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, self._after(clock, seconds))

    @responses.activate
    def test_a_throttled_answer_holds_back_every_backend_on_the_state(self, backend, clock, state):
        """An upstream that keeps being called after a 429 bans the client; every process backs off together."""
        responses.add(responses.GET, URL, status=429, headers={"Retry-After": "120"}, json={})
        responses.add(responses.GET, URL, json=PORTUGAL)
        backend.lookup(38.7, -9.1, max_wait_seconds=0)
        peer = _geocoder(clock=clock, state=state)

        assert peer.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, self._after(clock, 120))
        assert len(responses.calls) == 1

        clock.advance(121)

        assert peer.lookup(38.7, -9.1, max_wait_seconds=0).place == GeocodedPlace(country="Portugal")

    def test_a_shorter_back_off_never_cuts_a_longer_one_short(self, backend, clock):
        """Two requests in flight can come back with different ``Retry-After`` values, in either order."""
        backend._back_off("3600")
        backend._back_off("60")

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, self._after(clock, 3600))

    @responses.activate
    def test_a_back_off_that_cannot_be_shared_still_says_when_to_retry(self, clock, caplog):
        class _ReadOnly(MemoryState):
            def index_add(self, key, member, score, ttl_seconds):
                raise StateBackendUnavailableError("down")

        responses.add(responses.GET, URL, status=429, headers={"Retry-After": "120"}, json={})
        backend = _geocoder(clock=clock, state=_ReadOnly())

        with caplog.at_level("ERROR"):
            outcome = backend.lookup(38.7, -9.1, max_wait_seconds=0)

        assert outcome == GeocodingOutcome(None, self._after(clock, 120))
        assert "StateBackendUnavailableError" in caplog.text

    @responses.activate
    @pytest.mark.parametrize("status", [500, 502, 504])
    def test_a_server_error_says_when_to_retry_without_holding_back_the_others(self, backend, clock, status):
        responses.add(responses.GET, URL, status=status, json={})
        responses.add(responses.GET, URL, json=PORTUGAL)

        outcome = backend.lookup(38.7, -9.1, max_wait_seconds=0)

        assert outcome == GeocodingOutcome(None, self._after(clock, geocoding_http._RETRY_BACKOFF_SECONDS))
        assert backend.lookup(38.7, -9.1, max_wait_seconds=0).place == GeocodedPlace(country="Portugal")

    @responses.activate
    @pytest.mark.parametrize(
        "failure", [requests.Timeout("slow"), requests.ConnectionError("refused")], ids=["timeout", "connection"]
    )
    def test_a_transport_failure_says_when_to_retry(self, backend, clock, failure):
        responses.add(responses.GET, URL, body=failure)

        outcome = backend.lookup(38.7, -9.1, max_wait_seconds=0)

        assert outcome == GeocodingOutcome(None, self._after(clock, geocoding_http._RETRY_BACKOFF_SECONDS))

    @responses.activate
    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    def test_a_refused_request_is_final(self, backend, status):
        """A retry would be refused the same way; the operator reads why in the log."""
        responses.add(responses.GET, URL, status=status, json={})

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, None)

    @responses.activate
    def test_an_unparseable_answer_is_final(self, backend):
        responses.add(responses.GET, URL, body="<html>not json</html>", status=200)

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, None)

    def test_a_missing_requests_install_is_final(self, backend, monkeypatch):
        monkeypatch.setitem(__import__("sys").modules, "requests", None)

        assert backend.lookup(38.7, -9.1, max_wait_seconds=0) == GeocodingOutcome(None, None)

    @responses.activate
    def test_a_transfer_past_the_deadline_says_when_to_retry(self, backend, clock, monkeypatch):
        ticks = itertools.count(1000, 10)  # every read of the clock is ten seconds after the last
        monkeypatch.setattr("jasil.backends.geocoding_http.time.monotonic", lambda: next(ticks))
        responses.add(
            responses.GET, URL, json={"address": {"country": "Portugal", "note": "P" * (_MAX_RESPONSE_BYTES // 2)}}
        )

        outcome = backend.lookup(38.7, -9.1, max_wait_seconds=0)

        assert outcome == GeocodingOutcome(None, self._after(clock, geocoding_http._RETRY_BACKOFF_SECONDS))
