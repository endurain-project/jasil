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
import os
import socket
import ssl
import threading

import pytest
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
from jasil.providers import GeocodedPlace, GeocodingProvider

# Windows: shutdown() does not wake a thread blocked in recv(), so the watchdog cannot interrupt a read there; only the per-read timeout bounds one.
CANNOT_INTERRUPT_A_READ = pytest.mark.skipif(
    os.name == "nt", reason="Windows: shutdown() does not wake a thread blocked in recv()"
)

PUBLIC_IP = "93.184.216.34"
PRIVATE_IP = "10.0.0.5"


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
        backend = HttpGeocoding(service, "https://geo.test/reverse")

        assert backend._build_url(38.7, -9.1) == f"https://geo.test/reverse?{expected}"

    def test_the_api_key_is_sent_where_one_is_required(self):
        backend = HttpGeocoding("geocode", "https://geo.test/reverse", api_key="secret")

        assert "api_key=secret" in backend._build_url(38.7, -9.1)

    def test_a_missing_api_key_still_produces_a_url(self):
        """The container refuses this combination; the backend must not crash on it."""
        backend = HttpGeocoding("geocode", "https://geo.test/reverse")

        assert "api_key=" in backend._build_url(38.7, -9.1)


class TestResponseParsing:
    @pytest.fixture
    def backend(self):
        return HttpGeocoding("nominatim", "https://geo.test/reverse")

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
        backend = HttpGeocoding("photon", "https://geo.test/reverse")
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
        backend = HttpGeocoding("photon", "https://geo.test/reverse")
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
        return HttpGeocoding("nominatim", "https://geo.test/reverse", user_agent="my-app/1.0")

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


class TestFailuresNeverPropagate:
    """Enrichment must not fail the import or backfill that triggered it."""

    @pytest.fixture
    def backend(self):
        return HttpGeocoding("nominatim", "https://geo.test/reverse")

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
        backend = HttpGeocoding("geocode", "https://geo.test/reverse", api_key="super-secret-key")
        responses.add(responses.GET, "https://geo.test/reverse", status=status, json={})

        with caplog.at_level("ERROR"):
            assert backend.reverse(38.7, -9.1) is None

        assert "super-secret-key" not in caplog.text
        assert "api_key" not in caplog.text

    @responses.activate
    def test_the_status_code_survives_the_redaction(self, caplog):
        """An operator still has to be able to tell a 401 from a 429."""
        backend = HttpGeocoding("nominatim", "https://geo.test/reverse")
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
        return HttpGeocoding("nominatim", "https://geo.test/reverse")

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
            assert HttpGeocoding("nominatim", "https://geo.test/reverse").reverse(38.7, -9.1) is None

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
                assert HttpGeocoding("nominatim", "http://geo.test/reverse").reverse(38.7, -9.1) is None
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


class TestThrottling:
    def test_no_rate_limit_means_no_wait(self, monkeypatch):
        slept = []
        monkeypatch.setattr("jasil.backends.geocoding_http.time.sleep", slept.append)
        backend = HttpGeocoding("nominatim", "https://geo.test/reverse", min_interval_seconds=0)

        backend._throttle()
        backend._throttle()

        assert slept == []

    def test_a_second_call_waits_out_the_interval(self, monkeypatch):
        """Nominatim's policy is one request per second; exceeding it gets you banned."""
        slept = []
        clock = iter([100.0, 100.0, 100.2, 100.2])
        monkeypatch.setattr("jasil.backends.geocoding_http.time.monotonic", lambda: next(clock))
        monkeypatch.setattr("jasil.backends.geocoding_http.time.sleep", slept.append)
        backend = HttpGeocoding("nominatim", "https://geo.test/reverse", min_interval_seconds=1.0)

        backend._throttle()
        backend._throttle()

        assert slept == [pytest.approx(0.8)]

    def test_a_call_after_the_interval_does_not_wait(self, monkeypatch):
        slept = []
        clock = iter([100.0, 100.0, 105.0, 105.0])
        monkeypatch.setattr("jasil.backends.geocoding_http.time.monotonic", lambda: next(clock))
        monkeypatch.setattr("jasil.backends.geocoding_http.time.sleep", slept.append)
        backend = HttpGeocoding("nominatim", "https://geo.test/reverse", min_interval_seconds=1.0)

        backend._throttle()
        backend._throttle()

        assert slept == []

    def test_two_backends_do_not_throttle_each_other(self, monkeypatch):
        """Throttle state is per-instance, so one capability cannot stall another."""
        slept = []
        monkeypatch.setattr("jasil.backends.geocoding_http.time.sleep", slept.append)
        first = HttpGeocoding("nominatim", "https://a.test/reverse", min_interval_seconds=1.0)
        second = HttpGeocoding("photon", "https://b.test/reverse", min_interval_seconds=1.0)

        first._throttle()
        second._throttle()

        assert slept == []
