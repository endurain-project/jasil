"""The HTTP reverse-geocoding backend and the SSRF gate in front of it.

Geocoding is the only capability that dials an operator-supplied host, so it is
the only one carrying an SSRF guard — and the guard is what most of this module
is about. `build_reverse_endpoint` is the gate: it validates the host *once*, at
composition time, and a host it rejects produces no endpoint at all, so the
backend is never built.

The other property worth pinning is that `reverse` never raises. It is
best-effort enrichment hanging off an import or a backfill, and an upstream that
is down, slow, rate-limiting, or returning nonsense must degrade to "no location"
rather than fail the work that asked for it.

No DNS and no HTTP happen here: `getaddrinfo` is monkeypatched and `responses`
intercepts the requests.
"""

import socket

import pytest
import responses

import jasil._core.network as network
from jasil.backends.geocoding_http import (
    _MAX_RESPONSE_BYTES,
    HttpGeocoding,
    NullGeocoding,
    _failure_detail,
    build_reverse_endpoint,
)
from jasil.providers import GeocodedPlace, GeocodingProvider

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

        assert build_reverse_endpoint("nominatim.example.com", use_https=True) == (
            "https://nominatim.example.com/reverse"
        )

    def test_http_is_available_for_a_self_hosted_instance(self, resolves_to):
        resolves_to(PUBLIC_IP)

        assert build_reverse_endpoint("geo.example.com", use_https=False) == "http://geo.example.com/reverse"

    def test_a_port_is_preserved(self, resolves_to):
        resolves_to(PUBLIC_IP)

        assert build_reverse_endpoint("geo.example.com:8080", use_https=False) == (
            "http://geo.example.com:8080/reverse"
        )


class TestEgressValidation:
    """OWASP A10. The host comes from configuration, so it is checked before use."""

    def test_a_private_address_is_refused(self, resolves_to, caplog):
        resolves_to(PRIVATE_IP)

        with caplog.at_level("WARNING"):
            assert build_reverse_endpoint("internal.example.com", use_https=True) is None

        assert "non-public address" in caplog.text

    def test_the_refusal_says_how_to_allow_it(self, resolves_to, caplog):
        """An operator with a legitimate private instance needs the next step."""
        resolves_to(PRIVATE_IP)

        with caplog.at_level("WARNING"):
            build_reverse_endpoint("internal.example.com", use_https=True)

        assert "allow-listed" in caplog.text

    def test_an_unresolvable_host_is_refused(self, monkeypatch, caplog):
        def _fail(*args, **kwargs):
            raise socket.gaierror("nope")

        monkeypatch.setattr(network.socket, "getaddrinfo", _fail)

        with caplog.at_level("WARNING"):
            assert build_reverse_endpoint("nowhere.invalid", use_https=True) is None

        assert "could not be resolved" in caplog.text

    @pytest.mark.parametrize(
        "not_an_authority",
        ["http://geo.example.com", "geo.example.com/reverse", "geo.example.com:notaport", "user@geo.example.com"],
    )
    def test_a_value_that_is_not_a_bare_authority_is_refused(self, not_an_authority, resolves_to):
        """Otherwise it is interpolated into the URL and redirects the request."""
        resolves_to(PUBLIC_IP)

        assert build_reverse_endpoint(not_an_authority, use_https=True) is None

    def test_an_allow_listed_cidr_permits_a_private_instance(self, resolves_to):
        resolves_to(PRIVATE_IP)

        endpoint = build_reverse_endpoint("internal.example.com", use_https=False, allowed_hosts=["10.0.0.0/8"])

        assert endpoint == "http://internal.example.com/reverse"

    def test_an_allow_listed_hostname_permits_it_too(self, resolves_to):
        resolves_to(PRIVATE_IP)

        endpoint = build_reverse_endpoint(
            "internal.example.com", use_https=False, allowed_hosts=["internal.example.com"]
        )

        assert endpoint == "http://internal.example.com/reverse"

    def test_an_unrelated_allowlist_entry_does_not_help(self, resolves_to):
        resolves_to(PRIVATE_IP)

        assert build_reverse_endpoint("internal.example.com", use_https=False, allowed_hosts=["10.1.0.0/16"]) is None

    def test_every_allowlist_hit_is_logged(self, resolves_to, caplog):
        """Operators must be able to review what the exception is being used for."""
        resolves_to(PRIVATE_IP)

        with caplog.at_level("INFO"):
            build_reverse_endpoint("internal.example.com", use_https=False, allowed_hosts=["10.0.0.0/8"])

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
            json={"address": {"city": "Lisboa", "town": "Alvalade", "country": "Portugal"}},
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(city="Lisboa", town="Alvalade", country="Portugal")

    @responses.activate
    def test_a_photon_answer_is_mapped(self):
        """Photon names things differently: 'district' is the city, 'city' the town."""
        backend = HttpGeocoding("photon", "https://geo.test/reverse")
        responses.add(
            responses.GET,
            "https://geo.test/reverse",
            json={"features": [{"properties": {"district": "Lisboa", "city": "Alvalade", "country": "Portugal"}}]},
        )

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(city="Lisboa", town="Alvalade", country="Portugal")

    @responses.activate
    def test_an_empty_photon_feature_list_resolves_to_nothing(self):
        backend = HttpGeocoding("photon", "https://geo.test/reverse")
        responses.add(responses.GET, "https://geo.test/reverse", json={"features": []})

        assert backend.reverse(38.7, -9.1) is None

    @responses.activate
    def test_a_partial_answer_is_kept(self, backend):
        """A country alone is still worth recording."""
        responses.add(responses.GET, "https://geo.test/reverse", json={"address": {"country": "Portugal"}})

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(city=None, town=None, country="Portugal")

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

        assert backend.reverse(38.7, -9.1) == GeocodedPlace(city=None, town=None, country="Portugal")


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
