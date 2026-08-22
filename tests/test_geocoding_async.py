"""Async HTTP reverse-geocoding egress hardening.

The synchronous backend has the same public contract, but this file pins the
``httpx.AsyncClient`` transport decisions independently: redirects, streaming,
timeouts, and httpx's own request logging do not behave like ``requests``.
"""

import asyncio
import logging
import socket
from collections.abc import AsyncIterator, Callable, Iterator

import httpx
import pytest

import jasil._core.network as network
from jasil.backends.geocoding_http import MAX_RESPONSE_BYTES, build_reverse_endpoint
from jasil.backends.geocoding_http_async import (
    _REDACTION_INSTALLED_FLAG,
    AsyncHttpGeocoding,
    AsyncNullGeocoding,
    _ApiKeyRedactingFilter,
    install_api_key_log_redaction,
)
from jasil.container_async import _build_geocoding
from jasil.providers import GeocodedPlace
from jasil.providers_async import AsyncGeocodingProvider
from jasil.settings import GeocodingSettings, JasilSettings

PUBLIC_IP = "93.184.216.34"
PRIVATE_IP = "10.0.0.5"
LOOPBACK_IP = "127.0.0.1"
LINK_LOCAL_IP = "169.254.169.254"


@pytest.fixture
def resolves_to(monkeypatch: pytest.MonkeyPatch):
    """Point every hostname at chosen addresses, so no test does real DNS."""

    def _install(*addresses: str) -> None:
        def _fake_getaddrinfo(host: str, port: int | None, *args: object, **kwargs: object) -> list[tuple]:
            return [
                (
                    socket.AF_INET6 if ":" in address else socket.AF_INET,
                    socket.SOCK_STREAM,
                    6,
                    "",
                    (address, 0),
                )
                for address in addresses
            ]

        monkeypatch.setattr(network.socket, "getaddrinfo", _fake_getaddrinfo)

    return _install


@pytest.fixture
def clean_httpx_redaction() -> Iterator[logging.Logger]:
    """Let a test assert redaction installation without depending on global order."""

    httpx_logger = logging.getLogger("httpx")
    original_filters = list(httpx_logger.filters)
    sentinel = object()
    original_flag = getattr(httpx_logger, _REDACTION_INSTALLED_FLAG, sentinel)
    httpx_logger.filters[:] = [f for f in original_filters if not isinstance(f, _ApiKeyRedactingFilter)]
    if original_flag is not sentinel:
        delattr(httpx_logger, _REDACTION_INSTALLED_FLAG)
    try:
        yield httpx_logger
    finally:
        httpx_logger.filters[:] = original_filters
        if original_flag is sentinel:
            if hasattr(httpx_logger, _REDACTION_INSTALLED_FLAG):
                delattr(httpx_logger, _REDACTION_INSTALLED_FLAG)
        else:
            setattr(httpx_logger, _REDACTION_INSTALLED_FLAG, original_flag)


def _backend_with_transport(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    service: str = "nominatim",
    api_key: str | None = None,
) -> AsyncHttpGeocoding:
    backend = AsyncHttpGeocoding(service, "https://geo.test/reverse", api_key=api_key)
    backend._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
    return backend


class _OversizedStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        remaining = MAX_RESPONSE_BYTES + 1
        while remaining:
            chunk_size = min(8192, remaining)
            remaining -= chunk_size
            yield b"x" * chunk_size


class TestAsyncNullGeocoding:
    def test_it_satisfies_the_protocol(self):
        assert isinstance(AsyncNullGeocoding(), AsyncGeocodingProvider)

    async def test_it_resolves_nothing_without_io(self, monkeypatch: pytest.MonkeyPatch):
        def _fail(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("the null backend must not build an HTTP client")

        monkeypatch.setattr("jasil.backends.geocoding_http_async.install_api_key_log_redaction", _fail)

        assert await AsyncNullGeocoding().reverse(38.7, -9.1) is None


class TestAsyncEgressValidation:
    @pytest.mark.parametrize(
        "address",
        [
            pytest.param(PRIVATE_IP, id="private"),
            pytest.param(LOOPBACK_IP, id="loopback"),
            pytest.param(LINK_LOCAL_IP, id="link-local-metadata"),
        ],
    )
    def test_a_configured_host_resolving_to_a_non_public_address_is_refused(self, resolves_to, address, caplog):
        resolves_to(address)

        with caplog.at_level("WARNING"):
            assert build_reverse_endpoint("internal.example.com", use_https=True) is None

        assert "non-public address" in caplog.text

    def test_the_address_denylist_is_enforced_when_building_the_async_backend(self, resolves_to):
        resolves_to(PRIVATE_IP)
        settings = JasilSettings(
            geocoding=GeocodingSettings(provider="nominatim", nominatim_host="internal.example.com")
        )

        assert isinstance(_build_geocoding(settings), AsyncNullGeocoding)

    async def test_a_redirect_is_refused_not_followed(self):
        requested_hosts: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requested_hosts.append(request.url.host or "")
            if request.url.host == "geo.test":
                return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/meta-data/"})
            raise AssertionError("redirect target must not be fetched")

        backend = _backend_with_transport(handler)
        try:
            assert await backend.reverse(38.7, -9.1) is None
        finally:
            await backend.aclose()

        assert requested_hosts == ["geo.test"]


class TestAsyncRequestBehaviour:
    async def test_a_nominatim_answer_is_mapped(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["User-Agent"] == "jasil (ReverseGeocoding)"
            return httpx.Response(200, json={"address": {"city": "Lisboa", "town": "Alvalade", "country": "Portugal"}})

        backend = _backend_with_transport(handler)
        try:
            assert await backend.reverse(38.7, -9.1) == GeocodedPlace(
                city="Lisboa", town="Alvalade", country="Portugal"
            )
        finally:
            await backend.aclose()

    async def test_an_oversized_streamed_body_resolves_to_none(self, caplog):
        backend = _backend_with_transport(lambda _request: httpx.Response(200, stream=_OversizedStream()))

        with caplog.at_level("ERROR"):
            try:
                assert await backend.reverse(38.7, -9.1) is None
            finally:
                await backend.aclose()

        assert "ValueError" in caplog.text

    async def test_a_timeout_resolves_to_none(self, caplog):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

        backend = _backend_with_transport(handler)

        with caplog.at_level("ERROR"):
            try:
                assert await backend.reverse(38.7, -9.1) is None
            finally:
                await backend.aclose()

        assert "TimeoutException" in caplog.text

    @pytest.mark.parametrize(
        ("service", "response"),
        [
            pytest.param("nominatim", httpx.Response(200, content=b"<html>not json</html>"), id="malformed-json"),
            pytest.param("nominatim", httpx.Response(200, json={"address": {}}), id="empty-nominatim"),
            pytest.param("photon", httpx.Response(200, json={"features": []}), id="empty-photon"),
        ],
    )
    async def test_malformed_or_empty_payloads_resolve_to_none(self, service: str, response: httpx.Response):
        backend = _backend_with_transport(lambda _request: response, service=service)
        try:
            assert await backend.reverse(38.7, -9.1) is None
        finally:
            await backend.aclose()


class TestAsyncApiKeyLogRedaction:
    def test_httpx_info_request_logs_are_redacted_and_the_filter_is_idempotent(
        self, clean_httpx_redaction: logging.Logger, caplog: pytest.LogCaptureFixture
    ):
        install_api_key_log_redaction()
        install_api_key_log_redaction()

        assert sum(isinstance(filter_, _ApiKeyRedactingFilter) for filter_ in clean_httpx_redaction.filters) == 1

        with caplog.at_level(logging.INFO, logger="httpx"):
            clean_httpx_redaction.info(
                'HTTP Request: GET %s "HTTP/1.1 200 OK"',
                "https://geocode.maps.co/reverse?lat=38.7&lon=-9.1&api_key=super-secret-key",
            )

        assert "api_key=[REDACTED]" in caplog.text
        assert "super-secret-key" not in caplog.text


class TestAsyncThrottling:
    async def test_concurrent_requests_wait_on_the_same_lock(self, monkeypatch: pytest.MonkeyPatch):
        original_sleep = asyncio.sleep
        active_sleeps = 0
        max_active_sleeps = 0
        sleep_durations: list[float] = []
        request_count = 0

        async def fake_sleep(duration: float) -> None:
            nonlocal active_sleeps, max_active_sleeps
            sleep_durations.append(duration)
            active_sleeps += 1
            max_active_sleeps = max(max_active_sleeps, active_sleeps)
            await original_sleep(0)
            active_sleeps -= 1

        def handler(_request: httpx.Request) -> httpx.Response:
            nonlocal request_count
            request_count += 1
            return httpx.Response(200, json={"address": {"country": "Portugal"}})

        monkeypatch.setattr("jasil.backends.geocoding_http_async.asyncio.sleep", fake_sleep)
        backend = _backend_with_transport(handler)
        backend._min_interval = 1.0
        backend._last_call = asyncio.get_running_loop().time()

        try:
            await asyncio.gather(backend.reverse(38.7, -9.1), backend.reverse(38.8, -9.2))
        finally:
            await backend.aclose()

        assert request_count == 2
        assert len(sleep_durations) == 2
        assert max_active_sleeps == 1


class TestAsyncClientLifecycle:
    async def test_close_is_safe_before_and_after_client_creation(self):
        backend = AsyncHttpGeocoding("nominatim", "https://geo.test/reverse")

        await backend.aclose()
        client = backend._get_client()
        assert not client.is_closed

        await backend.aclose()
        assert client.is_closed
        assert backend._client is None

        await backend.aclose()
