"""Async HTTP ``GeocodingProvider`` backends (Nominatim / Photon / geocode.maps.co).

The asynchronous twin of :mod:`jasil.backends.geocoding_http`, over
``httpx.AsyncClient``. Everything that is not transport — building the request
URL, parsing the answer, redacting a failure, and validating the
operator-configured host — is imported from the synchronous module rather than
restated, so the two backends cannot resolve a coordinate differently and, more
importantly, cannot disagree about what egress is permitted.

Security (OWASP A10 — SSRF): the guarantees here are the same three, enforced the
same way and re-tested against this client rather than assumed to carry over from
the sync one.

* The upstream host is operator-configured, so it is validated through
  :func:`jasil._core.network.host_rejection_reason` — the same address denylist
  and the same allowlist escape hatch — before any request is made. That happens
  in :func:`~jasil.backends.geocoding_http.build_reverse_endpoint`, shared with
  the sync backend.
* Redirects are refused on every request (``follow_redirects=False``, which is
  also httpx's default — it is passed explicitly because a security property
  should not rest on a library default staying put), so a permitted host cannot
  3xx-pivot onto an internal target.
* The response body is read under a size cap, streamed so an oversized body is
  abandoned partway rather than buffered in full first.

Failures are logged without their message. ``httpx`` puts the full request URL in
an ``HTTPStatusError`` message, and the geocode.maps.co URL carries ``api_key`` in
its query string, so the message would leak the key into the host's logs.

That is not sufficient on its own, and this is the one place the async backend
needs a defence its synchronous twin does not. ``httpx`` logs a line of its own
for *every* request, successful ones included, at **INFO** — and that line
contains the fully-expanded URL, query string and all. ``requests`` logs the
equivalent at DEBUG, a level production hosts rarely enable, so the sync backend
never had to think about it; INFO they usually do. Left alone, simply switching a
deployment to the async backend would start writing the geocoding API key to the
host's log on every lookup.

:func:`install_api_key_log_redaction` therefore attaches a redacting filter to the
``httpx`` logger the first time a client is built. It is deliberately a *filter*
and not a level change: silencing the logger would take away request diagnostics
the operator may be relying on, and this library has no business making that
decision for them. Redacting the one query parameter that is a credential takes
nothing away.

The throttle is an ``asyncio.Lock`` around an ``await asyncio.sleep``, not a
``threading.Lock`` around ``time.sleep``. Both matter: sleeping without yielding
would block the loop, and holding the lock across the sleep is what makes the
rate limit hold when several coroutines geocode concurrently — releasing it
first would let them all sleep in parallel and then fire together, which is not a
rate limit at all.
"""

import asyncio
import json
import logging
import re
from typing import TYPE_CHECKING

from jasil.backends.geocoding_http import (
    MAX_RESPONSE_BYTES,
    READ_CHUNK_BYTES,
    TIMEOUT_SECONDS,
    build_request_url,
    failure_detail,
    parse_place,
)
from jasil.providers import GeocodedPlace

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger(__name__)

#: Matches an ``api_key`` query parameter and captures its value for replacement.
_API_KEY_PATTERN = re.compile(r"(api_key=)[^&\s]+")

#: Marker attribute so the redaction filter is only ever attached once, however
#: many geocoding backends a process builds.
_REDACTION_INSTALLED_FLAG = "_jasil_api_key_redaction"


class _ApiKeyRedactingFilter(logging.Filter):
    """Strips ``api_key`` values out of ``httpx``'s per-request log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact any API key in the record, and always keep the record.

        The message is rendered and replaced rather than the arguments being
        rewritten in place, because httpx passes the URL as a positional argument
        whose position is an implementation detail.

        Args:
            record: The record about to be emitted.

        Returns:
            Always True — this filter redacts, it does not drop.
        """
        message = record.getMessage()
        redacted = _API_KEY_PATTERN.sub(r"\1[REDACTED]", message)
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


def install_api_key_log_redaction() -> None:
    """Attach the ``api_key`` redaction filter to the ``httpx`` logger, once.

    Called when an :class:`AsyncHttpGeocoding` client is first constructed. It is
    idempotent: a marker attribute on the logger means a process building several
    geocoding backends still ends up with exactly one filter.

    Returns:
        None.
    """
    httpx_logger = logging.getLogger("httpx")
    if getattr(httpx_logger, _REDACTION_INSTALLED_FLAG, False):
        return
    httpx_logger.addFilter(_ApiKeyRedactingFilter())
    setattr(httpx_logger, _REDACTION_INSTALLED_FLAG, True)


class AsyncNullGeocoding:
    """``AsyncGeocodingProvider`` that resolves nothing.

    Selected when reverse geocoding is not configured, is configured with an
    unsupported provider, or is configured with a host that fails egress
    validation. Making "disabled" an explicit backend means the domain has one
    code path: it always has a provider and always just awaits it.
    """

    async def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None:
        """Return ``None`` — no geocoding is configured.

        Args:
            latitude: WGS-84 latitude in decimal degrees.
            longitude: WGS-84 longitude in decimal degrees.

        Returns:
            Always ``None``.
        """
        return None


class AsyncHttpGeocoding:
    """``AsyncGeocodingProvider`` backed by a reverse-geocoding HTTP service.

    Args:
        service: Which upstream to call — ``"nominatim"``, ``"photon"`` or
            ``"geocode"``.
        base_url: Fully-qualified reverse endpoint, e.g.
            ``"https://nominatim.openstreetmap.org/reverse"``. Built by the
            composition root, which is also where the host was validated.
        api_key: API key, for the services that require one (geocode.maps.co).
        min_interval_seconds: Minimum wall-clock gap between requests. ``0``
            disables throttling.
        user_agent: ``User-Agent`` sent upstream; Nominatim's usage policy
            requires an identifying value.
    """

    def __init__(
        self,
        service: str,
        base_url: str,
        *,
        api_key: str | None = None,
        min_interval_seconds: float = 0.0,
        user_agent: str = "jasil (ReverseGeocoding)",
    ) -> None:
        self._service = service
        self._base_url = base_url
        self._api_key = api_key
        self._min_interval = min_interval_seconds
        self._user_agent = user_agent
        # Throttle state is per-instance, guarded by its own lock, so two
        # configured backends cannot interfere and a test can drive the rate
        # limit deterministically.
        self._throttle_lock = asyncio.Lock()
        self._last_call = 0.0
        # The client is created on first use rather than in __init__: building an
        # httpx.AsyncClient binds it to the running loop, and a provider is
        # constructed by the composition root, which need not be on the loop that
        # will eventually use it.
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> "httpx.AsyncClient":
        """Return this backend's shared ``AsyncClient``, creating it on first use.

        A single client is reused so connections are pooled across requests, which
        is what keeps a backfill of thousands of coordinates from opening a TLS
        handshake per point.

        Returns:
            The backend's ``httpx.AsyncClient``.
        """
        if self._client is None:
            # Imported lazily: ``httpx`` is the optional ``geocoding-async``
            # extra, and the composition root imports this module unconditionally
            # for ``AsyncNullGeocoding``.
            import httpx

            # Must happen before the first request: httpx logs the expanded URL,
            # api_key included, at INFO for every request it makes.
            install_api_key_log_redaction()
            self._client = httpx.AsyncClient(
                timeout=TIMEOUT_SECONDS,
                # A permitted host must not 3xx-pivot the request onto an
                # internal target (SSRF defense in depth, OWASP A10). This is
                # httpx's default; it is stated anyway because the guarantee must
                # not depend on that default never changing.
                follow_redirects=False,
            )
        return self._client

    async def aclose(self) -> None:
        """Close the pooled HTTP client, if one was ever created.

        Returns:
            None.
        """
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _throttle(self) -> None:
        """Await as needed to respect the configured request rate.

        Returns:
            None.
        """
        if self._min_interval <= 0:
            return
        # The sleep happens *inside* the lock on purpose: concurrent callers must
        # queue behind each other, not all wait out the same interval at once and
        # then burst.
        async with self._throttle_lock:
            wait = self._min_interval - (asyncio.get_running_loop().time() - self._last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = asyncio.get_running_loop().time()

    @staticmethod
    async def _read_capped(response: "httpx.Response") -> bytes:
        """Read a response body, refusing one past :data:`MAX_RESPONSE_BYTES`.

        Streamed rather than taken from ``response.content`` so an oversized body
        is abandoned partway instead of being buffered in full first.

        Args:
            response: The streamed response to drain.

        Returns:
            The body bytes.

        Raises:
            ValueError: When the body exceeds the cap.
        """
        body = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=READ_CHUNK_BYTES):
            body.extend(chunk)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError(f"response body exceeded the {MAX_RESPONSE_BYTES}-byte limit")
        return bytes(body)

    async def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None:
        """Reverse-geocode a coordinate, returning ``None`` on any failure.

        Args:
            latitude: WGS-84 latitude in decimal degrees.
            longitude: WGS-84 longitude in decimal degrees.

        Returns:
            The resolved place, or ``None`` when nothing resolved or the request
            failed. Never raises — geocoding is best-effort enrichment and must
            not fail the import or backfill that triggered it.
        """
        # The coordinates are deliberately not logged: they are a location fix
        # belonging to the host's user, and this is a library — it does not get to
        # decide that someone's whereabouts are acceptable to write to their log.
        logger.debug("Reverse-geocoding via %s", self._service)
        await self._throttle()
        try:
            client = self._get_client()
            url = build_request_url(self._service, self._base_url, self._api_key, latitude, longitude)
            request = client.build_request("GET", url, headers={"User-Agent": self._user_agent})
            response = await client.send(request, stream=True)
            try:
                response.raise_for_status()
                body = await self._read_capped(response)
            finally:
                await response.aclose()
            return parse_place(self._service, json.loads(body))
        except Exception as error:
            logger.error("Reverse-geocoding via %s failed - %s", self._service, failure_detail(error))
            return None
