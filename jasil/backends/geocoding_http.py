"""HTTP ``GeocodingProvider`` backends (Nominatim / Photon / geocode.maps.co).

One class covers all three services because they differ only in how the request
URL is built and how the answer is shaped; everything that actually matters —
egress validation, redirect refusal, rate limiting, and never raising — is
identical and is written once here.

Security (OWASP A10 — SSRF): the upstream host is operator-configured, so it is
validated through :func:`jasil._core.network.host_rejection_reason` — the same
address denylist and allowlist escape hatch — before the first request, and
redirects are refused on every request so a permitted host cannot 3xx-pivot onto
an internal target. The response body is read under a size cap, the whole request
under a deadline a watchdog enforces by shutting its sockets down, and failures are
logged without their message, which would carry the request URL and with it the
API key.
"""

import contextlib
import email.utils
import functools
import json
import logging
import math
import socket
import threading
import time
from collections.abc import Sequence
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode, urlsplit

import jasil._core.network as network
import jasil._core.state_keys as state_keys
from jasil.providers import (
    ClockProvider,
    GeocodedPlace,
    GeocodingOutcome,
    StateBackendUnavailableError,
    StateProvider,
)

if TYPE_CHECKING:
    import requests

logger = logging.getLogger(__name__)

# Egress timeout for a single reverse-geocode request (seconds).
_TIMEOUT_SECONDS = 10

# Cap on a reverse-geocode response body. A resolved place is a few kilobytes;
# the limit is what stops an upstream that is hostile, compromised, or simply
# broken from exhausting memory, because reading a response buffers whatever it
# is sent and the timeout above bounds each read rather than the whole transfer.
_MAX_RESPONSE_BYTES = 1024 * 1024

# Read granularity while enforcing that cap.
_READ_CHUNK_BYTES = 8192

# Overall bound on one request, headers and body. The timeout above bounds each
# read, and restarts with every byte, so an upstream dripping one byte at a time
# would otherwise hold the caller for as long as the size cap allows. A watchdog
# shuts the request's sockets down when it passes, which ends a blocked read.
_DEADLINE_SECONDS = 30

# How long ``reverse`` waits for its turn under the rate limit before giving up on the lookup.
_REVERSE_MAX_WAIT_SECONDS = 60

# When to retry a lookup that failed in a way that may pass, when the upstream did not say.
_RETRY_BACKOFF_SECONDS = 60

# The longest back-off taken from an upstream's ``Retry-After``. The upstream is untrusted, and a 429 or 503 holds
# back every process sharing the state, so one answer must not stop geocoding for longer than this.
_MAX_RETRY_AFTER_SECONDS = 3600

# The answers that ask every caller to back off: the upstream is over its limit, or not serving.
_BACK_OFF_STATUSES = frozenset({429, 503})


class _Deadline:
    """The sockets one request opened, shut down together when its deadline passes.

    Each socket is watched through a duplicate made before any TLS wrap: the wrap detaches the original from
    its descriptor, so shutting that down would reach nothing, while the duplicate still names the connection.
    A socket opened after the deadline (a slow connect hands it over late) is shut down as soon as it is
    watched, so no read on it can start. :meth:`close` releases the duplicates once the request is over.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sockets: list[socket.socket] = []
        self._expired = False

    def watch(self, sock: socket.socket) -> None:
        """Track a duplicate of ``sock``, shutting it down at once if the deadline has already passed."""
        watched = sock.dup()
        with self._lock:
            self._sockets.append(watched)
            expired = self._expired
        if expired:
            _shut_down(watched)

    def expire(self) -> None:
        """Mark the deadline passed and shut down every socket watched so far."""
        with self._lock:
            self._expired = True
            sockets = list(self._sockets)
        for sock in sockets:
            _shut_down(sock)

    def close(self) -> None:
        """Release the duplicates; the connections themselves are closed by their owners."""
        with self._lock:
            sockets, self._sockets = self._sockets, []
        for sock in sockets:
            sock.close()


def _shut_down(sock: socket.socket) -> None:
    with contextlib.suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)


_current_deadline: ContextVar[_Deadline | None] = ContextVar("jasil_geocoding_deadline", default=None)


def _watch_current(sock: socket.socket) -> socket.socket:
    """Hand ``sock`` to the deadline of the request being made in this context, if any."""
    deadline = _current_deadline.get()
    if deadline is not None:
        deadline.watch(sock)
    return sock


@functools.cache
def _watched_adapter() -> Any:
    """Build, once, the ``requests`` adapter whose connections hand their socket to the current deadline.

    Built on first use because ``requests`` is the optional ``geocoding`` extra.
    """
    from requests.adapters import HTTPAdapter
    from urllib3.connection import HTTPConnection, HTTPSConnection
    from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool

    class _WatchedHTTPConnection(HTTPConnection):
        def _new_conn(self) -> socket.socket:
            return _watch_current(super()._new_conn())

    class _WatchedHTTPSConnection(HTTPSConnection):
        def _new_conn(self) -> socket.socket:
            return _watch_current(super()._new_conn())

    # ty rejects any ``ConnectionCls`` override, urllib3's own ``HTTPConnection`` included, against urllib3's
    # ``BaseHTTPConnection`` protocol (``default_socket_options`` is declared read-only there); mypy accepts it.
    class _WatchedHTTPPool(HTTPConnectionPool):
        ConnectionCls = _WatchedHTTPConnection  # ty: ignore[invalid-assignment]

    class _WatchedHTTPSPool(HTTPSConnectionPool):
        ConnectionCls = _WatchedHTTPSConnection  # ty: ignore[invalid-assignment]

    pool_classes = {"http": _WatchedHTTPPool, "https": _WatchedHTTPSPool}

    class _WatchedAdapter(HTTPAdapter):
        def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
            super().init_poolmanager(*args, **kwargs)
            self.poolmanager.pool_classes_by_scheme = pool_classes

        def proxy_manager_for(self, proxy: str, **proxy_kwargs: Any) -> Any:
            # A proxy gets a manager of its own, so it needs the watched pools too.
            manager = super().proxy_manager_for(proxy, **proxy_kwargs)
            manager.pool_classes_by_scheme = pool_classes
            return manager

    return _WatchedAdapter


def _failure_detail(error: Exception) -> str:
    """Describe a request failure without echoing its message.

    ``requests`` puts the full request URL in an ``HTTPError`` message, and the
    geocode.maps.co URL carries ``api_key`` in its query string — so neither the
    message nor a traceback may reach the log. The type and status code are what
    an operator actually needs to tell a 401 from a 429 from a timeout.

    Args:
        error: The exception raised while performing the request.

    Returns:
        A redacted, single-line description.
    """
    status = getattr(getattr(error, "response", None), "status_code", None)
    return f"{type(error).__name__} (HTTP {status})" if status is not None else type(error).__name__


# Longest name taken from the upstream. A real place name is a few dozen characters, and the full line a service
# composes ("display_name") a few hundred; anything longer is not a place.
_MAX_NAME_CHARACTERS = 256
_MAX_DISPLAY_NAME_CHARACTERS = 1024


def _first(data: dict, *names: str, limit: int = _MAX_NAME_CHARACTERS) -> str | None:
    """Return the first usable string value among ``names`` in ``data``.

    The upstream is untrusted, so a value is usable only when it is non-empty, at most ``limit`` characters
    and free of control characters; one that is not is treated as absent and the next name is tried.
    """
    for name in names:
        value = data.get(name)
        if isinstance(value, str) and value and len(value) <= limit and value.isprintable():
            return value
    return None


def _country_code(value: object) -> str | None:
    """Normalise an ISO 3166-1 alpha-2 code to upper case, or None when it is not one."""
    if isinstance(value, str) and len(value) == 2 and value.isascii() and value.isalpha():
        return value.upper()
    return None


class NullGeocoding:
    """``GeocodingProvider`` that resolves nothing.

    Selected when reverse geocoding is not configured, is configured with an
    unsupported provider, or is configured with a host that fails egress
    validation. Making "disabled" an explicit backend means the domain has one
    code path: it always has a provider and always just calls it.
    """

    def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None:
        """Return ``None`` — no geocoding is configured."""
        return None

    def lookup(
        self, latitude: float, longitude: float, *, max_wait_seconds: float, reservation_key: str | None = None
    ) -> GeocodingOutcome:
        """Return a final answer of nothing — no geocoding is configured.

        Raises:
            ValueError: When ``max_wait_seconds`` is negative or not finite, or ``reservation_key`` is empty or too
                long.
        """
        state_keys.check_max_wait(max_wait_seconds)
        if reservation_key is not None:
            state_keys.check_booking(reservation_key, name="reservation_key")
        return GeocodingOutcome()


class HttpGeocoding:
    """``GeocodingProvider`` backed by a reverse-geocoding HTTP service.

    The rate limit is a gate in the platform's state, keyed by service and upstream host, so every backend on the
    same state shares one budget for that upstream: every process on one Redis, and a backend and its rebuild
    after a settings reload. A 429 or 503 holds back every backend on that gate until the upstream's
    ``Retry-After``.

    Args:
        service: Which upstream to call — ``"nominatim"``, ``"photon"`` or
            ``"geocode"``.
        base_url: Fully-qualified reverse endpoint, e.g.
            ``"https://nominatim.openstreetmap.org/reverse"``. Built by the
            composition root, which is also where the host was validated.
        state: Holds the rate gate and the upstream's back-off.
        clock: Sets ``retry_at`` and reads the back-off against it.
        api_key: API key, for the services that require one (geocode.maps.co).
        min_interval_seconds: Minimum gap between two requests to this upstream, at most a day. ``0`` disables
            throttling.
        user_agent: ``User-Agent`` sent upstream; Nominatim's usage policy
            requires an identifying value.
    """

    def __init__(
        self,
        service: str,
        base_url: str,
        *,
        state: StateProvider,
        clock: ClockProvider,
        api_key: str | None = None,
        min_interval_seconds: float = 0.0,
        user_agent: str = "jasil (ReverseGeocoding)",
    ) -> None:
        self._service = service
        self._base_url = base_url
        self._state = state
        self._clock = clock
        self._api_key = api_key
        self._min_interval = min_interval_seconds
        self._user_agent = user_agent
        # Lower-cased: host names are case-insensitive, so two spellings of one upstream share one budget.
        upstream = f"{service}:{urlsplit(base_url).netloc.lower()}"
        self._gate_key = f"jasil:geocoding:gate:{upstream}"
        self._back_off_key = f"jasil:geocoding:back-off:{upstream}"

    def _build_url(self, latitude: float, longitude: float) -> str:
        """Build the reverse-geocode URL for the configured service."""
        params: dict[str, str | float]
        if self._service == "photon":
            params = {"lat": latitude, "lon": longitude}
        elif self._service == "geocode":
            params = {"lat": latitude, "lon": longitude, "api_key": self._api_key or ""}
        else:  # nominatim
            params = {"format": "jsonv2", "lat": latitude, "lon": longitude}
        return f"{self._base_url}?{urlencode(params)}"

    def _parse(self, payload: dict) -> GeocodedPlace | None:
        """Map a service response onto :class:`GeocodedPlace`, or None when empty.

        Each service names its levels differently; this is the one place that
        knows how, so a caller sees the same meaning whichever answered.
        """
        if self._service == "photon":
            features = payload.get("features", [])
            data = features[0].get("properties", {}) if features else {}
            place = GeocodedPlace(
                locality=_first(data, "city", "town", "village"),
                sublocality=_first(data, "district", "suburb"),
                region=_first(data, "state"),
                country=_first(data, "country"),
                country_code=_country_code(data.get("countrycode")),
            )
        else:
            # Nominatim and geocode.maps.co share a shape.
            data = payload.get("address", {})
            place = GeocodedPlace(
                locality=_first(data, "city", "town", "village", "municipality", "hamlet"),
                sublocality=_first(data, "suburb", "city_district", "borough", "neighbourhood", "quarter"),
                region=_first(data, "state", "region", "province"),
                country=_first(data, "country"),
                country_code=_country_code(data.get("country_code")),
                display_name=_first(payload, "display_name", limit=_MAX_DISPLAY_NAME_CHARACTERS),
            )
        return place if any(vars(place).values()) else None

    def _admit(self, max_wait_seconds: float, reservation_key: str | None) -> datetime | None:
        """Wait for this request's turn, or return when to retry instead of sending it.

        It is not sent while the upstream's back-off lasts, nor when no turn is free within ``max_wait_seconds``;
        with a ``reservation_key`` the next turn is then booked under it, and taken when the caller comes back.
        The back-off is read again after waiting for a turn, since a peer may have been told to back off meanwhile.
        Without the state the rate limit cannot be kept, so nothing is sent then either.
        """
        try:
            if (until := self._backed_off_until()) is not None:
                return until
            if self._min_interval <= 0:
                return None
            slot = self._state.reserve_send(
                self._gate_key, self._min_interval, max_wait_seconds=max_wait_seconds, booking=reservation_key
            )
            if not slot.reserved:
                logger.debug("Reverse-geocoding via %s deferred: no request slot within the wait bound", self._service)
                return self._clock.now() + timedelta(seconds=slot.wait_seconds)
            if slot.wait_seconds > 0:
                time.sleep(slot.wait_seconds)
                return self._backed_off_until()
            return None
        except StateBackendUnavailableError as error:
            # An outage: the lookup is deferred, and the next try reads the gate once the state is back.
            logger.warning(
                "Reverse-geocoding via %s could not read its rate gate - %s", self._service, type(error).__name__
            )
        except ValueError as error:
            # A gate value the state refuses to read, which no wait fixes.
            logger.error(
                "Reverse-geocoding via %s could not read its rate gate - %s", self._service, type(error).__name__
            )
        return self._clock.now() + timedelta(seconds=_RETRY_BACKOFF_SECONDS)

    def _backed_off_until(self) -> datetime | None:
        """Return when the upstream's longest back-off ends, while one lasts."""
        latest = self._state.index_range(self._back_off_key, limit=1)
        if latest and (until := latest[0][1]) > self._clock.now().timestamp():
            return datetime.fromtimestamp(until, UTC)
        return None

    def _back_off(self, retry_after: str | None) -> datetime:
        """Hold back every backend on this gate until the upstream's ``Retry-After``, and return that instant.

        Each back-off is a member of an index scored by its end, expiring with it, so a shorter one recorded
        after a longer one never cuts the longer one short.
        """
        now = self._clock.now()
        seconds = _retry_after_seconds(retry_after, now)
        retry_at = now + timedelta(seconds=seconds)
        if seconds > 0:
            until = retry_at.timestamp()
            try:
                self._state.index_add(self._back_off_key, repr(until), until, ttl_seconds=math.ceil(seconds))
            except StateBackendUnavailableError as error:
                logger.warning(
                    "Reverse-geocoding via %s could not share the upstream's back-off - %s",
                    self._service,
                    type(error).__name__,
                )
        return retry_at

    @staticmethod
    def _read_capped(response: "requests.Response", deadline: float) -> bytes:
        """Read a response body, refusing one past :data:`_MAX_RESPONSE_BYTES` or past ``deadline``.

        Streamed rather than taken from ``response.content`` so an oversized body
        is abandoned partway instead of being buffered in full first.

        Args:
            response: The streamed response to drain.
            deadline: The ``time.monotonic()`` value after which the transfer is abandoned.

        Returns:
            The body bytes.

        Raises:
            ValueError: When the body exceeds the cap.
            TimeoutError: When the transfer is still running at ``deadline``.
        """
        body = bytearray()
        for chunk in response.iter_content(chunk_size=_READ_CHUNK_BYTES):
            if time.monotonic() > deadline:
                raise TimeoutError(f"response body still arriving after {_DEADLINE_SECONDS} seconds")
            body.extend(chunk)
            if len(body) > _MAX_RESPONSE_BYTES:
                raise ValueError(f"response body exceeded the {_MAX_RESPONSE_BYTES}-byte limit")
        return bytes(body)

    def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None:
        """Reverse-geocode a coordinate, returning ``None`` on any failure.

        Waits up to :data:`_REVERSE_MAX_WAIT_SECONDS` for its turn under the rate limit, as :meth:`lookup`.

        Args:
            latitude: WGS-84 latitude in decimal degrees.
            longitude: WGS-84 longitude in decimal degrees.

        Returns:
            The resolved place, or ``None`` when nothing resolved, the request
            failed or it was not sent. Never raises — geocoding is best-effort
            enrichment and must not fail the caller's own work.
        """
        return self.lookup(latitude, longitude, max_wait_seconds=_REVERSE_MAX_WAIT_SECONDS).place

    def lookup(
        self, latitude: float, longitude: float, *, max_wait_seconds: float, reservation_key: str | None = None
    ) -> GeocodingOutcome:
        """Reverse-geocode a coordinate, or say when to ask again.

        A 2xx answer is final, as is a request the upstream refused (another 4xx) or an answer that cannot be
        read. A 429 or 503 retries at the upstream's ``Retry-After``, clamped to :data:`_MAX_RETRY_AFTER_SECONDS`;
        another 5xx, a timeout or a dropped connection after :data:`_RETRY_BACKOFF_SECONDS`.

        Args:
            latitude: WGS-84 latitude in decimal degrees.
            longitude: WGS-84 longitude in decimal degrees.
            max_wait_seconds: The longest this call sleeps for its turn under the rate limit.
            reservation_key: Books the next turn under this name when none is free within ``max_wait_seconds``,
                and runs at it when called again with the same name.

        Returns:
            The outcome. Never raises for an upstream or state failure.

        Raises:
            ValueError: When ``max_wait_seconds`` is negative or not finite, or ``reservation_key`` is empty or too
                long.
        """
        state_keys.check_max_wait(max_wait_seconds)
        if reservation_key is not None:
            state_keys.check_booking(reservation_key, name="reservation_key")
        # The coordinates are deliberately not logged: they are a location fix
        # belonging to the host's user, and this is a library — it does not get to
        # decide that someone's whereabouts are acceptable to write to their log.
        logger.debug("Reverse-geocoding via %s", self._service)
        try:
            # Imported lazily: ``requests`` is the optional `geocoding` extra, and
            # the composition root imports this module unconditionally for
            # ``NullGeocoding``.
            import requests
        except ImportError as error:
            logger.error("Reverse-geocoding via %s failed - %s", self._service, _failure_detail(error))
            return GeocodingOutcome()
        retry_at = self._admit(max_wait_seconds, reservation_key)
        if retry_at is not None:
            return GeocodingOutcome(retry_at=retry_at)
        deadline = time.monotonic() + _DEADLINE_SECONDS
        watched = _Deadline()
        finished = threading.Event()
        watchdog = threading.Thread(target=_watch, args=(watched, finished), daemon=True)
        token = _current_deadline.set(watched)
        watchdog.start()
        try:
            # A session of its own, so its connections are the watched ones and none outlives the request.
            with requests.Session() as session:
                # ``requests`` takes a proxy from the environment. Over TLS the proxy only relays the tunnel; a
                # cleartext ``+http`` request would hand it the coordinates, and the address check covered the
                # geocoder's host, not the proxy's, so cleartext goes direct.
                session.trust_env = self._base_url.startswith("https://")
                adapter = _watched_adapter()()
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                with session.get(
                    self._build_url(latitude, longitude),
                    headers={"User-Agent": self._user_agent},
                    timeout=_TIMEOUT_SECONDS,
                    # A permitted host must not 3xx-pivot the request onto an
                    # internal target (SSRF defense in depth, OWASP A10).
                    allow_redirects=False,
                    stream=True,
                ) as response:
                    if response.status_code in _BACK_OFF_STATUSES:
                        logger.error("Reverse-geocoding via %s failed - HTTP %s", self._service, response.status_code)
                        return GeocodingOutcome(retry_at=self._back_off(response.headers.get("Retry-After")))
                    response.raise_for_status()
                    return GeocodingOutcome(self._parse(json.loads(self._read_capped(response, deadline))))
        except Exception as error:
            logger.error("Reverse-geocoding via %s failed - %s", self._service, _failure_detail(error))
            if _is_transient(error):
                return GeocodingOutcome(retry_at=self._clock.now() + timedelta(seconds=_RETRY_BACKOFF_SECONDS))
            return GeocodingOutcome()
        finally:
            finished.set()
            watchdog.join()
            watched.close()
            _current_deadline.reset(token)


def _retry_after_seconds(value: str | None, now: datetime) -> float:
    """Read a ``Retry-After`` header as seconds from ``now``, clamped to ``[0, _MAX_RETRY_AFTER_SECONDS]``.

    Takes delta-seconds or an HTTP-date; an absent or unreadable value is :data:`_RETRY_BACKOFF_SECONDS`.
    """
    if value is None:
        return _RETRY_BACKOFF_SECONDS
    value = value.strip()
    if value.isascii() and value.isdigit():
        seconds = float(value)  # a float, so an absurdly long value saturates instead of overflowing
    else:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return _RETRY_BACKOFF_SECONDS
        # An HTTP-date is GMT; one written with "-0000" parses as naive.
        seconds = (when if when.tzinfo is not None else when.replace(tzinfo=UTC)).timestamp() - now.timestamp()
    return min(max(seconds, 0.0), _MAX_RETRY_AFTER_SECONDS)


def _is_transient(error: Exception) -> bool:
    """Whether a failed request may succeed later: a 5xx, a timeout, or a connection that failed or dropped."""
    import requests  # already imported: only a request that was attempted fails this way

    status = getattr(getattr(error, "response", None), "status_code", None)
    if status is not None:
        return status >= 500
    return isinstance(
        error,
        (
            TimeoutError,
            ConnectionError,
            requests.Timeout,
            requests.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ),
    )


def _watch(deadline: _Deadline, finished: threading.Event) -> None:
    """Watchdog: expire ``deadline`` unless the request finishes within :data:`_DEADLINE_SECONDS`."""
    if not finished.wait(_DEADLINE_SECONDS):
        deadline.expire()


def resolve_reverse_endpoint(
    host: str, *, use_https: bool, allowed_hosts: Sequence[str] = ()
) -> tuple[str | None, str | None]:
    """Validate an operator-configured host and build its reverse endpoint URL.

    Args:
        host: The configured bare ``host[:port]`` authority.
        use_https: Whether to address the host over HTTPS.
        allowed_hosts: Hostnames and CIDRs exempt from the SSRF address denylist,
            so a self-hosted instance on a private network stays reachable.

    Returns:
        ``(url, None)`` for an accepted host, else ``(None, reason)``. The reason
        is already logged.
    """
    reason = network.host_rejection_reason(host, allowed_hosts=allowed_hosts, purpose="reverse_geocoding")
    if reason is not None:
        logger.warning(
            "Reverse-geocoding host %r %s; reverse geocoding is disabled. "
            "A self-hosted instance on a private network must be allow-listed via "
            "the SSRF allowlist.",
            host,
            reason,
        )
        return None, reason
    if not use_https:
        # Cleartext carries the coordinates unencrypted, so it is for a self-hosted instance on a private
        # network only, as for mail.
        reason = network.non_public_rejection_reason(urlsplit(f"//{host}").hostname or "")
        if reason is not None:
            logger.warning(
                "Reverse-geocoding host %r %s, and cleartext (+http) needs a private host; reverse geocoding is "
                "disabled. Use the https scheme for a public instance.",
                host,
                reason,
            )
            return None, reason
        return f"http://{host}/reverse", None
    return f"https://{host}/reverse", None
