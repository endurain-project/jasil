"""Providers that follow settings the host edits at runtime.

One mechanism serves mail and geocoding: on every call the host's source is
asked for the current settings; the backend is rebuilt only when they differ
from the ones it was built from, and the *outcome* of a build — a backend or a
rejection — is cached against those settings, so a bad configuration is logged
once per change rather than once per call.

Private to JASIL: hosts reach it only through
``build_platform(..., sources=RuntimeSources(...))``.
"""

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass

from jasil.providers import (
    GeocodedPlace,
    GeocodingProvider,
    MailError,
    MailMessage,
    MailNotConfiguredError,
    MailProvider,
)
from jasil.settings import GeocodingSettings, MailSettings

logger = logging.getLogger(__name__)


class SourceUnavailableError(Exception):
    """The source raised and there is no usable backend to fall back on."""


@dataclass(frozen=True)
class Outcome[S, B]:
    """What building from ``settings`` produced: a ``backend``, or the ``rejection`` reason."""

    settings: S
    backend: B | None
    rejection: str | None = None


class ReloadingBackend[S, B]:
    """Resolve the backend for the settings a source currently returns.

    Args:
        label: Capability name for log lines (``"Mail"``, ``"Geocoding"``).
        source: Returns the current settings; called on every :meth:`resolve`,
            outside the lock, and may raise.
        build: Builds a backend from settings, raising ``ValueError`` (with a
            message that carries no setting's value) when they are unusable.
    """

    def __init__(self, label: str, source: Callable[[], S], build: Callable[[S], B]) -> None:
        self._label = label
        self._source = source
        self._build = build
        self._lock = threading.Lock()
        self._current: Outcome[S, B] | None = None
        self._source_failing = False

    def current_backend(self) -> B | None:
        """The backend built from the latest settings, without consulting the source."""
        current = self._current
        return current.backend if current is not None else None

    def resolve(self) -> Outcome[S, B]:
        """Return the outcome for the source's current settings.

        Raises:
            SourceUnavailableError: When the source raised and the last outcome was
                not a successful backend. A rejection is never papered over with
                an older backend: its credentials may be the very thing the
                operator just revoked.
        """
        try:
            fresh = self._source()
        except Exception as error:
            # Only the exception type is logged: its text is the host's and may
            # carry a secret from its store.
            self._note_source_failure(error)
            with self._lock:
                current = self._current
            if current is not None and current.backend is not None:
                return current
            raise SourceUnavailableError(f"{self._label} settings could not be loaded") from error
        self._source_failing = False
        with self._lock:
            current = self._current
            if current is None or current.settings != fresh:
                current = self._rebuild(fresh)
                self._current = current
            return current

    def _note_source_failure(self, error: Exception) -> None:
        level = logging.DEBUG if self._source_failing else logging.WARNING
        self._source_failing = True
        logger.log(
            level,
            "%s settings source raised %s; using the last working backend if there is one",
            self._label,
            type(error).__name__,
        )

    def _rebuild(self, fresh: S) -> Outcome[S, B]:
        try:
            return Outcome(fresh, self._build(fresh))
        except ValueError as error:
            logger.error("%s settings rejected: %s", self._label, error)
            return Outcome(fresh, None, str(error))


class ReloadingMail:
    """``MailProvider`` that sends through the backend the latest mail settings describe."""

    def __init__(self, source: Callable[[], MailSettings], build: Callable[[MailSettings], MailProvider]) -> None:
        self._backend: ReloadingBackend[MailSettings, MailProvider] = ReloadingBackend("Mail", source, build)

    def current_backend(self) -> MailProvider | None:
        """The backend built from the latest settings, without consulting the source."""
        return self._backend.current_backend()

    def send(self, message: MailMessage) -> str:
        """Send ``message`` through the current backend.

        Raises:
            MailNotConfiguredError: When the current settings are unset or rejected.
            MailError: When the settings could not be loaded and no working backend exists.
        """
        try:
            outcome = self._backend.resolve()
        except SourceUnavailableError as error:
            raise MailError("Mail settings could not be loaded") from error.__cause__
        if outcome.backend is None:
            raise MailNotConfiguredError("Mail settings are invalid")
        return outcome.backend.send(message)


class ReloadingGeocoding:
    """``GeocodingProvider`` that reverses through the backend the latest settings describe."""

    def __init__(
        self, source: Callable[[], GeocodingSettings], build: Callable[[GeocodingSettings], GeocodingProvider]
    ) -> None:
        self._backend: ReloadingBackend[GeocodingSettings, GeocodingProvider] = ReloadingBackend(
            "Geocoding", source, build
        )

    def current_backend(self) -> GeocodingProvider | None:
        """The backend built from the latest settings, without consulting the source."""
        return self._backend.current_backend()

    def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None:
        """Reverse-geocode through the current backend; ``None`` when there is none."""
        try:
            outcome = self._backend.resolve()
        except SourceUnavailableError:
            return None
        if outcome.backend is None:
            return None
        return outcome.backend.reverse(latitude, longitude)
