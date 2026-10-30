"""Providers that follow settings the host edits at runtime.

One mechanism serves mail and geocoding: on every call the host's source is
asked for the current settings; the backend is rebuilt only when they differ
from the ones it was built from, and the *outcome* of a build — a backend or a
rejection — is cached against those settings, so a bad configuration is logged
once per change rather than once per call.

Private to JASIL: hosts reach it only through
``build_platform(..., sources=RuntimeSources(...))``.
"""

import itertools
import logging
import threading
import time
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


class TransientBuildError(ValueError):
    """A build failed for a reason that may pass (a DNS lookup that did not answer).

    Still a ``ValueError``, so a startup build fails with it; a
    reloading provider caches it only for :data:`TRANSIENT_RETRY_SECONDS`.
    """


#: How long a transient build failure is remembered before the build is retried.
TRANSIENT_RETRY_SECONDS = 30.0


@dataclass(frozen=True)
class Outcome[S, B]:
    """What building from ``settings`` produced: a ``backend``, or the ``rejection`` reason.

    ``retry_at`` (a ``time.monotonic`` instant) is set only for a transient
    failure: until then the outcome is reused, after it the build runs again.
    """

    settings: S
    backend: B | None
    rejection: str | None = None
    retry_at: float | None = None

    @property
    def transient(self) -> bool:
        """True for a failure that is retried rather than remembered until the settings change."""
        return self.retry_at is not None


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
        # Numbers each read in the order it began, so a slow older read cannot overwrite what a newer one built.
        self._reads = itertools.count()
        self._applied_read = -1

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
        read = next(self._reads)
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
            raise SourceUnavailableError(f"{self._label} settings could not be loaded") from None
        with self._lock:
            self._source_failing = False
            current = self._current
            if current is not None and read < self._applied_read:
                # A read that began after this one has already been applied: its settings are the newer ones.
                return current
            self._applied_read = read
            # One build per change, under the lock: every other caller needs that
            # same new backend, so none gains by running ahead of it.
            if current is None or current.settings != fresh or self._retry_due(current):
                current = self._rebuild(fresh)
                self._current = current
            return current

    def _note_source_failure(self, error: Exception) -> None:
        with self._lock:
            level = logging.DEBUG if self._source_failing else logging.WARNING
            self._source_failing = True
        logger.log(
            level,
            "%s settings source raised %s; using the last working backend if there is one",
            self._label,
            type(error).__name__,
        )

    @staticmethod
    def _retry_due(current: Outcome[S, B]) -> bool:
        return current.retry_at is not None and time.monotonic() >= current.retry_at

    def _rebuild(self, fresh: S) -> Outcome[S, B]:
        try:
            return Outcome(fresh, self._build(fresh))
        except TransientBuildError as error:
            logger.warning("%s settings could not be verified, will retry: %s", self._label, error)
            return Outcome(fresh, None, str(error), retry_at=time.monotonic() + TRANSIENT_RETRY_SECONDS)
        except ValueError as error:
            logger.error("%s settings rejected: %s", self._label, error)
            return Outcome(fresh, None, str(error))
        except Exception as error:
            # Only the type: the text may carry a setting's value. Cached like a
            # rejection so a provider that must not raise (geocoding) never does.
            logger.error("%s settings rejected: building failed with %s", self._label, type(error).__name__)
            return Outcome(fresh, None, type(error).__name__)


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
            MailError: When the settings could not be loaded and no working backend
                exists, or the relay host could not be resolved to verify it (retried
                after a short delay).
        """
        try:
            outcome = self._backend.resolve()
        except SourceUnavailableError:
            # Not chained: the source's exception is the host's, and its text may carry a secret.
            raise MailError("Mail settings could not be loaded") from None
        if outcome.transient:
            raise MailError("Mail settings could not be verified; retry")
        if outcome.backend is None:
            # The reason carries no setting's value (see ``ReloadingBackend``).
            raise MailNotConfiguredError(f"Mail settings are invalid: {outcome.rejection}")
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
