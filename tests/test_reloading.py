"""Runtime sources: mail and geocoding that follow settings the host edits while running.

The contract under test is the failure policy: a source that raises falls back to
the last backend that *worked*, a rejected configuration never reuses old
credentials, and a bad configuration is logged once per change, never once per call.
"""

import logging
import threading
from dataclasses import replace

import pytest

import jasil.container as container
import jasil.settings as settings
from jasil.backends.geocoding_http import HttpGeocoding, NullGeocoding
from jasil.backends.mail_memory import MemoryMail, NullMail
from jasil.providers import (
    GeocodedPlace,
    GeocodingProvider,
    MailError,
    MailMessage,
    MailNotConfiguredError,
    MailProvider,
)
from jasil.settings import GeocodingSettings, JasilSettings, MailSettings, RuntimeSources

MESSAGE = MailMessage(to=["to@example.com"], subject="s", text="t")
GOOD_MAIL = MailSettings(uri="memory://", from_address="a@example.com")
BAD_MAIL = MailSettings(uri="memory://", from_address="not an address")
GOOD_GEO = GeocodingSettings(uri="geocode://", api_key="key-one")


class Source:
    """A settings source a test can change, break and count."""

    def __init__(self, value):
        self.value = value
        self.calls = 0
        self.error: Exception | None = None

    def __call__(self):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.value


def _mail(source, base: JasilSettings | None = None):
    return container._build_runtime_mail(base or JasilSettings(), container.SystemClock(), source)


def _geo(source, base: JasilSettings | None = None):
    return container._build_runtime_geocoding(base or JasilSettings(), source)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Geocoding backends built here must never reach the real upstream."""
    monkeypatch.setattr(HttpGeocoding, "reverse", lambda self, latitude, longitude: None)


def _count(caplog, level: int) -> int:
    """Records the reloading layer itself logged at ``level``."""
    return sum(r.levelno == level and r.name == "jasil._core.reloading" for r in caplog.records)


@pytest.fixture
def builds(monkeypatch):
    """Count how often each real builder runs."""
    counts = {"mail": 0, "geocoding": 0}
    real_mail, real_geo = container._build_mail, container._build_geocoding

    def mail(*args, **kwargs):
        counts["mail"] += 1
        return real_mail(*args, **kwargs)

    def geo(*args, **kwargs):
        counts["geocoding"] += 1
        return real_geo(*args, **kwargs)

    monkeypatch.setattr(container, "_build_mail", mail)
    monkeypatch.setattr(container, "_build_geocoding", geo)
    return counts


class TestLaziness:
    def test_build_platform_never_calls_a_source(self, tmp_path):
        mail, geo = Source(GOOD_MAIL), Source(GOOD_GEO)

        platform = container.build_platform(
            JasilSettings(data_dir=str(tmp_path)), sources=RuntimeSources(mail=mail, geocoding=geo)
        )

        assert (mail.calls, geo.calls) == (0, 0)
        assert isinstance(platform.mail, MailProvider)
        assert isinstance(platform.geocoding, GeocodingProvider)

    def test_the_first_use_calls_the_source(self):
        source = Source(GOOD_MAIL)

        _mail(source).send(MESSAGE)

        assert source.calls == 1

    def test_a_source_is_called_on_every_use(self):
        source = Source(GOOD_MAIL)
        provider = _mail(source)

        for _ in range(3):
            provider.send(MESSAGE)

        assert source.calls == 3

    def test_only_the_sourced_capability_is_reloading(self, tmp_path):
        platform = container.build_platform(
            JasilSettings(data_dir=str(tmp_path)), sources=RuntimeSources(mail=Source(GOOD_MAIL))
        )

        assert not isinstance(platform.geocoding, type(platform.mail))
        assert isinstance(platform.geocoding, NullGeocoding)


class TestRebuild:
    def test_equal_settings_reuse_the_backend(self, builds):
        source = Source(GOOD_MAIL)
        provider = _mail(source)

        for _ in range(5):
            provider.send(MESSAGE)
        source.value = replace(GOOD_MAIL)  # equal, not identical
        provider.send(MESSAGE)

        assert builds["mail"] == 1

    def test_changed_settings_rebuild(self, builds):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        provider.send(MESSAGE)

        source.value = replace(GOOD_MAIL, from_address="other@example.com")
        provider.send(MESSAGE)

        assert builds["mail"] == 2

    def test_a_password_change_alone_rebuilds(self, builds):
        smtp = MailSettings(uri="smtps://relay.example.com", from_address="a@example.com", username="u", password="p1")
        source = Source(smtp)
        provider = _mail(source)
        with pytest.raises(MailError):  # nothing listens; we only need the build
            provider.send(MESSAGE)
        source.value = replace(smtp, password="p2")
        with pytest.raises(MailError):
            provider.send(MESSAGE)

        assert builds["mail"] == 2

    def test_an_api_key_change_alone_rebuilds(self, builds):
        source = Source(GOOD_GEO)
        provider = _geo(source)
        provider.reverse(1.0, 2.0)

        source.value = replace(GOOD_GEO, api_key="key-two")
        provider.reverse(1.0, 2.0)

        assert builds["geocoding"] == 2
        assert provider.current_backend()._api_key == "key-two"

    def test_rebuilding_geocoding_resets_the_throttle(self):
        source = Source(GOOD_GEO)
        provider = _geo(source)
        provider.reverse(1.0, 2.0)
        first = provider.current_backend()

        source.value = replace(GOOD_GEO, rate_limit=2.0)
        provider.reverse(1.0, 2.0)

        assert provider.current_backend() is not first

    def test_settings_are_followed_in_both_directions(self):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        provider.send(MESSAGE)
        assert isinstance(provider.current_backend(), MemoryMail)

        source.value = MailSettings()
        with pytest.raises(MailNotConfiguredError):
            provider.send(MESSAGE)
        assert isinstance(provider.current_backend(), NullMail)

        source.value = GOOD_MAIL
        provider.send(MESSAGE)
        assert isinstance(provider.current_backend(), MemoryMail)

    def test_the_captured_network_policy_applies_on_rebuild(self):
        private = GeocodingSettings(uri="nominatim+http://10.0.0.5:8080")
        denied = _geo(Source(private))
        allowed = _geo(
            Source(private), JasilSettings(network=settings.NetworkSettings(ssrf_allowed_hosts=("10.0.0.0/8",)))
        )

        denied.reverse(1.0, 2.0)
        allowed.reverse(1.0, 2.0)

        assert isinstance(denied.current_backend(), NullGeocoding)
        assert isinstance(allowed.current_backend(), HttpGeocoding)


class TestUnsetSettings:
    def test_unset_mail_behaves_like_null_mail(self):
        provider = _mail(Source(MailSettings()))

        with pytest.raises(MailNotConfiguredError):
            provider.send(MESSAGE)
        assert isinstance(provider.current_backend(), NullMail)

    def test_unset_geocoding_behaves_like_null_geocoding(self):
        provider = _geo(Source(GeocodingSettings()))

        assert provider.reverse(1.0, 2.0) is None
        assert isinstance(provider.current_backend(), NullGeocoding)


class TestInvalidSettings:
    def test_invalid_mail_raises_not_configured(self):
        provider = _mail(Source(BAD_MAIL))

        with pytest.raises(MailNotConfiguredError, match="invalid"):
            provider.send(MESSAGE)

    def test_invalid_mail_never_reuses_the_previous_backend(self):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        provider.send(MESSAGE)
        good = provider.current_backend()

        source.value = BAD_MAIL
        with pytest.raises(MailNotConfiguredError):
            provider.send(MESSAGE)

        assert good.outbox  # the old backend was used before, and not again
        assert len(good.outbox) == 1

    def test_a_rejection_is_logged_once_per_change(self, caplog):
        source = Source(BAD_MAIL)
        provider = _mail(source)

        with caplog.at_level(logging.ERROR):
            for _ in range(5):
                with pytest.raises(MailNotConfiguredError):
                    provider.send(MESSAGE)

        assert caplog.text.count("settings rejected") == 1
        assert "mail.from_address is not a valid email address" in caplog.text

    def test_a_new_bad_value_is_logged_again(self, caplog):
        source = Source(BAD_MAIL)
        provider = _mail(source)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(MailNotConfiguredError):
                provider.send(MESSAGE)
            source.value = replace(BAD_MAIL, ehlo_hostname="bad host")
            with pytest.raises(MailNotConfiguredError):
                provider.send(MESSAGE)

        assert caplog.text.count("settings rejected") == 2

    def test_an_invalid_geocoding_value_is_logged_once_per_change(self, caplog, builds):
        provider = _geo(Source(GeocodingSettings(uri="mapquest://x")))

        with caplog.at_level(logging.WARNING):
            for _ in range(4):
                assert provider.reverse(1.0, 2.0) is None

        assert caplog.text.count("not supported") == 1
        assert builds["geocoding"] == 1


class TestSourceFailure:
    def test_mail_uses_the_last_good_backend(self):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        provider.send(MESSAGE)

        source.error = RuntimeError("db down")
        provider.send(MESSAGE)

        assert len(provider.current_backend().outbox) == 2

    def test_mail_without_a_good_backend_raises_a_transient_error(self):
        source = Source(GOOD_MAIL)
        source.error = RuntimeError("db down")

        with pytest.raises(MailError, match="could not be loaded") as caught:
            _mail(source).send(MESSAGE)

        assert not isinstance(caught.value, MailNotConfiguredError)
        assert caught.value.permanent is False
        assert isinstance(caught.value.__cause__, RuntimeError)

    def test_mail_after_a_rejection_raises_instead_of_reusing_old_credentials(self):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        provider.send(MESSAGE)
        source.value = BAD_MAIL
        with pytest.raises(MailNotConfiguredError):
            provider.send(MESSAGE)

        source.error = RuntimeError("db down")
        with pytest.raises(MailError, match="could not be loaded"):
            provider.send(MESSAGE)

    def test_geocoding_uses_the_last_good_backend(self, monkeypatch):
        place = GeocodedPlace(locality="Porto")
        monkeypatch.setattr(HttpGeocoding, "reverse", lambda self, lat, lon: place)
        source = Source(GOOD_GEO)
        provider = _geo(source)
        provider.reverse(1.0, 2.0)

        source.error = RuntimeError("db down")

        assert provider.reverse(1.0, 2.0) is place

    def test_geocoding_without_a_good_backend_returns_none(self):
        source = Source(GOOD_GEO)
        source.error = RuntimeError("db down")

        assert _geo(source).reverse(1.0, 2.0) is None

    def test_geocoding_after_a_rejection_returns_none(self, monkeypatch):
        monkeypatch.setattr(HttpGeocoding, "reverse", lambda self, lat, lon: GeocodedPlace(locality="Porto"))
        source = Source(GOOD_GEO)
        provider = _geo(source)
        provider.reverse(1.0, 2.0)
        source.value = GeocodingSettings(uri="mapquest://x")
        assert provider.reverse(1.0, 2.0) is None

        source.error = RuntimeError("db down")

        assert provider.reverse(1.0, 2.0) is None

    def test_a_failing_source_warns_once_then_goes_quiet_and_resets(self, caplog):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        provider.send(MESSAGE)

        with caplog.at_level(logging.DEBUG, logger="jasil._core.reloading"):
            source.error = RuntimeError("db down")
            for _ in range(3):
                provider.send(MESSAGE)
            assert _count(caplog, logging.WARNING) == 1
            assert _count(caplog, logging.DEBUG) == 2

            source.error = None
            provider.send(MESSAGE)
            source.error = RuntimeError("db down")
            provider.send(MESSAGE)

        assert _count(caplog, logging.WARNING) == 2

    def test_geocoding_without_a_backend_does_not_warn_on_every_call(self, caplog):
        source = Source(GOOD_GEO)
        source.error = RuntimeError("db down")
        provider = _geo(source)

        with caplog.at_level(logging.WARNING):
            for _ in range(5):
                provider.reverse(1.0, 2.0)

        assert _count(caplog, logging.WARNING) == 1


class TestMailConsistencyOnRebuild:
    MULTI = JasilSettings(web_workers=4)

    def test_enforced_a_multi_process_memory_edit_is_rejected(self):
        provider = _mail(Source(GOOD_MAIL), self.MULTI)

        with pytest.raises(MailNotConfiguredError):
            provider.send(MESSAGE)

    def test_not_enforced_it_warns_and_builds(self, caplog):
        provider = _mail(Source(GOOD_MAIL), replace(self.MULTI, enforce_deployment_consistency=False))

        with caplog.at_level(logging.WARNING):
            provider.send(MESSAGE)

        assert "in-memory mail recorder" in caplog.text
        assert isinstance(provider.current_backend(), MemoryMail)

    def test_unset_mail_is_not_a_consistency_problem(self):
        provider = _mail(Source(MailSettings()), self.MULTI)

        with pytest.raises(MailNotConfiguredError, match="not configured"):
            provider.send(MESSAGE)


class TestBuildPlatform:
    def test_without_sources_nothing_is_reloading(self, tmp_path):
        platform = container.build_platform(JasilSettings(data_dir=str(tmp_path)))

        assert isinstance(platform.mail, NullMail)
        assert isinstance(platform.geocoding, NullGeocoding)

    def test_a_sourced_capability_ignores_the_static_settings(self, tmp_path):
        static = JasilSettings(data_dir=str(tmp_path), mail=MailSettings(uri="mapquest://x"))

        platform = container.build_platform(static, sources=RuntimeSources(mail=Source(GOOD_MAIL)))

        platform.mail.send(MESSAGE)
        assert isinstance(platform.mail.current_backend(), MemoryMail)

    def test_the_startup_consistency_check_ignores_a_static_memory_uri_for_sourced_mail(self):
        static = JasilSettings(web_workers=4, mail=MailSettings(uri="memory://"))
        shared = replace(static, state_uri="redis://c/0", events_uri="redis://c/1", lock_uri="postgres-advisory://")

        with pytest.raises(ValueError, match=r"mail\.uri"):
            container._check_deployment_consistency(shared)
        container._check_deployment_consistency(shared, RuntimeSources(mail=Source(MailSettings())))

    def test_the_capability_log_never_calls_a_source(self, tmp_path, caplog):
        source = Source(GOOD_MAIL)
        with caplog.at_level(logging.INFO):
            container.build_platform(JasilSettings(data_dir=str(tmp_path)), sources=RuntimeSources(mail=source))

        assert "runtime source" in caplog.text
        assert source.calls == 0

    def test_close_needs_nothing_from_the_reloading_providers(self, tmp_path):
        platform = container.build_platform(
            JasilSettings(data_dir=str(tmp_path)),
            sources=RuntimeSources(mail=Source(GOOD_MAIL), geocoding=Source(GOOD_GEO)),
        )

        platform.close()


class TestConcurrency:
    def test_many_threads_trigger_one_rebuild(self, builds):
        source = Source(GOOD_MAIL)
        provider = _mail(source)
        barrier = threading.Barrier(16)
        errors: list[BaseException] = []

        def work():
            try:
                barrier.wait()
                for _ in range(20):
                    provider.send(MESSAGE)
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=work) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert builds["mail"] == 1
        assert len(provider.current_backend().outbox) == 16 * 20


class TestSecretsStayOutOfLogsAndErrors:
    PASSWORD = "hunter2-secret"
    API_KEY = "api-key-secret"
    USERNAME = "svc-user-secret"

    def test_nothing_secret_is_logged_or_raised(self, caplog):
        smtp = MailSettings(
            uri="smtps://relay.example.com",
            from_address="bad address",
            username=self.USERNAME,
            password=self.PASSWORD,
        )
        mail_source = Source(smtp)
        geo_source = Source(replace(GOOD_GEO, api_key=self.API_KEY, uri="mapquest://x"))
        mail, geo = _mail(mail_source), _geo(geo_source)
        texts: list[str] = []

        with caplog.at_level(logging.DEBUG):
            with pytest.raises(MailNotConfiguredError) as rejected:
                mail.send(MESSAGE)
            texts.append(str(rejected.value))
            geo.reverse(1.0, 2.0)
            mail_source.error = geo_source.error = RuntimeError(f"cannot reach store as {self.USERNAME}")
            with pytest.raises(MailError) as unavailable:
                mail.send(MESSAGE)
            texts.append(str(unavailable.value))
            geo.reverse(1.0, 2.0)
            mail_source.error = geo_source.error = None
            mail_source.value = replace(smtp, from_address="a@example.com")
            geo_source.value = replace(GOOD_GEO, api_key=self.API_KEY)
            geo.reverse(1.0, 2.0)

        everything = caplog.text + " ".join(texts) + " ".join(repr(r.args) for r in caplog.records)
        for secret in (self.PASSWORD, self.API_KEY, self.USERNAME):
            assert secret not in everything
