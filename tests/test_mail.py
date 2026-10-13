"""Outbound mail: the MIME builder, the backends, and what never leaks.

``smtplib`` is replaced by a fake, so no socket ever opens. The conformance suite
runs against both real backends: a host's tests (``MemoryMail``) must fail on the
same invalid message production (``SmtpMail``) would.
"""

import logging
import smtplib
import ssl
from datetime import UTC, datetime
from typing import ClassVar

import pytest

import jasil.backends.mail_smtp as mail_smtp
from jasil._core.mail_message import build_mime
from jasil.backends.mail_memory import DisabledMail, MemoryMail
from jasil.backends.mail_smtp import SmtpMail
from jasil.providers import MailError, MailMessage, MailNotConfiguredError, MailProvider

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
SENDER = "Support Team <support@example.com>"
ADDRESS = "secret.person@private.example"


class _Clock:
    def now(self) -> datetime:
        return NOW

    def monotonic(self) -> float:
        return 0.0


def _message(**overrides) -> MailMessage:
    fields = {"to": [ADDRESS], "subject": "Hello", "text": "body"}
    fields.update(overrides)
    return MailMessage(**fields)


class FakeSMTP:
    """Stands in for ``smtplib.SMTP`` / ``SMTP_SSL``; records the conversation."""

    instances: ClassVar[list["FakeSMTP"]] = []
    offers_starttls = True
    refused: ClassVar[dict] = {}
    fail_on: str | None = None

    def __init__(self, host, port, timeout=None, context=None):
        self.host, self.port, self.timeout, self.context = host, port, timeout, context
        self.calls: list[str] = []
        self.logged_in_as: str | None = None
        self.sent = None
        self.tls_started = False
        FakeSMTP.instances.append(self)
        if FakeSMTP.fail_on == "connect":
            raise OSError("connect failed for " + ADDRESS)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.calls.append("quit")

    def ehlo(self):
        self.calls.append("ehlo")

    def has_extn(self, name):
        return FakeSMTP.offers_starttls

    def starttls(self, context=None):
        self.calls.append("starttls")
        self.tls_started = True

    def login(self, username, password):
        self.calls.append("login")
        self.logged_in_as = username
        if FakeSMTP.fail_on == "auth":
            raise smtplib.SMTPAuthenticationError(535, ("bad creds for " + ADDRESS).encode())

    def send_message(self, mime):
        self.calls.append("send")
        self.sent = mime
        if FakeSMTP.fail_on == "data":
            raise smtplib.SMTPDataError(554, ("srvtext-xyz " + ADDRESS).encode())
        if FakeSMTP.fail_on == "all-refused":
            raise smtplib.SMTPRecipientsRefused({ADDRESS: (550, b"no")})
        if FakeSMTP.fail_on == "tls":
            raise ssl.SSLError("handshake")
        return FakeSMTP.refused


@pytest.fixture(autouse=True)
def fake_smtp(monkeypatch):
    FakeSMTP.instances = []
    FakeSMTP.offers_starttls = True
    FakeSMTP.refused = {}
    FakeSMTP.fail_on = None
    monkeypatch.setattr(mail_smtp.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(mail_smtp.smtplib, "SMTP_SSL", FakeSMTP)


def _smtp(**overrides) -> SmtpMail:
    kwargs = {
        "implicit_tls": False,
        "from_address": SENDER,
        "clock": _Clock(),
    }
    kwargs.update(overrides)
    return SmtpMail("relay.test", 587, **kwargs)


def _memory() -> MemoryMail:
    return MemoryMail(_Clock(), SENDER)


class TestBuildMime:
    def test_text_only_is_a_single_part(self):
        mime = build_mime(_message(), sender=SENDER, now=NOW)

        assert mime.get_content_type() == "text/plain"
        assert mime["To"] == ADDRESS
        assert mime["Subject"] == "Hello"

    def test_html_only(self):
        mime = build_mime(_message(text=None, html="<p>hi</p>"), sender=SENDER, now=NOW)

        assert mime.get_content_type() == "text/html"

    def test_both_bodies_make_an_alternative(self):
        mime = build_mime(_message(html="<p>hi</p>"), sender=SENDER, now=NOW)

        assert mime.get_content_type() == "multipart/alternative"

    def test_headers_come_from_the_sender_and_clock(self):
        mime = build_mime(_message(reply_to="r@example.com"), sender=SENDER, now=NOW)

        assert "support@example.com" in mime["From"]
        assert mime["Reply-To"] == "r@example.com"
        assert mime["Date"] == "Tue, 13 Oct 2026 12:00:00 +0000"
        assert mime["Message-ID"].startswith("<") and mime["Message-ID"].endswith("@example.com>")

    @pytest.mark.parametrize("subject", ["a\r\nBcc: evil@example.com", "a\nb"])
    def test_crlf_in_the_subject_is_rejected(self, subject):
        with pytest.raises(ValueError):
            build_mime(_message(subject=subject), sender=SENDER, now=NOW)

    @pytest.mark.parametrize(
        "address",
        ["a@b.c\r\nBcc: evil@example.com", "not-an-address", "a@b.c, d@e.f", "", "@b.c", "a@", "a@exa mple.com"],
    )
    def test_a_bad_recipient_is_rejected_without_echoing_it(self, address):
        with pytest.raises(ValueError) as caught:
            build_mime(_message(to=[address]), sender=SENDER, now=NOW)

        assert address.strip() == "" or address not in str(caught.value)

    def test_a_non_string_recipient_is_rejected(self):
        with pytest.raises(ValueError, match="recipient"):
            build_mime(_message(to=[None]), sender=SENDER, now=NOW)

    def test_a_parser_failure_is_a_value_error_without_the_address(self, monkeypatch):
        import email.errors

        import jasil._core.mail_message as mail_message

        def _boom(**_kwargs):
            raise email.errors.HeaderParseError(ADDRESS)

        monkeypatch.setattr(mail_message, "Address", _boom)

        with pytest.raises(ValueError) as caught:
            build_mime(_message(), sender=SENDER, now=NOW)

        assert ADDRESS not in str(caught.value) and caught.value.__cause__ is None

    def test_a_non_ascii_domain_builds(self):
        mime = build_mime(_message(to=["a@ünï.test"]), sender=SENDER, now=NOW)

        mime.as_bytes()

    def test_no_recipient_is_rejected(self):
        with pytest.raises(ValueError, match="recipient"):
            build_mime(_message(to=[]), sender=SENDER, now=NOW)

    @pytest.mark.parametrize("body", [{"text": None}, {"text": ""}])
    def test_no_body_is_rejected(self, body):
        with pytest.raises(ValueError, match="body"):
            build_mime(_message(**body), sender=SENDER, now=NOW)

    def test_a_bad_reply_to_is_rejected(self):
        with pytest.raises(ValueError):
            build_mime(_message(reply_to="nope"), sender=SENDER, now=NOW)


@pytest.fixture(params=["memory", "smtp"])
def backend(request) -> MailProvider:
    return _memory() if request.param == "memory" else _smtp()


class TestConformance:
    """Both real backends honour the same contract."""

    def test_it_satisfies_the_protocol(self, backend):
        assert isinstance(backend, MailProvider)

    def test_send_returns_the_message_id_of_the_built_message(self, backend):
        message_id = backend.send(_message())

        assert message_id.startswith("<") and message_id.endswith("@example.com>")
        if isinstance(backend, SmtpMail):
            assert FakeSMTP.instances[0].sent["Message-ID"] == message_id

    def test_ids_are_unique_per_send(self, backend):
        assert backend.send(_message()) != backend.send(_message())

    @pytest.mark.parametrize(
        "bad",
        [{"to": []}, {"text": None}, {"subject": "a\nb"}, {"to": ["nope"]}],
    )
    def test_an_invalid_message_raises_value_error(self, backend, bad):
        with pytest.raises(ValueError):
            backend.send(_message(**bad))
        assert not FakeSMTP.instances


class TestMemoryMail:
    def test_messages_are_recorded_in_order(self):
        mail = _memory()
        first, second = _message(subject="1"), _message(subject="2")

        mail.send(first)
        mail.send(second)

        assert mail.outbox == [first, second]

    def test_an_invalid_message_is_not_recorded(self):
        mail = _memory()
        with pytest.raises(ValueError):
            mail.send(_message(to=[]))

        assert mail.outbox == []

    def test_a_placeholder_sender_is_used_when_none_is_configured(self):
        assert MemoryMail(_Clock()).send(_message()).endswith("@localhost>")


class TestDisabledMail:
    def test_send_raises_not_configured(self):
        with pytest.raises(MailNotConfiguredError):
            DisabledMail().send(_message())

    def test_not_configured_is_a_mail_error(self):
        with pytest.raises(MailError):
            DisabledMail().send(_message())


class TestSmtpTransport:
    def test_starttls_is_required_before_anything_is_sent(self):
        _smtp().send(_message())

        calls = FakeSMTP.instances[0].calls
        assert calls.index("starttls") < calls.index("send")

    def test_a_server_without_starttls_is_refused_not_downgraded(self):
        FakeSMTP.offers_starttls = False

        with pytest.raises(MailError, match="STARTTLS"):
            _smtp().send(_message())

        assert "send" not in FakeSMTP.instances[0].calls

    def test_implicit_tls_skips_starttls(self):
        _smtp(implicit_tls=True).send(_message())

        assert "starttls" not in FakeSMTP.instances[0].calls
        assert isinstance(FakeSMTP.instances[0].context, ssl.SSLContext)

    def test_certificates_are_verified(self):
        _smtp(implicit_tls=True).send(_message())

        context = FakeSMTP.instances[0].context
        assert context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname

    def test_every_socket_operation_has_a_timeout(self):
        _smtp().send(_message())

        assert FakeSMTP.instances[0].timeout == mail_smtp._TIMEOUT_SECONDS

    def test_no_login_without_credentials(self):
        _smtp().send(_message())

        assert "login" not in FakeSMTP.instances[0].calls

    def test_login_happens_after_tls(self):
        _smtp(username="relay-user", password="pw").send(_message())

        calls = FakeSMTP.instances[0].calls
        assert calls.index("starttls") < calls.index("login") < calls.index("send")

    def test_login_and_sender_are_independent(self):
        """Relays log you in as a generated identity and verify the From separately."""
        _smtp(username="generated@smtp-relay.test", password="pw").send(_message())

        smtp = FakeSMTP.instances[0]
        assert smtp.logged_in_as == "generated@smtp-relay.test"
        assert "support@example.com" in smtp.sent["From"]

    def test_the_envelope_sender_is_the_bare_address_while_from_keeps_the_name(self):
        import email.utils

        _smtp().send(_message())

        sent = FakeSMTP.instances[0].sent
        assert sent["From"].startswith("Support Team")
        assert email.utils.parseaddr(sent["From"])[1] == "support@example.com"

    def test_a_partial_refusal_succeeds_and_warns_with_a_count(self, caplog):
        FakeSMTP.refused = {"gone@example.com": (550, b"unknown")}

        with caplog.at_level(logging.WARNING, logger=mail_smtp.logger.name):
            message_id = _smtp().send(_message(to=[ADDRESS, "gone@example.com"]))

        assert message_id
        warning = next(r for r in caplog.records if r.levelno == logging.WARNING)
        assert "1 of 2" in warning.getMessage()
        assert "gone@example.com" not in caplog.text


class TestFailuresAreRedacted:
    @pytest.mark.parametrize(
        ("mode", "expect"),
        [
            ("connect", "unreachable"),
            ("auth", "authentication failed (status 535)"),
            ("data", "status 554"),
            ("all-refused", "refused every recipient"),
            ("tls", "TLS negotiation"),
        ],
    )
    def test_each_failure_becomes_a_mail_error_with_no_server_text(self, mode, expect, caplog):
        FakeSMTP.fail_on = mode

        with caplog.at_level(logging.DEBUG), pytest.raises(MailError) as caught:
            _smtp(username="u", password="pw").send(_message(subject="Top secret subject", text="secret body"))

        assert expect in str(caught.value)
        assert caught.value.__cause__ is None and caught.value.__suppress_context__
        for leaked in (ADDRESS, "Top secret subject", "secret body", "bad creds", "srvtext-xyz"):
            assert leaked not in str(caught.value)
            assert leaked not in caplog.text

    def test_success_logs_no_personal_data(self, caplog):
        with caplog.at_level(logging.DEBUG):
            _smtp().send(_message(subject="Top secret subject"))

        assert ADDRESS not in caplog.text and "Top secret subject" not in caplog.text
