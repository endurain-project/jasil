"""``mail_problems``: the one copy of the mail rules, shared with the builder.

A host validates operator input with it on save, and the composition root raises
the first problem it reports, so the two can never disagree.
"""

import socket

import pytest

import jasil._core.network as network
import jasil.container as container
import jasil.settings as settings
from jasil.backends.clock_system import SystemClock
from jasil.settings import MailProblem, MailProblemCode, MailSettings, mail_problems

OK = {"from_address": "a@example.com"}


@pytest.fixture(autouse=True)
def private_dns(monkeypatch):
    """Resolve every host to a private address unless a test says otherwise."""
    monkeypatch.setattr(network.socket, "getaddrinfo", lambda *_a: [(0, 0, 0, "", ("10.0.0.5", 0))])


def _codes(**mail) -> list[MailProblemCode]:
    return [problem.code for problem in mail_problems(MailSettings(**mail))]


CASES = [
    (MailProblemCode.INVALID_URI, {"uri": "smtp:relay.example.com"}),
    (MailProblemCode.INVALID_URI, {"uri": "smtp://relay.example.com:99999"}),
    (MailProblemCode.UNSUPPORTED_SCHEME, {"uri": "imap://relay.example.com"}),
    (MailProblemCode.URI_CARRIES_CREDENTIALS, {"uri": "smtps://u:p@relay.example.com", **OK}),
    (MailProblemCode.URI_HAS_EXTRA_PARTS, {"uri": "smtps://relay.example.com/path", **OK}),
    (MailProblemCode.INVALID_HOST, {"uri": "smtps://", **OK}),
    (MailProblemCode.INVALID_PORT, {"uri": "smtps://relay.example.com:0", **OK}),
    (MailProblemCode.MISSING_FROM_ADDRESS, {"uri": "smtps://relay.example.com"}),
    (MailProblemCode.INVALID_FROM_ADDRESS, {"uri": "smtps://relay.example.com", "from_address": "no address"}),
    (MailProblemCode.INVALID_EHLO_HOSTNAME, {"uri": "smtps://relay.example.com", **OK, "ehlo_hostname": "bad host"}),
    (MailProblemCode.CREDENTIALS_INCOMPLETE, {"uri": "smtps://relay.example.com", **OK, "username": "u"}),
    (
        MailProblemCode.CREDENTIALS_NOT_ASCII,
        {"uri": "smtps://relay.example.com", **OK, "username": "u", "password": "pässword"},
    ),
    (MailProblemCode.MEMORY_HAS_HOST, {"uri": "memory://host"}),
    (MailProblemCode.MEMORY_CLEARTEXT_UNSUPPORTED, {"uri": "memory://", "secure": False}),
    (MailProblemCode.CLEARTEXT_WITH_IMPLICIT_TLS, {"uri": "smtps://relay.example.com", **OK, "secure": False}),
    (
        MailProblemCode.CLEARTEXT_WITH_CREDENTIALS,
        {"uri": "smtp://relay.example.com", **OK, "secure": False, "username": "u", "password": "p"},
    ),
]


class TestEachRule:
    @pytest.mark.parametrize(("code", "mail"), CASES, ids=[f"{c.name}-{i}" for i, (c, _) in enumerate(CASES)])
    def test_the_rule_is_reported(self, code, mail):
        assert code in _codes(**mail)

    def test_a_public_cleartext_relay_is_reported(self, monkeypatch):
        monkeypatch.setattr(network.socket, "getaddrinfo", lambda *_a: [(0, 0, 0, "", ("8.8.8.8", 0))])

        assert _codes(uri="smtp://relay.example.com", **OK, secure=False) == [MailProblemCode.CLEARTEXT_PUBLIC_HOST]

    def test_an_unresolvable_cleartext_relay_is_reported(self, monkeypatch):
        def refuse(*_a):
            raise socket.gaierror

        monkeypatch.setattr(network.socket, "getaddrinfo", refuse)

        assert _codes(uri="smtp://relay.example.com", **OK, secure=False) == [
            MailProblemCode.CLEARTEXT_HOST_UNRESOLVABLE
        ]

    def test_every_code_has_a_case(self):
        covered = {code for code, _ in CASES} | {
            MailProblemCode.CLEARTEXT_PUBLIC_HOST,
            MailProblemCode.CLEARTEXT_HOST_UNRESOLVABLE,
        }

        assert covered == set(MailProblemCode)

    def test_codes_are_lowercase_snake_case(self):
        assert all(code.value == code.name.lower() for code in MailProblemCode)


class TestValidSettings:
    def test_unset_is_not_a_problem(self):
        assert mail_problems(MailSettings()) == []
        assert mail_problems(MailSettings(uri="", secure=False, username="u")) == []

    @pytest.mark.parametrize(
        "mail",
        [
            {"uri": "memory://"},
            {"uri": "smtps://relay.example.com", **OK},
            {"uri": "smtp://relay.example.com:2525", **OK, "username": "u", "password": "p"},
            {"uri": "smtp://relay.example.com", **OK, "secure": False},
        ],
    )
    def test_sound_settings_have_no_problems(self, mail):
        assert mail_problems(MailSettings(**mail)) == []

    def test_memory_with_non_ascii_credentials_still_builds(self):
        mail = MailSettings(uri="memory://", username="ü", password="ü")

        assert mail_problems(mail) == []
        assert container._build_mail(settings.JasilSettings(mail=mail), SystemClock()) is not None

    def test_the_multi_process_memory_rule_is_not_included(self):
        assert mail_problems(MailSettings(uri="memory://")) == []


class TestCollection:
    def test_several_problems_are_reported_together(self):
        codes = _codes(uri="smtps://relay.example.com", from_address="no address", username="u", ehlo_hostname="a b")

        assert codes == [
            MailProblemCode.CREDENTIALS_INCOMPLETE,
            MailProblemCode.INVALID_FROM_ADDRESS,
            MailProblemCode.INVALID_EHLO_HOSTNAME,
        ]

    def test_a_uri_error_skips_the_checks_that_need_a_parsed_uri(self):
        codes = _codes(uri="imap://relay.example.com", from_address="no address", secure=False)

        assert codes == [MailProblemCode.UNSUPPORTED_SCHEME, MailProblemCode.INVALID_FROM_ADDRESS]

    def test_only_the_first_uri_error_is_reported(self):
        assert _codes(uri="smtps://u:p@relay.example.com/path", **OK) == [MailProblemCode.URI_CARRIES_CREDENTIALS]

    def test_the_order_is_deterministic(self):
        mail = MailSettings(uri="smtps://:0", from_address="x y", username="ü", ehlo_hostname="a b", secure=False)

        assert [p.code for p in mail_problems(mail)] == [p.code for p in mail_problems(mail)]

    def test_the_problems_are_frozen_value_objects(self):
        problem = MailProblem(MailProblemCode.INVALID_URI, "mail.uri", "m")

        with pytest.raises(AttributeError):
            problem.message = "x"  # type: ignore[misc]


MULTI = [
    {"uri": "smtps://u:p@relay.example.com/path", "from_address": "x y", "username": "u"},
    {"uri": "smtps://relay.example.com:0", "from_address": "", "secure": False, "username": "u", "password": "p"},
    {
        "uri": "smtp://relay.example.com",
        "from_address": "x y",
        "ehlo_hostname": "a b",
        "username": "ü",
        "password": "p",
    },
    {"uri": "memory://host", "secure": False, "username": "u"},
    {"uri": "smtps://", "from_address": "", "secure": False},
]


class TestTheBuilderRaisesTheFirstProblem:
    @pytest.mark.parametrize("mail", [m for _, m in CASES] + MULTI)
    def test_the_build_error_is_the_first_reported_problem(self, mail):
        cfg = settings.JasilSettings(mail=MailSettings(**mail))
        problems = mail_problems(cfg.mail)

        with pytest.raises(ValueError) as raised:
            container._build_mail(cfg, SystemClock())

        assert str(raised.value) == problems[0].message


class TestNothingSecretIsEchoed:
    SECRETS = ("hunter2-pw", "svc-user", "relay-secret.example.com")

    @pytest.mark.parametrize(
        "mail",
        [
            {
                "uri": "smtps://svc-user:hunter2-pw@relay-secret.example.com/x",
                "from_address": "bad",
                "ehlo_hostname": "b d",
            },
            {
                "uri": "smtp://relay-secret.example.com",
                "username": "svc-user",
                "password": "hunter2-pw",
                "secure": False,
            },
            {"uri": " relay-secret.example.com:svc-user:hunter2-pw"},
            {"uri": "smtps://relay-secret.example.com:0", "username": "svc-üser", "password": "hunter2-pw"},
        ],
    )
    def test_no_value_appears_in_a_message_or_field(self, mail):
        problems = mail_problems(MailSettings(**mail))

        assert problems
        for problem in problems:
            for secret in self.SECRETS:
                assert secret not in problem.message + problem.field
            assert problem.field in {
                "mail.uri",
                "mail.from_address",
                "mail.username",
                "mail.password",
                "mail.secure",
                "mail.ehlo_hostname",
            }
