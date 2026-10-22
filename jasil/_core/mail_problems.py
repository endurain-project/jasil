"""Validate :class:`~jasil.settings.MailSettings` without building a backend.

The one copy of the mail configuration rules. The composition root raises the
first problem found here when it builds the backend, and a host can call
:func:`mail_problems` to validate operator input on save. Public names are
re-exported from :mod:`jasil.settings`.
"""

import ipaddress
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import jasil._core.network as network
from jasil._core.mail_message import parse_address

if TYPE_CHECKING:
    from jasil.settings import MailSettings

#: Default ports of the SMTP schemes.
SMTP_PORTS = {"smtp": 587, "smtps": 465}


class MailProblemCode(StrEnum):
    """One value per distinct mail configuration rule."""

    INVALID_URI = "invalid_uri"
    UNSUPPORTED_SCHEME = "unsupported_scheme"
    URI_CARRIES_CREDENTIALS = "uri_carries_credentials"
    URI_HAS_EXTRA_PARTS = "uri_has_extra_parts"
    INVALID_HOST = "invalid_host"
    INVALID_PORT = "invalid_port"
    MISSING_FROM_ADDRESS = "missing_from_address"
    INVALID_FROM_ADDRESS = "invalid_from_address"
    INVALID_EHLO_HOSTNAME = "invalid_ehlo_hostname"
    CREDENTIALS_INCOMPLETE = "credentials_incomplete"
    CREDENTIALS_NOT_ASCII = "credentials_not_ascii"
    MEMORY_HAS_HOST = "memory_has_host"
    MEMORY_CLEARTEXT_UNSUPPORTED = "memory_cleartext_unsupported"
    CLEARTEXT_WITH_IMPLICIT_TLS = "cleartext_with_implicit_tls"
    CLEARTEXT_WITH_CREDENTIALS = "cleartext_with_credentials"
    CLEARTEXT_PUBLIC_HOST = "cleartext_public_host"
    CLEARTEXT_HOST_UNRESOLVABLE = "cleartext_host_unresolvable"


@dataclass(frozen=True)
class MailProblem:
    """A single reason mail settings cannot be used.

    Attributes:
        code: Which rule was broken.
        field: The setting at fault: ``"mail.uri"``, ``"mail.from_address"``,
            ``"mail.username"``, ``"mail.password"``, ``"mail.secure"`` or
            ``"mail.ehlo_hostname"``.
        message: A human-readable reason. Never contains a setting's value
            beyond the URI scheme, so it is safe to log and to show an operator.
    """

    code: MailProblemCode
    field: str
    message: str


def mail_problems(mail: "MailSettings") -> list[MailProblem]:
    """List everything wrong with ``mail``, without building a backend.

    Pure function of the settings, apart from one DNS lookup: with
    ``secure=False`` on ``smtp://`` the host is resolved to confirm it is a
    private, loopback or link-local relay, so this needs a working resolver
    in that case. The multi-process ``memory://`` rule is not included; it
    needs the deployment topology (see
    :func:`~jasil.capabilities.check_mail_consistency`).

    Problems come in the order the builder would raise them, so
    ``problems[0]`` is the error a build fails with. Checks that do not depend
    on the URI always run; once the URI itself is malformed, the checks that
    need its parsed parts are skipped.

    Args:
        mail: The settings to validate.

    Returns:
        Every problem found; empty when ``mail.uri`` is unset (mail disabled)
        or the settings are sound.
    """
    if not mail.uri:
        return []
    problems: list[MailProblem] = []

    def add(code: MailProblemCode, field: str, message: str) -> None:
        problems.append(MailProblem(code, field, message))

    # The URI is never echoed: it is operator input and may carry a mistyped
    # credential. ``urlsplit`` quietly strips whitespace and accepts ``memory:``;
    # require the literal form so what is built is what the consistency check inspected.
    parts = None
    port = hostname = None
    if mail.uri != mail.uri.strip() or "://" not in mail.uri:
        add(MailProblemCode.INVALID_URI, "mail.uri", "mail.uri is not a valid URI")
    else:
        try:
            parts = urlsplit(mail.uri)
            port = parts.port
            hostname = parts.hostname
        except ValueError:
            parts = None
            add(MailProblemCode.INVALID_URI, "mail.uri", "mail.uri is not a valid URI")
    scheme = parts.scheme if parts is not None else None
    if parts is not None:
        if scheme not in ("memory", *SMTP_PORTS):
            add(
                MailProblemCode.UNSUPPORTED_SCHEME,
                "mail.uri",
                f"Unsupported mail.uri scheme: {scheme or '(none)'!r}",
            )
            parts = None
        elif parts.username is not None or parts.password is not None:
            add(
                MailProblemCode.URI_CARRIES_CREDENTIALS,
                "mail.uri",
                "mail.uri must not carry credentials; use mail.username and mail.password",
            )
            parts = None
        elif parts.path or parts.query or parts.fragment:
            add(
                MailProblemCode.URI_HAS_EXTRA_PARTS,
                "mail.uri",
                "mail.uri must be scheme://host[:port] with no path, query or fragment",
            )
            parts = None

    if bool(mail.username) != bool(mail.password):
        add(
            MailProblemCode.CREDENTIALS_INCOMPLETE,
            "mail.password" if mail.username else "mail.username",
            "mail.username and mail.password must be set together",
        )
    if mail.from_address:
        try:
            parse_address(mail.from_address, role="sender")
        except ValueError:
            add(
                MailProblemCode.INVALID_FROM_ADDRESS,
                "mail.from_address",
                "mail.from_address is not a valid email address",
            )
    if mail.ehlo_hostname is not None and not network.is_valid_hostname(mail.ehlo_hostname):
        add(MailProblemCode.INVALID_EHLO_HOSTNAME, "mail.ehlo_hostname", "mail.ehlo_hostname is not a valid hostname")

    if parts is not None and scheme == "memory":
        if parts.netloc:
            add(MailProblemCode.MEMORY_HAS_HOST, "mail.uri", "mail.uri for memory:// takes no host")
        if not mail.secure:
            add(
                MailProblemCode.MEMORY_CLEARTEXT_UNSUPPORTED,
                "mail.secure",
                "mail.secure=False applies only to smtp://",
            )
    elif parts is not None:
        host_valid = bool(hostname)
        if hostname:
            try:
                ipaddress.ip_address(hostname)
            except ValueError:
                host_valid = network.is_valid_hostname(hostname)
        if not host_valid:
            add(MailProblemCode.INVALID_HOST, "mail.uri", "mail.uri has no valid host")
        if port == 0:
            add(MailProblemCode.INVALID_PORT, "mail.uri", "mail.uri has an invalid port")
        if not mail.from_address:
            add(
                MailProblemCode.MISSING_FROM_ADDRESS,
                "mail.from_address",
                "mail.from_address is required for smtp:// and smtps://",
            )
        if not mail.secure:
            # Cleartext is an explicit, narrow exception: a relay on the same host or
            # private network (a local MTA, a dev catcher). AUTH is never sent over it.
            if scheme == "smtps":
                add(
                    MailProblemCode.CLEARTEXT_WITH_IMPLICIT_TLS,
                    "mail.secure",
                    "mail.secure=False cannot be combined with smtps://",
                )
            if mail.username or mail.password:
                add(
                    MailProblemCode.CLEARTEXT_WITH_CREDENTIALS,
                    "mail.secure",
                    "mail.secure=False cannot be combined with credentials",
                )
            if scheme == "smtp" and host_valid and hostname:
                reason = network.non_public_rejection_reason(hostname)
                if reason is not None:
                    add(
                        MailProblemCode.CLEARTEXT_HOST_UNRESOLVABLE
                        if network.is_unresolvable(reason)
                        else MailProblemCode.CLEARTEXT_PUBLIC_HOST,
                        "mail.secure",
                        f"mail.secure=False requires a private or loopback relay; the host {reason}",
                    )

    # ``smtplib`` ASCII-encodes credentials; refuse here rather than raising a
    # ``UnicodeEncodeError`` from every send. Only the SMTP schemes use them.
    if scheme != "memory" and not ((mail.username or "").isascii() and (mail.password or "").isascii()):
        add(
            MailProblemCode.CREDENTIALS_NOT_ASCII,
            "mail.username" if not (mail.username or "").isascii() else "mail.password",
            "username and password must be ASCII",
        )
    return problems
