"""Build the MIME message every mail backend sends.

One builder, shared by the in-memory and SMTP backends, so a host's tests fail on
exactly the invalid message production would reject.

Header-injection defence (CWE-93): addresses are *parsed*, never concatenated;
the ``SMTP`` policy refuses CR/LF in a header value; and a caller cannot set an
arbitrary header, because the message type has no field for one.

Error messages here deliberately omit the offending value: a ``ValueError`` from
a durable-job handler is persisted and logged, and an address is personal data.
"""

from collections.abc import Sequence
from datetime import datetime
from email import policy
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import format_datetime, getaddresses, make_msgid
from typing import Protocol

from jasil._core.network import is_valid_hostname

# Bodies are 7-bit safe (quoted-printable/base64 when non-ASCII): ``smtplib`` does
# not declare ``BODY=8BITMIME``, so a raw 8bit body is not valid for a strict relay.
_MIME_POLICY = policy.SMTP.clone(cte_type="7bit")


class MailMessageLike(Protocol):
    """The shape :func:`build_mime` reads; ``jasil.providers.MailMessage`` fits.

    Structural rather than imported: ``jasil._core`` must not depend on the
    provider ports.
    """

    @property
    def to(self) -> Sequence[str]: ...
    @property
    def subject(self) -> str: ...
    @property
    def text(self) -> str | None: ...
    @property
    def html(self) -> str | None: ...
    @property
    def reply_to(self) -> str | None: ...


def _has_control_character(value: str) -> bool:
    """True when ``value`` has a C0/C1 control character other than a tab."""
    return any((ord(c) < 32 and c != "\t") or 127 <= ord(c) < 160 for c in value)


def parse_address(value: str, *, role: str) -> Address:
    """Parse exactly one ``addr`` or ``Name <addr>`` into an :class:`Address`.

    Args:
        value: The address text.
        role: What the address is for (``"recipient"``, ``"sender"``...), used in
            the error message in place of the value.

    Returns:
        The parsed address.

    Raises:
        ValueError: When ``value`` is not exactly one well-formed address.
    """
    invalid = ValueError(f"The {role} address is not a single valid email address")
    if not isinstance(value, str) or _has_control_character(value):
        raise invalid
    # ``getaddresses`` accepts ``Name <addr`` (no closing bracket) and repairs it.
    # Conservative: a display name that itself holds ``<`` or ``>`` is refused too.
    if value.count("<") != value.count(">"):
        raise invalid
    parsed = getaddresses([value])
    if len(parsed) != 1:
        raise invalid
    display_name, addr_spec = parsed[0]
    local, separator, domain = addr_spec.rpartition("@")
    # The parser is lenient and silently repairs some input (``a@exa mple.com``
    # -> ``a@example.com``); mailing a different address than the caller wrote
    # is worse than refusing, so the parsed address must appear verbatim.
    # A substring test is not enough: ``alice:bob@example.com`` parses as a group
    # label plus ``bob@example.com``. The address must be the whole value, or the
    # one inside the angle brackets.
    # ``endswith``, not ``in``: ``"x <a@b.com>" <a@b c.com>`` would otherwise be
    # satisfied by the display name's text.
    written = value.rstrip().endswith(f"<{addr_spec}>") if "<" in value else value.strip() == addr_spec
    if not (local and separator and domain) or not written:
        raise invalid
    # A quoted local part would be re-quoted on output, so the address mailed
    # would not be the one the caller wrote.
    if not local.isascii() or '"' in local:
        raise invalid
    # A dot-atom local part (RFC 5322): a strict relay answers a leading, trailing
    # or doubled dot with a permanent 5xx, which would retry a job that can never
    # succeed. Refuse it here, as a ``ValueError``, instead.
    if local.startswith(".") or local.endswith(".") or ".." in local:
        raise invalid
    try:
        # An internationalised domain travels as its ASCII (punycode) form: the
        # header parser would otherwise emit an RFC 2047 encoded-word inside the
        # addr-spec, which is invalid, and a relay without SMTPUTF8 would refuse it.
        ascii_domain = domain.encode("idna").decode("ascii")
        # The ``idna`` codec is IDNA 2003 and silently maps some labels to a
        # different name (``straße.de`` -> ``strasse.de``, a final sigma, a
        # dropped joiner). Anything that does not round-trip is refused.
        if not is_valid_hostname(ascii_domain):
            raise invalid
        # An all-numeric last label is an IP written without brackets
        # (``a@1.2.3.4``), not a domain: a relay answers it with a permanent 5xx.
        if ascii_domain.rsplit(".", 1)[-1].isdigit():
            raise invalid
        if not domain.isascii() and ascii_domain.encode("ascii").decode("idna").lower() != domain.lower():
            raise invalid
        # A written ``xn--`` label must be valid punycode: a relay answers one that
        # is not with a permanent 5xx, which would retry a job that can never succeed.
        if any(label.startswith("xn--") for label in ascii_domain.lower().split(".")):
            ascii_domain.encode("ascii").decode("idna")
        address = Address(display_name=display_name, username=local, domain=ascii_domain)
        # ``Address`` re-quotes a local part holding a space, a backslash or other
        # specials; the address mailed must be the one the caller wrote.
        if address.addr_spec != f"{local}@{ascii_domain}":
            raise invalid
        return address
    except Exception:
        # Deliberately not chained: the parser's message quotes the address.
        raise invalid from None


def build_mime(message: MailMessageLike, *, sender: str, now: datetime) -> EmailMessage:
    """Validate ``message`` and build the MIME message to send.

    Args:
        message: The message to build.
        sender: The configured sender, ``addr`` or ``Name <addr>``.
        now: The ``Date`` header value, from the platform clock.

    Returns:
        A complete message with ``From``, ``To``, ``Date`` and ``Message-ID`` set.

    Raises:
        ValueError: When the message or sender is invalid.
    """
    if isinstance(message.to, str) or not isinstance(message.to, Sequence):
        raise ValueError("The mail recipients must be a sequence of addresses, not a single string")
    recipients = [parse_address(value, role="recipient") for value in message.to]
    if not recipients:
        raise ValueError("A mail message needs at least one recipient")
    for body in (message.text, message.html):
        if body is not None and not isinstance(body, str):
            raise ValueError("The mail text and html bodies must be strings")
    if not message.text and not message.html:
        raise ValueError("A mail message needs a text or html body")
    if not isinstance(message.subject, str) or _has_control_character(message.subject):
        raise ValueError("The mail subject must be a single line")
    from_address = parse_address(sender, role="sender")
    reply_to = parse_address(message.reply_to, role="reply-to") if message.reply_to else None

    mime = EmailMessage(policy=_MIME_POLICY)
    mime["From"] = from_address
    mime["To"] = recipients
    if reply_to is not None:
        mime["Reply-To"] = reply_to
    mime["Subject"] = message.subject
    mime["Date"] = format_datetime(now)
    mime["Message-ID"] = make_msgid(domain=from_address.domain)

    if message.text and message.html:
        mime.set_content(message.text)
        mime.add_alternative(message.html, subtype="html")
    elif message.html:
        mime.set_content(message.html, subtype="html")
    else:
        mime.set_content(message.text or "")
    return mime
