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
    if not isinstance(value, str) or "\r" in value or "\n" in value:
        raise invalid
    parsed = getaddresses([value])
    if len(parsed) != 1:
        raise invalid
    display_name, addr_spec = parsed[0]
    local, separator, domain = addr_spec.rpartition("@")
    # The parser is lenient and silently repairs some input (``a@exa mple.com``
    # -> ``a@example.com``); mailing a different address than the caller wrote
    # is worse than refusing, so the parsed address must appear verbatim.
    if not (local and separator and domain) or addr_spec not in value:
        raise invalid
    if not local.isascii():
        raise invalid
    try:
        # An internationalised domain travels as its ASCII (punycode) form: the
        # header parser would otherwise emit an RFC 2047 encoded-word inside the
        # addr-spec, which is invalid, and a relay without SMTPUTF8 would refuse it.
        ascii_domain = domain.encode("idna").decode("ascii")
        return Address(display_name=display_name, username=local, domain=ascii_domain)
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
    recipients = [parse_address(value, role="recipient") for value in message.to]
    if not recipients:
        raise ValueError("A mail message needs at least one recipient")
    if not message.text and not message.html:
        raise ValueError("A mail message needs a text or html body")
    if not isinstance(message.subject, str) or "\r" in message.subject or "\n" in message.subject:
        raise ValueError("The mail subject must be a single line")
    from_address = parse_address(sender, role="sender")
    reply_to = parse_address(message.reply_to, role="reply-to") if message.reply_to else None

    mime = EmailMessage(policy=policy.SMTP)
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
