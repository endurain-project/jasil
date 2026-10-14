"""``MailProvider`` backends that never touch a network.

``NullMail`` is what an unconfigured platform gets, so the domain has one
code path and always just calls ``send``. ``MemoryMail`` records messages for a
host's test suite.
"""

import copy
import threading
from dataclasses import replace
from email.message import EmailMessage

from jasil._core.mail_message import build_mime
from jasil.providers import ClockProvider, MailMessage, MailNotConfiguredError

# Used only when ``memory://`` is configured without a ``from_address``.
_DEFAULT_SENDER = "jasil@localhost"


class NullMail:
    """``MailProvider`` for a deployment with no mail backend configured.

    The counterpart of ``NullGeocoding``: "unconfigured" is an explicit backend,
    so the domain always has a provider and never branches on whether mail
    exists. The one deliberate difference is that ``send`` raises instead of
    returning nothing, because mail is not best-effort enrichment.
    """

    def send(self, message: MailMessage) -> str:
        """Always raise: a message must never be dropped silently.

        Raises:
            MailNotConfiguredError: Always.
        """
        raise MailNotConfiguredError("Mail is not configured")


class MemoryMail:
    """``MailProvider`` that appends each message to :attr:`outbox`.

    Messages are validated and given a real ``Message-ID`` by the same builder
    the SMTP backend uses, so a test fails on the same invalid message
    production would reject.

    Args:
        clock: Time source for the ``Date`` header.
        from_address: Sender identity; a placeholder when empty.
    """

    def __init__(self, clock: ClockProvider, from_address: str = "") -> None:
        self._clock = clock
        self._from_address = from_address or _DEFAULT_SENDER
        self._lock = threading.Lock()
        self.outbox: list[MailMessage] = []
        #: The built MIME message for each ``outbox`` entry, same order, so a
        #: test can assert on the generated headers and ``Message-ID``.
        self.built: list[EmailMessage] = []

    def send(self, message: MailMessage) -> str:
        """Validate ``message``, record it, and return its ``Message-ID``.

        Raises:
            ValueError: When the message is invalid.
        """
        mime = build_mime(message, sender=self._from_address, now=self._clock.now())
        # Recipients copied as sent: the caller's list may be reused or mutated
        # after ``send`` returns, which would rewrite what the test asserts on. A
        # shallow copy keeps the type, so ``outbox == [message]`` still holds.
        recorded = replace(message, to=copy.copy(message.to))
        with self._lock:
            self.outbox.append(recorded)
            self.built.append(mime)
        return str(mime["Message-ID"])

    def clear(self) -> None:
        """Forget every recorded message, e.g. between two phases of a test."""
        with self._lock:
            self.outbox.clear()
            self.built.clear()
