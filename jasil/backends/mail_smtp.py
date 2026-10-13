"""SMTP ``MailProvider`` backend.

Security:

- TLS is mandatory and verified (``ssl.create_default_context``); there is no
  cleartext mode and no opt-out. ``smtp://`` upgrades with STARTTLS and refuses
  a server that does not offer it, rather than falling back.
- AUTH happens only after TLS is up.
- Nothing from the server or the message reaches an exception or a log line:
  :class:`~jasil.providers.MailError` carries a category and status code only,
  and is raised ``from None``, because a job runner persists ``str(error)``.

One connection per ``send``: stateless, thread-safe, nothing to close.
"""

import logging
import smtplib
import ssl

from jasil._core.mail_message import build_mime
from jasil.providers import ClockProvider, MailError, MailMessage

logger = logging.getLogger(__name__)

# Applied to every socket operation of a send (seconds).
_TIMEOUT_SECONDS = 30


class SmtpMail:
    """``MailProvider`` that delivers through an SMTP relay.

    Args:
        host: Relay hostname.
        port: Relay port.
        implicit_tls: Connect over TLS (``smtps``) instead of STARTTLS.
        from_address: Sender identity (``From`` header; the envelope sender is
            its bare address). Independent of ``username``.
        clock: Time source for the ``Date`` header.
        username: Login, or ``None`` for an unauthenticated relay.
        password: Password for ``username``.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        implicit_tls: bool,
        from_address: str,
        clock: ClockProvider,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._implicit_tls = implicit_tls
        self._from_address = from_address
        self._clock = clock
        self._username = username
        self._password = password

    def send(self, message: MailMessage) -> str:
        """Build and deliver ``message``, returning its ``Message-ID``.

        Raises:
            ValueError: When the message is invalid.
            MailError: When the server could not be reached or refused the
                message. If only *some* recipients are refused the send succeeds
                and a warning records the count, since failing would make a
                retry re-send to recipients that already got it.
        """
        mime = build_mime(message, sender=self._from_address, now=self._clock.now())
        message_id = str(mime["Message-ID"])
        try:
            refused = self._deliver(mime)
        except MailError:
            raise
        except smtplib.SMTPAuthenticationError as error:
            raise MailError(f"SMTP authentication failed (status {error.smtp_code})") from None
        except smtplib.SMTPRecipientsRefused:
            raise MailError("SMTP server refused every recipient") from None
        except smtplib.SMTPResponseException as error:
            raise MailError(f"SMTP server rejected the message (status {error.smtp_code})") from None
        except ssl.SSLError:
            raise MailError("TLS negotiation with the SMTP server failed") from None
        except (smtplib.SMTPException, OSError):
            raise MailError("SMTP server unreachable or connection lost") from None
        if refused:
            logger.warning(
                "Mail %s accepted with %d of %d recipients refused",
                message_id,
                len(refused),
                len(message.to),
            )
        logger.info("Mail %s sent to %d recipient(s)", message_id, len(message.to))
        return message_id

    def _deliver(self, mime) -> dict:
        """Connect, secure, authenticate and send; returns refused recipients."""
        context = ssl.create_default_context()
        connection: smtplib.SMTP
        if self._implicit_tls:
            connection = smtplib.SMTP_SSL(self._host, self._port, timeout=_TIMEOUT_SECONDS, context=context)
        else:
            connection = smtplib.SMTP(self._host, self._port, timeout=_TIMEOUT_SECONDS)
        with connection as smtp:
            if not self._implicit_tls:
                smtp.ehlo()
                if not smtp.has_extn("starttls"):
                    raise MailError("SMTP server does not offer STARTTLS")
                smtp.starttls(context=context)
                smtp.ehlo()
            if self._username and self._password:
                smtp.login(self._username, self._password)
            return smtp.send_message(mime)
