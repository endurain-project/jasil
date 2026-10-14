"""SMTP ``MailProvider`` backend.

Security:

- TLS is verified (``ssl.create_default_context``). ``smtp://`` upgrades with
  STARTTLS and refuses a server that does not offer it, rather than falling
  back. The only opt-out is ``tls=False``, which the composition root grants
  solely to a private-network relay and which refuses credentials outright.
- AUTH happens only after TLS is up, and never without it.
- Nothing from the server or the message reaches an exception or a log line:
  :class:`~jasil.providers.MailError` carries a category and status code only,
  and is raised ``from None``, because a job runner persists ``str(error)``.

One connection per ``send``: stateless, thread-safe, nothing to close.
"""

import contextlib
import logging
import smtplib
import socket
import ssl
import threading
import time
from email.message import EmailMessage

import jasil._core.network as network
from jasil._core.mail_message import build_mime
from jasil.providers import ClockProvider, MailError, MailMessage

logger = logging.getLogger(__name__)

# Applied to every socket operation of a send (seconds).
_TIMEOUT_SECONDS = 30
# Ceiling on a whole send, so a slow server cannot outlive a job lease and be
# requeued while still being delivered to. A socket timeout alone cannot enforce
# it: every read restarts the timeout, so a server dripping its reply a byte at a
# time never trips one. A watchdog shuts the socket down when it expires, and
# keeps doing so until the send ends: the socket may not exist yet at the
# deadline (a slow connect), and a single shutdown would then be missed.
_DEADLINE_SECONDS = 90
_WATCH_INTERVAL_SECONDS = 0.1
_DEADLINE_EXCEEDED = "SMTP send exceeded its overall deadline"


def _default_local_hostname() -> str:
    """The EHLO name when none is configured: this host's name, if usable.

    ``smtplib`` ASCII-encodes commands, so a name that is not a valid hostname
    would surface as a ``UnicodeEncodeError`` from every send.
    """
    name = socket.gethostname()
    return name if name and network.is_valid_hostname(name) else "localhost"


class SmtpMail:
    """``MailProvider`` that delivers through an SMTP relay.

    Args:
        host: Relay hostname.
        port: Relay port.
        implicit_tls: Connect over TLS (``smtps``) instead of STARTTLS.
        tls: ``False`` sends in cleartext (no STARTTLS). Cannot be combined with
            credentials or ``implicit_tls``.
        from_address: Sender identity (``From`` header; the envelope sender is
            its bare address). Independent of ``username``.
        clock: Time source for the ``Date`` header.
        username: Login, or ``None`` for an unauthenticated relay.
        password: Password for ``username``.
        local_hostname: Name announced in ``EHLO``. Defaults to this host's name;
            set a fully qualified one for a relay that rejects a bare hostname.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        implicit_tls: bool,
        tls: bool = True,
        from_address: str,
        clock: ClockProvider,
        username: str | None = None,
        password: str | None = None,
        local_hostname: str | None = None,
    ) -> None:
        if bool(username) != bool(password):
            raise ValueError("username and password must be set together")
        # ``smtplib`` ASCII-encodes credentials; refuse here, at build time, rather
        # than raising a ``UnicodeEncodeError`` from every ``send``.
        if not ((username or "").isascii() and (password or "").isascii()):
            raise ValueError("username and password must be ASCII")
        if not tls and (username or password):
            raise ValueError("credentials are never sent without TLS")
        if not tls and implicit_tls:
            raise ValueError("implicit TLS cannot be combined with tls=False")
        self._host = host
        self._port = port
        self._implicit_tls = implicit_tls
        self._tls = tls
        # Built once: loading the trust store on every send is wasted work.
        self._context = ssl.create_default_context()
        self._from_address = from_address
        self._clock = clock
        self._username = username
        self._password = password
        # An explicit name: otherwise ``smtplib`` calls ``socket.getfqdn()`` on
        # every connection, a DNS lookup that no socket timeout bounds.
        self._local_hostname = local_hostname or _default_local_hostname()

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
        except smtplib.SMTPRecipientsRefused as error:
            # Codes only: 4xx (greylisting, retry) and 5xx (unknown user) differ.
            codes = sorted({code for code, _ in error.recipients.values()})
            raise MailError("SMTP server refused every recipient (status " + ", ".join(map(str, codes)) + ")") from None
        except smtplib.SMTPConnectError as error:
            raise MailError(f"SMTP server refused the connection (status {error.smtp_code})") from None
        except smtplib.SMTPResponseException as error:
            raise MailError(f"SMTP server rejected the message (status {error.smtp_code})") from None
        except smtplib.SMTPNotSupportedError:
            raise MailError("SMTP server lacks a required feature (AUTH or SMTPUTF8)") from None
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
        logger.info("Mail %s sent to %d recipient(s)", message_id, len(message.to) - len(refused))
        return message_id

    @staticmethod
    def _arm(connection: smtplib.SMTP, deadline: float) -> None:
        """Cap the next step to the time left, or abort when none is."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MailError(_DEADLINE_EXCEEDED)
        sock = getattr(connection, "sock", None)
        if sock is not None:
            sock.settimeout(min(_TIMEOUT_SECONDS, remaining))

    @staticmethod
    def _watch(connection: smtplib.SMTP, expired: threading.Event, finished: threading.Event) -> None:
        """Watchdog: at the deadline, flag it and keep unblocking reads until done."""
        if finished.wait(_DEADLINE_SECONDS):
            return
        expired.set()
        while not finished.is_set():
            sock = getattr(connection, "sock", None)
            if sock is not None:
                # The plain-socket method, on purpose: ``SSLSocket.shutdown`` also
                # tears down TLS state the blocked thread is still using.
                with contextlib.suppress(OSError):
                    socket.socket.shutdown(sock, socket.SHUT_RDWR)
            finished.wait(_WATCH_INTERVAL_SECONDS)

    def _connect_host(self) -> str:
        """The host to dial: the configured name, or for cleartext a vetted address.

        Cleartext is allowed only to a private relay, and the name was checked
        once at startup. DNS can change since, so it is resolved and checked again
        for each send, and the connection goes to the address that was checked
        rather than to the name, so it cannot be re-resolved in between.

        Raises:
            MailError: When the host no longer resolves to private addresses.
        """
        if self._tls:
            return self._host
        addresses, reason = network.resolve_private_addresses(self._host)
        if reason is not None or not addresses:
            raise MailError("SMTP relay no longer resolves to a private address; cleartext refused")
        return addresses[0]

    def _deliver(self, mime: EmailMessage) -> dict[str, tuple[int, bytes]]:
        """Connect, secure, authenticate and send; returns refused recipients."""
        context = self._context
        deadline = time.monotonic() + _DEADLINE_SECONDS
        # Built without a host so nothing is dialed before the watchdog is armed:
        # the greeting is a read like any other, and can be dripped as slowly.
        connection: smtplib.SMTP
        if self._implicit_tls:
            connection = smtplib.SMTP_SSL(
                local_hostname=self._local_hostname, timeout=_TIMEOUT_SECONDS, context=context
            )
        else:
            connection = smtplib.SMTP(local_hostname=self._local_hostname, timeout=_TIMEOUT_SECONDS)
        expired = threading.Event()
        finished = threading.Event()
        watchdog = threading.Thread(target=self._watch, args=(connection, expired, finished), daemon=True)
        watchdog.start()
        try:
            try:
                code, reply = connection.connect(self._connect_host(), self._port)
                if code != 220:
                    raise smtplib.SMTPConnectError(code, reply)
                if self._tls and not self._implicit_tls:
                    self._arm(connection, deadline)
                    connection.ehlo()
                    if not connection.has_extn("starttls"):
                        raise MailError("SMTP server does not offer STARTTLS")
                    self._arm(connection, deadline)
                    connection.starttls(context=context)
                    connection.ehlo()
                if self._username and self._password:
                    self._arm(connection, deadline)
                    self._login(connection, self._username, self._password)
                self._arm(connection, deadline)
                # Explicit envelope sender: the bare address, never the display name.
                return connection.send_message(mime, from_addr=mime["From"].addresses[0].addr_spec)
            except (smtplib.SMTPException, OSError):
                # The watchdog's shutdown surfaces as a dropped connection.
                if expired.is_set():
                    raise MailError(_DEADLINE_EXCEEDED) from None
                raise
        finally:
            # Not ``with``: its exit raises on a non-221 reply to QUIT, which would
            # report a message the server already accepted as a failure (a retry
            # would then send it twice). Still under the watchdog, so QUIT is bounded.
            with contextlib.suppress(smtplib.SMTPException, OSError):
                connection.quit()
            finished.set()
            connection.close()

    @staticmethod
    def _login(connection: smtplib.SMTP, username: str, password: str) -> None:
        """Authenticate, naming the one failure ``smtplib`` raises untyped."""
        try:
            connection.login(username, password)
        except smtplib.SMTPException as error:
            # A bare ``SMTPException`` from ``login`` means the server advertises
            # AUTH but no mechanism ``smtplib`` implements.
            if type(error) is smtplib.SMTPException:
                raise MailError("SMTP server offers no supported authentication mechanism") from None
            raise
