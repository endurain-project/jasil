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
import functools
import logging
import smtplib
import socket
import ssl
import threading
import time
from email.message import EmailMessage

import jasil._core.network as network
from jasil._core.mail_message import build_mime, parse_address
from jasil.providers import ClockProvider, MailError, MailMessage

logger = logging.getLogger(__name__)

# Applied to every socket operation of a send (seconds).
_TIMEOUT_SECONDS = 30
# Ceiling on one ``send`` (public: the composition root compares the job lease to
# it). It bounds one message, not one job: a handler sending N messages can take N
# times this, plus the DNS lookups it cannot interrupt, and its lease must cover
# all of that or the reaper requeues it mid-delivery. A socket timeout alone
# cannot enforce it: every read restarts the timeout, so a server dripping its
# reply a byte at a time never trips one. A watchdog shuts the socket down when it expires, and
# keeps doing so until the send ends: the socket may not exist yet at the
# deadline (a slow connect), and a single shutdown would then be missed. The
# watchdog holds a duplicate of the raw socket, because during a TLS handshake
# ``connection.sock`` is the pre-wrap socket, which the wrap has already detached.
# A DNS lookup is the one step it cannot interrupt.
SEND_DEADLINE_SECONDS = 90
_WATCH_INTERVAL_SECONDS = 0.1
_DEADLINE_EXCEEDED = "SMTP send exceeded its overall deadline"


def _default_local_hostname() -> str:
    """The EHLO name when none is configured: this host's name, if usable.

    ``smtplib`` ASCII-encodes commands, so a name that is not a valid hostname
    would surface as a ``UnicodeEncodeError`` from every send.
    """
    name = socket.gethostname()
    return name if name and network.is_valid_hostname(name) else "localhost"


@functools.cache
def _tracked(base: type[smtplib.SMTP]) -> type[smtplib.SMTP]:
    """Subclass ``base`` so the raw socket it dials stays reachable to the watchdog.

    ``watched`` is a duplicate of the connected socket made *before* any TLS wrap,
    so shutting it down unblocks a handshake that is still reading.

    This leans on private ``smtplib`` details: ``SMTP._get_socket``, and the
    ``_host``, ``context`` and ``source_address`` attributes of ``SMTP_SSL``.
    ``tests/test_mail.py`` pins their shape so an interpreter change fails loudly
    instead of silently disarming the watchdog.
    """

    class Tracked(base):  # type: ignore[valid-type, misc]
        watched: socket.socket | None = None
        deadline: float | None = None

        def _dial(self, host: str, port: int, timeout: float) -> socket.socket:
            """Connect like ``socket.create_connection``, stopping at the deadline.

            ``create_connection`` gives every address of a multi-homed name the
            full timeout, which the watchdog cannot interrupt (there is no socket
            yet), so a few dead addresses would run the connect past the deadline.
            """
            source = getattr(self, "source_address", None)
            if self.deadline is None:
                return socket.create_connection((host, port), timeout, source)
            error: OSError = TimeoutError(_DEADLINE_EXCEEDED)
            for family, kind, proto, _, address in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    break
                candidate = socket.socket(family, kind, proto)
                try:
                    candidate.settimeout(min(timeout, remaining))
                    if source:
                        candidate.bind(source)
                    candidate.connect(address)
                    return candidate
                except OSError as failure:
                    error = failure
                    candidate.close()
            raise error

        def _get_socket(self, host: str, port: int, timeout: float) -> socket.socket:
            if issubclass(base, smtplib.SMTP_SSL):
                raw = self._dial(host, port, timeout)
                self.watched = raw.dup()
                try:
                    return self.context.wrap_socket(raw, server_hostname=self._host)
                except BaseException:
                    raw.close()
                    raise
            raw = self._dial(host, port, timeout)
            self.watched = raw.dup()
            return raw

    return Tracked


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
        # Parsed here so a bad sender fails at build time, not as a ``ValueError``
        # from every ``send`` that a job runner would retry to exhaustion.
        parse_address(from_address, role="sender")
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
        if finished.wait(SEND_DEADLINE_SECONDS):
            return
        expired.set()
        while not finished.is_set():
            sock = getattr(connection, "watched", None) or getattr(connection, "sock", None)
            if sock is not None:
                # The plain-socket method, on purpose: ``SSLSocket.shutdown`` also
                # tears down TLS state the blocked thread is still using.
                with contextlib.suppress(OSError):
                    socket.socket.shutdown(sock, socket.SHUT_RDWR)
            finished.wait(_WATCH_INTERVAL_SECONDS)

    def _connect_hosts(self) -> list[str]:
        """The hosts to dial, in order: the configured name, or vetted addresses.

        Cleartext is allowed only to a private relay, and the name was checked
        once at startup. DNS can change since, so it is resolved and checked again
        for each send, and the connection goes to an address that was checked
        rather than to the name, so it cannot be re-resolved in between.

        Raises:
            MailError: When the host no longer resolves to private addresses.
        """
        if self._tls:
            return [self._host]
        addresses, reason = network.resolve_private_addresses(self._host)
        if reason is not None or not addresses:
            raise MailError("SMTP relay no longer resolves to a private address; cleartext refused")
        return addresses

    def _connect(self, connection: smtplib.SMTP, deadline: float) -> tuple[int, bytes]:
        """Connect to the first reachable host; ``smtplib`` would also try each address."""
        hosts = self._connect_hosts()
        for index, host in enumerate(hosts):
            # A connect is not interruptible by the watchdog, so do not start one
            # after the deadline.
            if time.monotonic() >= deadline:
                raise MailError(_DEADLINE_EXCEEDED)
            # ``connection.timeout`` bounds each address tried; cap it to the time
            # left, and the dial itself stops at the deadline between addresses,
            # so a connect begun near the deadline cannot run past it.
            connection.timeout = max(min(_TIMEOUT_SECONDS, deadline - time.monotonic()), 0.001)
            try:
                return connection.connect(host, self._port)
            # ``SMTPException`` is an ``OSError``, so this also covers a server
            # that drops the connection before greeting (``SMTPServerDisconnected``).
            except OSError:
                if index == len(hosts) - 1:
                    raise
                # A socket left half-open by the failed attempt must not leak.
                connection.close()
                watched = getattr(connection, "watched", None)
                if watched is not None:
                    connection.watched = None  # type: ignore[attr-defined]
                    watched.close()
        raise AssertionError("unreachable")  # pragma: no cover

    def _deliver(self, mime: EmailMessage) -> dict[str, tuple[int, bytes]]:
        """Connect, secure, authenticate and send; returns refused recipients."""
        context = self._context
        deadline = time.monotonic() + SEND_DEADLINE_SECONDS
        # Built without a host so nothing is dialed before the watchdog is armed:
        # the greeting is a read like any other, and can be dripped as slowly.
        connection: smtplib.SMTP
        if self._implicit_tls:
            connection = _tracked(smtplib.SMTP_SSL)(  # type: ignore[call-arg]
                local_hostname=self._local_hostname, timeout=_TIMEOUT_SECONDS, context=context
            )
        else:
            connection = _tracked(smtplib.SMTP)(local_hostname=self._local_hostname, timeout=_TIMEOUT_SECONDS)
        connection.deadline = deadline  # type: ignore[attr-defined]
        expired = threading.Event()
        finished = threading.Event()
        watchdog = threading.Thread(target=self._watch, args=(connection, expired, finished), daemon=True)
        watchdog.start()
        try:
            try:
                code, reply = self._connect(connection, deadline)
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
                # The watchdog's shutdown surfaces as a dropped connection, and the
                # last step's socket timeout is the time left, so it can fire first.
                if expired.is_set() or time.monotonic() >= deadline:
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
            watched = getattr(connection, "watched", None)
            if watched is not None:
                watched.close()

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
