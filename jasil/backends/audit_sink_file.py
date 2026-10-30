"""Write-once ``AuditSink`` backed by a directory.

Selected by ``audit.external_sink_uri = "file:///absolute/directory"`` (``file:///C:/directory`` on Windows).
Point it at a mount the application can write but not rewrite — an append-only volume, or a share the host
copies off — for the copy to be worth anything; on the application's own writable disk it only guards
against accidents.
"""

import contextlib
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from jasil.providers import AuditSinkConflictError

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class FileAuditSink:
    """``AuditSink`` that stores each object as a read-only file in one directory."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory
        directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_uri(cls, uri: str) -> "FileAuditSink":
        """Build a sink from ``file:///absolute/directory``.

        Args:
            uri: A ``file://`` URI with an absolute path and no host, query or fragment.

        Returns:
            The sink; the directory is created when missing.

        Raises:
            ValueError: When the URI names a host, a relative path, a query or a fragment (none of which
                the sink would honour).
        """
        parsed = urlparse(uri)
        path = Path(url2pathname(parsed.path))
        if parsed.netloc or parsed.query or parsed.fragment or not path.is_absolute():
            raise ValueError("a file audit sink URI must be file:///absolute/directory")
        return cls(path)

    def write_once(self, name: str, content: bytes) -> None:
        """Store ``content`` as ``name``, unless that file already exists with the same bytes.

        The bytes go to a temporary file first and are published under ``name`` with a hard link, which is
        atomic and refuses an existing name. A crash or a full disk therefore leaves no truncated file under
        the final name for a retry to trip over.

        Args:
            name: A flat file name.
            content: The bytes to store.

        Raises:
            ValueError: When ``name`` is not a plain file name.
            AuditSinkConflictError: When the file exists with different bytes.
        """
        if not _NAME.fullmatch(name):
            raise ValueError(f"invalid audit sink object name: {name!r}")
        path = self._directory / name
        descriptor, temporary = tempfile.mkstemp(dir=self._directory, prefix=".tmp-")
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != content:
                    raise AuditSinkConflictError(f"{name} already exists with different content") from None
                # An identical copy is there; still make sure its name is durable and it is read-only.
        finally:
            with contextlib.suppress(OSError):
                os.unlink(temporary)
        self._sync_directory()
        # Also on an identical copy: one published by a run that stopped before this line is still writable.
        # A copy that is already read-only is left alone, as it may belong to another user.
        if path.stat().st_mode & 0o222:
            path.chmod(0o400)

    def read(self, name: str) -> bytes | None:
        """Return the bytes stored as ``name``, or ``None`` when there is no such file.

        Raises:
            ValueError: When ``name`` is not a plain file name.
        """
        if not _NAME.fullmatch(name):
            raise ValueError(f"invalid audit sink object name: {name!r}")
        try:
            return (self._directory / name).read_bytes()
        except FileNotFoundError:
            return None

    def _sync_directory(self) -> None:
        """Make the new directory entry durable, so a crash cannot lose a checkpoint the database then keeps.

        Syncing the file does not sync the name that points to it. A failure propagates, so the checkpoint is
        not stored and the window is retried. Windows cannot open a directory for syncing and journals the
        metadata itself, so there it is skipped.
        """
        if os.name == "nt":
            return
        descriptor = os.open(self._directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
