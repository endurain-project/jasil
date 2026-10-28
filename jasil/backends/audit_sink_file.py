"""Write-once ``AuditSink`` backed by a directory.

Selected by ``audit.external_sink_uri = "file:///absolute/directory"``. Point it at a mount the
application can write but not rewrite — an append-only volume, or a share the host copies off — for the
copy to be worth anything; on the application's own writable disk it only guards against accidents.
"""

import os
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

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
            uri: A ``file://`` URI with an absolute path and no host.

        Returns:
            The sink; the directory is created when missing.

        Raises:
            ValueError: When the URI names a host or a relative path.
        """
        parsed = urlparse(uri)
        path = unquote(parsed.path)
        if parsed.netloc or not path.startswith("/"):
            raise ValueError("a file audit sink URI must be file:///absolute/directory")
        return cls(Path(path))

    def write_once(self, name: str, content: bytes) -> None:
        """Store ``content`` as ``name``, unless that file already exists with the same bytes.

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
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        except FileExistsError:
            if path.read_bytes() != content:
                raise AuditSinkConflictError(f"{name} already exists with different content") from None
            return
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
