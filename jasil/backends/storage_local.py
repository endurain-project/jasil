"""Local-filesystem ``StorageProvider`` backend."""

import logging
from pathlib import Path
from urllib.parse import quote

from jasil._core.storage_keys import check_segment

logger = logging.getLogger(__name__)

#: Mirrors the ``StorageProvider.url`` protocol default. Used as a sentinel for
#: "the caller did not ask for a particular lifetime", which is the only way to
#: tell an indifferent caller from one that will not get what it asked for.
_DEFAULT_URL_EXPIRY_SECONDS = 3600


class LocalStorage:
    """``StorageProvider`` storing blobs as files under a per-area subdirectory.

    A blob for ``(area, key)`` lives at ``{base_dir}/{area}/{key}`` and is served
    at ``{url_prefix}/{area}/{key}``. Keys are server-generated (e.g. ``42.webp``);
    both area and key are validated so a stray value can never escape the base
    directory.

    **:meth:`url` cannot expire.** It returns a plain path for the host's own web
    server to serve, and JASIL neither runs that server nor holds a key to sign
    with — so access control here is the host's, and ``expires_in`` is ignored.
    The S3 backend, which does hold credentials, honours it.

    Args:
        base_dir: Absolute storage root every area subdirectory lives under.
        url_prefix: URL path prefix returned by :meth:`url` (default: root).
    """

    def __init__(self, base_dir: str, url_prefix: str = "") -> None:
        self._base = Path(base_dir)
        self._url_prefix = url_prefix.rstrip("/")
        self._warned_about_expiry = False

    def _resolve(self, area: str, key: str) -> Path:
        """Resolve ``(area, key)`` to an absolute path, rejecting traversal outside base."""
        check_segment(area, "area")
        check_segment(key, "key")
        base = self._base.resolve()
        candidate = (base / area / key).resolve()
        if not candidate.is_relative_to(base):
            raise ValueError(f"Storage key escapes base directory: {area}/{key!r}")
        return candidate

    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        path = self._resolve(area, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return key

    def get(self, area: str, key: str) -> bytes | None:
        path = self._resolve(area, key)
        if not path.is_file():
            return None
        return path.read_bytes()

    def exists(self, area: str, key: str) -> bool:
        return self._resolve(area, key).is_file()

    def delete(self, area: str, key: str) -> None:
        path = self._resolve(area, key)
        if path.is_file():
            path.unlink()

    def list_keys(self, area: str, prefix: str = "") -> list[str]:
        check_segment(area, "area")
        if prefix:
            check_segment(prefix, "prefix")
        base = self._base.resolve()
        area_dir = (base / area).resolve()
        if not area_dir.is_relative_to(base) or not area_dir.is_dir():
            return []
        keys = []
        # Recursive, because ``save`` accepts a nested key and creates the
        # directories for it. Listing only the top level would hide those blobs
        # here while the S3 backend, whose listing is a flat prefix scan, returned
        # them. ``rglob`` does not descend into symlinked directories.
        for candidate in area_dir.rglob("*"):
            # Never follow a symlink out of the area directory.
            resolved = candidate.resolve()
            if not resolved.is_relative_to(area_dir) or not resolved.is_file():
                continue
            key = candidate.relative_to(area_dir).as_posix()
            if key.startswith(prefix):
                keys.append(key)
        return sorted(keys)

    def url(self, area: str, key: str, expires_in: int = _DEFAULT_URL_EXPIRY_SECONDS) -> str:
        check_segment(area, "area")
        check_segment(key, "key")
        if expires_in != _DEFAULT_URL_EXPIRY_SECONDS and not self._warned_about_expiry:
            # Once per backend: ``url`` is called per serialized record, and a
            # caller who has to be told this has to be told it exactly once.
            self._warned_about_expiry = True
            logger.warning(
                "Local storage cannot expire a URL, so expires_in=%s was ignored and the link is permanent. "
                "Restrict %r in the web server that serves it, or use the s3:// backend for signed URLs.",
                expires_in,
                self._url_prefix or "/",
            )
        # Percent-encoded, because a key is only validated against traversal: one
        # holding ``?`` or ``#`` would otherwise end the path early, and ``%``
        # would change it. ``/`` stays safe — a key may be nested.
        return f"{self._url_prefix}/{quote(area, safe='/')}/{quote(key, safe='/')}"
