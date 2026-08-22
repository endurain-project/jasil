"""Async local-filesystem ``AsyncStorageProvider`` backend.

This is the awaitable twin of :mod:`jasil.backends.storage_local`; the
area/key model, path-traversal validation, and URL behaviour are all identical
— read that module for the full contract.

**Design decision — anyio thread offload.**
The standard-library filesystem calls used here (``Path.mkdir``,
``Path.write_bytes``, ``Path.read_bytes``, ``Path.unlink``, ``Path.rglob``,
``Path.is_file``, ``Path.resolve``) are synchronous, blocking system calls.
Rather than calling them directly on the event-loop thread — which would stall
every other coroutine for the duration of the disk I/O — every method wraps its
filesystem work in :func:`anyio.to_thread.run_sync`, which dispatches to a
worker-thread pool managed by the current async backend (asyncio or trio). The
event loop remains free during the wait.

This is a deliberate trade-off: the worker-thread pool has a bounded size, and
at high concurrency threads queue behind each other rather than running in
parallel. If true async filesystem I/O is needed (e.g. ``aiofiles``), the
protocol is stable and the backend can be swapped without changing any caller.
"""

import logging
from pathlib import Path
from urllib.parse import quote

import anyio
import anyio.to_thread

from jasil._core.storage_keys import check_segment

logger = logging.getLogger(__name__)

#: Mirrors the ``StorageProvider.url`` protocol default — same sentinel as the
#: sync module so callers that share configuration across both faces see the same
#: constant.
_DEFAULT_URL_EXPIRY_SECONDS = 3600


class AsyncLocalStorage:
    """``AsyncStorageProvider`` storing blobs as files under a per-area subdirectory.

    A blob for ``(area, key)`` lives at ``{base_dir}/{area}/{key}`` and is served
    at ``{url_prefix}/{area}/{key}``. Keys are server-generated (e.g. ``42.webp``);
    both area and key are validated so a stray value can never escape the base
    directory.

    **:meth:`url` cannot expire.** It returns a plain path for the host's own web
    server to serve. JASIL neither runs that server nor holds a signing key, so
    access control is the host's and ``expires_in`` is ignored. The S3 async
    backend, which does hold credentials, honours it.

    All filesystem operations are dispatched to a worker thread via
    ``anyio.to_thread.run_sync``; the event loop is never blocked.

    Args:
        base_dir: Absolute storage root every area subdirectory lives under.
        url_prefix: URL path prefix returned by :meth:`url` (default: root).
    """

    def __init__(self, base_dir: str, url_prefix: str = "") -> None:
        self._base = Path(base_dir)
        self._url_prefix = url_prefix.rstrip("/")
        self._warned_about_expiry = False

    def _resolve(self, area: str, key: str) -> Path:
        """Resolve ``(area, key)`` to an absolute path, rejecting traversal outside base.

        This runs synchronously inside a worker thread (see callers) so blocking
        ``Path.resolve()`` is safe.

        Args:
            area: The storage area name.
            key: The blob key within the area.

        Returns:
            The absolute :class:`~pathlib.Path` for the blob file.

        Raises:
            ValueError: If area or key fail segment validation, or the resolved
                path escapes the base directory.
        """
        check_segment(area, "area")
        check_segment(key, "key")
        base = self._base.resolve()
        candidate = (base / area / key).resolve()
        if not candidate.is_relative_to(base):
            raise ValueError(f"Storage key escapes base directory: {area}/{key!r}")
        return candidate

    async def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        """Write ``data`` to ``{base_dir}/{area}/{key}``, creating directories as needed.

        Args:
            area: Storage area (e.g. ``"uploads"``).
            key: Blob key within the area (e.g. ``"42.webp"``).
            data: Raw bytes to persist.
            content_type: Ignored for local storage (no metadata store).

        Returns:
            The ``key`` argument unchanged, mirroring the sync backend's
            convention so callers can record the key without separate computation.
        """

        def _write() -> str:
            path = self._resolve(area, key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            return key

        return await anyio.to_thread.run_sync(_write)

    async def get(self, area: str, key: str) -> bytes | None:
        """Read and return the blob at ``{base_dir}/{area}/{key}``.

        Args:
            area: Storage area.
            key: Blob key within the area.

        Returns:
            Raw bytes if the file exists, ``None`` if it does not.
        """

        def _read() -> bytes | None:
            path = self._resolve(area, key)
            if not path.is_file():
                return None
            return path.read_bytes()

        return await anyio.to_thread.run_sync(_read)

    async def exists(self, area: str, key: str) -> bool:
        """Return whether a blob exists at ``{base_dir}/{area}/{key}``.

        Args:
            area: Storage area.
            key: Blob key within the area.

        Returns:
            ``True`` iff the file is present and is a regular file.
        """

        def _exists() -> bool:
            return self._resolve(area, key).is_file()

        return await anyio.to_thread.run_sync(_exists)

    async def delete(self, area: str, key: str) -> None:
        """Delete the blob at ``{base_dir}/{area}/{key}`` if it exists.

        A missing file is silently ignored, matching the sync backend and the
        protocol's idempotency expectation.

        Args:
            area: Storage area.
            key: Blob key within the area.
        """

        def _delete() -> None:
            path = self._resolve(area, key)
            if path.is_file():
                path.unlink()

        await anyio.to_thread.run_sync(_delete)

    async def list_keys(self, area: str, prefix: str = "") -> list[str]:
        """Return sorted keys in ``area`` whose name starts with ``prefix``.

        Recurses into subdirectories — ``save`` accepts nested keys and creates
        directories for them; a flat-only listing would hide those blobs while the
        S3 backend (whose listing is a flat prefix scan) returned them.

        Args:
            area: Storage area.
            prefix: Optional key prefix filter (empty means return all keys).

        Returns:
            Alphabetically sorted list of relative POSIX key strings.
        """

        def _list() -> list[str]:
            check_segment(area, "area")
            if prefix:
                check_segment(prefix, "prefix")
            base = self._base.resolve()
            area_dir = (base / area).resolve()
            if not area_dir.is_relative_to(base) or not area_dir.is_dir():
                return []
            keys = []
            # Recursive walk — see the sync module's comment for the rationale.
            # ``rglob`` does not descend into symlinked directories.
            for candidate in area_dir.rglob("*"):
                resolved = candidate.resolve()
                if not resolved.is_relative_to(area_dir) or not resolved.is_file():
                    continue
                key = candidate.relative_to(area_dir).as_posix()
                if key.startswith(prefix):
                    keys.append(key)
            return sorted(keys)

        return await anyio.to_thread.run_sync(_list)

    async def url(self, area: str, key: str, expires_in: int = _DEFAULT_URL_EXPIRY_SECONDS) -> str:
        """Return a URL path for the blob, optionally warning when ``expires_in`` is set.

        The URL is a plain, permanent path; this backend cannot sign or expire
        it. Access control is the host web-server's responsibility.

        Args:
            area: Storage area.
            key: Blob key within the area.
            expires_in: Ignored — logged once as a warning when it differs from
                the protocol default, to avoid silently surprising callers.

        Returns:
            Percent-encoded URL path ready to embed in a response.
        """
        check_segment(area, "area")
        check_segment(key, "key")
        if expires_in != _DEFAULT_URL_EXPIRY_SECONDS and not self._warned_about_expiry:
            # Once per backend: ``url`` is called per serialised record, and a
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
