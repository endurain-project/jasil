"""Local-filesystem ``StorageProvider`` backend."""

import logging
import os
import shutil
import stat
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO
from urllib.parse import quote
from uuid import uuid4

from jasil._core.storage_keys import check_segment
from jasil._core.storage_streams import non_seekable_reader, validate_stream_range
from jasil.providers import ObjectStat, ServeFile, StorageBackendUnavailableError, StorageSizeLimitError

logger = logging.getLogger(__name__)

#: Mirrors the ``StorageProvider.url`` protocol default. Used as a sentinel for
#: "the caller did not ask for a particular lifetime", which is the only way to
#: tell an indifferent caller from one that will not get what it asked for.
_DEFAULT_URL_EXPIRY_SECONDS = 3600
_STREAM_CHUNK_BYTES = 1024 * 1024


def _translate_local_stream_error(error: Exception) -> None:
    if isinstance(error, OSError):
        raise StorageBackendUnavailableError("Local storage backend is unavailable") from error


class LocalStorage:
    """``StorageProvider`` storing blobs as files under a per-area subdirectory.

    A blob for ``(area, key)`` lives at ``{base_dir}/{area}/{key}`` and is served
    at ``{url_prefix}/{area}/{key}``. Keys are server-generated (e.g. ``42.webp``);
    both area and key are validated so a stray value can never escape the base
    directory.

    **:meth:`url` cannot enforce response controls.** It returns a plain path for
    the host's own web server to serve, and JASIL neither runs that server nor
    holds a key to sign with — so expiry, download disposition, and response
    media type are host policy. The S3 backend honours all three.

    Args:
        base_dir: Absolute storage root every area subdirectory lives under.
        url_prefix: URL path prefix returned by :meth:`url` (default: root).
    """

    def __init__(self, base_dir: str, url_prefix: str = "") -> None:
        self._base = Path(base_dir)
        self._url_prefix = url_prefix.rstrip("/")
        self._warned_about_delivery_controls = False

    def _resolve(self, area: str, key: str) -> Path:
        """Resolve ``(area, key)`` to an absolute path, rejecting traversal outside base."""
        check_segment(area, "area")
        check_segment(key, "key")
        try:
            base = self._base.resolve()
            requested = base / area / key
            candidate = requested.resolve()
        except RuntimeError as error:
            raise ValueError(f"Storage path contains a symbolic link loop: {area}/{key!r}") from error
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if not candidate.is_relative_to(base):
            raise ValueError(f"Storage key escapes base directory: {area}/{key!r}")
        if candidate != requested:
            raise ValueError(f"Storage path resolves through a symbolic link: {area}/{key!r}")
        return candidate

    def _prune_empty_directories(self, directory: Path) -> None:
        try:
            base = self._base.resolve()
        except OSError:
            return
        while directory != base and directory.is_relative_to(base):
            try:
                directory.rmdir()
            except OSError:
                return
            directory = directory.parent

    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        path = self._resolve(area, key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return key

    def save_stream(
        self,
        area: str,
        key: str,
        source: BinaryIO,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> int:
        path = self._resolve(area, key)
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Storage stream max_bytes must not be negative")
        temporary_path = path.with_name(f".{path.name}.{uuid4().hex}")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            destination = temporary_path.open("xb")
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        total = 0
        try:
            while True:
                read_size = _STREAM_CHUNK_BYTES
                if max_bytes is not None:
                    read_size = min(read_size, max_bytes - total + 1)
                chunk = source.read(read_size)
                if not chunk:
                    break
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise StorageSizeLimitError(f"Storage stream exceeds max_bytes={max_bytes}")
                try:
                    destination.write(chunk)
                except OSError as error:
                    raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
            try:
                destination.close()
                os.replace(temporary_path, path)
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        except BaseException:
            if not destination.closed:
                with suppress(OSError):
                    destination.close()
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            raise
        return total

    def get(self, area: str, key: str) -> bytes | None:
        try:
            stream = self.open_stream(area, key)
        except FileNotFoundError:
            return None
        with stream:
            return stream.read()

    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        path = self._resolve(area, key)
        validate_stream_range(offset, length)
        try:
            source = path.open("rb")
        except FileNotFoundError:
            raise
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        try:
            source.seek(offset)
        except OSError as error:
            source.close()
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return non_seekable_reader(source, length=length, translate_error=_translate_local_stream_error)

    def stat(self, area: str, key: str) -> ObjectStat | None:
        path = self._resolve(area, key)
        try:
            metadata = path.stat()
        except FileNotFoundError:
            return None
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if not stat.S_ISREG(metadata.st_mode):
            return None
        return ObjectStat(size=metadata.st_size, modified_epoch=metadata.st_mtime)

    def serve(
        self,
        area: str,
        key: str,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
        expires_in: int = _DEFAULT_URL_EXPIRY_SECONDS,
    ) -> ServeFile:
        path = self._resolve(area, key)
        try:
            with path.open("rb"):
                pass
        except FileNotFoundError:
            raise
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        self._warn_ignored_delivery_controls(
            expires_in=expires_in,
            download_as=download_as,
            content_type=content_type,
        )
        return ServeFile(path)

    def exists(self, area: str, key: str) -> bool:
        path = self._resolve(area, key)
        try:
            return stat.S_ISREG(path.stat().st_mode)
        except FileNotFoundError:
            return False
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def delete(self, area: str, key: str) -> None:
        path = self._resolve(area, key)
        try:
            if stat.S_ISREG(path.stat().st_mode):
                path.unlink()
                self._prune_empty_directories(path.parent)
        except FileNotFoundError:
            return
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def delete_prefix(self, area: str, prefix: str) -> int:
        check_segment(area, "area")
        check_segment(prefix, "prefix")
        root = self._resolve(area, prefix)
        try:
            mode = root.stat().st_mode
        except FileNotFoundError:
            return 0
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        if stat.S_ISREG(mode):
            try:
                root.unlink()
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
            self._prune_empty_directories(root.parent)
            return 1
        if not stat.S_ISDIR(mode):
            return 0

        count = 0
        try:
            for candidate in root.rglob("*"):
                if candidate.is_symlink():
                    continue
                if stat.S_ISREG(candidate.stat().st_mode):
                    count += 1
            shutil.rmtree(root)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        self._prune_empty_directories(root.parent)
        return count

    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None:
        check_segment(src_area, "source area")
        check_segment(src_key, "source key")
        check_segment(dst_area, "destination area")
        check_segment(dst_key, "destination key")
        source_path = self._resolve(src_area, src_key)
        destination_path = self._resolve(dst_area, dst_key)
        try:
            source = source_path.open("rb")
        except FileNotFoundError:
            raise
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        with source:
            if source_path == destination_path:
                return
            self.save_stream(dst_area, dst_key, source)

    def list_keys(self, area: str, prefix: str = "") -> list[str]:
        return sorted(key for key, _ in self.iter_objects(area, prefix))

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        check_segment(area, "area")
        if prefix:
            check_segment(prefix, "prefix")
        try:
            base = self._base.resolve()
            requested_area = base / area
            area_dir = requested_area.resolve()
        except RuntimeError as error:
            raise ValueError(f"Storage path contains a symbolic link loop: {area!r}") from error
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if not area_dir.is_relative_to(base):
            return iter(())
        if area_dir != requested_area:
            raise ValueError(f"Storage path resolves through a symbolic link: {area!r}")
        try:
            if not stat.S_ISDIR(area_dir.stat().st_mode):
                return iter(())
        except FileNotFoundError:
            return iter(())
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        def objects() -> Iterator[tuple[str, float]]:
            try:
                for candidate in area_dir.rglob("*"):
                    resolved = candidate.resolve()
                    if resolved != candidate or not resolved.is_relative_to(area_dir):
                        continue
                    try:
                        metadata = resolved.stat()
                    except FileNotFoundError:
                        continue
                    if not stat.S_ISREG(metadata.st_mode):
                        continue
                    key = candidate.relative_to(area_dir).as_posix()
                    if key.startswith(prefix):
                        yield key, metadata.st_mtime
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        return objects()

    def check_writable(self) -> None:
        try:
            base = self._base.resolve(strict=True)
            if not base.is_dir():
                raise NotADirectoryError(base)
            with NamedTemporaryFile(dir=base, prefix=".jasil-write-probe-") as probe:
                probe.write(b"")
                probe.flush()
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def _warn_ignored_delivery_controls(
        self,
        *,
        expires_in: int,
        download_as: str | None,
        content_type: str | None,
    ) -> None:
        ignored_controls = []
        if expires_in != _DEFAULT_URL_EXPIRY_SECONDS:
            ignored_controls.append(f"expires_in={expires_in}")
        if download_as is not None:
            ignored_controls.append("download_as")
        if content_type is not None:
            ignored_controls.append("content_type")
        if not ignored_controls or self._warned_about_delivery_controls:
            return
        self._warned_about_delivery_controls = True
        verb = "was" if len(ignored_controls) == 1 else "were"
        logger.warning(
            "Local storage cannot enforce delivery controls, so %s %s ignored and local access is permanent. "
            "Apply those controls in the web server that serves %r, or use the s3:// backend.",
            ", ".join(ignored_controls),
            verb,
            self._url_prefix or "/",
        )

    def url(
        self,
        area: str,
        key: str,
        expires_in: int = _DEFAULT_URL_EXPIRY_SECONDS,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
    ) -> str:
        check_segment(area, "area")
        check_segment(key, "key")
        self._warn_ignored_delivery_controls(
            expires_in=expires_in,
            download_as=download_as,
            content_type=content_type,
        )
        return f"{self._url_prefix}/{quote(area, safe='/')}/{quote(key, safe='/')}"
