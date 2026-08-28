"""Local-filesystem ``StorageProvider`` backend."""

import hashlib
import json
import logging
import math
import os
import shutil
import stat
import time
from collections.abc import Iterator, Sequence
from contextlib import suppress
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO
from urllib.parse import quote
from uuid import UUID, uuid4

from jasil._core.storage_keys import UPLOAD_STAGING_AREA, check_area, check_listing_prefix, check_segment
from jasil._core.storage_streams import non_seekable_reader, validate_stream_range
from jasil.providers import (
    ObjectStat,
    PartRef,
    ServeFile,
    StorageBackendUnavailableError,
    StorageSizeLimitError,
    StorageUploadSessionError,
    UploadSession,
)

logger = logging.getLogger(__name__)

#: Mirrors the ``StorageProvider.url`` protocol default. Used as a sentinel for
#: "the caller did not ask for a particular lifetime", which is the only way to
#: tell an indifferent caller from one that will not get what it asked for.
_DEFAULT_URL_EXPIRY_SECONDS = 3600
_STREAM_CHUNK_BYTES = 1024 * 1024
_UPLOADS_DIRECTORY = UPLOAD_STAGING_AREA
_UPLOAD_MANIFEST = "session.json"
_UPLOAD_PARTS_DIRECTORY = "parts"
_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024
_UPLOAD_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
_UPLOAD_MAX_PARTS = 10_000


def _part_etag(data: bytes) -> str:
    return f'"sha256-{hashlib.sha256(data).hexdigest()}"'


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
        check_area(area)
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

    def _upload_root(self) -> Path:
        try:
            base = self._base.resolve()
            requested = base / _UPLOADS_DIRECTORY
            resolved = requested.resolve()
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if resolved != requested or not resolved.is_relative_to(base):
            raise StorageBackendUnavailableError("Local upload staging path is unsafe")
        return requested

    def _validate_upload_session(self, session: UploadSession) -> Path:
        try:
            check_area(session.area)
            check_segment(session.key, "key")
            normalized_upload_id = UUID(session.upload_id).hex
        except (AttributeError, TypeError, ValueError) as error:
            raise StorageUploadSessionError("Upload session is not valid for local storage") from error
        if (
            normalized_upload_id != session.upload_id
            or (session.max_bytes is not None and session.max_bytes < 0)
            or session.min_part_size != _UPLOAD_MIN_PART_SIZE
            or session.max_part_size != _UPLOAD_MAX_PART_SIZE
            or session.max_parts != _UPLOAD_MAX_PARTS
        ):
            raise StorageUploadSessionError("Upload session is not valid for local storage")
        return self._upload_root() / session.upload_id

    def _load_upload_session(self, session: UploadSession) -> Path:
        session_directory = self._validate_upload_session(session)
        manifest_path = session_directory / _UPLOAD_MANIFEST
        try:
            if session_directory.is_symlink() or manifest_path.is_symlink():
                raise StorageUploadSessionError("Upload session staging path is unsafe")
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except FileNotFoundError as error:
            raise StorageUploadSessionError("Upload session is not active") from error
        except json.JSONDecodeError as error:
            raise StorageUploadSessionError("Upload session manifest is invalid") from error
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        expected = {
            "version": 1,
            "area": session.area,
            "key": session.key,
            "upload_id": session.upload_id,
            "max_bytes": session.max_bytes,
            "min_part_size": session.min_part_size,
            "max_part_size": session.max_part_size,
            "max_parts": session.max_parts,
        }
        if not isinstance(manifest, dict) or any(manifest.get(field) != value for field, value in expected.items()):
            raise StorageUploadSessionError("Upload session does not match its durable state")
        return session_directory

    def _uploaded_parts(self, session_directory: Path) -> dict[int, tuple[Path, int]]:
        parts_directory = session_directory / _UPLOAD_PARTS_DIRECTORY
        uploaded: dict[int, tuple[Path, int]] = {}
        try:
            if parts_directory.is_symlink():
                raise StorageUploadSessionError("Upload parts staging path is unsafe")
            if not stat.S_ISDIR(parts_directory.stat().st_mode):
                raise StorageUploadSessionError("Upload parts staging path is not a directory")
            for candidate in parts_directory.glob("*.part"):
                if candidate.is_symlink():
                    raise StorageUploadSessionError("Upload part staging path is unsafe")
                metadata = candidate.stat()
                if not stat.S_ISREG(metadata.st_mode):
                    raise StorageUploadSessionError("Upload part staging path is not a file")
                try:
                    part_number = int(candidate.stem)
                except ValueError as error:
                    raise StorageUploadSessionError("Upload part staging name is invalid") from error
                if part_number in uploaded:
                    raise StorageUploadSessionError("Upload session contains duplicate part state")
                uploaded[part_number] = (candidate, metadata.st_size)
        except FileNotFoundError as error:
            raise StorageUploadSessionError("Upload session is not active") from error
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return uploaded

    @staticmethod
    def _validate_part_number(session: UploadSession, part_number: int) -> None:
        if not 1 <= part_number <= session.max_parts:
            raise ValueError(f"part_number must be between 1 and {session.max_parts}")

    def _validate_completion_parts(
        self,
        session: UploadSession,
        parts: Sequence[PartRef],
    ) -> tuple[list[PartRef], int]:
        ordered = list(parts)
        if not ordered:
            raise ValueError("At least one upload part is required")
        if len(ordered) > session.max_parts:
            raise ValueError(f"An upload may contain at most {session.max_parts} parts")
        previous_part_number = 0
        for part in ordered:
            self._validate_part_number(session, part.part_number)
            if part.part_number <= previous_part_number:
                raise ValueError("Upload parts must be unique and ordered by part_number")
            previous_part_number = part.part_number
        total = 0
        for index, part in enumerate(ordered):
            if part.size < 0 or part.size > session.max_part_size:
                raise ValueError(f"Upload part size must be between 0 and {session.max_part_size}")
            if not part.etag:
                raise ValueError("Upload part etag must not be empty")
            if index < len(ordered) - 1 and part.size < session.min_part_size:
                raise ValueError(f"Every upload part except the last must be at least {session.min_part_size} bytes")
            total += part.size
        if session.max_bytes is not None and total > session.max_bytes:
            raise StorageSizeLimitError(f"Resumable upload exceeds max_bytes={session.max_bytes}")
        return ordered, total

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

    def begin_upload(
        self,
        area: str,
        key: str,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> UploadSession:
        self._resolve(area, key)
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Resumable upload max_bytes must not be negative")
        session = UploadSession(
            area=area,
            key=key,
            upload_id=uuid4().hex,
            max_bytes=max_bytes,
            min_part_size=_UPLOAD_MIN_PART_SIZE,
            max_part_size=_UPLOAD_MAX_PART_SIZE,
            max_parts=_UPLOAD_MAX_PARTS,
        )
        session_directory = self._validate_upload_session(session)
        created_epoch = time.time()
        manifest = {
            "version": 1,
            "area": area,
            "key": key,
            "upload_id": session.upload_id,
            "max_bytes": max_bytes,
            "min_part_size": session.min_part_size,
            "max_part_size": session.max_part_size,
            "max_parts": session.max_parts,
            "content_type": content_type,
            "created_epoch": created_epoch,
        }
        try:
            session_directory.mkdir(parents=True)
            (session_directory / _UPLOAD_PARTS_DIRECTORY).mkdir()
            (session_directory / _UPLOAD_MANIFEST).write_text(
                json.dumps(manifest, separators=(",", ":")),
                encoding="utf-8",
            )
            os.utime(session_directory, (created_epoch, created_epoch))
        except OSError as error:
            with suppress(OSError):
                shutil.rmtree(session_directory)
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return session

    def upload_part(self, session: UploadSession, part_number: int, data: bytes) -> PartRef:
        session_directory = self._load_upload_session(session)
        self._validate_part_number(session, part_number)
        size = len(data)
        if size > session.max_part_size:
            raise StorageSizeLimitError(f"Upload part exceeds max_part_size={session.max_part_size}")
        uploaded = self._uploaded_parts(session_directory)
        staged_total = sum(part_size for number, (_, part_size) in uploaded.items() if number != part_number)
        if session.max_bytes is not None and staged_total + size > session.max_bytes:
            raise StorageSizeLimitError(f"Resumable upload exceeds max_bytes={session.max_bytes}")

        parts_directory = session_directory / _UPLOAD_PARTS_DIRECTORY
        part_path = parts_directory / f"{part_number:05d}.part"
        temporary_path = parts_directory / f".{part_path.name}.{uuid4().hex}.tmp"
        try:
            temporary_path.write_bytes(data)
            os.replace(temporary_path, part_path)
        except OSError as error:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return PartRef(part_number=part_number, size=size, etag=_part_etag(data))

    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int:
        session_directory = self._load_upload_session(session)
        ordered, total = self._validate_completion_parts(session, parts)
        uploaded = self._uploaded_parts(session_directory)
        if set(uploaded) != {part.part_number for part in ordered}:
            raise StorageUploadSessionError("Completion must reference every uploaded part exactly once")
        for part in ordered:
            if uploaded[part.part_number][1] != part.size:
                raise StorageUploadSessionError(f"Upload part {part.part_number} size does not match")

        destination_path = self._resolve(session.area, session.key)
        temporary_path = destination_path.with_name(f".{destination_path.name}.{uuid4().hex}")
        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            with temporary_path.open("xb") as destination:
                for part in ordered:
                    part_path = uploaded[part.part_number][0]
                    digest = hashlib.sha256()
                    copied = 0
                    try:
                        source = part_path.open("rb")
                    except FileNotFoundError as error:
                        raise StorageUploadSessionError(
                            f"Upload part {part.part_number} is no longer active"
                        ) from error
                    with source:
                        while chunk := source.read(_STREAM_CHUNK_BYTES):
                            destination.write(chunk)
                            digest.update(chunk)
                            copied += len(chunk)
                    if copied != part.size or f'"sha256-{digest.hexdigest()}"' != part.etag:
                        raise StorageUploadSessionError(f"Upload part {part.part_number} does not match its reference")
            os.replace(temporary_path, destination_path)
        except OSError as error:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        except BaseException:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            raise
        try:
            shutil.rmtree(session_directory)
        except OSError:
            logger.warning("Failed to remove a completed local upload session")
        return total

    def abort_upload(self, session: UploadSession) -> None:
        session_directory = self._validate_upload_session(session)
        if not session_directory.exists():
            return
        self._load_upload_session(session)
        try:
            shutil.rmtree(session_directory)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def cleanup_uploads(self, *, older_than_epoch: float) -> int:
        if not math.isfinite(older_than_epoch):
            raise ValueError("older_than_epoch must be finite")
        upload_root = self._upload_root()
        try:
            candidates = list(upload_root.iterdir())
        except FileNotFoundError:
            return 0
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        removed = 0
        for candidate in candidates:
            try:
                if candidate.is_symlink() or not candidate.is_dir():
                    continue
                created_epoch = candidate.stat().st_mtime
                if created_epoch >= older_than_epoch:
                    continue
                shutil.rmtree(candidate)
                removed += 1
            except FileNotFoundError:
                continue
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return removed

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
        check_area(area)
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
        check_area(src_area)
        check_segment(src_key, "source key")
        check_area(dst_area)
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
        check_area(area)
        if prefix:
            check_listing_prefix(prefix)
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
        check_area(area)
        check_segment(key, "key")
        self._warn_ignored_delivery_controls(
            expires_in=expires_in,
            download_as=download_as,
            content_type=content_type,
        )
        return f"{self._url_prefix}/{quote(area, safe='/')}/{quote(key, safe='/')}"
