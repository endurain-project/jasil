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

from jasil._core.storage_keys import (
    OBJECT_STORAGE_AREA,
    UPLOAD_STAGING_AREA,
    check_area,
    check_listing_prefix,
    check_segment,
)
from jasil._core.storage_streams import ExactSizeReader, non_seekable_reader, validate_stream_range
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
_OBJECTS_DIRECTORY = OBJECT_STORAGE_AREA
_OBJECTS_LAYOUT_VERSION = "v1"
_OBJECT_AREAS_DIRECTORY = "areas"
_OBJECT_KEYS_DIRECTORY = "objects"
_OBJECT_PAYLOAD_FILE = "object"
_OBJECT_AREA_FILE = "area"
_OBJECT_KEY_FILE = "key"
_UPLOADS_DIRECTORY = UPLOAD_STAGING_AREA
_UPLOAD_MANIFEST = "session.json"
_UPLOAD_PARTS_DIRECTORY = "parts"
_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024
_UPLOAD_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
_UPLOAD_MAX_PARTS = 10_000


def _path_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _translate_local_stream_error(error: Exception) -> None:
    if isinstance(error, OSError):
        raise StorageBackendUnavailableError("Local storage backend is unavailable") from error


class LocalStorage:
    """``StorageProvider`` storing blobs in a versioned filesystem layout.

    A logical ``(area, key)`` maps to a leaf file under
    ``{base_dir}/.jasil-objects/v1``. The private encoding lets one key coexist
    with its descendants, matching object storage. Objects written by JASIL
    0.3 and earlier at ``{base_dir}/{area}/{key}`` remain readable and are
    migrated when overwritten. Keys are server-generated (e.g. ``42.webp``);
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

    def _resolve_storage_path(
        self,
        area: str,
        key: str | None = None,
        *,
        legacy: bool = False,
        object_file: bool = False,
    ) -> Path:
        check_area(area)
        if key is not None:
            check_segment(key, "key")
        try:
            base = self._base.resolve()
            requested = base
            if not legacy:
                requested = requested / _OBJECTS_DIRECTORY / _OBJECTS_LAYOUT_VERSION / _OBJECT_AREAS_DIRECTORY
                area_digest = _path_digest(area)
                requested = requested / area_digest[:2] / area_digest[2:]
                requested /= _OBJECT_KEYS_DIRECTORY
                if key is not None:
                    key_digest = _path_digest(key)
                    requested = requested / key_digest[:2] / key_digest[2:]
                    if object_file:
                        requested /= _OBJECT_PAYLOAD_FILE
            else:
                requested /= area
                if key is not None:
                    requested /= key
            candidate = requested.resolve()
        except RuntimeError as error:
            raise ValueError(f"Storage path contains a symbolic link loop: {area}/{key or ''!r}") from error
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if not candidate.is_relative_to(base):
            raise ValueError(f"Storage key escapes base directory: {area}/{key or ''!r}")
        if candidate != requested:
            raise ValueError(f"Storage path resolves through a symbolic link: {area}/{key or ''!r}")
        return candidate

    def _resolve(self, area: str, key: str) -> Path:
        """Resolve a logical object address into the versioned local layout."""
        return self._resolve_storage_path(area, key, object_file=True)

    def _resolve_area(self, area: str, *, legacy: bool = False) -> Path:
        return self._resolve_storage_path(area, legacy=legacy)

    def _resolve_legacy(self, area: str, key: str) -> Path:
        return self._resolve_storage_path(area, key, legacy=True)

    def _write_identity(self, path: Path, value: str) -> None:
        try:
            if path.is_symlink():
                raise StorageBackendUnavailableError("Local storage object identity path is unsafe")
            existing = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            temporary_path = path.with_name(f".{path.name}.{uuid4().hex}")
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary_path.write_text(value, encoding="utf-8")
                os.replace(temporary_path, path)
            except OSError as error:
                with suppress(OSError):
                    temporary_path.unlink(missing_ok=True)
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
            return
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if existing != value:
            raise StorageBackendUnavailableError("Local storage object identity does not match its address")

    def _prepare_object(self, area: str, key: str) -> Path:
        path = self._resolve(area, key)
        area_file = self._resolve_area(area).parent / _OBJECT_AREA_FILE
        self._write_identity(area_file, area)
        self._write_identity(path.parent / _OBJECT_KEY_FILE, key)
        return path

    def _verify_object_identity(self, area: str, key: str, path: Path) -> None:
        area_file = self._resolve_area(area).parent / _OBJECT_AREA_FILE
        key_file = path.parent / _OBJECT_KEY_FILE
        try:
            if area_file.is_symlink() or key_file.is_symlink():
                raise StorageBackendUnavailableError("Local storage object identity path is unsafe")
            stored_area = area_file.read_text(encoding="utf-8")
            stored_key = key_file.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise StorageBackendUnavailableError("Local storage object identity is unavailable") from error
        if stored_area != area or stored_key != key:
            raise StorageBackendUnavailableError("Local storage object identity does not match its address")

    @staticmethod
    def _is_regular_file(path: Path) -> bool:
        try:
            return stat.S_ISREG(path.stat().st_mode)
        except (FileNotFoundError, NotADirectoryError):
            return False
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def _current_object_path(self, area: str, key: str) -> Path | None:
        path = self._resolve(area, key)
        if self._is_regular_file(path):
            self._verify_object_identity(area, key, path)
            return path
        return None

    def _existing_object_path(self, area: str, key: str) -> Path | None:
        path = self._current_object_path(area, key)
        if path is not None:
            return path
        legacy_path = self._resolve_legacy(area, key)
        if self._is_regular_file(legacy_path):
            return legacy_path
        return None

    def _remove_legacy_object(self, area: str, key: str) -> None:
        path = self._resolve_legacy(area, key)
        try:
            if stat.S_ISREG(path.stat().st_mode):
                path.unlink()
                self._prune_empty_directories(path.parent)
        except (FileNotFoundError, NotADirectoryError):
            return
        except OSError:
            logger.warning("Failed to remove a migrated legacy local storage object")

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
            normalized_session_id = UUID(session.session_id).hex
        except (AttributeError, TypeError, ValueError) as error:
            raise StorageUploadSessionError("Upload session is not valid for local storage") from error
        if (
            normalized_session_id != session.session_id
            or (session.max_bytes is not None and session.max_bytes < 0)
            or session.min_part_size != _UPLOAD_MIN_PART_SIZE
            or session.max_part_size != _UPLOAD_MAX_PART_SIZE
            or session.max_parts != _UPLOAD_MAX_PARTS
        ):
            raise StorageUploadSessionError("Upload session is not valid for local storage")
        return self._upload_root() / session.session_id

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
            "session_id": session.session_id,
            "max_bytes": session.max_bytes,
            "min_part_size": session.min_part_size,
            "max_part_size": session.max_part_size,
            "max_parts": session.max_parts,
        }
        created_epoch = manifest.get("created_epoch") if isinstance(manifest, dict) else None
        if (
            not isinstance(manifest, dict)
            or any(manifest.get(field) != value for field, value in expected.items())
            or isinstance(created_epoch, bool)
            or not isinstance(created_epoch, int | float)
            or not math.isfinite(created_epoch)
        ):
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
            if not part.validator:
                raise ValueError("Upload part validator must not be empty")
            if index < len(ordered) - 1 and part.size < session.min_part_size:
                raise ValueError(f"Every upload part except the last must be at least {session.min_part_size} bytes")
            total += part.size
        if session.max_bytes is not None and total > session.max_bytes:
            raise StorageSizeLimitError(f"Resumable upload exceeds max_bytes={session.max_bytes}")
        return ordered, total

    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        path = self._prepare_object(area, key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        self._remove_legacy_object(area, key)
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
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Storage stream max_bytes must not be negative")
        path = self._prepare_object(area, key)
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
        self._remove_legacy_object(area, key)
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
            session_id=uuid4().hex,
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
            "session_id": session.session_id,
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

    def upload_part(
        self,
        session: UploadSession,
        part_number: int,
        source: BinaryIO,
        *,
        size: int,
    ) -> PartRef:
        self._validate_upload_session(session)
        self._validate_part_number(session, part_number)
        reader = ExactSizeReader(source, size)
        if size > session.max_part_size:
            raise StorageSizeLimitError(f"Upload part exceeds max_part_size={session.max_part_size}")
        session_directory = self._load_upload_session(session)
        uploaded = self._uploaded_parts(session_directory)
        staged_total = sum(part_size for number, (_, part_size) in uploaded.items() if number != part_number)
        if session.max_bytes is not None and staged_total + size > session.max_bytes:
            raise StorageSizeLimitError(f"Resumable upload exceeds max_bytes={session.max_bytes}")

        parts_directory = session_directory / _UPLOAD_PARTS_DIRECTORY
        part_path = parts_directory / f"{part_number:05d}.part"
        temporary_path = parts_directory / f".{part_path.name}.{uuid4().hex}.tmp"
        try:
            destination = temporary_path.open("xb")
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        digest = hashlib.sha256()
        try:
            while chunk := reader.read(_STREAM_CHUNK_BYTES):
                try:
                    destination.write(chunk)
                except OSError as error:
                    raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
                digest.update(chunk)
            reader.verify_complete()
            try:
                destination.close()
                os.replace(temporary_path, part_path)
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        except BaseException:
            if not destination.closed:
                with suppress(OSError):
                    destination.close()
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            raise
        return PartRef(
            part_number=part_number,
            size=size,
            validator=f'"sha256-{digest.hexdigest()}"',
        )

    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int:
        session_directory = self._load_upload_session(session)
        ordered, total = self._validate_completion_parts(session, parts)
        uploaded = self._uploaded_parts(session_directory)
        if set(uploaded) != {part.part_number for part in ordered}:
            raise StorageUploadSessionError("Completion must reference every uploaded part exactly once")
        for part in ordered:
            if uploaded[part.part_number][1] != part.size:
                raise StorageUploadSessionError(f"Upload part {part.part_number} size does not match")

        destination_path = self._prepare_object(session.area, session.key)
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
                    if copied != part.size or f'"sha256-{digest.hexdigest()}"' != part.validator:
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
        self._remove_legacy_object(session.area, session.key)
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
            candidates = upload_root.iterdir()
        except FileNotFoundError:
            return 0
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        removed = 0
        first_error: OSError | None = None
        try:
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
                    if first_error is None:
                        first_error = error
        except FileNotFoundError:
            pass
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if first_error is not None:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from first_error
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
        path = self._current_object_path(area, key)
        validate_stream_range(offset, length)
        if path is None:
            path = self._resolve_legacy(area, key)
            try:
                source = path.open("rb")
            except (FileNotFoundError, NotADirectoryError) as error:
                raise FileNotFoundError(f"Storage object not found: {area}/{key}") from error
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        else:
            try:
                source = path.open("rb")
            except FileNotFoundError as error:
                raise FileNotFoundError(f"Storage object not found: {area}/{key}") from error
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        try:
            source.seek(offset)
        except OSError as error:
            source.close()
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return non_seekable_reader(source, length=length, translate_error=_translate_local_stream_error)

    def stat(self, area: str, key: str) -> ObjectStat | None:
        path = self._current_object_path(area, key)
        if path is None:
            path = self._resolve_legacy(area, key)
        try:
            metadata = path.stat()
        except (FileNotFoundError, NotADirectoryError):
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
        path = self._existing_object_path(area, key)
        if path is None:
            raise FileNotFoundError(f"Storage object not found: {area}/{key}")
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
        return self._existing_object_path(area, key) is not None

    def delete(self, area: str, key: str) -> None:
        current_path = self._current_object_path(area, key)
        if current_path is not None:
            try:
                current_path.unlink()
                (current_path.parent / _OBJECT_KEY_FILE).unlink(missing_ok=True)
                self._prune_empty_directories(current_path.parent)
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        legacy_path = self._resolve_legacy(area, key)
        try:
            if stat.S_ISREG(legacy_path.stat().st_mode):
                legacy_path.unlink()
                self._prune_empty_directories(legacy_path.parent)
        except (FileNotFoundError, NotADirectoryError):
            return
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def delete_prefix(self, area: str, prefix: str) -> int:
        check_area(area)
        check_segment(prefix, "prefix")
        self._resolve_legacy(area, prefix)
        descendant_prefix = f"{prefix}/"
        keys = [key for key, _ in self.iter_objects(area, prefix) if key == prefix or key.startswith(descendant_prefix)]
        for key in keys:
            self.delete(area, key)
        return len(keys)

    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None:
        check_area(src_area)
        check_segment(src_key, "source key")
        check_area(dst_area)
        check_segment(dst_key, "destination key")
        if src_area == dst_area and src_key == dst_key:
            if not self.exists(src_area, src_key):
                raise FileNotFoundError(f"Storage object not found: {src_area}/{src_key}")
            return
        try:
            source = self.open_stream(src_area, src_key)
        except FileNotFoundError:
            raise
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        with source:
            self.save_stream(dst_area, dst_key, source)

    def list_keys(self, area: str, prefix: str = "") -> list[str]:
        return sorted(key for key, _ in self.iter_objects(area, prefix))

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        check_area(area)
        if prefix:
            check_listing_prefix(prefix)
        area_dir = self._resolve_area(area)
        legacy_area = self._resolve_area(area, legacy=True)

        def objects() -> Iterator[tuple[str, float]]:
            try:
                if stat.S_ISDIR(area_dir.stat().st_mode):
                    area_file = area_dir.parent / _OBJECT_AREA_FILE
                    if area_file.is_symlink():
                        raise StorageBackendUnavailableError("Local storage area identity path is unsafe")
                    area_identity = area_file.read_text(encoding="utf-8")
                    if area_identity != area:
                        raise StorageBackendUnavailableError("Local storage area identity does not match its address")
                    for candidate in area_dir.rglob("*"):
                        resolved = candidate.resolve()
                        if (
                            resolved != candidate
                            or not resolved.is_relative_to(area_dir)
                            or candidate.name != _OBJECT_PAYLOAD_FILE
                        ):
                            continue
                        try:
                            metadata = resolved.stat()
                        except FileNotFoundError:
                            continue
                        if not stat.S_ISREG(metadata.st_mode):
                            continue
                        try:
                            key_file = candidate.parent / _OBJECT_KEY_FILE
                            if key_file.is_symlink():
                                continue
                            key = key_file.read_text(encoding="utf-8")
                            check_segment(key, "key")
                        except (FileNotFoundError, UnicodeError, ValueError):
                            continue
                        if candidate != self._resolve(area, key):
                            continue
                        if key.startswith(prefix):
                            yield key, metadata.st_mtime
            except FileNotFoundError:
                pass
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

            try:
                if not stat.S_ISDIR(legacy_area.stat().st_mode):
                    return
                for candidate in legacy_area.rglob("*"):
                    resolved = candidate.resolve()
                    if resolved != candidate or not resolved.is_relative_to(legacy_area):
                        continue
                    try:
                        metadata = resolved.stat()
                    except FileNotFoundError:
                        continue
                    if not stat.S_ISREG(metadata.st_mode):
                        continue
                    key = candidate.relative_to(legacy_area).as_posix()
                    if not key.startswith(prefix):
                        continue
                    try:
                        if self._current_object_path(area, key) is not None:
                            continue
                    except ValueError:
                        continue
                    yield key, metadata.st_mtime
            except FileNotFoundError:
                return
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
        path = self._existing_object_path(area, key) or self._resolve(area, key)
        try:
            relative_path = path.relative_to(self._base.resolve()).as_posix()
        except (OSError, ValueError) as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return f"{self._url_prefix}/{quote(relative_path, safe='/')}"
