"""Local-filesystem ``StorageProvider`` backend."""

import errno
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
from typing import IO, Any, BinaryIO
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
    StorageKeyConflictError,
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
_PLAIN_TEMP_PREFIX = ".jasil-plain-write-"
_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024
_UPLOAD_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
_UPLOAD_MAX_PARTS = 10_000


def _path_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _is_absent(error: OSError) -> bool:
    """True when ``error`` says the path does not exist, or its name is too long for any file to carry it."""
    return isinstance(error, FileNotFoundError | NotADirectoryError) or error.errno == errno.ENAMETOOLONG


def _translate_local_stream_error(error: Exception) -> None:
    if isinstance(error, OSError):
        raise StorageBackendUnavailableError("Local storage backend is unavailable") from error


class LocalStorage:
    """``StorageProvider`` storing blobs in a versioned filesystem layout.

    By default, a logical ``(area, key)`` maps to a leaf file under
    ``{base_dir}/.jasil-objects/v1``. Areas in ``plain_areas`` instead use
    ``{base_dir}/{area}/{key}`` for human browsing. Keys are server-generated
    (e.g. ``42.webp``); both area and key are validated so a stray value can never
    escape the base directory.

    The flat layout is opt-in. ``plain_areas`` deliberately exposes every regular
    file in the selected area. For other areas, objects written by JASIL 0.3 and
    earlier, and by the application JASIL was extracted from, sit at
    ``{base_dir}/{area}/{key}``; ``legacy_layout=True`` makes those objects
    readable, listed and deletable, and migrates them to the versioned layout when
    overwritten. Off (the default), those other directories are never consulted.

    **:meth:`url` cannot enforce response controls.** It returns a plain path for
    the host's own web server to serve, and JASIL neither runs that server nor
    holds a key to sign with — so expiry, download disposition, and response
    media type are host policy. The S3 backend honours all three.

    Args:
        base_dir: Absolute storage root every area subdirectory lives under.
        url_prefix: URL path prefix returned by :meth:`url` (default: root).
        legacy_layout: Also read, list and delete objects in the flat
            ``{base_dir}/{area}/{key}`` layout for areas not selected by
            ``plain_areas`` (see above).
        plain_areas: Areas whose objects use the readable
            ``{base_dir}/{area}/{key}`` layout. A key cannot coexist with its
            descendants in these areas.
    """

    def __init__(
        self,
        base_dir: str,
        url_prefix: str = "",
        *,
        legacy_layout: bool = False,
        plain_areas: frozenset[str] = frozenset(),
    ) -> None:
        self._base = Path(base_dir)
        self._url_prefix = url_prefix.rstrip("/")
        self._legacy_layout = legacy_layout
        self._plain_areas = plain_areas
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

    def _resolve_plain(self, area: str, key: str) -> Path:
        check_area(area)
        check_segment(key, "key")
        if self._is_plain_temporary_name(key.rsplit("/", 1)[-1]):
            raise ValueError("Storage key uses a reserved local staging name")
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

    def _write_identity(self, path: Path, value: str) -> None:
        try:
            if path.is_symlink():
                raise StorageBackendUnavailableError("Local storage object identity path is unsafe")
            existing = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            temporary_path = path.with_name(f".{path.name}.{uuid4().hex}")
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with temporary_path.open("x", encoding="utf-8") as handle:
                    handle.write(value)
                    _sync(handle)
                self._install(temporary_path, path)
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
        if area in self._plain_areas:
            path = self._resolve_plain(area, key)
            self._check_plain_key_conflicts(area, key, path)
            return path
        path = self._resolve(area, key)
        self._write_object_identities(area, key, path)
        return path

    def _write_object_identities(self, area: str, key: str, path: Path) -> None:
        """Make sure the area and key identity files beside an opaque object exist; idempotent."""
        area_file = self._resolve_area(area).parent / _OBJECT_AREA_FILE
        self._write_identity(area_file, area)
        self._write_identity(path.parent / _OBJECT_KEY_FILE, key)

    def _check_plain_key_conflicts(self, area: str, key: str, path: Path) -> None:
        """Reject file/directory conflicts across plain and opaque objects."""
        area_path = self._resolve_area(area, legacy=True)
        current = area_path
        try:
            for segment in key.split("/")[:-1]:
                current /= segment
                if current.exists() and not current.is_dir():
                    raise self._plain_key_conflict(area, key)
            if path.is_dir():
                raise self._plain_key_conflict(area, key)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

        segments = key.split("/")
        for index in range(1, len(segments)):
            ancestor = "/".join(segments[:index])
            if self._current_object_path(area, ancestor) is not None:
                raise self._plain_key_conflict(area, key)
        if next(self._iter_opaque_objects(area, f"{key}/"), None) is not None:
            raise self._plain_key_conflict(area, key)

    @staticmethod
    def _plain_key_conflict(area: str, key: str) -> StorageKeyConflictError:
        """Build the error for a key that cannot coexist with a path prefix."""
        return StorageKeyConflictError(f"Storage key has a file/directory conflict: {area}/{key}")

    def _temporary_object_path(self, area: str, destination: Path) -> Path:
        """Choose a same-filesystem temporary path outside plain listings."""
        if area not in self._plain_areas:
            return destination.with_name(f".{destination.name}.{uuid4().hex}")
        try:
            staging_root = self._upload_root()
            staging_root.mkdir(parents=True, exist_ok=True)
            destination_parent = destination.parent
            while not destination_parent.exists():
                destination_parent = destination_parent.parent
            if staging_root.stat().st_dev == destination_parent.stat().st_dev:
                return staging_root / f"{_PLAIN_TEMP_PREFIX}{uuid4().hex}.tmp"
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return destination.with_name(f"{_PLAIN_TEMP_PREFIX}{uuid4().hex}.tmp")

    @staticmethod
    def _is_plain_temporary_name(name: str) -> bool:
        if not name.startswith(_PLAIN_TEMP_PREFIX) or not name.endswith(".tmp"):
            return False
        token = name.removeprefix(_PLAIN_TEMP_PREFIX).removesuffix(".tmp")
        if len(token) != 32:
            return False
        try:
            return UUID(token).hex == token
        except ValueError:
            return False

    def _write_object_bytes(self, area: str, path: Path, data: bytes) -> None:
        """Write ``data`` to a temporary file and move it over ``path``, so a failed overwrite leaves the old bytes."""
        temporary_path = self._temporary_object_path(area, path)
        try:
            if temporary_path.parent == path.parent:
                path.parent.mkdir(parents=True, exist_ok=True)
            with temporary_path.open("xb") as destination:
                destination.write(data)
                _sync(destination)
            self._install(temporary_path, path)
        except OSError as error:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            self._prune_empty_directories(path.parent)
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    @staticmethod
    def _install(temporary_path: Path, destination: Path) -> None:
        """Atomically move a synced temporary file over ``destination`` and make the new name durable.

        Recreates a parent that a concurrent prune removed, once.
        """
        for attempt in range(2):
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.replace(temporary_path, destination)
                break
            except FileNotFoundError:
                if attempt:
                    raise
        _sync_directory(destination.parent)

    def _finish_commit(self, area: str, key: str) -> None:
        """Settle a freshly committed object: keep its identity and remove the prior representation.

        An opaque object's identity files are written before its payload, so a delete running in between
        removes them and the payload would be published without any; writing them again, now that the payload
        is in place, leaves a committed object that can always be read.
        """
        if area in self._plain_areas:
            self._remove_opaque_object(area, key, best_effort=True)
        else:
            self._write_object_identities(area, key, self._resolve(area, key))
            self._remove_legacy_object(area, key)

    def _remove_opaque_object(self, area: str, key: str, *, best_effort: bool) -> None:
        try:
            path = self._current_object_path(area, key)
            if path is None:
                return
            path.unlink()
            (path.parent / _OBJECT_KEY_FILE).unlink(missing_ok=True)
            self._prune_empty_directories(path.parent)
        except (OSError, ValueError, StorageBackendUnavailableError) as error:
            if best_effort:
                logger.warning("Failed to remove a migrated opaque local storage object")
                return
            if isinstance(error, OSError):
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
            raise

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
        except OSError as error:
            if _is_absent(error):
                return False
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def _current_object_path(self, area: str, key: str) -> Path | None:
        path = self._resolve(area, key)
        if self._is_regular_file(path):
            self._verify_object_identity(area, key, path)
            return path
        return None

    def _existing_object_path(self, area: str, key: str) -> Path | None:
        if area in self._plain_areas:
            plain_path = self._resolve_plain(area, key)
            if self._is_regular_file(plain_path):
                return plain_path
        path = self._current_object_path(area, key)
        if path is not None:
            return path
        if area in self._plain_areas or not self._legacy_layout:
            return None
        legacy_path = self._resolve_legacy(area, key)
        if self._is_regular_file(legacy_path):
            return legacy_path
        return None

    def _remove_legacy_object(self, area: str, key: str) -> None:
        if not self._legacy_layout:
            return
        path = self._resolve_legacy(area, key)
        try:
            if stat.S_ISREG(path.stat().st_mode):
                path.unlink()
                self._prune_empty_directories(path.parent)
        except OSError as error:
            if not _is_absent(error):
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
        """Write ``data`` to a temporary file and move it over the object; ``content_type`` is ignored."""
        path = self._prepare_object(area, key)
        self._write_object_bytes(area, path, data)
        self._finish_commit(area, key)
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
        """Stream ``source`` into a temporary file and move it into place; ``content_type`` is ignored."""
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Storage stream max_bytes must not be negative")
        path = self._prepare_object(area, key)
        temporary_path = self._temporary_object_path(area, path)
        try:
            if area not in self._plain_areas or temporary_path.parent == path.parent:
                path.parent.mkdir(parents=True, exist_ok=True)
            destination = temporary_path.open("xb")
        except OSError as error:
            if area in self._plain_areas:
                self._prune_empty_directories(path.parent)
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
                _sync(destination)
                destination.close()
                self._install(temporary_path, path)
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        except BaseException:
            if not destination.closed:
                with suppress(OSError):
                    destination.close()
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            if area in self._plain_areas:
                self._prune_empty_directories(path.parent)
            raise
        self._finish_commit(area, key)
        return total

    def begin_upload(
        self,
        area: str,
        key: str,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> UploadSession:
        """Create the session directory and manifest in the upload staging root, recording ``content_type``."""
        if area in self._plain_areas:
            path = self._resolve_plain(area, key)
            self._check_plain_key_conflicts(area, key, path)
        else:
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
            with (session_directory / _UPLOAD_MANIFEST).open("x", encoding="utf-8") as handle:
                handle.write(json.dumps(manifest, separators=(",", ":")))
                _sync(handle)
            os.utime(session_directory, (created_epoch, created_epoch))
            _sync_directory(session_directory)
            _sync_directory(session_directory.parent)
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
        """Stage the part as a file in the session directory; the validator is a quoted SHA-256 of its bytes."""
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
                _sync(destination)
                destination.close()
                self._install(temporary_path, part_path)
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
        """Concatenate the staged parts into the object, checking each part's size and SHA-256.

        The session is removed once the object is in place.
        """
        session_directory = self._load_upload_session(session)
        ordered, total = self._validate_completion_parts(session, parts)
        uploaded = self._uploaded_parts(session_directory)
        if set(uploaded) != {part.part_number for part in ordered}:
            raise StorageUploadSessionError("Completion must reference every uploaded part exactly once")
        for part in ordered:
            if uploaded[part.part_number][1] != part.size:
                raise StorageUploadSessionError(f"Upload part {part.part_number} size does not match")

        destination_path = self._prepare_object(session.area, session.key)
        temporary_path = self._temporary_object_path(session.area, destination_path)
        try:
            if session.area not in self._plain_areas or temporary_path.parent == destination_path.parent:
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
                _sync(destination)
            self._install(temporary_path, destination_path)
        except OSError as error:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            if session.area in self._plain_areas:
                self._prune_empty_directories(destination_path.parent)
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        except BaseException:
            with suppress(OSError):
                temporary_path.unlink(missing_ok=True)
            if session.area in self._plain_areas:
                self._prune_empty_directories(destination_path.parent)
            raise
        self._finish_commit(session.area, session.key)
        try:
            shutil.rmtree(session_directory)
        except OSError:
            logger.warning("Failed to remove a completed local upload session")
        return total

    def abort_upload(self, session: UploadSession) -> None:
        """Remove the session directory; return silently when it does not exist."""
        session_directory = self._validate_upload_session(session)
        if not session_directory.exists():
            return
        self._load_upload_session(session)
        try:
            shutil.rmtree(session_directory)
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def cleanup_uploads(self, *, older_than_epoch: float) -> int:
        """Remove session directories and orphaned temporary files whose modification time is before the cutoff.

        Returns:
            The number of session directories removed.

        Raises:
            ValueError: When ``older_than_epoch`` is not finite.
            StorageBackendUnavailableError: After the pass, when any entry could not be removed.
        """
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
                    if candidate.is_symlink():
                        continue
                    if self._is_plain_temporary_name(candidate.name):
                        if candidate.stat().st_mtime < older_than_epoch:
                            candidate.unlink()
                        continue
                    if not candidate.is_dir():
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
        """Return the object bytes read through :meth:`open_stream`, or ``None`` when it does not exist."""
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
        """Open the object file, seek to ``offset`` and return a non-seekable reader limited to ``length`` bytes."""
        validate_stream_range(offset, length)
        path = self._existing_object_path(area, key)
        if path is None:
            raise FileNotFoundError(f"Storage object not found: {area}/{key}")
        try:
            source = path.open("rb")
        except (FileNotFoundError, NotADirectoryError) as error:
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
        """Return the file size and modification time; ``content_type`` and ``etag`` are always ``None``."""
        path = self._existing_object_path(area, key)
        if path is None:
            return None
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
        """Return a :class:`ServeFile` for the object's path after checking the file can be opened.

        ``download_as``, ``content_type`` and a non-default ``expires_in`` cannot be enforced; the first such call
        logs a warning.
        """
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
        """Return True when the object resolves to an existing file."""
        return self._existing_object_path(area, key) is not None

    def delete(self, area: str, key: str) -> None:
        """Delete the object file and prune the directories it leaves empty; a missing object is ignored."""
        if area in self._plain_areas:
            plain_path = self._resolve_plain(area, key)
            try:
                if stat.S_ISREG(plain_path.stat().st_mode):
                    plain_path.unlink()
                    self._prune_empty_directories(plain_path.parent)
            except (FileNotFoundError, NotADirectoryError):
                pass
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
            self._remove_opaque_object(area, key, best_effort=False)
            return

        current_path = self._current_object_path(area, key)
        if current_path is not None:
            try:
                current_path.unlink()
                (current_path.parent / _OBJECT_KEY_FILE).unlink(missing_ok=True)
                self._prune_empty_directories(current_path.parent)
            except OSError as error:
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        if not self._legacy_layout:
            return
        legacy_path = self._resolve_legacy(area, key)
        try:
            if stat.S_ISREG(legacy_path.stat().st_mode):
                legacy_path.unlink()
                self._prune_empty_directories(legacy_path.parent)
        except OSError as error:
            if not _is_absent(error):
                raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def delete_prefix(self, area: str, prefix: str) -> int:
        """Delete the object at ``prefix`` and every object under ``prefix/``, one at a time."""
        check_area(area)
        check_segment(prefix, "prefix")
        if area in self._plain_areas:
            self._resolve_plain(area, prefix)
        elif self._legacy_layout:
            self._resolve_legacy(area, prefix)
        descendant_prefix = f"{prefix}/"
        keys = [key for key, _ in self.iter_objects(area, prefix) if key == prefix or key.startswith(descendant_prefix)]
        for key in keys:
            self.delete(area, key)
        return len(keys)

    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None:
        """Stream the source object into the destination through :meth:`save_stream`."""
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
        """Return the keys from :meth:`iter_objects`, sorted."""
        return sorted(key for key, _ in self.iter_objects(area, prefix))

    def _iter_opaque_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        area_dir = self._resolve_area(area)
        try:
            if not stat.S_ISDIR(area_dir.stat().st_mode):
                return
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
                if candidate == self._resolve(area, key) and key.startswith(prefix):
                    yield key, metadata.st_mtime
        except FileNotFoundError:
            return
        except OSError as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        """Lazily yield ``(key, modified_epoch)`` from the plain, opaque and, when enabled, legacy layouts.

        Each key is yielded once; temporary files and symlinks that leave the area are skipped.
        """
        check_area(area)
        if prefix:
            check_listing_prefix(prefix)

        def objects() -> Iterator[tuple[str, float]]:
            seen: set[str] = set()
            if area in self._plain_areas:
                plain_area = self._resolve_area(area, legacy=True)
                try:
                    if stat.S_ISDIR(plain_area.stat().st_mode):
                        for candidate in plain_area.rglob("*"):
                            if self._is_plain_temporary_name(candidate.name):
                                continue
                            try:
                                resolved = candidate.resolve()
                            except RuntimeError:
                                continue
                            if resolved != candidate or not resolved.is_relative_to(plain_area):
                                continue
                            try:
                                metadata = resolved.stat()
                            except FileNotFoundError:
                                continue
                            if not stat.S_ISREG(metadata.st_mode):
                                continue
                            key = candidate.relative_to(plain_area).as_posix()
                            try:
                                check_segment(key, "key")
                            except ValueError:
                                continue
                            if key.startswith(prefix):
                                seen.add(key)
                                yield key, metadata.st_mtime
                except FileNotFoundError:
                    pass
                except OSError as error:
                    raise StorageBackendUnavailableError("Local storage backend is unavailable") from error

            for key, modified_epoch in self._iter_opaque_objects(area, prefix):
                if key not in seen:
                    seen.add(key)
                    yield key, modified_epoch

            if not self._legacy_layout or area in self._plain_areas:
                return
            legacy_area = self._resolve_area(area, legacy=True)
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
        """Create and discard a temporary file in the base directory."""
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
        """Return the configured URL prefix joined with the object's path relative to the base directory.

        The path is built whether or not the object exists. ``expires_in``, ``download_as`` and ``content_type``
        cannot be enforced; the first non-default value logs a warning.
        """
        check_area(area)
        check_segment(key, "key")
        self._warn_ignored_delivery_controls(
            expires_in=expires_in,
            download_as=download_as,
            content_type=content_type,
        )
        path = self._existing_object_path(area, key)
        if path is None:
            path = self._resolve_plain(area, key) if area in self._plain_areas else self._resolve(area, key)
        try:
            relative_path = path.relative_to(self._base.resolve()).as_posix()
        except (OSError, ValueError) as error:
            raise StorageBackendUnavailableError("Local storage backend is unavailable") from error
        return f"{self._url_prefix}/{quote(relative_path, safe='/')}"


def _sync(handle: IO[Any]) -> None:
    """Flush ``handle`` to the disk, so the rename that publishes it cannot outlive its bytes in a crash."""
    handle.flush()
    os.fsync(handle.fileno())


def _sync_directory(directory: Path) -> None:
    """Make a new entry in ``directory`` durable: syncing a file does not sync the name pointing to it.

    Windows cannot open a directory for syncing and journals the metadata itself, so there it is skipped.
    """
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
