"""S3-compatible ``StorageProvider`` backend.

Only imported by the composition root when ``storage_uri`` uses the ``s3://``
scheme, so local deployments never load ``boto3``. Install it with the ``s3``
extra (``pip install jasil[s3]``).
"""

import functools
import io
import json
import logging
import math
import tempfile
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, BinaryIO, Concatenate
from urllib.parse import parse_qs, quote, urlparse
from uuid import UUID, uuid4

from jasil._core import optional_deps

try:
    import boto3
except ModuleNotFoundError as error:
    if error.name != "boto3":
        raise
    raise optional_deps.not_installed("boto3", extra="s3", feature="S3 storage") from error
from boto3.exceptions import Boto3Error
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

import jasil._core.network as network
from jasil._core.storage_keys import (
    UPLOAD_STAGING_AREA,
    check_area,
    check_key_length,
    check_listing_prefix,
    check_segment,
)
from jasil._core.storage_streams import ExactSizeReader, non_seekable_reader, validate_stream_range
from jasil.providers import (
    ObjectStat,
    PartRef,
    ServeRedirect,
    StorageBackendUnavailableError,
    StorageSizeLimitError,
    StorageUploadSessionError,
    UploadSession,
)

logger = logging.getLogger(__name__)

# HeadObject error codes that mean "the object is not there" (as opposed to auth,
# region, throttling, or 5xx failures, which must surface rather than masquerade
# as a missing blob). "404" is what S3 returns for HeadObject; the aliases cover
# S3-compatible providers (MinIO, R2, Ceph).
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_INVALID_RANGE_CODES = frozenset({"416", "InvalidRange", "RequestedRangeNotSatisfiable"})
_MISSING_UPLOAD_CODES = frozenset({"404", "NoSuchUpload", "NotFound"})
_OBJECT_EXISTS_CODES = frozenset({"412", "PreconditionFailed"})
#: The longest expiry a Signature Version 4 presigned URL can carry: seven days.
_MAX_URL_EXPIRY_SECONDS = 7 * 24 * 3600

#: The client's socket timeouts. botocore defaults both to a minute; nothing else bounds a stalled endpoint.
CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 60
_MULTIPART_PART_BYTES = 8 * 1024 * 1024
_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024
_UPLOAD_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
_UPLOAD_MAX_PARTS = 10_000
_UPLOAD_MANIFEST_VERSION = 1
_UPLOAD_MANIFEST_PREFIX = f"{UPLOAD_STAGING_AREA}/v1"
_UPLOAD_MANIFEST_CONTENT_TYPE = "application/json"


@dataclass(frozen=True)
class _S3UploadState:
    area: str
    key: str
    session_id: str
    upload_id: str
    max_bytes: int | None
    min_part_size: int
    max_part_size: int
    max_parts: int
    created_epoch: float


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def _translate_s3_stream_error(error: Exception) -> None:
    if isinstance(error, (Boto3Error, BotoCoreError, ClientError)):
        raise StorageBackendUnavailableError("S3 storage backend is unavailable") from error


def _translate_s3_errors[**P, R](
    method: Callable[Concatenate["S3Storage", P], R],
) -> Callable[Concatenate["S3Storage", P], R]:
    @functools.wraps(method)
    def wrapper(self: "S3Storage", *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return method(self, *args, **kwargs)
        except (Boto3Error, BotoCoreError, ClientError) as error:
            raise StorageBackendUnavailableError("S3 storage backend is unavailable") from error

    return wrapper


def _read_part(source: BinaryIO, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = source.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _has_control_character(value: str) -> bool:
    """True for a C0 control (other than tab) or DEL: none may appear in a header value, and key validation refuses them too."""
    return any((ord(char) < 0x20 and char != "\t") or ord(char) == 0x7F for char in value)


def _content_disposition(filename: str) -> str:
    if not filename or _has_control_character(filename):
        raise ValueError("download_as must be a non-empty filename without control characters")
    fallback = filename.encode("ascii", "replace").decode("ascii").replace("\\", "\\\\").replace('"', '\\"')
    encoded = quote(filename, safe="")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


def _validate_response_content_type(content_type: str) -> None:
    if not content_type or _has_control_character(content_type):
        raise ValueError("content_type must be non-empty and contain no control characters")


def _check_endpoint(endpoint_url: str) -> None:
    """Refuse an ``endpoint_url`` that is not HTTPS, unless it is cleartext to a private host.

    Request signatures, object contents and presigned URLs cross the connection, so cleartext is for an
    S3-compatible store on the same private network only, as for mail and geocoding.

    Raises:
        ValueError: When the URL is not ``http(s)://host``, or is ``http://`` to a host that resolves to a public
            address (or does not resolve).
    """
    endpoint = urlparse(endpoint_url)
    if endpoint.scheme not in ("http", "https") or not endpoint.hostname:
        raise ValueError("endpoint_url must be an https:// URL, or http:// to a private host")
    if endpoint.username is not None or endpoint.password is not None:
        raise ValueError("endpoint_url must not carry credentials")
    if endpoint.scheme == "http":
        reason = network.non_public_rejection_reason(endpoint.hostname)
        if reason is not None:
            raise ValueError(f"A cleartext http:// endpoint_url needs a private host; the host {reason}")


class S3Storage:
    """``StorageProvider`` backed by S3-compatible object storage.

    Blobs are stored at ``{prefix}/{area}/{key}`` in a bucket, where *area* is the
    domain-owned namespace; ``url`` returns a presigned GET URL. The DB stores
    only the key, so migrating local -> S3 needs no data migration. Build via
    :meth:`from_uri`.
    """

    def __init__(self, client: Any, bucket: str, prefix: str = "") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    @classmethod
    def from_uri(cls, uri: str) -> "S3Storage":
        """Build a backend from ``s3://bucket/prefix?region=…&endpoint_url=…``.

        Args:
            uri: An ``s3://`` storage URI. ``region`` and ``endpoint_url`` query
                parameters are optional (``endpoint_url`` enables S3-compatible
                providers such as MinIO or R2).

        Returns:
            A configured ``S3Storage``.

        Raises:
            ValueError: When the URI has no bucket, carries credentials, or has an ``endpoint_url`` that is not
                an ``https://`` URL or an ``http://`` one whose host resolves only to private addresses.
        """
        parsed = urlparse(uri)
        bucket = parsed.netloc
        if not bucket:
            raise ValueError("An s3 storage_uri is missing a bucket")
        # The netloc would otherwise become the bucket name, credentials included, and surface in botocore's errors.
        if "@" in bucket:
            raise ValueError("An s3 storage_uri must not carry credentials")
        params = parse_qs(parsed.query)
        endpoint_url = params.get("endpoint_url", [None])[0]
        if endpoint_url is not None:
            _check_endpoint(endpoint_url)
        try:
            client = boto3.client(
                "s3",
                region_name=params.get("region", [None])[0],
                endpoint_url=endpoint_url,
                # SigV4, which presigned URLs do not default to: SigV2 query auth cannot fetch SSE-KMS objects
                # and is refused by stores that speak only SigV4.
                config=Config(
                    retries={"max_attempts": 3, "mode": "standard"},
                    signature_version="s3v4",
                    connect_timeout=CONNECT_TIMEOUT_SECONDS,
                    read_timeout=READ_TIMEOUT_SECONDS,
                ),
            )
        except BotoCoreError as error:
            raise StorageBackendUnavailableError("S3 storage backend is unavailable") from error
        return cls(client, bucket, parsed.path)

    def _object_key(self, area: str, key: str) -> str:
        # ``..`` is a literal in an S3 key rather than a traversal, so an unchecked
        # segment is stored under a nonsense key instead of being refused. The
        # local backend rejects it, and one contract has to hold on both.
        check_area(area)
        check_segment(key, "key")
        object_key = "/".join(part for part in (self._prefix, area, key) if part)
        # The prefix and area count toward S3's limit as well; refused here it is the caller's error, not an outage.
        check_key_length(object_key, "object key (prefix, area and key)")
        return object_key

    def _validate_upload_session(self, session: UploadSession) -> str:
        try:
            object_key = self._object_key(session.area, session.key)
            normalized_session_id = UUID(session.session_id).hex
        except (AttributeError, TypeError, ValueError) as error:
            raise StorageUploadSessionError("Upload session is not valid for S3 storage") from error
        if (
            normalized_session_id != session.session_id
            or (session.max_bytes is not None and session.max_bytes < 0)
            or session.min_part_size != _UPLOAD_MIN_PART_SIZE
            or session.max_part_size != _UPLOAD_MAX_PART_SIZE
            or session.max_parts != _UPLOAD_MAX_PARTS
        ):
            raise StorageUploadSessionError("Upload session is not valid for S3 storage")
        return object_key

    def _upload_manifest_root(self) -> str:
        return "/".join(part for part in (self._prefix, _UPLOAD_MANIFEST_PREFIX) if part)

    def _upload_manifest_key(self, session_id: str) -> str:
        return f"{self._upload_manifest_root()}/{session_id}.json"

    @staticmethod
    def _encode_upload_state(state: _S3UploadState) -> bytes:
        return json.dumps(
            {
                "version": _UPLOAD_MANIFEST_VERSION,
                "area": state.area,
                "key": state.key,
                "session_id": state.session_id,
                "upload_id": state.upload_id,
                "max_bytes": state.max_bytes,
                "min_part_size": state.min_part_size,
                "max_part_size": state.max_part_size,
                "max_parts": state.max_parts,
                "created_epoch": state.created_epoch,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

    @staticmethod
    def _decode_upload_state(data: bytes) -> _S3UploadState:
        try:
            manifest = json.loads(data)
            if not isinstance(manifest, dict) or manifest.get("version") != _UPLOAD_MANIFEST_VERSION:
                raise ValueError
            state = _S3UploadState(
                area=manifest["area"],
                key=manifest["key"],
                session_id=manifest["session_id"],
                upload_id=manifest["upload_id"],
                max_bytes=manifest["max_bytes"],
                min_part_size=manifest["min_part_size"],
                max_part_size=manifest["max_part_size"],
                max_parts=manifest["max_parts"],
                created_epoch=manifest["created_epoch"],
            )
            if not all(
                isinstance(value, str) and value for value in (state.area, state.key, state.session_id, state.upload_id)
            ):
                raise ValueError
            if UUID(state.session_id).hex != state.session_id:
                raise ValueError
            check_area(state.area)
            check_segment(state.key, "key")
            if (
                (
                    state.max_bytes is not None
                    and (
                        isinstance(state.max_bytes, bool) or not isinstance(state.max_bytes, int) or state.max_bytes < 0
                    )
                )
                or state.min_part_size != _UPLOAD_MIN_PART_SIZE
                or state.max_part_size != _UPLOAD_MAX_PART_SIZE
                or state.max_parts != _UPLOAD_MAX_PARTS
                or isinstance(state.created_epoch, bool)
                or not isinstance(state.created_epoch, int | float)
                or not math.isfinite(state.created_epoch)
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise StorageUploadSessionError("S3 upload session manifest is invalid") from error
        return state

    def _load_upload_state_by_key(self, manifest_key: str, *, missing_ok: bool = False) -> _S3UploadState | None:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=manifest_key)
        except ClientError as error:
            if _error_code(error) in _MISSING_OBJECT_CODES:
                if missing_ok:
                    return None
                raise StorageUploadSessionError("Upload session is not active") from error
            raise
        body = response["Body"]
        try:
            data = body.read()
        finally:
            body.close()
        state = self._decode_upload_state(data)
        if self._upload_manifest_key(state.session_id) != manifest_key:
            raise StorageUploadSessionError("S3 upload session manifest key does not match its state")
        return state

    def _load_upload_state(self, session: UploadSession, *, missing_ok: bool = False) -> _S3UploadState | None:
        self._validate_upload_session(session)
        state = self._load_upload_state_by_key(
            self._upload_manifest_key(session.session_id),
            missing_ok=missing_ok,
        )
        if state is None:
            return None
        expected = (
            session.area,
            session.key,
            session.session_id,
            session.max_bytes,
            session.min_part_size,
            session.max_part_size,
            session.max_parts,
        )
        actual = (
            state.area,
            state.key,
            state.session_id,
            state.max_bytes,
            state.min_part_size,
            state.max_part_size,
            state.max_parts,
        )
        if actual != expected:
            raise StorageUploadSessionError("Upload session does not match its durable state")
        return state

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

    def _list_upload_parts(self, state: _S3UploadState) -> dict[int, tuple[int, str]]:
        object_key = self._object_key(state.area, state.key)
        uploaded: dict[int, tuple[int, str]] = {}
        marker: int | None = None
        while True:
            request: dict[str, Any] = {
                "Bucket": self._bucket,
                "Key": object_key,
                "UploadId": state.upload_id,
            }
            if marker is not None:
                request["PartNumberMarker"] = marker
            try:
                response = self._client.list_parts(**request)
            except ClientError as error:
                if _error_code(error) in _MISSING_UPLOAD_CODES:
                    raise StorageUploadSessionError("Upload session is not active") from error
                raise
            for part in response.get("Parts", []):
                part_number = int(part["PartNumber"])
                if part_number in uploaded:
                    raise StorageUploadSessionError("Upload session contains duplicate part state")
                uploaded[part_number] = (int(part["Size"]), str(part["ETag"]))
            if not response.get("IsTruncated"):
                return uploaded
            marker = int(response["NextPartNumberMarker"])

    @_translate_s3_errors
    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        """Store ``data`` with one ``PutObject``; ``content_type`` becomes the object's ``Content-Type``."""
        extra = {"ContentType": content_type} if content_type else {}
        self._client.put_object(Bucket=self._bucket, Key=self._object_key(area, key), Body=data, **extra)
        return key

    @_translate_s3_errors
    def create(self, area: str, key: str, data: bytes, content_type: str | None = None) -> bool:
        """Store ``data`` under ``area``/``key`` only if no object is there yet.

        One conditional ``PutObject`` (``If-None-Match: *``), so no other writer can slip in between a check
        and the write, and a bucket policy can deny every ``PutObject`` without it.

        Args:
            area: The storage area.
            key: The key within the area.
            data: The bytes to store.
            content_type: The object's ``Content-Type``, if any.

        Returns:
            True when this call stored the object; False when an object was already there.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address.
            StorageBackendUnavailableError: When S3 fails, including a concurrent conditional write to the
                same key (``409``), which a retry settles.
        """
        extra = {"ContentType": content_type} if content_type else {}
        try:
            self._client.put_object(
                Bucket=self._bucket, Key=self._object_key(area, key), Body=data, IfNoneMatch="*", **extra
            )
        except ClientError as error:
            if _error_code(error) in _OBJECT_EXISTS_CODES:
                return False
            raise
        return True

    @_translate_s3_errors
    def save_stream(
        self,
        area: str,
        key: str,
        source: BinaryIO,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> int:
        """Upload ``source`` as a multipart upload, or one ``PutObject`` when it is empty.

        An upload that fails is aborted, so no partial object remains.
        """
        object_key = self._object_key(area, key)
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Storage stream max_bytes must not be negative")
        upload_id: str | None = None
        parts: list[dict[str, Any]] = []
        total = 0
        try:
            while True:
                read_size = _MULTIPART_PART_BYTES
                if max_bytes is not None:
                    read_size = min(read_size, max_bytes - total + 1)
                part = _read_part(source, read_size)
                if not part:
                    break
                total += len(part)
                if max_bytes is not None and total > max_bytes:
                    raise StorageSizeLimitError(f"Storage stream exceeds max_bytes={max_bytes}")
                if upload_id is None:
                    create_args = {"Bucket": self._bucket, "Key": object_key}
                    if content_type:
                        create_args["ContentType"] = content_type
                    upload_id = self._client.create_multipart_upload(**create_args)["UploadId"]
                part_number = len(parts) + 1
                response = self._client.upload_part(
                    Bucket=self._bucket,
                    Key=object_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=part,
                )
                parts.append({"ETag": response["ETag"], "PartNumber": part_number})

            if upload_id is None:
                put_args = {"Bucket": self._bucket, "Key": object_key, "Body": b""}
                if content_type:
                    put_args["ContentType"] = content_type
                self._client.put_object(**put_args)
                return 0
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            upload_id = None
            return total
        except BaseException:
            if upload_id is not None:
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._bucket,
                        Key=object_key,
                        UploadId=upload_id,
                    )
                except (BotoCoreError, ClientError):
                    logger.warning("Failed to abort an incomplete S3 multipart upload")
            raise

    @_translate_s3_errors
    def begin_upload(
        self,
        area: str,
        key: str,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> UploadSession:
        """Start an S3 multipart upload and store a session manifest object that records its upload id."""
        object_key = self._object_key(area, key)
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Resumable upload max_bytes must not be negative")
        request = {"Bucket": self._bucket, "Key": object_key}
        if content_type:
            request["ContentType"] = content_type
        response = self._client.create_multipart_upload(**request)
        session = UploadSession(
            area=area,
            key=key,
            session_id=uuid4().hex,
            max_bytes=max_bytes,
            min_part_size=_UPLOAD_MIN_PART_SIZE,
            max_part_size=_UPLOAD_MAX_PART_SIZE,
            max_parts=_UPLOAD_MAX_PARTS,
        )
        state = _S3UploadState(
            area=area,
            key=key,
            session_id=session.session_id,
            upload_id=response["UploadId"],
            max_bytes=max_bytes,
            min_part_size=session.min_part_size,
            max_part_size=session.max_part_size,
            max_parts=session.max_parts,
            created_epoch=time.time(),
        )
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._upload_manifest_key(session.session_id),
                Body=self._encode_upload_state(state),
                ContentType=_UPLOAD_MANIFEST_CONTENT_TYPE,
            )
        except (Boto3Error, BotoCoreError, ClientError):
            try:
                self._client.abort_multipart_upload(
                    Bucket=self._bucket,
                    Key=object_key,
                    UploadId=state.upload_id,
                )
            except (Boto3Error, BotoCoreError, ClientError):
                logger.warning("Failed to abort an S3 multipart upload whose session manifest could not be stored")
            raise
        return session

    @_translate_s3_errors
    def upload_part(
        self,
        session: UploadSession,
        part_number: int,
        source: BinaryIO,
        *,
        size: int,
    ) -> PartRef:
        """Stage the part in a spooled temporary file and send it with ``UploadPart``; the validator is its ETag."""
        self._validate_upload_session(session)
        self._validate_part_number(session, part_number)
        reader = ExactSizeReader(source, size)
        if size > session.max_part_size:
            raise StorageSizeLimitError(f"Upload part exceeds max_part_size={session.max_part_size}")
        if size == 0:
            reader.verify_complete()
        state = self._load_upload_state(session)
        if state is None:  # pragma: no cover - missing_ok is false above
            raise AssertionError("Non-optional upload state was not loaded")
        object_key = self._object_key(state.area, state.key)
        if session.max_bytes is not None:
            uploaded = self._list_upload_parts(state)
            staged_total = sum(part_size for number, (part_size, _) in uploaded.items() if number != part_number)
            if staged_total + size > session.max_bytes:
                raise StorageSizeLimitError(f"Resumable upload exceeds max_bytes={session.max_bytes}")
        # botocore rewinds the body to checksum it over plain HTTP and to resend it after a transient failure,
        # which a read-once source cannot do. Staging the part also proves its size before any request is sent.
        with tempfile.SpooledTemporaryFile(max_size=_MULTIPART_PART_BYTES) as body:
            while chunk := reader.read(_MULTIPART_PART_BYTES):
                try:
                    body.write(chunk)
                except OSError as error:
                    raise StorageBackendUnavailableError("S3 storage backend is unavailable") from error
            reader.verify_complete()
            body.seek(0)
            try:
                response = self._client.upload_part(
                    Bucket=self._bucket,
                    Key=object_key,
                    UploadId=state.upload_id,
                    PartNumber=part_number,
                    Body=body,
                    ContentLength=size,
                )
            except ClientError as error:
                if _error_code(error) in _MISSING_UPLOAD_CODES:
                    raise StorageUploadSessionError("Upload session is not active") from error
                raise
        return PartRef(part_number=part_number, size=size, validator=response["ETag"])

    @_translate_s3_errors
    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int:
        """Check the parts against those S3 lists, complete the multipart upload and delete the session manifest."""
        self._validate_upload_session(session)
        ordered, total = self._validate_completion_parts(session, parts)
        state = self._load_upload_state(session)
        if state is None:  # pragma: no cover - missing_ok is false above
            raise AssertionError("Non-optional upload state was not loaded")
        object_key = self._object_key(state.area, state.key)
        uploaded = self._list_upload_parts(state)
        if set(uploaded) != {part.part_number for part in ordered}:
            raise StorageUploadSessionError("Completion must reference every uploaded part exactly once")
        for part in ordered:
            if uploaded[part.part_number] != (part.size, part.validator):
                raise StorageUploadSessionError(f"Upload part {part.part_number} does not match its reference")
        try:
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=object_key,
                UploadId=state.upload_id,
                MultipartUpload={
                    "Parts": [{"ETag": part.validator, "PartNumber": part.part_number} for part in ordered]
                },
            )
        except ClientError as error:
            if _error_code(error) in _MISSING_UPLOAD_CODES:
                raise StorageUploadSessionError("Upload session is not active") from error
            raise
        try:
            self._client.delete_object(
                Bucket=self._bucket,
                Key=self._upload_manifest_key(session.session_id),
            )
        except (Boto3Error, BotoCoreError, ClientError):
            logger.warning("Failed to remove a completed S3 upload session manifest")
        return total

    @_translate_s3_errors
    def abort_upload(self, session: UploadSession) -> None:
        """Abort the multipart upload and delete the session manifest; return silently when there is no manifest."""
        state = self._load_upload_state(session, missing_ok=True)
        if state is None:
            return
        object_key = self._object_key(state.area, state.key)
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=object_key,
                UploadId=state.upload_id,
            )
        except ClientError as error:
            if _error_code(error) in _MISSING_UPLOAD_CODES:
                pass
            else:
                raise
        self._client.delete_object(
            Bucket=self._bucket,
            Key=self._upload_manifest_key(session.session_id),
        )

    @_translate_s3_errors
    def cleanup_uploads(self, *, older_than_epoch: float) -> int:
        """Abort the multipart uploads whose manifest was last modified before the cutoff, and delete the manifests.

        Returns:
            The number of sessions removed.

        Raises:
            ValueError: When ``older_than_epoch`` is not finite.
            StorageBackendUnavailableError: After the pass, when any session could not be removed; the cause is
                chained, and an invalid manifest is named in the log.
        """
        if not math.isfinite(older_than_epoch):
            raise ValueError("older_than_epoch must be finite")
        removed = 0
        first_error: Boto3Error | BotoCoreError | ClientError | StorageUploadSessionError | None = None
        start_after: str | None = None
        while True:
            request = {
                "Bucket": self._bucket,
                "Prefix": f"{self._upload_manifest_root()}/",
                "MaxKeys": 1000,
            }
            if start_after is not None:
                request["StartAfter"] = start_after
            page = self._client.list_objects_v2(**request)
            contents = page.get("Contents", [])
            for entry in contents:
                if entry["LastModified"].timestamp() >= older_than_epoch:
                    continue
                manifest_key = entry["Key"]
                try:
                    state = self._load_upload_state_by_key(manifest_key, missing_ok=True)
                except StorageUploadSessionError as error:
                    # Reported after the pass: listing order is stable, so stopping here would stop every pass here.
                    logger.warning("Skipping invalid upload session manifest %s: %s", manifest_key, error)
                    if first_error is None:
                        first_error = error
                    continue
                except (Boto3Error, BotoCoreError, ClientError) as error:
                    logger.warning("Could not read upload session manifest %s: %s", manifest_key, type(error).__name__)
                    if first_error is None:
                        first_error = error
                    continue
                if state is None:
                    continue
                object_key = self._object_key(state.area, state.key)
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._bucket,
                        Key=object_key,
                        UploadId=state.upload_id,
                    )
                except (Boto3Error, BotoCoreError, ClientError) as error:
                    if not (isinstance(error, ClientError) and _error_code(error) in _MISSING_UPLOAD_CODES):
                        if first_error is None:
                            first_error = error
                        continue
                try:
                    self._client.delete_object(Bucket=self._bucket, Key=manifest_key)
                except (Boto3Error, BotoCoreError, ClientError) as error:
                    if first_error is None:
                        first_error = error
                    continue
                removed += 1
            if not page.get("IsTruncated") or not contents:
                break
            start_after = contents[-1]["Key"]
        if first_error is not None:
            raise StorageBackendUnavailableError("S3 storage backend is unavailable") from first_error
        return removed

    def get(self, area: str, key: str) -> bytes | None:
        """Return the object bytes read through :meth:`open_stream`, or ``None`` when it does not exist."""
        try:
            stream = self.open_stream(area, key)
        except FileNotFoundError:
            return None
        with stream:
            return stream.read()

    @_translate_s3_errors
    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Open a ranged ``GetObject`` as a non-seekable reader; a range past the end of the object yields no bytes."""
        object_key = self._object_key(area, key)
        validate_stream_range(offset, length)
        if length == 0:
            try:
                self._client.head_object(Bucket=self._bucket, Key=object_key)
            except ClientError as error:
                if _error_code(error) in _MISSING_OBJECT_CODES:
                    raise FileNotFoundError(f"Storage object not found: {area}/{key}") from error
                raise
            return non_seekable_reader(io.BytesIO(), length=0, translate_error=_translate_s3_stream_error)

        request = {"Bucket": self._bucket, "Key": object_key}
        if length is not None:
            request["Range"] = f"bytes={offset}-{offset + length - 1}"
        elif offset:
            request["Range"] = f"bytes={offset}-"
        try:
            response = self._client.get_object(**request)
        except ClientError as error:
            code = _error_code(error)
            if code in _MISSING_OBJECT_CODES:
                raise FileNotFoundError(f"Storage object not found: {area}/{key}") from error
            if code in _INVALID_RANGE_CODES:
                return non_seekable_reader(io.BytesIO(), length=0, translate_error=_translate_s3_stream_error)
            raise
        return non_seekable_reader(response["Body"], length=length, translate_error=_translate_s3_stream_error)

    @_translate_s3_errors
    def stat(self, area: str, key: str) -> ObjectStat | None:
        """Return the ``HeadObject`` size, modification time, content type and ETag."""
        try:
            response = self._client.head_object(Bucket=self._bucket, Key=self._object_key(area, key))
        except ClientError as error:
            if _error_code(error) in _MISSING_OBJECT_CODES:
                return None
            raise
        return ObjectStat(
            size=int(response["ContentLength"]),
            modified_epoch=response["LastModified"].timestamp(),
            content_type=response.get("ContentType"),
            etag=response.get("ETag"),
        )

    def serve(
        self,
        area: str,
        key: str,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
        expires_in: int = 3600,
    ) -> ServeRedirect:
        """Return a :class:`ServeRedirect` to a presigned URL, after checking the object exists.

        Raises:
            FileNotFoundError: When the object does not exist.
            ValueError: When ``expires_in`` is outside 1 second to 7 days, or ``download_as`` or
            ``content_type`` is invalid.
        """
        if self.stat(area, key) is None:
            raise FileNotFoundError(f"Storage object not found: {area}/{key}")
        return ServeRedirect(
            self.url(
                area,
                key,
                expires_in=expires_in,
                download_as=download_as,
                content_type=content_type,
            )
        )

    @_translate_s3_errors
    def exists(self, area: str, key: str) -> bool:
        """Return True when ``HeadObject`` finds the object."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._object_key(area, key))
        except ClientError as error:
            if _error_code(error) in _MISSING_OBJECT_CODES:
                return False
            raise
        return True

    @_translate_s3_errors
    def delete(self, area: str, key: str) -> None:
        """Delete the object with ``DeleteObject``."""
        self._client.delete_object(Bucket=self._bucket, Key=self._object_key(area, key))

    @_translate_s3_errors
    def delete_prefix(self, area: str, prefix: str) -> int:
        """Delete the object at ``prefix`` and every object listed under ``prefix/``, in batches."""
        root = self._object_key(area, prefix)
        count = 0
        try:
            self._client.head_object(Bucket=self._bucket, Key=root)
        except ClientError as error:
            if _error_code(error) not in _MISSING_OBJECT_CODES:
                raise
        else:
            self._delete_objects([root])
            count += 1

        descendant_prefix = f"{root}/"
        # Delete each first page before listing again. Carrying a continuation
        # token across a listing being mutated can skip keys; S3 makes a
        # successful deletion visible to the next LIST request.
        while True:
            page = self._client.list_objects_v2(
                Bucket=self._bucket,
                Prefix=descendant_prefix,
                MaxKeys=1000,
            )
            keys = [entry["Key"] for entry in page.get("Contents", [])]
            if not keys:
                return count
            self._delete_objects(keys)
            count += len(keys)

    def _delete_objects(self, keys: list[str]) -> None:
        response = self._client.delete_objects(
            Bucket=self._bucket,
            Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
        )
        if errors := response.get("Errors"):
            raise StorageBackendUnavailableError(f"S3 failed to delete {len(errors)} storage objects")

    @_translate_s3_errors
    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None:
        """Copy the object server-side with the client's managed ``copy``."""
        check_segment(src_area, "source area")
        check_segment(src_key, "source key")
        check_segment(dst_area, "destination area")
        check_segment(dst_key, "destination key")
        source_key = self._object_key(src_area, src_key)
        destination_key = self._object_key(dst_area, dst_key)
        if source_key == destination_key:
            if self.stat(src_area, src_key) is None:
                raise FileNotFoundError(f"Storage object not found: {src_area}/{src_key}")
            return
        try:
            self._client.copy(
                {"Bucket": self._bucket, "Key": source_key},
                self._bucket,
                destination_key,
            )
        except ClientError as error:
            if _error_code(error) in _MISSING_OBJECT_CODES:
                raise FileNotFoundError(f"Storage object not found: {src_area}/{src_key}") from error
            raise

    def list_keys(self, area: str, prefix: str = "") -> list[str]:
        """Return the keys from :meth:`iter_objects`, sorted."""
        return sorted(key for key, _ in self.iter_objects(area, prefix))

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        """Lazily yield ``(key, modified_epoch)`` from a paginated ``ListObjectsV2`` under the area and prefix."""
        check_area(area)
        if prefix:
            check_listing_prefix(prefix)
        area_root = "/".join(part for part in (self._prefix, area) if part)
        object_prefix = f"{area_root}/{prefix}"
        strip_from = len(area_root) + 1

        def objects() -> Iterator[tuple[str, float]]:
            try:
                paginator = self._client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self._bucket, Prefix=object_prefix):
                    for entry in page.get("Contents", []):
                        yield entry["Key"][strip_from:], entry["LastModified"].timestamp()
            except (BotoCoreError, ClientError) as error:
                raise StorageBackendUnavailableError("S3 storage backend is unavailable") from error

        return objects()

    @_translate_s3_errors
    def check_writable(self) -> None:
        """Put and delete an empty probe object under the configured prefix."""
        probe_key = "/".join(part for part in (self._prefix, ".jasil-write-probe", uuid4().hex) if part)
        self._client.put_object(Bucket=self._bucket, Key=probe_key, Body=b"")
        self._client.delete_object(Bucket=self._bucket, Key=probe_key)

    @_translate_s3_errors
    def url(
        self,
        area: str,
        key: str,
        expires_in: int = 3600,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
    ) -> str:
        """Return a presigned ``GetObject`` URL; the object is not checked.

        Raises:
            ValueError: When ``expires_in`` is outside 1 second to 7 days, or ``download_as`` or
            ``content_type`` is invalid.
        """
        if not 1 <= expires_in <= _MAX_URL_EXPIRY_SECONDS:
            raise ValueError(f"expires_in must be between 1 and {_MAX_URL_EXPIRY_SECONDS} seconds, not {expires_in}")
        params = {"Bucket": self._bucket, "Key": self._object_key(area, key)}
        if download_as is not None:
            params["ResponseContentDisposition"] = _content_disposition(download_as)
        if content_type is not None:
            _validate_response_content_type(content_type)
            params["ResponseContentType"] = content_type
        return self._client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=expires_in,
        )
