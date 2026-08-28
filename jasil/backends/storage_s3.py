"""S3-compatible ``StorageProvider`` backend.

Only imported by the composition root when ``storage_uri`` uses the ``s3://``
scheme, so local deployments never load ``boto3``. Install it with the ``s3``
extra (``pip install jasil[s3]``).
"""

import functools
import io
import logging
import math
from collections.abc import Callable, Iterator, Sequence
from typing import Any, BinaryIO, Concatenate
from urllib.parse import parse_qs, quote, urlparse
from uuid import uuid4

import boto3
from boto3.exceptions import Boto3Error
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from jasil._core.storage_keys import check_area, check_listing_prefix, check_segment
from jasil._core.storage_streams import non_seekable_reader, validate_stream_range
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
_MULTIPART_PART_BYTES = 8 * 1024 * 1024
_UPLOAD_MIN_PART_SIZE = 5 * 1024 * 1024
_UPLOAD_MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
_UPLOAD_MAX_PARTS = 10_000


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


def _content_disposition(filename: str) -> str:
    if not filename or "\r" in filename or "\n" in filename:
        raise ValueError("download_as must be a non-empty filename without line breaks")
    fallback = filename.encode("ascii", "replace").decode("ascii").replace("\\", "\\\\").replace('"', '\\"')
    encoded = quote(filename, safe="")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


def _validate_response_content_type(content_type: str) -> None:
    if not content_type or "\r" in content_type or "\n" in content_type:
        raise ValueError("content_type must be non-empty and contain no line breaks")


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
            ValueError: When the URI has no bucket.
        """
        parsed = urlparse(uri)
        bucket = parsed.netloc
        if not bucket:
            raise ValueError(f"An s3 storage_uri is missing a bucket: {uri!r}")
        params = parse_qs(parsed.query)
        try:
            client = boto3.client(
                "s3",
                region_name=params.get("region", [None])[0],
                endpoint_url=params.get("endpoint_url", [None])[0],
                config=Config(retries={"max_attempts": 3, "mode": "standard"}),
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
        return "/".join(part for part in (self._prefix, area, key) if part)

    def _validate_upload_session(self, session: UploadSession) -> str:
        try:
            object_key = self._object_key(session.area, session.key)
        except (AttributeError, TypeError, ValueError) as error:
            raise StorageUploadSessionError("Upload session is not valid for S3 storage") from error
        if (
            not session.upload_id
            or (session.max_bytes is not None and session.max_bytes < 0)
            or session.min_part_size != _UPLOAD_MIN_PART_SIZE
            or session.max_part_size != _UPLOAD_MAX_PART_SIZE
            or session.max_parts != _UPLOAD_MAX_PARTS
        ):
            raise StorageUploadSessionError("Upload session is not valid for S3 storage")
        return object_key

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

    def _list_upload_parts(self, session: UploadSession) -> dict[int, tuple[int, str]]:
        object_key = self._validate_upload_session(session)
        uploaded: dict[int, tuple[int, str]] = {}
        marker: int | None = None
        while True:
            request: dict[str, Any] = {
                "Bucket": self._bucket,
                "Key": object_key,
                "UploadId": session.upload_id,
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
        extra = {"ContentType": content_type} if content_type else {}
        self._client.put_object(Bucket=self._bucket, Key=self._object_key(area, key), Body=data, **extra)
        return key

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
        object_key = self._object_key(area, key)
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("Resumable upload max_bytes must not be negative")
        request = {"Bucket": self._bucket, "Key": object_key}
        if content_type:
            request["ContentType"] = content_type
        response = self._client.create_multipart_upload(**request)
        return UploadSession(
            area=area,
            key=key,
            upload_id=response["UploadId"],
            max_bytes=max_bytes,
            min_part_size=_UPLOAD_MIN_PART_SIZE,
            max_part_size=_UPLOAD_MAX_PART_SIZE,
            max_parts=_UPLOAD_MAX_PARTS,
        )

    @_translate_s3_errors
    def upload_part(self, session: UploadSession, part_number: int, data: bytes) -> PartRef:
        object_key = self._validate_upload_session(session)
        self._validate_part_number(session, part_number)
        size = len(data)
        if size > session.max_part_size:
            raise StorageSizeLimitError(f"Upload part exceeds max_part_size={session.max_part_size}")
        if session.max_bytes is not None:
            uploaded = self._list_upload_parts(session)
            staged_total = sum(part_size for number, (part_size, _) in uploaded.items() if number != part_number)
            if staged_total + size > session.max_bytes:
                raise StorageSizeLimitError(f"Resumable upload exceeds max_bytes={session.max_bytes}")
        try:
            response = self._client.upload_part(
                Bucket=self._bucket,
                Key=object_key,
                UploadId=session.upload_id,
                PartNumber=part_number,
                Body=data,
            )
        except ClientError as error:
            if _error_code(error) in _MISSING_UPLOAD_CODES:
                raise StorageUploadSessionError("Upload session is not active") from error
            raise
        return PartRef(part_number=part_number, size=size, etag=response["ETag"])

    @_translate_s3_errors
    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int:
        object_key = self._validate_upload_session(session)
        ordered, total = self._validate_completion_parts(session, parts)
        uploaded = self._list_upload_parts(session)
        if set(uploaded) != {part.part_number for part in ordered}:
            raise StorageUploadSessionError("Completion must reference every uploaded part exactly once")
        for part in ordered:
            if uploaded[part.part_number] != (part.size, part.etag):
                raise StorageUploadSessionError(f"Upload part {part.part_number} does not match its reference")
        try:
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=object_key,
                UploadId=session.upload_id,
                MultipartUpload={"Parts": [{"ETag": part.etag, "PartNumber": part.part_number} for part in ordered]},
            )
        except ClientError as error:
            if _error_code(error) in _MISSING_UPLOAD_CODES:
                raise StorageUploadSessionError("Upload session is not active") from error
            raise
        return total

    @_translate_s3_errors
    def abort_upload(self, session: UploadSession) -> None:
        object_key = self._validate_upload_session(session)
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket,
                Key=object_key,
                UploadId=session.upload_id,
            )
        except ClientError as error:
            if _error_code(error) in _MISSING_UPLOAD_CODES:
                return
            raise

    @_translate_s3_errors
    def cleanup_uploads(self, *, older_than_epoch: float) -> int:
        if not math.isfinite(older_than_epoch):
            raise ValueError("older_than_epoch must be finite")
        prefix = f"{self._prefix}/" if self._prefix else ""
        key_marker: str | None = None
        upload_id_marker: str | None = None
        removed = 0
        while True:
            request = {"Bucket": self._bucket, "Prefix": prefix}
            if key_marker is not None:
                request["KeyMarker"] = key_marker
            if upload_id_marker is not None:
                request["UploadIdMarker"] = upload_id_marker
            response = self._client.list_multipart_uploads(**request)
            for upload in response.get("Uploads", []):
                if upload["Initiated"].timestamp() >= older_than_epoch:
                    continue
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._bucket,
                        Key=upload["Key"],
                        UploadId=upload["UploadId"],
                    )
                except ClientError as error:
                    if _error_code(error) in _MISSING_UPLOAD_CODES:
                        continue
                    raise
                removed += 1
            if not response.get("IsTruncated"):
                return removed
            key_marker = response["NextKeyMarker"]
            upload_id_marker = response["NextUploadIdMarker"]

    def get(self, area: str, key: str) -> bytes | None:
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
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._object_key(area, key))
        except ClientError as error:
            if _error_code(error) in _MISSING_OBJECT_CODES:
                return False
            raise
        return True

    @_translate_s3_errors
    def delete(self, area: str, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._object_key(area, key))

    @_translate_s3_errors
    def delete_prefix(self, area: str, prefix: str) -> int:
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
        return sorted(key for key, _ in self.iter_objects(area, prefix))

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
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
