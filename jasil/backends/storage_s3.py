"""S3-compatible ``StorageProvider`` backend.

Only imported by the composition root when ``storage_uri`` uses the ``s3://``
scheme, so local deployments never load ``boto3``. Install it with the ``s3``
extra (``pip install jasil[s3]``).
"""

import functools
import io
import logging
from collections.abc import Callable, Iterator
from typing import Any, BinaryIO, Concatenate
from urllib.parse import parse_qs, quote, urlparse
from uuid import uuid4

import boto3
from boto3.exceptions import Boto3Error
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from jasil._core.storage_keys import check_segment
from jasil._core.storage_streams import non_seekable_reader, validate_stream_range
from jasil.providers import ObjectStat, ServeRedirect, StorageBackendUnavailableError, StorageSizeLimitError

logger = logging.getLogger(__name__)

# HeadObject error codes that mean "the object is not there" (as opposed to auth,
# region, throttling, or 5xx failures, which must surface rather than masquerade
# as a missing blob). "404" is what S3 returns for HeadObject; the aliases cover
# S3-compatible providers (MinIO, R2, Ceph).
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_INVALID_RANGE_CODES = frozenset({"416", "InvalidRange", "RequestedRangeNotSatisfiable"})
_MULTIPART_PART_BYTES = 8 * 1024 * 1024


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
        check_segment(area, "area")
        check_segment(key, "key")
        return "/".join(part for part in (self._prefix, area, key) if part)

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
        check_segment(area, "area")
        if prefix:
            check_segment(prefix, "prefix")
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
