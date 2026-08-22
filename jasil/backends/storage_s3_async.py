"""Async S3-compatible ``AsyncStorageProvider`` backend.

This is the awaitable twin of :mod:`jasil.backends.storage_s3`; the bucket,
prefix, key-validation, and presigned-URL behaviour are identical — read that
module for the full contract.

Only imported by the async composition root when ``storage_uri`` uses the
``s3://`` scheme, so local deployments never load ``boto3``. Install it with
the ``s3`` extra (``pip install jasil[s3]``).

**Design decision — boto3 thread offload, not a native async client.**
``boto3`` has no async API. The canonical async alternative is ``aioboto3``,
which wraps ``botocore`` in ``aiohttp`` sessions. This backend deliberately
does *not* use ``aioboto3`` for two reasons:

1.  **Parity with the sync backend at zero extra dependency cost.** The sync
    backend already owns client construction and retry configuration via
    :meth:`S3Storage.from_uri`; reusing its helpers means behaviour, retry
    policy, and endpoint compatibility stay in lockstep without a second code
    path to maintain.

2.  **Thread offload is sufficient for this workload.** S3 calls are
    network-latency-dominated (milliseconds to hundreds of milliseconds), not
    CPU-bound, and the worker-thread pool is bounded anyway.  The event loop
    stays unblocked either way; the difference is only in *how* the blocking
    wait is hidden.

Each blocking boto3 call is dispatched to a worker thread via
:func:`anyio.to_thread.run_sync`. An ``aioboto3``-based backend that *is*
natively async could replace this one in the future without changing any
caller, because the :class:`~jasil.providers_async.AsyncStorageProvider`
protocol is stable.
"""

import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

import anyio
import anyio.to_thread
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from jasil._core.storage_keys import check_segment

logger = logging.getLogger(__name__)

# HeadObject error codes that mean "the object is not there" — identical to the
# sync module's list and carried here so the two backends always agree on what
# counts as a clean miss.
_MISSING_OBJECT_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class AsyncS3Storage:
    """``AsyncStorageProvider`` backed by S3-compatible object storage.

    Blobs are stored at ``{prefix}/{area}/{key}`` in a bucket; ``url`` returns a
    presigned GET URL. The DB stores only the key, so migrating local → S3 needs
    no data migration. Build via :meth:`from_uri`.

    All boto3 calls are dispatched to a worker thread via
    ``anyio.to_thread.run_sync``; the event loop is never blocked.
    """

    def __init__(self, client: Any, bucket: str, prefix: str = "") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    @classmethod
    def from_uri(cls, uri: str) -> "AsyncS3Storage":
        """Build a backend from ``s3://bucket/prefix?region=…&endpoint_url=…``.

        Client construction is synchronous and cheap (no network call), so it is
        safe to run in ``__init__`` or here at import time — no factory coroutine
        is needed.

        Args:
            uri: An ``s3://`` storage URI. ``region`` and ``endpoint_url`` query
                parameters are optional (``endpoint_url`` enables S3-compatible
                providers such as MinIO or R2).

        Returns:
            A configured :class:`AsyncS3Storage`.

        Raises:
            ValueError: When the URI has no bucket.
        """
        parsed = urlparse(uri)
        bucket = parsed.netloc
        if not bucket:
            raise ValueError(f"An s3 storage_uri is missing a bucket: {uri!r}")
        params = parse_qs(parsed.query)
        # Reuse the same client configuration as the sync backend: three retries
        # in standard mode, identical region and endpoint resolution. A caller
        # that switches from the sync to the async backend sees the same network
        # behaviour.
        client = boto3.client(
            "s3",
            region_name=params.get("region", [None])[0],
            endpoint_url=params.get("endpoint_url", [None])[0],
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
        return cls(client, bucket, parsed.path)

    def _object_key(self, area: str, key: str) -> str:
        """Compose the S3 object key from area and blob key, rejecting bad segments.

        ``..`` is a literal in an S3 key rather than a traversal, so an unchecked
        segment is stored under a nonsense key instead of being refused. The local
        backend rejects it, and one contract has to hold on both.

        Args:
            area: Storage area name.
            key: Blob key within the area.

        Returns:
            The full S3 object key string including the configured prefix.
        """
        check_segment(area, "area")
        check_segment(key, "key")
        return "/".join(part for part in (self._prefix, area, key) if part)

    async def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        """Upload ``data`` as an S3 object at ``{prefix}/{area}/{key}``.

        Args:
            area: Storage area.
            key: Blob key within the area.
            data: Raw bytes to upload.
            content_type: Optional MIME type set as the object's ``ContentType``.

        Returns:
            The ``key`` argument unchanged.
        """
        object_key = self._object_key(area, key)
        extra = {"ContentType": content_type} if content_type else {}

        def _put() -> str:
            self._client.put_object(Bucket=self._bucket, Key=object_key, Body=data, **extra)
            return key

        return await anyio.to_thread.run_sync(_put)

    async def get(self, area: str, key: str) -> bytes | None:
        """Download and return the S3 object at ``{prefix}/{area}/{key}``.

        Args:
            area: Storage area.
            key: Blob key within the area.

        Returns:
            Raw bytes if the object exists, ``None`` if it does not.

        Raises:
            botocore.exceptions.ClientError: For any S3 error other than a clean
                missing-object response.
        """
        object_key = self._object_key(area, key)

        def _get() -> bytes | None:
            try:
                response = self._client.get_object(Bucket=self._bucket, Key=object_key)
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") in _MISSING_OBJECT_CODES:
                    return None
                raise
            body: bytes = response["Body"].read()
            return body

        return await anyio.to_thread.run_sync(_get)

    async def exists(self, area: str, key: str) -> bool:
        """Return whether an S3 object exists at ``{prefix}/{area}/{key}``.

        Args:
            area: Storage area.
            key: Blob key within the area.

        Returns:
            ``True`` iff a HeadObject call succeeds.

        Raises:
            botocore.exceptions.ClientError: For any S3 error other than a clean
                missing-object response.
        """
        object_key = self._object_key(area, key)

        def _head() -> bool:
            try:
                self._client.head_object(Bucket=self._bucket, Key=object_key)
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") in _MISSING_OBJECT_CODES:
                    return False
                raise
            return True

        return await anyio.to_thread.run_sync(_head)

    async def delete(self, area: str, key: str) -> None:
        """Delete the S3 object at ``{prefix}/{area}/{key}``.

        S3's ``DeleteObject`` is idempotent: deleting a non-existent key succeeds
        silently, matching the protocol's expectation.

        Args:
            area: Storage area.
            key: Blob key within the area.
        """
        object_key = self._object_key(area, key)

        def _delete() -> None:
            self._client.delete_object(Bucket=self._bucket, Key=object_key)

        await anyio.to_thread.run_sync(_delete)

    async def list_keys(self, area: str, prefix: str = "") -> list[str]:
        """Return sorted keys in ``area`` whose name starts with ``prefix``.

        Uses the S3 ``ListObjectsV2`` paginator to avoid materialising the full
        listing in a single response. Returned keys are relative to the area root
        (i.e. the configured prefix and area directory are stripped).

        Args:
            area: Storage area.
            prefix: Optional key prefix filter (empty means return all keys).

        Returns:
            Alphabetically sorted list of key strings.
        """
        check_segment(area, "area")
        if prefix:
            check_segment(prefix, "prefix")
        object_prefix = "/".join(part for part in (self._prefix, area, prefix) if part)
        # Strip back to the area root so the returned values are plain keys.
        strip_from = len("/".join(part for part in (self._prefix, area) if part)) + 1

        def _list() -> list[str]:
            keys = []
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=object_prefix):
                for entry in page.get("Contents", []):
                    keys.append(entry["Key"][strip_from:])
            return sorted(keys)

        return await anyio.to_thread.run_sync(_list)

    async def url(self, area: str, key: str, expires_in: int = 3600) -> str:
        """Generate a presigned S3 GET URL for the object at ``{prefix}/{area}/{key}``.

        Unlike the local backend, this URL actually expires — S3 signs it with
        credentials, so ``expires_in`` is honoured.

        Args:
            area: Storage area.
            key: Blob key within the area.
            expires_in: URL lifetime in seconds (default: 3 600).

        Returns:
            A presigned HTTPS URL string.
        """
        object_key = self._object_key(area, key)

        def _presign() -> str:
            return self._client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": object_key},
                ExpiresIn=expires_in,
            )

        return await anyio.to_thread.run_sync(_presign)
