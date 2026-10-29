"""Write-once ``AuditSink`` over S3, normally a bucket with Object Lock.

Selected by ``audit.external_sink_uri = "s3://bucket/prefix"``. The bucket is a *separate* bucket from the
application's own, ideally in another account, with Object Lock in compliance mode and a default retention
period.

Each checkpoint is written with one conditional ``PutObject`` (``If-None-Match: *``), so an object already
under the name is never replaced by this sink, and a bucket policy that denies every ``PutObject`` without
that condition (the ``s3:if-none-match`` condition key) stops any other holder of the credentials from
replacing one either. Object Lock keeps each stored version from being deleted or overwritten in place.
"""

from typing import TYPE_CHECKING

from jasil.providers import AuditSinkConflictError

if TYPE_CHECKING:
    from jasil.backends.storage_s3 import S3Storage

#: Storage area (key prefix) the checkpoints are written under.
AUDIT_SINK_AREA = "audit-checkpoints"


class StorageAuditSink:
    """``AuditSink`` that stores each object in an S3 storage area and never replaces one."""

    def __init__(self, storage: "S3Storage", area: str = AUDIT_SINK_AREA) -> None:
        self._storage = storage
        self._area = area

    def write_once(self, name: str, content: bytes) -> None:
        """Store ``content`` as ``name``, unless that object already exists with the same bytes.

        The object is read only when the conditional write finds one already there, to tell a retry of the
        same checkpoint from a conflicting one.

        Args:
            name: The object key within the area.
            content: The bytes to store.

        Raises:
            AuditSinkConflictError: When the object exists with different bytes.
            StorageBackendUnavailableError: When the storage backend fails.
        """
        if self._storage.create(self._area, name, content, "application/json"):
            return
        if self._storage.get(self._area, name) != content:
            raise AuditSinkConflictError(f"{name} already exists with different content")

    def read(self, name: str) -> bytes | None:
        """Return the object stored as ``name``, or ``None`` when there is none.

        Raises:
            StorageBackendUnavailableError: When the storage backend fails.
        """
        return self._storage.get(self._area, name)
