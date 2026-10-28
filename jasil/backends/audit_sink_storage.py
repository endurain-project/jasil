"""Write-once ``AuditSink`` over a storage backend, normally S3 with Object Lock.

Selected by ``audit.external_sink_uri = "s3://bucket/prefix"``. The bucket is a *separate* bucket from the
application's own, ideally in another account, with Object Lock in compliance mode and credentials that
may ``PutObject`` but not delete or overwrite: that is what makes the checkpoints evidence.

The write-once check is a read before the write, which JASIL's maintenance lock serializes across
replicas; S3 Object Lock is what enforces it against everyone else.
"""

from jasil.providers import AuditSinkConflictError, StorageObjects

#: Storage area (key prefix) the checkpoints are written under.
AUDIT_SINK_AREA = "audit-checkpoints"


class StorageAuditSink:
    """``AuditSink`` that stores each object in a ``StorageObjects`` area and never replaces one."""

    def __init__(self, storage: StorageObjects, area: str = AUDIT_SINK_AREA) -> None:
        self._storage = storage
        self._area = area

    def write_once(self, name: str, content: bytes) -> None:
        """Store ``content`` as ``name``, unless that object already exists with the same bytes.

        Args:
            name: The object key within the area.
            content: The bytes to store.

        Raises:
            AuditSinkConflictError: When the object exists with different bytes.
            StorageBackendUnavailableError: When the storage backend fails.
        """
        existing = self._storage.get(self._area, name)
        if existing is None:
            self._storage.save(self._area, name, content, "application/json")
        elif existing != content:
            raise AuditSinkConflictError(f"{name} already exists with different content")
