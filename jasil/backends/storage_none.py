"""Disabled ``StorageProvider`` backend (``storage_uri=none://``)."""

from collections.abc import Iterator, Sequence
from typing import BinaryIO, NoReturn

from jasil.providers import ObjectStat, PartRef, ServePlan, StorageBackendUnavailableError, UploadSession


def _disabled() -> NoReturn:
    """Raise the error every ``NullStorage`` operation ends in.

    Raises:
        StorageBackendUnavailableError: Always.
    """
    raise StorageBackendUnavailableError("Storage is disabled in this deployment (storage_uri=none://)")


class NullStorage:
    """``StorageProvider`` for a deployment that has no storage.

    Every operation raises, arguments unchecked, so a host that uses storage it
    disabled fails loudly instead of losing data. ``check_writable`` raises too,
    so a readiness probe reports the capability as unavailable.
    """

    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def get(self, area: str, key: str) -> bytes | None:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def stat(self, area: str, key: str) -> ObjectStat | None:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def exists(self, area: str, key: str) -> bool:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def delete(self, area: str, key: str) -> None:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def list_keys(self, area: str, prefix: str = "") -> list[str]:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        """Always raise, when called rather than when first iterated.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def save_stream(
        self,
        area: str,
        key: str,
        source: BinaryIO,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> int:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def serve(
        self,
        area: str,
        key: str,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
        expires_in: int = 3600,
    ) -> ServePlan:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def url(
        self,
        area: str,
        key: str,
        expires_in: int = 3600,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
    ) -> str:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def delete_prefix(self, area: str, prefix: str) -> int:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def check_writable(self) -> None:
        """Always raise, so a readiness probe fails.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def begin_upload(
        self,
        area: str,
        key: str,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> UploadSession:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def upload_part(
        self,
        session: UploadSession,
        part_number: int,
        source: BinaryIO,
        *,
        size: int,
    ) -> PartRef:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def abort_upload(self, session: UploadSession) -> None:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()

    def cleanup_uploads(self, *, older_than_epoch: float) -> int:
        """Always raise.

        Raises:
            StorageBackendUnavailableError: Always.
        """
        _disabled()
