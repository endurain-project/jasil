"""Internal helpers that keep storage stream behavior backend-independent."""

import io
from collections.abc import Callable
from typing import Any, BinaryIO

ErrorTranslator = Callable[[Exception], None]


def validate_stream_range(offset: int, length: int | None) -> None:
    """Validate a byte range before a backend performs I/O."""
    if offset < 0:
        raise ValueError("Storage stream offset must not be negative")
    if length is not None and length < 0:
        raise ValueError("Storage stream length must not be negative")


class ExactSizeReader:
    """Read one source exactly once under a caller-declared byte size.

    The caller retains ownership of ``source``. This wrapper never seeks or
    closes it.
    """

    def __init__(self, source: BinaryIO, size: int) -> None:
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("Storage part size must be a non-negative integer")
        self._source = source
        self._size = size
        self._remaining = size
        self._eof_verified = False

    def __len__(self) -> int:
        return self._size

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._size - self._remaining

    def read(self, size: int = -1) -> bytes:
        if self._remaining == 0:
            self.verify_complete()
            return b""
        if size == 0:
            return b""
        read_size = self._remaining if size < 0 else min(size, self._remaining)
        data = self._source.read(read_size)
        if not data:
            raise ValueError(f"Storage part source ended before declared size={self._size}")
        if len(data) > read_size:
            raise ValueError(f"Storage part source returned more than requested for declared size={self._size}")
        self._remaining -= len(data)
        if self._remaining == 0:
            self._verify_eof()
        return data

    def _verify_eof(self) -> None:
        if not self._eof_verified:
            if self._source.read(1):
                raise ValueError(f"Storage part source exceeds declared size={self._size}")
            self._eof_verified = True

    def verify_complete(self) -> None:
        """Require the declared bytes to be consumed and the source to be at EOF."""
        if self._remaining:
            raise ValueError(f"Storage part source was not consumed to declared size={self._size}")
        self._verify_eof()


class _NonSeekableRawReader(io.RawIOBase):
    def __init__(
        self,
        source: Any,
        *,
        length: int | None,
        translate_error: ErrorTranslator,
    ) -> None:
        super().__init__()
        self._source = source
        self._remaining = length
        self._translate_error = translate_error

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def readinto(self, buffer: Any) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        view = memoryview(buffer).cast("B")
        read_size = len(view)
        if self._remaining is not None:
            if self._remaining == 0:
                return 0
            read_size = min(read_size, self._remaining)
        try:
            data = self._source.read(read_size)
        except Exception as error:
            self._translate_error(error)
            raise
        if not data:
            return 0
        view[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._source.close()
        except Exception as error:
            self._translate_error(error)
            raise
        finally:
            super().close()


def non_seekable_reader(
    source: Any,
    *,
    length: int | None,
    translate_error: ErrorTranslator,
) -> BinaryIO:
    """Wrap a binary source in the read-once contract shared by all backends."""
    return io.BufferedReader(_NonSeekableRawReader(source, length=length, translate_error=translate_error))
