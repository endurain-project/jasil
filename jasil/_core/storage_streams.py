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
