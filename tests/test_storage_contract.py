"""One behavioral contract, run against both ``StorageProvider`` backends.

Local disk and S3 are swapped by configuration alone, so a value one backend
reads differently, leaves behind after failure, or reports with different
metadata is a bug that only appears after a deployment change. The S3 backend is
driven through an in-memory client here; ``test_storage_s3`` separately uses
botocore's Stubber to validate the exact AWS request shapes.
"""

import io
from datetime import UTC, datetime
from itertools import count

import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from jasil.backends.storage_local import LocalStorage
from jasil.backends.storage_s3 import S3Storage
from jasil.providers import StorageProvider, StorageSizeLimitError

# Values that must never address a blob, whichever backend is configured.
UNSAFE_SEGMENTS = ["../escape", "/etc/passwd", "a/../../b", ".."]
AREA = "packages"
KEY = "release.bin"
MODIFIED = datetime(2026, 8, 25, tzinfo=UTC)
S3_PART_BYTES = 8 * 1024 * 1024


def _client_error(operation: str, code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        operation,
    )


class _MemoryPaginator:
    def __init__(self, client):
        self._client = client

    def paginate(self, **request):
        prefix = request["Prefix"]
        contents = [
            {"Key": key, "LastModified": metadata[1]}
            for key, metadata in sorted(self._client.objects.items())
            if key.startswith(prefix)
        ]
        yield {"Contents": contents} if contents else {}


class _MemoryS3Client:
    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self._upload_ids = count(1)

    def put_object(self, **request):
        self.objects[request["Key"]] = (bytes(request["Body"]), MODIFIED, request.get("ContentType"))
        return {}

    def get_object(self, **request):
        key = request["Key"]
        if key not in self.objects:
            raise _client_error("GetObject", "NoSuchKey", 404)
        data = self.objects[key][0]
        byte_range = request.get("Range")
        if byte_range is not None:
            start_text, end_text = byte_range.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            if start >= len(data):
                raise _client_error("GetObject", "InvalidRange", 416)
            end = int(end_text) if end_text else len(data) - 1
            data = data[start : end + 1]
        return {"Body": StreamingBody(io.BytesIO(data), len(data))}

    def head_object(self, **request):
        key = request["Key"]
        if key not in self.objects:
            raise _client_error("HeadObject", "404", 404)
        return {"LastModified": self.objects[key][1]}

    def delete_object(self, **request):
        self.objects.pop(request["Key"], None)
        return {}

    def create_multipart_upload(self, **request):
        upload_id = str(next(self._upload_ids))
        self.uploads[upload_id] = {
            "key": request["Key"],
            "parts": {},
            "content_type": request.get("ContentType"),
        }
        return {"UploadId": upload_id}

    def upload_part(self, **request):
        upload_id = request["UploadId"]
        part_number = request["PartNumber"]
        self.uploads[upload_id]["parts"][part_number] = bytes(request["Body"])
        return {"ETag": f'"part-{part_number}"'}

    def complete_multipart_upload(self, **request):
        upload = self.uploads.pop(request["UploadId"])
        data = b"".join(upload["parts"][part["PartNumber"]] for part in request["MultipartUpload"]["Parts"])
        self.objects[request["Key"]] = (data, MODIFIED, upload["content_type"])
        return {}

    def abort_multipart_upload(self, **request):
        self.uploads.pop(request["UploadId"], None)
        return {}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return _MemoryPaginator(self)

    def generate_presigned_url(self, operation, **request):
        return f"https://s3.test/{request['Params']['Key']}?expires={request['ExpiresIn']}"


class _ReadOnceSource(io.BytesIO):
    def seek(self, *args, **kwargs):
        raise AssertionError("save_stream must not seek its source")

    def seekable(self):
        return False


@pytest.fixture(params=["local", "s3"])
def storage(request, tmp_path):
    if request.param == "local":
        return LocalStorage(str(tmp_path))
    return S3Storage(_MemoryS3Client(), "blobs", "jasil")


class TestStorageProtocolConformance:
    def test_the_backend_satisfies_the_runtime_protocol(self, storage):
        assert isinstance(storage, StorageProvider)


class TestStreamingConformance:
    def test_a_non_seekable_source_round_trips(self, storage):
        source = _ReadOnceSource(b"package bytes")

        stored = storage.save_stream(AREA, KEY, source)

        assert stored == len(b"package bytes")
        assert storage.get(AREA, KEY) == b"package bytes"

    def test_an_empty_stream_creates_a_zero_byte_object(self, storage):
        assert storage.save_stream(AREA, KEY, _ReadOnceSource(b"")) == 0

        assert storage.exists(AREA, KEY) is True
        with storage.open_stream(AREA, KEY) as stream:
            assert stream.read() == b""

    def test_an_oversized_stream_leaves_no_partial_object(self, storage):
        source = _ReadOnceSource(b"x" * (S3_PART_BYTES + 1))

        with pytest.raises(StorageSizeLimitError):
            storage.save_stream(AREA, KEY, source, max_bytes=S3_PART_BYTES)

        assert storage.exists(AREA, KEY) is False

    def test_a_negative_size_limit_is_refused_before_writing(self, storage):
        with pytest.raises(ValueError, match="must not be negative"):
            storage.save_stream(AREA, KEY, _ReadOnceSource(b"x"), max_bytes=-1)

        assert storage.exists(AREA, KEY) is False

    def test_an_open_stream_is_read_once_and_non_seekable(self, storage):
        storage.save(AREA, KEY, b"package bytes")

        with storage.open_stream(AREA, KEY) as stream:
            assert stream.seekable() is False
            with pytest.raises(io.UnsupportedOperation):
                stream.seek(0)
            assert stream.read() == b"package bytes"

    @pytest.mark.parametrize(
        ("offset", "length", "expected"),
        [
            (0, None, b"0123456789"),
            (3, None, b"3456789"),
            (2, 4, b"2345"),
            (2, 0, b""),
            (20, None, b""),
        ],
    )
    def test_a_range_is_selected_before_the_stream_is_opened(self, storage, offset, length, expected):
        storage.save(AREA, KEY, b"0123456789")

        with storage.open_stream(AREA, KEY, offset=offset, length=length) as stream:
            assert stream.read() == expected

    def test_opening_a_missing_object_raises(self, storage):
        with pytest.raises(FileNotFoundError):
            storage.open_stream(AREA, "missing.bin")

    def test_a_zero_length_open_still_raises_for_a_missing_object(self, storage):
        with pytest.raises(FileNotFoundError):
            storage.open_stream(AREA, "missing.bin", length=0)

    @pytest.mark.parametrize(("offset", "length"), [(-1, None), (0, -1)])
    def test_a_negative_range_is_refused(self, storage, offset, length):
        storage.save(AREA, KEY, b"x")

        with pytest.raises(ValueError, match="must not be negative"):
            storage.open_stream(AREA, KEY, offset=offset, length=length)


class TestListingConformance:
    def test_objects_are_yielded_lazily_with_modified_epochs(self, storage):
        storage.save(AREA, "b.bin", b"b")
        storage.save(AREA, "a.bin", b"a")

        objects = storage.iter_objects(AREA)

        assert iter(objects) is objects
        listed = list(objects)
        assert {key for key, _ in listed} == {"a.bin", "b.bin"}
        assert all(isinstance(modified_epoch, float) and modified_epoch > 0 for _, modified_epoch in listed)

    def test_a_prefix_filters_the_lazy_listing(self, storage):
        storage.save(AREA, "release-1.bin", b"1")
        storage.save(AREA, "draft.bin", b"2")

        assert [key for key, _ in storage.iter_objects(AREA, "release-")] == ["release-1.bin"]

    def test_a_similarly_named_area_is_not_included(self, storage):
        storage.save(AREA, KEY, b"package")
        storage.save(f"{AREA}-old", KEY, b"old")

        assert storage.list_keys(AREA) == [KEY]


class TestWritableConformance:
    def test_a_write_probe_completes_without_leaving_an_object(self, storage):
        storage.check_writable()

        assert storage.list_keys(AREA) == []


class TestSegmentValidationConformance:
    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize("operation", ["save", "save_stream", "get", "open_stream", "exists", "delete", "url"])
    def test_a_traversing_segment_is_refused(self, storage, unsafe, field, operation):
        arguments = {"area": "avatars", "key": "42.webp", field: unsafe}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else (_ReadOnceSource(b"x"),) if operation == "save_stream" else ()

        with pytest.raises(ValueError, match="escapes base directory"):
            call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize("operation", ["save", "save_stream", "get", "open_stream", "exists", "delete", "url"])
    def test_an_empty_segment_is_refused(self, storage, field, operation):
        """An empty segment collapses the address onto the storage root itself."""
        arguments = {"area": "avatars", "key": "42.webp", field: ""}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else (_ReadOnceSource(b"x"),) if operation == "save_stream" else ()

        with pytest.raises(ValueError, match="must not be empty"):
            call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("operation", ["list_keys", "iter_objects"])
    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    def test_listing_refuses_a_traversing_area(self, storage, unsafe, operation):
        with pytest.raises(ValueError, match="escapes base directory"):
            getattr(storage, operation)(unsafe)

    @pytest.mark.parametrize("operation", ["list_keys", "iter_objects"])
    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    def test_listing_refuses_a_traversing_prefix(self, storage, unsafe, operation):
        with pytest.raises(ValueError, match="escapes base directory"):
            getattr(storage, operation)("avatars", unsafe)

    @pytest.mark.parametrize("operation", ["list_keys", "iter_objects"])
    def test_listing_refuses_an_empty_area(self, storage, operation):
        with pytest.raises(ValueError, match="must not be empty"):
            getattr(storage, operation)("")
