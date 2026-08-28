"""One behavioral contract, run against both ``StorageProvider`` backends.

Local disk and S3 are swapped by configuration alone, so a value one backend
reads differently, leaves behind after failure, or reports with different
metadata is a bug that only appears after a deployment change. The S3 backend is
driven through an in-memory client here; ``test_storage_s3`` separately uses
botocore's Stubber to validate the exact AWS request shapes.
"""

import hashlib
import io
from dataclasses import replace
from datetime import UTC, datetime
from itertools import count
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from jasil.backends.storage_local import LocalStorage
from jasil.backends.storage_s3 import S3Storage
from jasil.providers import (
    ServeFile,
    ServeRedirect,
    ServeStream,
    StorageProvider,
    StorageSizeLimitError,
    StorageUploadSessionError,
)

# Values that must never address a blob, whichever backend is configured.
UNSAFE_SEGMENTS = ["../escape", "/etc/passwd", "a/../../b", ".."]
NON_CANONICAL_SEGMENTS = [".", "./pkg", "pkg/./1", "pkg//1", "pkg/1/", "pkg\\1"]
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
        data, modified, content_type = self.objects[key]
        return {
            "ContentLength": len(data),
            "LastModified": modified,
            "ContentType": content_type,
            "ETag": f'"memory-{len(data)}"',
        }

    def delete_object(self, **request):
        self.objects.pop(request["Key"], None)
        return {}

    def delete_objects(self, **request):
        for entry in request["Delete"]["Objects"]:
            self.objects.pop(entry["Key"], None)
        return {}

    def list_objects_v2(self, **request):
        prefix = request["Prefix"]
        limit = request.get("MaxKeys", 1000)
        contents = [
            {"Key": key, "LastModified": metadata[1]}
            for key, metadata in sorted(self.objects.items())
            if key.startswith(prefix)
        ][:limit]
        return {"Contents": contents} if contents else {}

    def copy(self, copy_source, bucket, key):
        source_key = copy_source["Key"]
        if source_key not in self.objects:
            raise _client_error("CopyObject", "NoSuchKey", 404)
        data, _, content_type = self.objects[source_key]
        self.objects[key] = (data, MODIFIED, content_type)

    def create_multipart_upload(self, **request):
        upload_id = str(next(self._upload_ids))
        self.uploads[upload_id] = {
            "key": request["Key"],
            "parts": {},
            "content_type": request.get("ContentType"),
            "initiated": MODIFIED,
        }
        return {"UploadId": upload_id}

    def upload_part(self, **request):
        upload_id = request["UploadId"]
        if upload_id not in self.uploads or self.uploads[upload_id]["key"] != request["Key"]:
            raise _client_error("UploadPart", "NoSuchUpload", 404)
        part_number = request["PartNumber"]
        data = bytes(request["Body"])
        self.uploads[upload_id]["parts"][part_number] = data
        return {"ETag": f'"part-{hashlib.sha256(data).hexdigest()}"'}

    def list_parts(self, **request):
        upload_id = request["UploadId"]
        if upload_id not in self.uploads or self.uploads[upload_id]["key"] != request["Key"]:
            raise _client_error("ListParts", "NoSuchUpload", 404)
        marker = request.get("PartNumberMarker", 0)
        parts = [
            {
                "PartNumber": part_number,
                "Size": len(data),
                "ETag": f'"part-{hashlib.sha256(data).hexdigest()}"',
            }
            for part_number, data in sorted(self.uploads[upload_id]["parts"].items())
            if part_number > marker
        ]
        return {"Parts": parts, "IsTruncated": False}

    def complete_multipart_upload(self, **request):
        upload_id = request["UploadId"]
        if upload_id not in self.uploads or self.uploads[upload_id]["key"] != request["Key"]:
            raise _client_error("CompleteMultipartUpload", "NoSuchUpload", 404)
        upload = self.uploads.pop(upload_id)
        data = b"".join(upload["parts"][part["PartNumber"]] for part in request["MultipartUpload"]["Parts"])
        self.objects[request["Key"]] = (data, MODIFIED, upload["content_type"])
        return {}

    def abort_multipart_upload(self, **request):
        upload_id = request["UploadId"]
        if upload_id not in self.uploads or self.uploads[upload_id]["key"] != request["Key"]:
            raise _client_error("AbortMultipartUpload", "NoSuchUpload", 404)
        self.uploads.pop(upload_id)
        return {}

    def list_multipart_uploads(self, **request):
        prefix = request.get("Prefix", "")
        uploads = [
            {
                "Key": upload["key"],
                "UploadId": upload_id,
                "Initiated": upload["initiated"],
            }
            for upload_id, upload in self.uploads.items()
            if upload["key"].startswith(prefix)
        ]
        return {"Uploads": uploads, "IsTruncated": False}

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


class TestResumableUploadConformance:
    def test_parts_upload_out_of_order_and_commit_atomically(self, storage):
        storage.save(AREA, KEY, b"previous")
        session = storage.begin_upload(
            AREA,
            KEY,
            max_bytes=5 * 1024 * 1024 + 4,
            content_type="application/octet-stream",
        )
        tail = storage.upload_part(session, 2, b"tail")
        first = storage.upload_part(session, 1, b"a" * session.min_part_size)

        assert session.min_part_size == 5 * 1024 * 1024
        assert session.max_part_size == 5 * 1024 * 1024 * 1024
        assert session.max_parts == 10_000
        assert storage.get(AREA, KEY) == b"previous"
        assert storage.list_keys(AREA) == [KEY]

        stored = storage.complete_upload(session, [first, tail])

        assert stored == first.size + tail.size
        assert storage.get(AREA, KEY) == b"a" * session.min_part_size + b"tail"

    def test_an_active_upload_is_not_an_object(self, storage):
        session = storage.begin_upload(AREA, KEY)
        storage.upload_part(session, 1, b"partial")

        assert storage.exists(AREA, KEY) is False
        assert storage.list_keys(AREA) == []

    def test_a_zero_byte_final_part_creates_a_zero_byte_object(self, storage):
        session = storage.begin_upload(AREA, KEY, max_bytes=0)
        part = storage.upload_part(session, 1, b"")

        assert storage.complete_upload(session, [part]) == 0
        assert storage.get(AREA, KEY) == b""

    def test_reuploading_a_part_invalidates_the_old_reference(self, storage):
        session = storage.begin_upload(AREA, KEY)
        stale = storage.upload_part(session, 1, b"old")
        current = storage.upload_part(session, 1, b"new")

        with pytest.raises(StorageUploadSessionError, match="does not match"):
            storage.complete_upload(session, [stale])

        assert storage.complete_upload(session, [current]) == 3

    def test_the_total_limit_is_enforced_before_a_part_is_replaced(self, storage):
        session = storage.begin_upload(AREA, KEY, max_bytes=4)
        storage.upload_part(session, 1, b"1234")

        with pytest.raises(StorageSizeLimitError, match="max_bytes=4"):
            storage.upload_part(session, 2, b"5")

        replacement = storage.upload_part(session, 1, b"12")
        assert storage.complete_upload(session, [replacement]) == 2

    def test_completion_requires_every_part_in_strict_order(self, storage):
        session = storage.begin_upload(AREA, KEY)
        first = storage.upload_part(session, 1, b"a" * session.min_part_size)
        second = storage.upload_part(session, 2, b"tail")

        with pytest.raises(ValueError, match="ordered"):
            storage.complete_upload(session, [second, first])
        with pytest.raises(StorageUploadSessionError, match="every uploaded part"):
            storage.complete_upload(session, [first])

        assert storage.complete_upload(session, [first, second]) == first.size + second.size

    def test_a_foreign_target_makes_the_session_invalid(self, storage):
        session = storage.begin_upload(AREA, KEY)
        foreign = replace(session, key="other.bin")

        with pytest.raises(StorageUploadSessionError):
            storage.upload_part(foreign, 1, b"data")

    def test_abort_is_idempotent_and_terminal(self, storage):
        session = storage.begin_upload(AREA, KEY)
        part = storage.upload_part(session, 1, b"partial")

        storage.abort_upload(session)
        storage.abort_upload(session)

        with pytest.raises(StorageUploadSessionError, match="not active"):
            storage.complete_upload(session, [part])
        assert storage.exists(AREA, KEY) is False

    def test_cleanup_aborts_only_sessions_before_the_cutoff(self, storage):
        session = storage.begin_upload(AREA, KEY)

        assert storage.cleanup_uploads(older_than_epoch=0.0) == 0
        assert storage.cleanup_uploads(older_than_epoch=1_000_000_000_000.0) == 1

        with pytest.raises(StorageUploadSessionError, match="not active"):
            storage.upload_part(session, 1, b"data")

    def test_a_negative_session_limit_is_refused(self, storage):
        with pytest.raises(ValueError, match="must not be negative"):
            storage.begin_upload(AREA, KEY, max_bytes=-1)


class TestServingConformance:
    def test_a_serve_plan_reads_the_stored_object(self, storage):
        storage.save(AREA, KEY, b"package bytes", content_type="application/octet-stream")

        plan = storage.serve(
            AREA,
            KEY,
            download_as="release.bin",
            content_type="application/octet-stream",
            expires_in=60,
        )

        if isinstance(plan, ServeFile):
            assert plan.path.is_absolute()
            assert plan.path.is_file()
            assert plan.path.read_bytes() == b"package bytes"
        elif isinstance(plan, ServeRedirect):
            object_key = urlparse(plan.url).path.lstrip("/")
            assert storage._client.objects[object_key][0] == b"package bytes"
        elif isinstance(plan, ServeStream):
            with plan.stream:
                assert plan.stream.read() == b"package bytes"
        else:
            raise AssertionError(f"Unknown serve plan: {plan!r}")

    def test_serving_a_missing_object_raises(self, storage):
        with pytest.raises(FileNotFoundError):
            storage.serve(AREA, "missing.bin")


class TestObjectStatConformance:
    def test_stat_agrees_with_stream_size_and_listing_epoch(self, storage):
        stored = storage.save_stream(
            AREA,
            KEY,
            _ReadOnceSource(b"package bytes"),
            content_type="application/octet-stream",
        )

        metadata = storage.stat(AREA, KEY)
        listed = dict(storage.iter_objects(AREA))

        assert metadata is not None
        assert metadata.size == stored
        assert metadata.modified_epoch == listed[KEY]

    def test_a_missing_object_has_no_stat(self, storage):
        assert storage.stat(AREA, "missing.bin") is None


class TestCopyConformance:
    def test_copy_preserves_the_source_and_overwrites_the_destination(self, storage):
        storage.save(AREA, "source.bin", b"source")
        storage.save("copies", "destination.bin", b"old")

        storage.copy(AREA, "source.bin", "copies", "destination.bin")

        assert storage.get(AREA, "source.bin") == b"source"
        assert storage.get("copies", "destination.bin") == b"source"

    def test_copying_a_missing_source_raises_without_changing_the_destination(self, storage):
        storage.save(AREA, "destination.bin", b"original")

        with pytest.raises(FileNotFoundError):
            storage.copy(AREA, "missing.bin", AREA, "destination.bin")

        assert storage.get(AREA, "destination.bin") == b"original"

    def test_copying_an_object_onto_itself_is_a_no_op(self, storage):
        storage.save(AREA, KEY, b"package")
        metadata_before = storage.stat(AREA, KEY)

        storage.copy(AREA, KEY, AREA, KEY)

        assert storage.get(AREA, KEY) == b"package"
        assert storage.stat(AREA, KEY) == metadata_before

    def test_copying_a_missing_object_onto_itself_raises(self, storage):
        with pytest.raises(FileNotFoundError):
            storage.copy(AREA, "missing.bin", AREA, "missing.bin")


class TestDeletePrefixConformance:
    def test_only_the_exact_key_root_and_its_descendants_are_deleted(self, storage):
        storage.save(AREA, "pkg/1/a.bin", b"a")
        storage.save(AREA, "pkg/1/nested/b.bin", b"b")
        storage.save(AREA, "pkg/10/c.bin", b"c")
        storage.save(AREA, "pkg/1-other.bin", b"other")

        deleted = storage.delete_prefix(AREA, "pkg/1")

        assert deleted == 2
        assert storage.list_keys(AREA) == ["pkg/1-other.bin", "pkg/10/c.bin"]

    def test_an_exact_object_is_deleted(self, storage):
        storage.save(AREA, KEY, b"package")

        assert storage.delete_prefix(AREA, KEY) == 1
        assert storage.exists(AREA, KEY) is False

    def test_an_unknown_prefix_deletes_nothing(self, storage):
        assert storage.delete_prefix(AREA, "unknown") == 0

    def test_an_empty_prefix_is_refused(self, storage):
        with pytest.raises(ValueError, match="must not be empty"):
            storage.delete_prefix(AREA, "")

    @pytest.mark.parametrize("prefix", NON_CANONICAL_SEGMENTS)
    def test_a_non_canonical_prefix_is_refused_without_deleting_objects(self, storage, prefix):
        storage.save(AREA, "keep/a.bin", b"a")
        storage.save(AREA, "keep/b.bin", b"b")

        with pytest.raises(ValueError, match="canonical slash-delimited path"):
            storage.delete_prefix(AREA, prefix)

        assert storage.list_keys(AREA) == ["keep/a.bin", "keep/b.bin"]


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

    def test_a_trailing_slash_filters_one_key_subtree(self, storage):
        storage.save(AREA, "release/one.bin", b"1")
        storage.save(AREA, "release-old/two.bin", b"2")

        assert storage.list_keys(AREA, "release/") == ["release/one.bin"]

    def test_a_similarly_named_area_is_not_included(self, storage):
        storage.save(AREA, KEY, b"package")
        storage.save(f"{AREA}-old", KEY, b"old")

        assert storage.list_keys(AREA) == [KEY]


class TestWritableConformance:
    def test_a_write_probe_completes_without_leaving_an_object(self, storage):
        storage.check_writable()

        assert storage.list_keys(AREA) == []


class TestSegmentValidationConformance:
    @pytest.mark.parametrize(
        "area",
        [".jasil-upload-sessions", ".jasil-upload-sessions/session"],
    )
    def test_the_private_upload_staging_area_is_reserved(self, storage, area):
        with pytest.raises(ValueError, match="reserved"):
            storage.begin_upload(area, KEY)
        with pytest.raises(ValueError, match="reserved"):
            storage.save(area, KEY, b"data")
        with pytest.raises(ValueError, match="reserved"):
            storage.list_keys(area)

    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize(
        "operation",
        [
            "save",
            "save_stream",
            "begin_upload",
            "get",
            "open_stream",
            "stat",
            "serve",
            "exists",
            "delete",
            "delete_prefix",
            "url",
        ],
    )
    def test_every_object_entry_point_refuses_a_dot_alias(self, storage, field, operation):
        arguments = {"area": AREA, "key": KEY, field: "."}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else (_ReadOnceSource(b"x"),) if operation == "save_stream" else ()

        with pytest.raises(ValueError, match="canonical slash-delimited path"):
            call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("field", ["src_area", "src_key", "dst_area", "dst_key"])
    def test_copy_refuses_a_dot_alias_in_every_address_component(self, storage, field):
        arguments = {"src_area": AREA, "src_key": KEY, "dst_area": "copies", "dst_key": KEY, field: "."}

        with pytest.raises(ValueError, match="canonical slash-delimited path"):
            storage.copy(**arguments)

    @pytest.mark.parametrize("operation", ["list_keys", "iter_objects"])
    @pytest.mark.parametrize("field", ["area", "prefix"])
    def test_listing_refuses_a_dot_alias(self, storage, operation, field):
        arguments = {"area": AREA, "prefix": "", field: "."}

        with pytest.raises(ValueError, match="canonical slash-delimited path"):
            list(getattr(storage, operation)(arguments["area"], arguments["prefix"]))

    def test_a_dotted_filename_remains_valid(self, storage):
        storage.save(AREA, ".metadata.json", b"data")

        assert storage.get(AREA, ".metadata.json") == b"data"

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

    @pytest.mark.parametrize("operation", ["stat", "serve", "delete_prefix"])
    @pytest.mark.parametrize("unsafe", [*UNSAFE_SEGMENTS, ""])
    @pytest.mark.parametrize("field", ["area", "key"])
    def test_new_entry_points_refuse_traversing_segments(self, storage, operation, unsafe, field):
        arguments = {"area": AREA, "key": KEY, field: unsafe}

        with pytest.raises(ValueError, match=r"escapes base directory|must not be empty"):
            getattr(storage, operation)(arguments["area"], arguments["key"])

    @pytest.mark.parametrize("unsafe", [*UNSAFE_SEGMENTS, ""])
    @pytest.mark.parametrize("field", ["src_area", "src_key", "dst_area", "dst_key"])
    def test_copy_refuses_every_traversing_segment(self, storage, unsafe, field):
        arguments = {"src_area": AREA, "src_key": KEY, "dst_area": "copies", "dst_key": KEY, field: unsafe}

        with pytest.raises(ValueError, match=r"escapes base directory|must not be empty"):
            storage.copy(**arguments)

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
