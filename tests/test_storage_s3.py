"""The S3-compatible storage backend, against a stubbed AWS client.

``botocore``'s own ``Stubber`` is used rather than a hand-written fake: it
validates every request against the real S3 API model, so a wrong parameter name
or shape fails here instead of in production. No credentials are read and no
network call is made.

The interesting behaviour is key composition (the database stores a bare key, so
the bucket prefix and the area have to be added and stripped symmetrically) and
error classification (a missing blob is ``None``; an auth or throttling failure
must not masquerade as one).
"""

import io
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import boto3
import pytest
from botocore.exceptions import ClientError, ReadTimeoutError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

import jasil.backends.storage_s3 as storage_s3
from jasil.backends.storage_s3 import S3Storage
from jasil.providers import ServeRedirect, StorageBackendUnavailableError, StorageSizeLimitError

BUCKET = "blobs"
PREFIX = "jasil"
AREA = "avatars"
KEY = "42.webp"
# What ``_object_key`` must compose from the three parts above.
OBJECT_KEY = f"{PREFIX}/{AREA}/{KEY}"
MODIFIED = datetime(2026, 1, 2, tzinfo=UTC)


@pytest.fixture
def client():
    """A real boto3 client with stubbed responses and deliberately fake credentials."""
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def stub(client):
    with Stubber(client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture
def storage(client):
    return S3Storage(client, BUCKET, PREFIX)


def _body(data: bytes) -> StreamingBody:
    return StreamingBody(io.BytesIO(data), len(data))


class _FailingBody:
    def read(self, size=-1):
        raise ReadTimeoutError(endpoint_url="https://s3.test")

    def close(self):
        pass


class TestFromUri:
    def test_the_bucket_comes_from_the_authority(self):
        assert S3Storage.from_uri("s3://my-bucket")._bucket == "my-bucket"

    def test_the_path_becomes_the_key_prefix(self):
        assert S3Storage.from_uri("s3://my-bucket/nested/prefix")._prefix == "nested/prefix"

    def test_a_bare_bucket_has_no_prefix(self):
        assert S3Storage.from_uri("s3://my-bucket")._prefix == ""

    def test_a_uri_without_a_bucket_is_refused(self):
        """A silently bucket-less client would fail on the first upload instead."""
        with pytest.raises(ValueError, match="missing a bucket"):
            S3Storage.from_uri("s3://")

    def test_an_endpoint_url_selects_a_compatible_provider(self):
        """MinIO, R2 and Ceph are addressed this way."""
        storage = S3Storage.from_uri("s3://my-bucket?endpoint_url=https://minio.test")

        assert storage._client.meta.endpoint_url == "https://minio.test"

    def test_the_region_is_honoured(self):
        assert S3Storage.from_uri("s3://my-bucket?region=eu-west-1")._client.meta.region_name == "eu-west-1"

    def test_a_client_creation_failure_is_provider_neutral(self, monkeypatch):
        def fail_to_create_client(*_args, **_kwargs):
            raise ReadTimeoutError(endpoint_url="https://s3.test")

        monkeypatch.setattr(storage_s3.boto3, "client", fail_to_create_client)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            S3Storage.from_uri("s3://my-bucket")

        assert isinstance(excinfo.value.__cause__, ReadTimeoutError)


class TestSave:
    def test_it_writes_under_the_composed_key_and_returns_the_bare_one(self, storage, stub):
        """The database stores only the key, so local -> S3 needs no migration."""
        stub.add_response("put_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY, "Body": b"bytes"})

        assert storage.save(AREA, KEY, b"bytes") == KEY

    def test_a_content_type_is_forwarded(self, storage, stub):
        stub.add_response(
            "put_object",
            {},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "Body": b"bytes", "ContentType": "image/webp"},
        )

        storage.save(AREA, KEY, b"bytes", content_type="image/webp")

    def test_no_prefix_yields_an_area_rooted_key(self, client, stub):
        storage = S3Storage(client, BUCKET)
        stub.add_response("put_object", {}, {"Bucket": BUCKET, "Key": f"{AREA}/{KEY}", "Body": b"bytes"})

        storage.save(AREA, KEY, b"bytes")


class TestSaveStream:
    def test_it_completes_a_multipart_upload(self, storage, stub):
        stub.add_response(
            "create_multipart_upload",
            {"UploadId": "upload-1"},
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )
        stub.add_response(
            "upload_part",
            {"ETag": '"etag-1"'},
            {
                "Bucket": BUCKET,
                "Key": OBJECT_KEY,
                "UploadId": "upload-1",
                "PartNumber": 1,
                "Body": b"streamed",
            },
        )
        stub.add_response(
            "complete_multipart_upload",
            {},
            {
                "Bucket": BUCKET,
                "Key": OBJECT_KEY,
                "UploadId": "upload-1",
                "MultipartUpload": {"Parts": [{"ETag": '"etag-1"', "PartNumber": 1}]},
            },
        )

        assert storage.save_stream(AREA, KEY, io.BytesIO(b"streamed")) == 8

    def test_content_type_is_set_when_the_upload_is_created(self, storage, stub):
        stub.add_response(
            "create_multipart_upload",
            {"UploadId": "upload-1"},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "ContentType": "application/zip"},
        )
        stub.add_response(
            "upload_part",
            {"ETag": '"etag-1"'},
            {
                "Bucket": BUCKET,
                "Key": OBJECT_KEY,
                "UploadId": "upload-1",
                "PartNumber": 1,
                "Body": b"x",
            },
        )
        stub.add_response(
            "complete_multipart_upload",
            {},
            {
                "Bucket": BUCKET,
                "Key": OBJECT_KEY,
                "UploadId": "upload-1",
                "MultipartUpload": {"Parts": [{"ETag": '"etag-1"', "PartNumber": 1}]},
            },
        )

        storage.save_stream(AREA, KEY, io.BytesIO(b"x"), content_type="application/zip")

    def test_an_empty_stream_uses_a_zero_byte_object(self, storage, stub):
        stub.add_response(
            "put_object",
            {},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "Body": b"", "ContentType": "application/zip"},
        )

        assert storage.save_stream(AREA, KEY, io.BytesIO(), content_type="application/zip") == 0

    def test_a_mid_stream_limit_breach_aborts_the_upload(self, storage, stub, monkeypatch):
        monkeypatch.setattr(storage_s3, "_MULTIPART_PART_BYTES", 4)
        stub.add_response(
            "create_multipart_upload",
            {"UploadId": "upload-1"},
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )
        stub.add_response(
            "upload_part",
            {"ETag": '"etag-1"'},
            {
                "Bucket": BUCKET,
                "Key": OBJECT_KEY,
                "UploadId": "upload-1",
                "PartNumber": 1,
                "Body": b"1234",
            },
        )
        stub.add_response(
            "abort_multipart_upload",
            {},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "UploadId": "upload-1"},
        )

        with pytest.raises(StorageSizeLimitError):
            storage.save_stream(AREA, KEY, io.BytesIO(b"12345"), max_bytes=4)


class TestGet:
    def test_it_returns_the_stored_bytes(self, storage, stub):
        stub.add_response("get_object", {"Body": _body(b"bytes")}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        assert storage.get(AREA, KEY) == b"bytes"

    @pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
    def test_a_missing_blob_is_none(self, storage, stub, code):
        """S3 says 404; the compatible providers say one of the other two."""
        stub.add_client_error("get_object", service_error_code=code, http_status_code=404)

        assert storage.get(AREA, KEY) is None

    def test_an_access_failure_is_provider_neutral(self, storage, stub):
        """Swallowing this would silently serve empty data on a credentials mistake."""
        stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.get(AREA, KEY)

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestOpenStream:
    def test_a_bounded_range_is_requested_from_s3(self, storage, stub):
        stub.add_response(
            "get_object",
            {"Body": _body(b"2345")},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "Range": "bytes=2-5"},
        )

        with storage.open_stream(AREA, KEY, offset=2, length=4) as stream:
            assert stream.read() == b"2345"

    def test_an_unbounded_offset_is_requested_to_the_end(self, storage, stub):
        stub.add_response(
            "get_object",
            {"Body": _body(b"23456789")},
            {"Bucket": BUCKET, "Key": OBJECT_KEY, "Range": "bytes=2-"},
        )

        with storage.open_stream(AREA, KEY, offset=2) as stream:
            assert stream.read() == b"23456789"

    def test_a_zero_length_range_checks_existence_without_fetching_a_body(self, storage, stub):
        stub.add_response("head_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        with storage.open_stream(AREA, KEY, length=0) as stream:
            assert stream.read() == b""

    def test_a_zero_length_access_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.open_stream(AREA, KEY, length=0)

        assert isinstance(excinfo.value.__cause__, ClientError)

    def test_a_missing_object_raises_file_not_found(self, storage, stub):
        stub.add_client_error("get_object", service_error_code="NoSuchKey", http_status_code=404)

        with pytest.raises(FileNotFoundError):
            storage.open_stream(AREA, KEY)

    def test_an_offset_past_the_end_is_an_empty_stream(self, storage, stub):
        stub.add_client_error("get_object", service_error_code="InvalidRange", http_status_code=416)

        with storage.open_stream(AREA, KEY, offset=100) as stream:
            assert stream.read() == b""

    def test_a_body_read_failure_is_provider_neutral(self, storage, stub):
        stub.add_response(
            "get_object",
            {"Body": _FailingBody()},
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )

        with storage.open_stream(AREA, KEY) as stream, pytest.raises(StorageBackendUnavailableError) as excinfo:
            stream.read()

        assert isinstance(excinfo.value.__cause__, ReadTimeoutError)


class TestStat:
    def test_it_returns_head_object_metadata(self, storage, stub):
        stub.add_response(
            "head_object",
            {
                "ContentLength": 5,
                "LastModified": MODIFIED,
                "ContentType": "image/webp",
                "ETag": '"etag"',
            },
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )

        metadata = storage.stat(AREA, KEY)

        assert metadata is not None
        assert metadata.size == 5
        assert metadata.modified_epoch == MODIFIED.timestamp()
        assert metadata.content_type == "image/webp"
        assert metadata.etag == '"etag"'

    def test_a_missing_object_has_no_stat(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="404", http_status_code=404)

        assert storage.stat(AREA, KEY) is None

    def test_an_access_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.stat(AREA, KEY)

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestServe:
    def test_it_returns_a_presigned_redirect_with_response_controls(self, storage, stub):
        stub.add_response(
            "head_object",
            {"ContentLength": 5, "LastModified": MODIFIED},
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )

        plan = storage.serve(
            AREA,
            KEY,
            download_as="photo.webp",
            content_type="application/octet-stream",
            expires_in=60,
        )

        assert isinstance(plan, ServeRedirect)
        query = parse_qs(urlparse(plan.url).query)
        assert query["response-content-disposition"][0].startswith('attachment; filename="photo.webp";')
        assert query["response-content-type"] == ["application/octet-stream"]

    def test_a_missing_object_raises_before_signing(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="NoSuchKey", http_status_code=404)

        with pytest.raises(FileNotFoundError):
            storage.serve(AREA, KEY)


class TestExists:
    def test_a_present_blob_is_true(self, storage, stub):
        stub.add_response("head_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        assert storage.exists(AREA, KEY) is True

    def test_a_missing_blob_is_false(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="404", http_status_code=404)

        assert storage.exists(AREA, KEY) is False

    def test_a_throttling_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="SlowDown", http_status_code=503)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.exists(AREA, KEY)

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestDelete:
    def test_it_deletes_the_composed_key(self, storage, stub):
        stub.add_response("delete_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        storage.delete(AREA, KEY)

    def test_a_client_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("delete_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.delete(AREA, KEY)

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestDeletePrefix:
    def test_it_deletes_the_exact_key_and_strict_descendants(self, storage, stub):
        root = f"{PREFIX}/{AREA}/pkg/1"
        descendants = [f"{root}/a.webp", f"{root}/nested/b.webp"]
        stub.add_response("head_object", {}, {"Bucket": BUCKET, "Key": root})
        stub.add_response(
            "delete_objects",
            {},
            {"Bucket": BUCKET, "Delete": {"Objects": [{"Key": root}], "Quiet": True}},
        )
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": key} for key in descendants]},
            {"Bucket": BUCKET, "Prefix": f"{root}/", "MaxKeys": 1000},
        )
        stub.add_response(
            "delete_objects",
            {},
            {"Bucket": BUCKET, "Delete": {"Objects": [{"Key": key} for key in descendants], "Quiet": True}},
        )
        stub.add_response(
            "list_objects_v2",
            {},
            {"Bucket": BUCKET, "Prefix": f"{root}/", "MaxKeys": 1000},
        )

        assert storage.delete_prefix(AREA, "pkg/1") == 3

    def test_it_batches_more_than_one_thousand_descendants(self, storage, stub):
        root = f"{PREFIX}/{AREA}/pkg/1"
        first_batch = [f"{root}/{index:04}.webp" for index in range(1000)]
        second_batch = [f"{root}/1000.webp"]
        stub.add_client_error("head_object", service_error_code="404", http_status_code=404)
        for keys in (first_batch, second_batch):
            stub.add_response(
                "list_objects_v2",
                {
                    "Contents": [{"Key": key} for key in keys],
                    "IsTruncated": len(keys) == 1000,
                    **({"NextContinuationToken": "ignored-after-delete"} if len(keys) == 1000 else {}),
                },
                {"Bucket": BUCKET, "Prefix": f"{root}/", "MaxKeys": 1000},
            )
            stub.add_response(
                "delete_objects",
                {},
                {"Bucket": BUCKET, "Delete": {"Objects": [{"Key": key} for key in keys], "Quiet": True}},
            )
        stub.add_response(
            "list_objects_v2",
            {},
            {"Bucket": BUCKET, "Prefix": f"{root}/", "MaxKeys": 1000},
        )

        assert storage.delete_prefix(AREA, "pkg/1") == 1001

    def test_a_delete_error_is_provider_neutral(self, storage, stub):
        root = f"{PREFIX}/{AREA}/pkg/1"
        stub.add_response("head_object", {}, {"Bucket": BUCKET, "Key": root})
        stub.add_response(
            "delete_objects",
            {"Errors": [{"Key": root, "Code": "AccessDenied", "Message": "denied"}]},
            {"Bucket": BUCKET, "Delete": {"Objects": [{"Key": root}], "Quiet": True}},
        )

        with pytest.raises(StorageBackendUnavailableError):
            storage.delete_prefix(AREA, "pkg/1")

    def test_an_access_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.delete_prefix(AREA, "pkg/1")

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestCopy:
    def test_it_uses_boto3_managed_copy(self, storage, stub):
        destination_key = f"{PREFIX}/copies/{KEY}"
        stub.add_response(
            "head_object",
            {"ContentLength": 5},
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )
        stub.add_response(
            "copy_object",
            {},
            {"Bucket": BUCKET, "Key": destination_key, "CopySource": {"Bucket": BUCKET, "Key": OBJECT_KEY}},
        )

        storage.copy(AREA, KEY, "copies", KEY)

    def test_a_missing_source_raises_file_not_found(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="NoSuchKey", http_status_code=404)

        with pytest.raises(FileNotFoundError):
            storage.copy(AREA, KEY, "copies", KEY)

    def test_an_access_failure_is_provider_neutral(self, storage, stub):
        destination_key = f"{PREFIX}/copies/{KEY}"
        stub.add_response(
            "head_object",
            {"ContentLength": 5},
            {"Bucket": BUCKET, "Key": OBJECT_KEY},
        )
        stub.add_client_error(
            "copy_object",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params={
                "Bucket": BUCKET,
                "Key": destination_key,
                "CopySource": {"Bucket": BUCKET, "Key": OBJECT_KEY},
            },
        )

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.copy(AREA, KEY, "copies", KEY)

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestListKeys:
    def test_it_strips_the_bucket_prefix_and_the_area(self, storage, stub):
        """Callers hold bare keys, so a listing has to hand back the same shape."""
        stub.add_response(
            "list_objects_v2",
            {
                "Contents": [
                    {"Key": f"{PREFIX}/{AREA}/b.webp", "LastModified": MODIFIED},
                    {"Key": f"{PREFIX}/{AREA}/a.webp", "LastModified": MODIFIED},
                ]
            },
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/"},
        )

        assert storage.list_keys(AREA) == ["a.webp", "b.webp"]

    def test_a_key_prefix_narrows_the_listing(self, storage, stub):
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/user-1.webp", "LastModified": MODIFIED}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/user-"},
        )

        assert storage.list_keys(AREA, "user-") == ["user-1.webp"]

    def test_every_page_is_read(self, storage, stub):
        """An area larger than one page must not be silently truncated."""
        stub.add_response(
            "list_objects_v2",
            {
                "Contents": [{"Key": f"{PREFIX}/{AREA}/a.webp", "LastModified": MODIFIED}],
                "IsTruncated": True,
                "NextContinuationToken": "next",
            },
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/"},
        )
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/b.webp", "LastModified": MODIFIED}], "IsTruncated": False},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/", "ContinuationToken": "next"},
        )

        assert storage.list_keys(AREA) == ["a.webp", "b.webp"]

    def test_an_empty_area_lists_nothing(self, storage, stub):
        stub.add_response("list_objects_v2", {}, {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/"})

        assert storage.list_keys(AREA) == []

    def test_a_nested_key_is_listed_by_its_full_path(self, storage, stub):
        """A flat prefix scan sees every depth, and the local backend must match."""
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/2026/01/a.webp", "LastModified": MODIFIED}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/"},
        )

        assert storage.list_keys(AREA) == ["2026/01/a.webp"]

    def test_the_lazy_variant_yields_the_last_modified_epoch(self, storage, stub):
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": OBJECT_KEY, "LastModified": MODIFIED}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/"},
        )

        assert list(storage.iter_objects(AREA)) == [(KEY, MODIFIED.timestamp())]

    def test_a_listing_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("list_objects_v2", service_error_code="SlowDown", http_status_code=503)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            list(storage.iter_objects(AREA))

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestCheckWritable:
    def test_it_writes_and_removes_a_unique_probe(self, storage, stub):
        stub.add_response(
            "put_object",
            {},
            {"Bucket": BUCKET, "Key": ANY, "Body": b""},
        )
        stub.add_response(
            "delete_object",
            {},
            {"Bucket": BUCKET, "Key": ANY},
        )

        storage.check_writable()

    def test_a_probe_failure_is_provider_neutral(self, storage, stub):
        stub.add_client_error("put_object", service_error_code="SlowDown", http_status_code=503)

        with pytest.raises(StorageBackendUnavailableError) as excinfo:
            storage.check_writable()

        assert isinstance(excinfo.value.__cause__, ClientError)


class TestUrl:
    def test_it_presigns_the_composed_key(self, storage):
        """Computed at serialization time, so nothing signed is ever persisted.

        Asserted on the parts JASIL controls — bucket, key, and that the URL is
        signed at all — rather than on a parameter name, which differs between
        botocore's signature versions.
        """
        url = storage.url(AREA, KEY)

        assert OBJECT_KEY in url
        assert BUCKET in url
        assert "Signature" in url

    def test_the_expiry_is_carried_into_the_signature(self, storage):
        short = storage.url(AREA, KEY, expires_in=60)
        long = storage.url(AREA, KEY, expires_in=86_400)

        assert short != long

    def test_download_headers_are_pinned_in_the_signature(self, storage):
        url = storage.url(
            AREA,
            KEY,
            download_as="release 2026.zip",
            content_type="application/octet-stream",
        )

        query = parse_qs(urlparse(url).query)
        disposition = query["response-content-disposition"][0]
        assert disposition.startswith('attachment; filename="release 2026.zip";')
        assert "filename*=UTF-8''release%202026.zip" in disposition
        assert query["response-content-type"] == ["application/octet-stream"]

    @pytest.mark.parametrize(
        ("arguments", "match"),
        [
            ({"download_as": "bad\r\nname.zip"}, "download_as"),
            ({"content_type": "text/plain\r\nX-Test: bad"}, "content_type"),
        ],
    )
    def test_response_headers_refuse_line_breaks(self, storage, arguments, match):
        with pytest.raises(ValueError, match=match):
            storage.url(AREA, KEY, **arguments)
