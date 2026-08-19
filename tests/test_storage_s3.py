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

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from jasil.backends.storage_s3 import S3Storage

BUCKET = "blobs"
PREFIX = "jasil"
AREA = "avatars"
KEY = "42.webp"
# What ``_object_key`` must compose from the three parts above.
OBJECT_KEY = f"{PREFIX}/{AREA}/{KEY}"


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


class TestGet:
    def test_it_returns_the_stored_bytes(self, storage, stub):
        stub.add_response("get_object", {"Body": _body(b"bytes")}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        assert storage.get(AREA, KEY) == b"bytes"

    @pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
    def test_a_missing_blob_is_none(self, storage, stub, code):
        """S3 says 404; the compatible providers say one of the other two."""
        stub.add_client_error("get_object", service_error_code=code, http_status_code=404)

        assert storage.get(AREA, KEY) is None

    def test_an_access_failure_is_not_a_missing_blob(self, storage, stub):
        """Swallowing this would silently serve empty data on a credentials mistake."""
        stub.add_client_error("get_object", service_error_code="AccessDenied", http_status_code=403)

        with pytest.raises(ClientError):
            storage.get(AREA, KEY)


class TestExists:
    def test_a_present_blob_is_true(self, storage, stub):
        stub.add_response("head_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        assert storage.exists(AREA, KEY) is True

    def test_a_missing_blob_is_false(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="404", http_status_code=404)

        assert storage.exists(AREA, KEY) is False

    def test_a_throttling_failure_surfaces(self, storage, stub):
        stub.add_client_error("head_object", service_error_code="SlowDown", http_status_code=503)

        with pytest.raises(ClientError):
            storage.exists(AREA, KEY)


class TestDelete:
    def test_it_deletes_the_composed_key(self, storage, stub):
        stub.add_response("delete_object", {}, {"Bucket": BUCKET, "Key": OBJECT_KEY})

        storage.delete(AREA, KEY)


class TestListKeys:
    def test_it_strips_the_bucket_prefix_and_the_area(self, storage, stub):
        """Callers hold bare keys, so a listing has to hand back the same shape."""
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/b.webp"}, {"Key": f"{PREFIX}/{AREA}/a.webp"}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"},
        )

        assert storage.list_keys(AREA) == ["a.webp", "b.webp"]

    def test_a_key_prefix_narrows_the_listing(self, storage, stub):
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/user-1.webp"}]},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}/user-"},
        )

        assert storage.list_keys(AREA, "user-") == ["user-1.webp"]

    def test_every_page_is_read(self, storage, stub):
        """An area larger than one page must not be silently truncated."""
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/a.webp"}], "IsTruncated": True, "NextContinuationToken": "next"},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"},
        )
        stub.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": f"{PREFIX}/{AREA}/b.webp"}], "IsTruncated": False},
            {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}", "ContinuationToken": "next"},
        )

        assert storage.list_keys(AREA) == ["a.webp", "b.webp"]

    def test_an_empty_area_lists_nothing(self, storage, stub):
        stub.add_response("list_objects_v2", {}, {"Bucket": BUCKET, "Prefix": f"{PREFIX}/{AREA}"})

        assert storage.list_keys(AREA) == []


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
