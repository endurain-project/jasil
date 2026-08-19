"""One address-validation contract, run against both ``StorageProvider`` backends.

Local disk and S3 are swapped by configuration alone, so a value one backend
refuses and the other accepts is a bug that only appears after a deployment
change. The reasons differ — a segment escapes the base directory on disk, while
``..`` is a literal character in an S3 key and quietly produces a nonsense
object — but the rule a caller sees has to be identical, which is what this
module pins.

The rejection happens before any filesystem access or client call, so the S3
backend needs no stubbed response here: reaching the network would itself be the
failure.
"""

import boto3
import pytest

from jasil.backends.storage_local import LocalStorage
from jasil.backends.storage_s3 import S3Storage

# Values that must never address a blob, whichever backend is configured.
UNSAFE_SEGMENTS = ["../escape", "/etc/passwd", "a/../../b", ".."]


@pytest.fixture(params=["local", "s3"])
def storage(request, tmp_path):
    if request.param == "local":
        return LocalStorage(str(tmp_path))
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    return S3Storage(client, "blobs", "jasil")


class TestSegmentValidationConformance:
    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize("operation", ["save", "get", "exists", "delete", "url"])
    def test_a_traversing_segment_is_refused(self, storage, unsafe, field, operation):
        arguments = {"area": "avatars", "key": "42.webp", field: unsafe}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else ()

        with pytest.raises(ValueError, match="escapes base directory"):
            call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("field", ["area", "key"])
    @pytest.mark.parametrize("operation", ["save", "get", "exists", "delete", "url"])
    def test_an_empty_segment_is_refused(self, storage, field, operation):
        """An empty segment collapses the address onto the storage root itself."""
        arguments = {"area": "avatars", "key": "42.webp", field: ""}
        call = getattr(storage, operation)
        extra = (b"x",) if operation == "save" else ()

        with pytest.raises(ValueError, match="must not be empty"):
            call(arguments["area"], arguments["key"], *extra)

    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    def test_listing_refuses_a_traversing_area(self, storage, unsafe):
        with pytest.raises(ValueError, match="escapes base directory"):
            storage.list_keys(unsafe)

    @pytest.mark.parametrize("unsafe", UNSAFE_SEGMENTS)
    def test_listing_refuses_a_traversing_prefix(self, storage, unsafe):
        with pytest.raises(ValueError, match="escapes base directory"):
            storage.list_keys("avatars", unsafe)

    def test_listing_refuses_an_empty_area(self, storage):
        with pytest.raises(ValueError, match="must not be empty"):
            storage.list_keys("")
