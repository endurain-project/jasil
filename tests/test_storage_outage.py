"""``is_storage_unavailable``: an outage the call will survive, told apart from a refusal it will not."""

import pytest

from jasil.providers import StorageBackendUnavailableError, is_storage_unavailable


class DomainError(Exception):
    """What a host raises around a storage failure."""


def _raised(build):
    try:
        build()
    except Exception as error:
        return error
    raise AssertionError("nothing was raised")


class TestIsStorageUnavailable:
    def test_an_unavailable_backend_is_an_outage_by_default(self):
        error = StorageBackendUnavailableError("S3 storage backend is unavailable")

        assert error.transient is True
        assert is_storage_unavailable(error)

    def test_a_refusal_is_not_an_outage(self):
        error = StorageBackendUnavailableError("S3 refused the request", transient=False)

        assert error.transient is False
        assert not is_storage_unavailable(error)

    @pytest.mark.parametrize("error", [RuntimeError("boom"), ValueError("bad key"), OSError("disk")])
    def test_another_error_is_not_an_outage(self, error):
        assert not is_storage_unavailable(error)

    def test_an_explicit_cause_is_followed(self):
        def build():
            raise DomainError("could not delete") from StorageBackendUnavailableError("down")

        assert is_storage_unavailable(_raised(build))

    def test_an_implicit_context_is_followed(self):
        def clean_up():
            raise DomainError("cleanup failed")

        def build():
            try:
                raise StorageBackendUnavailableError("down")
            except StorageBackendUnavailableError:
                clean_up()

        assert is_storage_unavailable(_raised(build))

    def test_a_suppressed_context_is_not_followed(self):
        def build():
            try:
                raise StorageBackendUnavailableError("down")
            except StorageBackendUnavailableError:
                raise DomainError("unrelated") from None

        assert not is_storage_unavailable(_raised(build))

    def test_a_chain_several_links_deep_is_followed(self):
        def build():
            try:
                raise DomainError("inner") from StorageBackendUnavailableError("down")
            except DomainError as error:
                raise RuntimeError("outer") from error

        assert is_storage_unavailable(_raised(build))

    def test_a_refusal_wrapped_in_a_domain_error_is_not_an_outage(self):
        def build():
            raise DomainError("could not delete") from StorageBackendUnavailableError("denied", transient=False)

        assert not is_storage_unavailable(_raised(build))

    def test_a_cyclic_chain_ends(self):
        first, second = DomainError("first"), DomainError("second")
        first.__cause__, second.__cause__ = second, first

        assert not is_storage_unavailable(first)
