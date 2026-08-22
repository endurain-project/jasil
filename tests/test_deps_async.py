"""The FastAPI dependencies exposing the *async* platform providers to routes.

The async counterpart of ``test_deps.py``. The resolution order is the same —
``app.state.async_platform`` first, then the process-wide async slot — and it is
tested the same way, against a real ``starlette`` request over a real ``FastAPI``
app rather than a duck-typed stub.

What is new here, and the reason this file exists rather than a few extra cases
in ``test_deps.py``, is the *slot separation*: the two faces read different
attributes and different process slots, so a host that publishes one cannot
accidentally have a route handed the other's providers. Getting that wrong would
surface as a coroutine never awaited, or a blocking call inside the loop — both
of which are far cheaper to catch here.
"""

import pytest
from fastapi import FastAPI
from starlette.requests import Request

import jasil.container as container
import jasil.container_async as container_async
import jasil.deps as deps
import jasil.runtime as platform_runtime
import jasil.settings as settings


@pytest.fixture(autouse=True)
def _unpublish_platform():
    """Leave no process-wide platform behind for the next test."""
    yield
    platform_runtime.reset()


def _request_against(app: FastAPI) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [], "app": app})


@pytest.fixture
async def async_platform(tmp_path):
    platform = await container_async.build_async_platform(settings.JasilSettings(data_dir=str(tmp_path)))
    yield platform
    await platform.aclose()


@pytest.fixture
def request_for(async_platform):
    """A request against an app carrying the async platform, as startup would leave it."""
    app = FastAPI()
    app.state.async_platform = async_platform
    return _request_against(app)


class TestAsyncPlatformDependencies:
    def test_the_platform_is_the_one_startup_attached(self, request_for, async_platform):
        assert deps.get_async_platform(request_for) is async_platform

    @pytest.mark.parametrize(
        ("dependency", "attribute"),
        [
            pytest.param(deps.get_async_state, "state", id="state"),
            pytest.param(deps.get_async_storage, "storage", id="storage"),
            pytest.param(deps.get_async_events, "events", id="events"),
            pytest.param(deps.get_async_lock, "lock", id="lock"),
            pytest.param(deps.get_async_clock, "clock", id="clock"),
        ],
    )
    def test_each_capability_resolves_to_its_provider(self, request_for, async_platform, dependency, attribute):
        assert dependency(request_for) is getattr(async_platform, attribute)


class TestResolutionOrder:
    """A host that followed the quick start never touches ``app.state``."""

    def test_the_process_wide_platform_is_used_when_the_app_carries_none(self, async_platform):
        platform_runtime.set_active_async_platform(async_platform)

        assert deps.get_async_platform(_request_against(FastAPI())) is async_platform

    @pytest.mark.parametrize(
        ("dependency", "attribute"),
        [
            pytest.param(deps.get_async_state, "state", id="state"),
            pytest.param(deps.get_async_storage, "storage", id="storage"),
            pytest.param(deps.get_async_events, "events", id="events"),
            pytest.param(deps.get_async_lock, "lock", id="lock"),
            pytest.param(deps.get_async_clock, "clock", id="clock"),
        ],
    )
    def test_every_capability_falls_back_too(self, async_platform, dependency, attribute):
        platform_runtime.set_active_async_platform(async_platform)

        assert dependency(_request_against(FastAPI())) is getattr(async_platform, attribute)

    async def test_an_attached_platform_wins_over_the_process_wide_one(self, request_for, async_platform, tmp_path):
        """Per-app isolation is the point of ``app.state``; it must not be shadowed."""
        other = await container_async.build_async_platform(settings.JasilSettings(data_dir=str(tmp_path / "other")))
        platform_runtime.set_active_async_platform(other)

        assert deps.get_async_platform(request_for) is async_platform
        await other.aclose()

    def test_no_platform_anywhere_fails_loudly(self):
        """Better than handing a route a half-built app and failing mid-request."""
        with pytest.raises(RuntimeError, match="not initialized"):
            deps.get_async_platform(_request_against(FastAPI()))


class TestTheFacesDoNotShadowEachOther:
    """A single platform is entirely sync or entirely async — never a mixture."""

    def test_a_published_sync_platform_does_not_satisfy_an_async_route(self, tmp_path):
        platform_runtime.set_active_platform(container.build_platform(settings.JasilSettings(data_dir=str(tmp_path))))

        with pytest.raises(RuntimeError, match="not initialized"):
            deps.get_async_platform(_request_against(FastAPI()))

    def test_a_published_async_platform_does_not_satisfy_a_sync_route(self, async_platform):
        platform_runtime.set_active_async_platform(async_platform)

        with pytest.raises(RuntimeError, match="not initialized"):
            deps.get_platform(_request_against(FastAPI()))

    def test_an_attached_sync_platform_does_not_satisfy_an_async_route(self, tmp_path):
        app = FastAPI()
        app.state.platform = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))

        with pytest.raises(RuntimeError, match="not initialized"):
            deps.get_async_platform(_request_against(app))

    def test_both_faces_can_be_published_at_once(self, async_platform, tmp_path):
        """An async API beside a synchronous worker is a reasonable topology."""
        sync_platform = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        platform_runtime.set_active_platform(sync_platform)
        platform_runtime.set_active_async_platform(async_platform)
        request = _request_against(FastAPI())

        assert deps.get_platform(request) is sync_platform
        assert deps.get_async_platform(request) is async_platform
