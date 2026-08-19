"""The FastAPI dependencies exposing the platform providers to routes.

These are the seam that keeps route code depending on a provider rather than
importing a backend, so what matters is that each one reaches the *same*
platform the host published, and hands back the right capability.

There are two ways a host publishes one — ``app.state.platform`` for per-app
isolation, and ``jasil.runtime`` for the process — so both are exercised here,
along with which of them wins when a host has set both.

A real ``starlette`` request built over a real ``FastAPI`` app is used rather
than a stub object: the whole point is that ``request.app.state`` resolves, and a
duck-typed stand-in would prove nothing about that.
"""

import pytest
from fastapi import FastAPI
from starlette.requests import Request

import jasil.container as container
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
def platform(tmp_path):
    return container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))


@pytest.fixture
def request_for(platform):
    """A request against an app carrying the platform, as startup would leave it."""
    app = FastAPI()
    app.state.platform = platform
    return _request_against(app)


class TestPlatformDependencies:
    def test_the_platform_is_the_one_startup_attached(self, request_for, platform):
        assert deps.get_platform(request_for) is platform

    @pytest.mark.parametrize(
        ("dependency", "attribute"),
        [
            pytest.param(deps.get_state, "state", id="state"),
            pytest.param(deps.get_storage, "storage", id="storage"),
            pytest.param(deps.get_events, "events", id="events"),
            pytest.param(deps.get_lock, "lock", id="lock"),
            pytest.param(deps.get_clock, "clock", id="clock"),
        ],
    )
    def test_each_capability_resolves_to_its_provider(self, request_for, platform, dependency, attribute):
        assert dependency(request_for) is getattr(platform, attribute)


class TestResolutionOrder:
    """A host that followed the quick start never touches ``app.state``."""

    def test_the_process_wide_platform_is_used_when_the_app_carries_none(self, platform):
        platform_runtime.set_active_platform(platform)

        assert deps.get_platform(_request_against(FastAPI())) is platform

    @pytest.mark.parametrize(
        ("dependency", "attribute"),
        [
            pytest.param(deps.get_state, "state", id="state"),
            pytest.param(deps.get_storage, "storage", id="storage"),
            pytest.param(deps.get_events, "events", id="events"),
            pytest.param(deps.get_lock, "lock", id="lock"),
            pytest.param(deps.get_clock, "clock", id="clock"),
        ],
    )
    def test_every_capability_falls_back_too(self, platform, dependency, attribute):
        platform_runtime.set_active_platform(platform)

        assert dependency(_request_against(FastAPI())) is getattr(platform, attribute)

    def test_an_attached_platform_wins_over_the_process_wide_one(self, request_for, platform, tmp_path):
        """Per-app isolation is the point of ``app.state``; it must not be shadowed."""
        other = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path / "other")))
        platform_runtime.set_active_platform(other)

        assert deps.get_platform(request_for) is platform

    def test_no_platform_anywhere_fails_loudly(self):
        """Better than handing a route a half-built app and failing mid-request."""
        with pytest.raises(RuntimeError, match="Platform is not initialized"):
            deps.get_platform(_request_against(FastAPI()))
