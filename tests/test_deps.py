"""The FastAPI dependencies exposing the platform providers to routes.

These are the seam that keeps route code depending on a provider rather than
importing a backend, so what matters is that each one reaches the *same*
process-wide platform the composition root published, and hands back the right
capability.

A real ``starlette`` request built over a real ``FastAPI`` app is used rather
than a stub object: the whole point is that ``request.app.state`` resolves, and a
duck-typed stand-in would prove nothing about that.
"""

import pytest
from fastapi import FastAPI
from starlette.requests import Request

import jasil.container as container
import jasil.deps as deps
import jasil.settings as settings


@pytest.fixture
def platform(tmp_path):
    return container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))


@pytest.fixture
def request_for(platform):
    """A request against an app carrying the platform, as startup would leave it."""
    app = FastAPI()
    app.state.platform = platform
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [], "app": app})


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

    def test_an_app_without_a_platform_fails_loudly(self, platform):
        """Better than handing a route a half-built app and failing mid-request."""
        app = FastAPI()
        request = Request({"type": "http", "method": "GET", "path": "/", "headers": [], "app": app})

        with pytest.raises(AttributeError):
            deps.get_platform(request)
