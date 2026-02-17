"""
Shared test fixtures for Xpedit API tests.

Mocks all external services (Supabase, Sentry, Gemini, Twitter, Stripe)
so tests can run without any env vars or real connections.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# 1. Set dummy environment variables BEFORE any application import so that
#    module-level code (create_client, stripe, etc.) never touches real services.
# ---------------------------------------------------------------------------
os.environ.setdefault("SUPABASE_URL", "https://fake.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "fake-key")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "fake-service-key")
os.environ.setdefault("SUPABASE_JWT_SECRET", "super-secret-jwt-test-key-at-least-32-chars-long")
os.environ.setdefault("STRIPE_SECRET_KEY", "sk_test_fake")
os.environ.setdefault("STRIPE_WEBHOOK_SECRET", "whsec_fake")
os.environ.setdefault("SENTRY_DSN", "")
os.environ.setdefault("GOOGLE_AI_API_KEY", "")
os.environ.setdefault("TWITTER_CONSUMER_KEY", "")
os.environ.setdefault("TWITTER_CONSUMER_SECRET", "")
os.environ.setdefault("TWITTER_ACCESS_TOKEN", "")
os.environ.setdefault("TWITTER_ACCESS_TOKEN_SECRET", "")
os.environ.setdefault("SENTRY_ENVIRONMENT", "test")

# ---------------------------------------------------------------------------
# 2. Build a reusable mock Supabase client that behaves like the real one
#    for chaining (.table().select().eq()... .execute()).
# ---------------------------------------------------------------------------


class _ChainableMock(MagicMock):
    """A MagicMock whose every attribute call returns itself, allowing
    Supabase-style chained calls like table("x").select("y").eq("z", 1).execute().
    .execute() returns a result object with .data and .count attributes."""

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, name):
        if name in ("data", "count", "_mock_name", "_mock_children",
                     "_mock_methods", "_mock_unsafe", "assert_called",
                     "assert_called_once", "return_value", "side_effect",
                     "_mock_check_sig"):
            return super().__getattr__(name)
        # Always return self to allow chaining
        child = super().__getattr__(name)
        if not isinstance(child, _ChainableMock):
            child = _ChainableMock(name=name)
            setattr(self, name, child)
        return child

    def execute(self):
        """Return a result object with sensible defaults."""
        result = MagicMock()
        result.data = []
        result.count = 0
        return result


def make_mock_supabase():
    """Create a mock Supabase client with chainable table/select/insert/etc."""
    mock = MagicMock()

    # --- table() returns a chainable builder ---
    def _table(name):
        chain = _ChainableMock(name=f"table:{name}")
        return chain

    mock.table = MagicMock(side_effect=_table)

    # --- auth admin ---
    mock.auth = MagicMock()
    mock.auth.admin = MagicMock()
    mock.auth.admin.delete_user = MagicMock(return_value=True)
    mock.auth.admin.update_user_by_id = MagicMock(return_value=MagicMock())

    # --- storage ---
    bucket = MagicMock()
    bucket.upload = MagicMock(return_value=None)
    bucket.remove = MagicMock(return_value=None)
    mock.storage = MagicMock()
    mock.storage.from_ = MagicMock(return_value=bucket)

    # --- rpc ---
    mock.rpc = MagicMock(return_value=MagicMock(execute=MagicMock()))

    return mock


# ---------------------------------------------------------------------------
# 3. Patch the Supabase create_client BEFORE importing main, then import app.
# ---------------------------------------------------------------------------

_mock_supabase = make_mock_supabase()

# We need to inject a fake supabase module BEFORE main.py imports it,
# because the real supabase package may have dependency issues (e.g. jwt).
_fake_supabase_mod = MagicMock()
_fake_supabase_mod.create_client = MagicMock(return_value=_mock_supabase)
sys.modules.setdefault("supabase", _fake_supabase_mod)

# Also pre-mock sentry_sdk to avoid real initialization
_fake_sentry = MagicMock()
_fake_sentry.init = MagicMock()
_fake_sentry.capture_exception = MagicMock()
_fake_sentry.capture_message = MagicMock()
_fake_sentry.set_user = MagicMock()
sys.modules.setdefault("sentry_sdk", _fake_sentry)

# Mock PyJWKClient which may not exist in older jwt versions
import jwt as _jwt_mod

if not hasattr(_jwt_mod, "PyJWKClient"):
    _jwt_mod.PyJWKClient = MagicMock()

from main import app, get_current_user, require_admin

# ---------------------------------------------------------------------------
# 4. Fixtures
# ---------------------------------------------------------------------------

FAKE_USER_ID = "user-00000000-0000-0000-0000-000000000001"
FAKE_DRIVER_ID = "driver-00000000-0000-0000-0000-000000000001"
FAKE_ADMIN_USER_ID = "admin-00000000-0000-0000-0000-000000000099"


@pytest.fixture
def mock_supabase():
    """Return the shared mock Supabase client and reset it before each test."""
    fresh = make_mock_supabase()
    return fresh


@pytest.fixture
def fake_user():
    """A regular authenticated user dict as returned by get_current_user."""
    return {
        "id": FAKE_USER_ID,
        "email": "testuser@example.com",
        "role": "driver",
        "company_id": None,
    }


@pytest.fixture
def fake_admin_user():
    """An admin authenticated user dict."""
    return {
        "id": FAKE_ADMIN_USER_ID,
        "email": "admin@xpedit.es",
        "role": "admin",
        "company_id": None,
    }


@pytest_asyncio.fixture
async def client(fake_user):
    """
    httpx.AsyncClient wired to the FastAPI app via ASGITransport.
    Auth is bypassed: get_current_user always returns fake_user.
    """
    from httpx import ASGITransport, AsyncClient

    async def _override_get_current_user():
        return fake_user

    app.dependency_overrides[get_current_user] = _override_get_current_user
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def admin_client(fake_admin_user):
    """
    httpx.AsyncClient where the user is an admin.
    Both get_current_user and require_admin are overridden.
    """
    from httpx import ASGITransport, AsyncClient

    async def _override_get_current_user():
        return fake_admin_user

    async def _override_require_admin():
        return fake_admin_user

    app.dependency_overrides[get_current_user] = _override_get_current_user
    app.dependency_overrides[require_admin] = _override_require_admin
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def unauth_client():
    """
    httpx.AsyncClient with NO auth overrides -- requests will fail auth.
    """
    from httpx import ASGITransport, AsyncClient

    # Clear any leftover overrides
    app.dependency_overrides.clear()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def clear_rate_limits():
    """Clear in-memory rate limits before each test to avoid cross-test interference."""
    from main import _rate_limits
    _rate_limits.clear()
    yield
    _rate_limits.clear()
