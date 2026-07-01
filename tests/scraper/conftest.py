"""
Scraper test conftest.

Sets up:
1. sys.path so tests can import from scraper/ directly
   (processors.fingerprint, db, app, etc.)
2. sys.modules stubs for heavy native deps that are only present inside Docker
   (curl_cffi).
"""
import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Path: make scraper/ importable as the top-level package
# ---------------------------------------------------------------------------
_SCRAPER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "scraper")
)
if _SCRAPER_DIR not in sys.path:
    sys.path.insert(0, _SCRAPER_DIR)

# ---------------------------------------------------------------------------
# Stub out native/Docker-only deps before anything imports them
# ---------------------------------------------------------------------------

def _stub_module(name, **attrs):
    """Insert a MagicMock into sys.modules under *name* if not already there."""
    if name not in sys.modules:
        m = MagicMock()
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
    return sys.modules[name]


# curl_cffi (TLS-fingerprinting HTTP client, only in scraper Docker image)
_curl_requests = _stub_module("curl_cffi.requests")
_stub_module("curl_cffi")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _mock_external_io(mocker):
    """
    Prevent scraper tests from making real MinIO or Postgres connections.

    scrape_detail_fetch unconditionally tries to write to MinIO and then insert
    into artifacts_queue via psycopg2. Without this fixture both calls block for
    ~20 s waiting for TCP timeouts to unreachable Docker hostnames (minio:9000,
    postgres:5432). Tests that need specific behaviour (MinIO failure, different
    artifact_id, etc.) override these patches with their own mocker.patch calls,
    which are applied after this fixture and take priority.
    """
    mocker.patch(
        "shared.minio.make_key",
        return_value="html/year=2026/month=1/artifact_type=detail_page/stub.html.zst",
    )
    mocker.patch(
        "shared.minio.write_html",
        return_value="s3://bronze/html/year=2026/month=1/artifact_type=detail_page/stub.html.zst",
    )
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mocker.patch("shared.db.get_conn", return_value=mock_conn)


@pytest.fixture(autouse=True)
def clear_jobs():
    """
    The scraper app keeps an in-memory _jobs dict at module level.
    Clear it before and after each test so tests don't bleed state.
    """
    try:
        import scraper.app as scraper_app
        scraper_app._jobs.clear()
        yield
        scraper_app._jobs.clear()
    except Exception:
        yield


@pytest.fixture
def mock_scraper_client(mocker):
    """
    TestClient for the scraper FastAPI app.
    Patches app.get_pool / app.close_pool (the names imported into app.py)
    so the async lifespan hook never touches a real database connection.
    """
    # app.py does `from db import get_pool, close_pool` so we must patch
    # the names in the app module, not the db module.
    mocker.patch("scraper.app.get_pool", new_callable=AsyncMock, return_value=MagicMock())
    mocker.patch("scraper.app.close_pool", new_callable=AsyncMock)
    from fastapi.testclient import TestClient

    import scraper.app as scraper_app
    return TestClient(scraper_app.app)


@pytest.fixture
def mock_async_pool(mocker):
    """
    Returns (mock_pool, mock_conn) where mock_pool.acquire() is an async
    context manager that yields mock_conn.
    """
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    acquire_cm = mock_pool.acquire.return_value
    acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=False)
    transaction_cm = acquire_cm.transaction.return_value
    transaction_cm.__aenter__ = AsyncMock(return_value=None)
    transaction_cm.__aexit__ = AsyncMock(return_value=False)
    return mock_pool, mock_conn


@pytest.fixture
def mock_cf_session(mocker):
    """
    Mock for curl_cffi.requests.Session used by scrape_detail_fetch.
    Patches make_cf_session (where it's imported in scrape_detail) to return a
    controllable mock session, and patches get_cf_credentials to return a cache hit
    (no bootstrap HTML), forcing the code into the session.get() path.
    Returns (mock_session, mock_response).
    """
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"content-type": "text/html; charset=utf-8"}
    mock_resp.content = b"<html><body>detail</body></html>"
    mock_resp.url = "https://www.cars.com/vehicledetail/abc123-0000-0000-0000-000000000001/"

    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp

    # Patch where the names are used, not where they are defined.
    mocker.patch("scraper.processors.scrape_detail.make_cf_session", return_value=mock_session)
    mocker.patch(
        "scraper.processors.scrape_detail.get_cf_credentials",
        return_value=({"cookies": {}, "user_agent": "test-ua"}, None, None),
    )

    return mock_session, mock_resp
