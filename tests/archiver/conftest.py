"""
Archiver test conftest.

Sets up sys.path so tests can import from archiver/ directly
(processors.archive_artifacts, processors.cleanup_artifacts, app, etc.).

pyarrow and s3fs are real packages available in the dev environment.
psycopg2 calls are mocked per-test.
"""
import os
import sys
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Path: make archiver/ importable as the top-level package
# ---------------------------------------------------------------------------
_ARCHIVER_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "archiver")
)
if _ARCHIVER_DIR not in sys.path:
    sys.path.insert(0, _ARCHIVER_DIR)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_archiver_client():
    """TestClient for the archiver FastAPI app (no lifespan hooks to patch)."""
    import archiver.app as archiver_app
    from fastapi.testclient import TestClient
    return TestClient(archiver_app.app)


@pytest.fixture
def db_kwargs():
    """Dummy psycopg2 connection kwargs — never actually connect in unit tests."""
    return {"host": "localhost", "dbname": "cartracker", "user": "test", "password": "test"}


@pytest.fixture
def mock_s3fs(mocker):
    """
    Patches s3fs.S3FileSystem so no real MinIO connection is needed.
    Returns the mock filesystem instance.
    """
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True  # bucket already exists by default
    mocker.patch("s3fs.S3FileSystem", return_value=mock_fs)
    return mock_fs
