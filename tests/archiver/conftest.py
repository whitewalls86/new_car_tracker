"""
Archiver test conftest.

pyarrow and s3fs are real packages available in the dev environment.
psycopg2 calls are mocked per-test.
"""
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_archiver_client():
    """TestClient for the archiver FastAPI app (no lifespan hooks to patch)."""
    from fastapi.testclient import TestClient

    import archiver.app as archiver_app
    return TestClient(archiver_app.app)


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
