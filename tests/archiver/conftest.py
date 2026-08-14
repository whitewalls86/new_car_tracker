"""
Archiver test conftest.

pyarrow and s3fs are real packages available in the dev environment.
psycopg2 calls are mocked per-test.
"""
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def no_deploy_pause(mocker):
    """No deploy is pending, unless a test says otherwise.

    Plan 131 Stage 5 D3b made both pack processors read ``deploy_intent`` at
    their boundaries. Unmocked that is a real psycopg2 connect per call: it
    fails open, so the tests still pass, but they pay the connect timeout to
    get there — this fixture is why the archiver suite runs in seconds instead
    of minutes.

    Tests that exercise the pause patch the same targets with ``return_value``
    or a ``side_effect`` sequence.
    """
    mocker.patch(
        "archiver.processors.pack_bronze_html.long_jobs_paused", return_value=False
    )
    mocker.patch(
        "archiver.processors.delete_packed_source_html.long_jobs_paused",
        return_value=False,
    )


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
