"""
Processing test conftest.

Provides a TestClient for processing/app.py with mocked DB and MinIO.
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_processing_client(mocker):
    """
    TestClient for the processing FastAPI app.

    Patches shared.db.db_cursor and shared.minio.read_html so no real
    DB or MinIO connections are made. Tests override per-case.
    """
    mocker.patch("shared.job_counter._count", 0)
    mocker.patch("processing.routers.batch._claim_batch", return_value=[])

    import processing.app as processing_app
    return TestClient(processing_app.app)
