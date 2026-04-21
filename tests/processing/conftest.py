"""
Processing test conftest.

Provides a TestClient for processing/app.py with no real DB or MinIO.
Both `claim_batch` and `process_artifact` (imported into app.py) are
patched at the app-module level so tests control queue behaviour entirely.
"""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def mock_processing_client(mocker):
    """
    TestClient for the processing FastAPI app.

    Patches the three functions imported into processing.app so no real
    DB or MinIO connections are made.  Tests override return values per-case.
    """
    mocker.patch("processing.app.claim_batch", return_value=[])
    mocker.patch("processing.app.process_artifact", return_value={"status": "complete"})
    mocker.patch("processing.app.queue_is_empty", return_value=True)

    import processing.app as processing_app
    return TestClient(processing_app.app)
