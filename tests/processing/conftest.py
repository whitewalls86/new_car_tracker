"""
Processing test conftest.

Provides a TestClient for processing/app.py with mocked DB and MinIO.
"""

import gzip
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "html"


def load_html_fixture(name: str) -> str:
    """Load a gzip-compressed captured HTML artifact from tests/fixtures/html."""
    return gzip.decompress((_FIXTURE_DIR / f"{name}.html.gz").read_bytes()).decode(
        "utf-8", errors="replace"
    )


@pytest.fixture
def mock_processing_client(mocker, mock_cursor_context):
    """
    TestClient for the processing FastAPI app.

    Patches shared.db.db_cursor and shared.minio.read_html so no real
    DB or MinIO connections are made. Tests override per-case.
    """
    mocker.patch("shared.job_counter._count", 0)
    mocker.patch("processing.routers.batch._claim_batch", return_value=[])

    import processing.app as processing_app
    return TestClient(processing_app.app)
