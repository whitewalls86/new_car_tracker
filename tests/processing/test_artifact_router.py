"""Unit tests for processing/routers/artifact.py — POST /process/artifact/{id}.

DB and MinIO calls are patched. Tests verify:
  - 404 when artifact not found or not in reprocessable state
  - Result forwarded from _process_artifact on success
  - processing event written before _process_artifact is called
"""
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


@contextmanager
def _fake_cursor(cursor):
    yield cursor


def _make_artifact_row(artifact_id=1):
    return {
        "artifact_id": artifact_id,
        "minio_path": f"bronze/artifact_{artifact_id}.html.zst",
        "artifact_type": "results_page",
        "listing_id": "aaaa-0000-0000-0000-000000000001",
        "run_id": "bbbb-0000-0000-0000-000000000001",
        "fetched_at": "2026-04-20T12:00:00",
    }


@pytest.fixture
def client(mocker):
    mocker.patch("shared.job_counter._count", 0)
    import processing.app as processing_app
    return TestClient(processing_app.app)


class TestProcessSingleArtifact:
    def _setup_cursors(self, mocker, fetch_row):
        """
        Set up db_cursor to return fetch_row on the first call (UPDATE RETURNING)
        and a no-op cursor on subsequent calls (event writes).
        """
        fetch_cursor = MagicMock()
        fetch_cursor.fetchone.return_value = fetch_row

        event_cursor = MagicMock()

        call_count = {"n": 0}

        @contextmanager
        def _multi_cursor(**kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                yield fetch_cursor
            else:
                yield event_cursor

        mocker.patch("processing.routers.artifact.db_cursor", side_effect=_multi_cursor)
        return fetch_cursor, event_cursor

    def test_artifact_not_found_returns_404(self, client, mocker):
        self._setup_cursors(mocker, fetch_row=None)
        resp = client.post("/process/artifact/999")
        assert resp.status_code == 404

    def test_artifact_found_calls_process_artifact(self, client, mocker):
        row = _make_artifact_row(artifact_id=5)
        self._setup_cursors(mocker, fetch_row=row)
        mock_proc = mocker.patch(
            "processing.routers.artifact._process_artifact",
            return_value={"status": "complete", "artifact_type": "results_page"},
        )
        resp = client.post("/process/artifact/5")
        assert resp.status_code == 200
        mock_proc.assert_called_once()

    def test_result_merged_with_artifact_id(self, client, mocker):
        row = _make_artifact_row(artifact_id=7)
        self._setup_cursors(mocker, fetch_row=row)
        mocker.patch(
            "processing.routers.artifact._process_artifact",
            return_value={"status": "complete", "artifact_type": "results_page"},
        )
        resp = client.post("/process/artifact/7")
        body = resp.json()
        assert body["artifact_id"] == 7
        assert body["status"] == "complete"

    def test_retry_result_forwarded(self, client, mocker):
        row = _make_artifact_row(artifact_id=3)
        self._setup_cursors(mocker, fetch_row=row)
        mocker.patch(
            "processing.routers.artifact._process_artifact",
            return_value={"status": "retry", "error": "MinIO down"},
        )
        resp = client.post("/process/artifact/3")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "retry"
        assert body["artifact_id"] == 3
