"""Unit tests for processing/app.py — all three HTTP endpoints.

No real DB or MinIO: claim_batch, process_artifact, and queue_is_empty
are patched at the processing.app module level via mock_processing_client.
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_artifact(artifact_id=1, artifact_type="results_page"):
    return {
        "artifact_id": artifact_id,
        "minio_path": f"bronze/run-001/artifact_{artifact_id}.html.zst",
        "artifact_type": artifact_type,
        "listing_id": f"aaaa0000-0000-0000-0000-00000000000{artifact_id}",
        "run_id": "bbbb0000-0000-0000-0000-000000000001",
        "fetched_at": "2026-04-20T12:00:00",
    }


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_returns_ok(self, mock_processing_client):
        resp = mock_processing_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


# ---------------------------------------------------------------------------
# GET /ready
# ---------------------------------------------------------------------------

class TestReady:
    def test_ready_when_queue_empty(self, mock_processing_client, mocker):
        mocker.patch("processing.app.queue_is_empty", return_value=True)
        resp = mock_processing_client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ready"] is True
        assert body["queue_empty"] is True

    def test_not_ready_when_queue_has_items(self, mock_processing_client, mocker):
        mocker.patch("processing.app.queue_is_empty", return_value=False)
        resp = mock_processing_client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ready"] is False
        assert body["queue_empty"] is False


# ---------------------------------------------------------------------------
# POST /process/batch
# ---------------------------------------------------------------------------

class TestProcessBatch:
    def test_empty_queue_returns_zero_counts(self, mock_processing_client, mocker):
        mocker.patch("processing.app.claim_batch", return_value=[])
        resp = mock_processing_client.post("/process/batch")
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"claimed": 0, "complete": 0, "retry": 0, "skipped": 0, "results": []}

    def test_batch_size_default_passed_to_claim_batch(self, mock_processing_client, mocker):
        spy = mocker.patch("processing.app.claim_batch", return_value=[])
        mock_processing_client.post("/process/batch")
        spy.assert_called_once_with(batch_size=20, artifact_type=None)

    def test_batch_size_query_param_forwarded(self, mock_processing_client, mocker):
        spy = mocker.patch("processing.app.claim_batch", return_value=[])
        mock_processing_client.post("/process/batch?batch_size=5")
        spy.assert_called_once_with(batch_size=5, artifact_type=None)

    def test_artifact_type_filter_forwarded(self, mock_processing_client, mocker):
        spy = mocker.patch("processing.app.claim_batch", return_value=[])
        mock_processing_client.post("/process/batch?artifact_type=detail_page")
        spy.assert_called_once_with(batch_size=20, artifact_type="detail_page")

    def test_batch_size_below_min_rejected(self, mock_processing_client):
        resp = mock_processing_client.post("/process/batch?batch_size=0")
        assert resp.status_code == 422

    def test_batch_size_above_max_rejected(self, mock_processing_client):
        resp = mock_processing_client.post("/process/batch?batch_size=201")
        assert resp.status_code == 422

    def test_single_complete_artifact_counted(self, mock_processing_client, mocker):
        artifact = _make_artifact(1, "results_page")
        mocker.patch("processing.app.claim_batch", return_value=[artifact])
        mocker.patch("processing.app.process_artifact", return_value={"status": "complete"})

        resp = mock_processing_client.post("/process/batch")
        assert resp.status_code == 200
        body = resp.json()
        assert body["claimed"] == 1
        assert body["complete"] == 1
        assert body["retry"] == 0
        assert body["skipped"] == 0
        assert len(body["results"]) == 1
        assert body["results"][0]["artifact_id"] == 1
        assert body["results"][0]["status"] == "complete"

    def test_retry_artifact_counted(self, mock_processing_client, mocker):
        artifact = _make_artifact(2, "detail_page")
        mocker.patch("processing.app.claim_batch", return_value=[artifact])
        mocker.patch(
            "processing.app.process_artifact",
            return_value={"status": "retry", "error": "MinIO down"},
        )

        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["claimed"] == 1
        assert body["retry"] == 1
        assert body["complete"] == 0

    def test_skip_artifact_counted(self, mock_processing_client, mocker):
        artifact = _make_artifact(3, "unknown_type")
        mocker.patch("processing.app.claim_batch", return_value=[artifact])
        mocker.patch(
            "processing.app.process_artifact",
            return_value={"status": "skip", "reason": "unknown type"},
        )

        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["claimed"] == 1
        assert body["skipped"] == 1
        assert body["complete"] == 0

    def test_mixed_batch_counts_each_status(self, mock_processing_client, mocker):
        artifacts = [_make_artifact(i, "results_page") for i in range(1, 5)]
        statuses = [
            {"status": "complete"},
            {"status": "complete"},
            {"status": "retry", "error": "timeout"},
            {"status": "skip"},
        ]
        mocker.patch("processing.app.claim_batch", return_value=artifacts)
        mocker.patch("processing.app.process_artifact", side_effect=statuses)

        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["claimed"] == 4
        assert body["complete"] == 2
        assert body["retry"] == 1
        assert body["skipped"] == 1
        assert len(body["results"]) == 4

    def test_results_include_artifact_id(self, mock_processing_client, mocker):
        artifacts = [_make_artifact(10), _make_artifact(11)]
        mocker.patch("processing.app.claim_batch", return_value=artifacts)
        mocker.patch(
            "processing.app.process_artifact",
            side_effect=[{"status": "complete"}, {"status": "complete"}],
        )

        resp = mock_processing_client.post("/process/batch")
        ids = [r["artifact_id"] for r in resp.json()["results"]]
        assert ids == [10, 11]

    def test_process_artifact_called_for_each_claimed(self, mock_processing_client, mocker):
        artifacts = [_make_artifact(1), _make_artifact(2)]
        mocker.patch("processing.app.claim_batch", return_value=artifacts)
        spy = mocker.patch(
            "processing.app.process_artifact",
            side_effect=[{"status": "complete"}, {"status": "complete"}],
        )

        mock_processing_client.post("/process/batch")
        assert spy.call_count == 2
        spy.assert_any_call(artifacts[0])
        spy.assert_any_call(artifacts[1])
