"""Unit tests for processing/routers/batch.py — POST /process/batch endpoint.

All DB and MinIO calls are patched. Tests verify:
  - Empty queue returns zero counts
  - Query params forwarded correctly
  - Response shape matches Plan 71 Airflow expectations
  - Count aggregation (srp_count, detail_count, retry_count, skip_count)
"""


def _make_artifact(artifact_id=1, artifact_type="results_page"):
    return {
        "artifact_id": artifact_id,
        "minio_path": f"bronze/run-001/artifact_{artifact_id}.html.zst",
        "artifact_type": artifact_type,
        "listing_id": f"aaaa0000-0000-0000-0000-00000000000{artifact_id}",
        "run_id": "bbbb0000-0000-0000-0000-000000000001",
        "fetched_at": "2026-04-20T12:00:00",
    }


class TestProcessBatch:
    def test_empty_queue_returns_zero_counts(self, mock_processing_client, mocker):
        mocker.patch("processing.routers.batch._claim_batch", return_value=[])
        resp = mock_processing_client.post("/process/batch")
        assert resp.status_code == 200
        body = resp.json()
        assert body == {
            "srp_count": 0,
            "detail_count": 0,
            "retry_count": 0,
            "skip_count": 0,
            "silver_write_failures": 0,
        }

    def test_batch_size_below_min_rejected(self, mock_processing_client):
        resp = mock_processing_client.post("/process/batch?batch_size=0")
        assert resp.status_code == 422

    def test_batch_size_above_max_rejected(self, mock_processing_client):
        resp = mock_processing_client.post("/process/batch?batch_size=2501")
        assert resp.status_code == 422

    def test_dag_batch_size_within_endpoint_limits(self, mock_processing_client):
        """DAG's BATCH_SIZE must be accepted by the endpoint — catches caller/endpoint drift."""
        from airflow.dags.results_processing import BATCH_SIZE
        resp = mock_processing_client.post(f"/process/batch?batch_size={BATCH_SIZE}")
        assert resp.status_code != 422, (
            f"DAG BATCH_SIZE={BATCH_SIZE} is rejected by the endpoint (422). "
            "Update BATCH_SIZE in results_processing.py or raise le= in the router."
        )

    def test_srp_complete_counted(self, mock_processing_client, mocker):
        mocker.patch(
            "processing.routers.batch._claim_batch",
            return_value=[_make_artifact(1, "results_page")],
        )
        mocker.patch(
            "processing.routers.batch._process_artifact",
            return_value={
                "status": "complete",
                "artifact_type": "results_page",
                "silver_written": 3,
            },
        )
        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["srp_count"] == 1
        assert body["detail_count"] == 0

    def test_detail_complete_counted(self, mock_processing_client, mocker):
        mocker.patch(
            "processing.routers.batch._claim_batch",
            return_value=[_make_artifact(2, "detail_page")],
        )
        mocker.patch(
            "processing.routers.batch._process_artifact",
            return_value={
                "status": "complete",
                "artifact_type": "detail_page",
                "silver_written": 1,
            },
        )
        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["srp_count"] == 0
        assert body["detail_count"] == 1

    def test_retry_counted(self, mock_processing_client, mocker):
        mocker.patch(
            "processing.routers.batch._claim_batch",
            return_value=[_make_artifact(3, "results_page")],
        )
        mocker.patch(
            "processing.routers.batch._process_artifact",
            return_value={"status": "retry", "error": "MinIO down"},
        )
        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["retry_count"] == 1
        assert body["srp_count"] == 0

    def test_skip_counted(self, mock_processing_client, mocker):
        mocker.patch(
            "processing.routers.batch._claim_batch",
            return_value=[_make_artifact(4, "unknown")],
        )
        mocker.patch(
            "processing.routers.batch._process_artifact",
            return_value={"status": "skip", "reason": "unknown type"},
        )
        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["skip_count"] == 1

    def test_silver_write_failures_counted_when_silver_written_zero(
        self, mock_processing_client, mocker
    ):
        """complete artifact with silver_written=0 increments silver_write_failures."""
        mocker.patch(
            "processing.routers.batch._claim_batch",
            return_value=[_make_artifact(1, "results_page")],
        )
        mocker.patch(
            "processing.routers.batch._process_artifact",
            return_value={
                "status": "complete",
                "artifact_type": "results_page",
                "silver_written": 0,
            },
        )
        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["silver_write_failures"] == 1
        assert body["srp_count"] == 1

    def test_claim_db_error_returns_503(self, mock_processing_client, mocker):
        mocker.patch(
            "processing.routers.batch._claim_batch",
            side_effect=Exception("connection refused"),
        )
        resp = mock_processing_client.post("/process/batch")
        assert resp.status_code == 503

    def test_mixed_batch(self, mock_processing_client, mocker):
        artifacts = [
            _make_artifact(1, "results_page"),
            _make_artifact(2, "detail_page"),
            _make_artifact(3, "results_page"),
            _make_artifact(4, "detail_page"),
        ]
        mocker.patch("processing.routers.batch._claim_batch", return_value=artifacts)
        mocker.patch(
            "processing.routers.batch._process_artifact",
            side_effect=[
                {"status": "complete", "artifact_type": "results_page", "silver_written": 5},
                {"status": "complete", "artifact_type": "detail_page", "silver_written": 1},
                {"status": "retry", "error": "parse failed"},
                {"status": "skip", "reason": "block_page"},
            ],
        )
        resp = mock_processing_client.post("/process/batch")
        body = resp.json()
        assert body["srp_count"] == 1
        assert body["detail_count"] == 1
        assert body["retry_count"] == 1
        assert body["skip_count"] == 1
