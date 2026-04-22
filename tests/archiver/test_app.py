"""Unit tests for archiver/app.py — all HTTP endpoints."""


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_health_returns_ok(self, mock_archiver_client):
        resp = mock_archiver_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


# ---------------------------------------------------------------------------
# POST /archive/artifacts
# ---------------------------------------------------------------------------

class TestArchiveArtifactsEndpoint:
    def test_empty_artifacts_returns_zero_counts(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._archive_artifacts", return_value=[])
        resp = mock_archiver_client.post("/archive/artifacts", json={"artifacts": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["archived"] == 0
        assert data["failed"] == 0

    def test_all_archived(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._archive_artifacts", return_value=[
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": True, "reason": None},
        ])
        resp = mock_archiver_client.post("/archive/artifacts", json={
            "artifacts": [
                {"artifact_id": 1, "filepath": "/a.html"},
                {"artifact_id": 2, "filepath": "/b.html"},
            ]
        })
        data = resp.json()
        assert data["total"] == 2
        assert data["archived"] == 2
        assert data["failed"] == 0

    def test_partial_failure(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._archive_artifacts", return_value=[
            {"artifact_id": 1, "archived": True, "reason": None},
            {"artifact_id": 2, "archived": False, "reason": "parquet_write_error: boom"},
        ])
        resp = mock_archiver_client.post("/archive/artifacts", json={
            "artifacts": [
                {"artifact_id": 1, "filepath": "/a.html"},
                {"artifact_id": 2, "filepath": "/b.html"},
            ]
        })
        data = resp.json()
        assert data["archived"] == 1
        assert data["failed"] == 1

    def test_results_list_included(self, mock_archiver_client, mocker):
        fake = [{"artifact_id": 1, "archived": True, "reason": None}]
        mocker.patch("archiver.app._archive_artifacts", return_value=fake)
        payload = {"artifacts": [{"artifact_id": 1, "filepath": "/a.html"}]}
        resp = mock_archiver_client.post("/archive/artifacts", json=payload)
        assert resp.json()["results"] == fake

    def test_artifacts_forwarded_to_processor(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._archive_artifacts", return_value=[])
        payload = {"artifacts": [{"artifact_id": 7, "filepath": "/x.html"}]}
        mock_archiver_client.post("/archive/artifacts", json=payload)
        call_artifacts = mock_fn.call_args[0][0]
        assert call_artifacts == payload["artifacts"]


# ---------------------------------------------------------------------------
# POST /cleanup/artifacts
# ---------------------------------------------------------------------------

class TestCleanupArtifactsEndpoint:
    def test_empty_artifacts(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app.cleanup_artifacts", return_value=[])
        resp = mock_archiver_client.post("/cleanup/artifacts", json={"artifacts": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["deleted"] == 0
        assert data["failed"] == 0

    def test_all_deleted(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app.cleanup_artifacts", return_value=[
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": True, "reason": None},
        ])
        resp = mock_archiver_client.post("/cleanup/artifacts", json={
            "artifacts": [
                {"artifact_id": 1, "filepath": "/a.html"},
                {"artifact_id": 2, "filepath": "/b.html"},
            ]
        })
        data = resp.json()
        assert data["total"] == 2
        assert data["deleted"] == 2
        assert data["failed"] == 0

    def test_partial_failure(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app.cleanup_artifacts", return_value=[
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": False, "reason": "PermissionError: denied"},
        ])
        resp = mock_archiver_client.post("/cleanup/artifacts", json={
            "artifacts": [
                {"artifact_id": 1, "filepath": "/a.html"},
                {"artifact_id": 2, "filepath": "/b.html"},
            ]
        })
        data = resp.json()
        assert data["deleted"] == 1
        assert data["failed"] == 1


# ---------------------------------------------------------------------------
# POST /cleanup/parquet
# ---------------------------------------------------------------------------

class TestCleanupParquetEndpoint:
    def test_empty_paths(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_parquet", return_value=[])
        resp = mock_archiver_client.post("/cleanup/parquet", json={"paths": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["deleted"] == 0
        assert data["failed"] == 0

    def test_all_deleted(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_parquet", return_value=[
            {"path": "bronze/html/year=2025/month=11/", "deleted": True, "reason": None},
            {"path": "bronze/html/year=2025/month=12/", "deleted": True, "reason": None},
        ])
        resp = mock_archiver_client.post("/cleanup/parquet", json={
            "paths": [
                "bronze/html/year=2025/month=11/",
                "bronze/html/year=2025/month=12/",
            ]
        })
        data = resp.json()
        assert data["total"] == 2
        assert data["deleted"] == 2
        assert data["failed"] == 0

    def test_partial_failure(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_parquet", return_value=[
            {"path": "bronze/html/year=2025/month=11/", "deleted": True, "reason": None},
            {"path": "bronze/html/year=2025/month=10/", "deleted": False, "reason": "timeout"},
        ])
        resp = mock_archiver_client.post("/cleanup/parquet", json={
            "paths": [
                "bronze/html/year=2025/month=11/",
                "bronze/html/year=2025/month=10/",
            ]
        })
        data = resp.json()
        assert data["deleted"] == 1
        assert data["failed"] == 1

    def test_paths_forwarded_to_processor(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._cleanup_parquet", return_value=[])
        paths = ["bronze/html/year=2025/month=09/"]
        mock_archiver_client.post("/cleanup/parquet", json={"paths": paths})
        assert mock_fn.call_args[0][0] == paths

    def test_already_deleted_counts_as_deleted(self, mock_archiver_client, mocker):
        cleanup_result = [{
            "path": "bronze/html/year=2025/month=08/",
            "deleted": True,
            "reason": "already_deleted",
        }]
        mocker.patch("archiver.app._cleanup_parquet", return_value=cleanup_result)
        paths_payload = {"paths": ["bronze/html/year=2025/month=08/"]}
        resp = mock_archiver_client.post("/cleanup/parquet", json=paths_payload)
        data = resp.json()
        assert data["deleted"] == 1
        assert data["failed"] == 0


# ---------------------------------------------------------------------------
# POST /cleanup/parquet/run
# ---------------------------------------------------------------------------

class TestCleanupParquetRunEndpoint:
    def test_delegates_to_run_cleanup_parquet(self, mock_archiver_client, mocker):
        fake = {"total": 2, "deleted": 2, "failed": 0, "results": []}
        mocker.patch("archiver.app._run_cleanup_parquet", return_value=fake)
        resp = mock_archiver_client.post("/cleanup/parquet/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zeros(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._run_cleanup_parquet",
            return_value={"total": 0, "deleted": 0, "failed": 0, "results": []},
        )
        resp = mock_archiver_client.post("/cleanup/parquet/run")
        assert resp.json()["total"] == 0


# ---------------------------------------------------------------------------
# POST /cleanup/artifacts/run
# ---------------------------------------------------------------------------

class TestCleanupArtifactsRunEndpoint:
    def test_delegates_to_run_cleanup_artifacts(self, mock_archiver_client, mocker):
        fake = {"total": 3, "archived": 3, "deleted": 3, "failed": 0, "results": []}
        mocker.patch("archiver.app._run_cleanup_artifacts", return_value=fake)
        resp = mock_archiver_client.post("/cleanup/artifacts/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zeros(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._run_cleanup_artifacts",
            return_value={"total": 0, "archived": 0, "deleted": 0, "failed": 0, "results": []},
        )
        resp = mock_archiver_client.post("/cleanup/artifacts/run")
        assert resp.json()["total"] == 0


# ---------------------------------------------------------------------------
# POST /cleanup/queue  (Plan 97 — batch delete by caller-supplied IDs)
# ---------------------------------------------------------------------------

class TestCleanupQueueEndpoint:
    def test_empty_artifact_ids_returns_zeros(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_queue", return_value=[])
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": []})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["deleted"] == 0
        assert data["failed"] == 0

    def test_all_deleted(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_queue", return_value=[
            {"artifact_id": 1, "deleted": True, "reason": None},
            {"artifact_id": 2, "deleted": True, "reason": None},
        ])
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [1, 2]})
        data = resp.json()
        assert data["total"] == 2
        assert data["deleted"] == 2
        assert data["failed"] == 0

    def test_partial_failure(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._cleanup_queue", return_value=[
            {"artifact_id": 1, "deleted": True, "reason": None},
            {
                "artifact_id": 2, 
                "deleted": False, 
                "reason": "not deleted — row missing or status not in (complete, skip)"
            },
        ])
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [1, 2]})
        data = resp.json()
        assert data["deleted"] == 1
        assert data["failed"] == 1

    def test_artifact_ids_forwarded_as_ints(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._cleanup_queue", return_value=[])
        mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [10, 20]})
        called_ids = mock_fn.call_args[0][0]
        assert called_ids == [10, 20]
        assert all(isinstance(i, int) for i in called_ids)

    def test_results_included_in_response(self, mock_archiver_client, mocker):
        fake = [{"artifact_id": 5, "deleted": True, "reason": None}]
        mocker.patch("archiver.app._cleanup_queue", return_value=fake)
        resp = mock_archiver_client.post("/cleanup/queue", json={"artifact_ids": [5]})
        assert resp.json()["results"] == fake


# ---------------------------------------------------------------------------
# POST /cleanup/queue/run  (Plan 97 — full sweep of complete/skip rows)
# ---------------------------------------------------------------------------

class TestCleanupQueueRunEndpoint:
    def test_delegates_to_run_cleanup_queue(self, mock_archiver_client, mocker):
        fake = {"total": 3, "deleted": 3, "failed": 0, "results": []}
        mocker.patch("archiver.app._run_cleanup_queue", return_value=fake)
        resp = mock_archiver_client.post("/cleanup/queue/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zeros(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._run_cleanup_queue",
            return_value={"total": 0, "deleted": 0, "failed": 0, "results": []},
        )
        resp = mock_archiver_client.post("/cleanup/queue/run")
        assert resp.json()["total"] == 0


# ---------------------------------------------------------------------------
# POST /flush/staging/run
# ---------------------------------------------------------------------------

class TestFlushStagingRunEndpoint:
    def test_delegates_to_flush_staging_events(self, mock_archiver_client, mocker):
        fake = {"total_flushed": 42, "tables": [], "error": None}
        mocker.patch("archiver.app._flush_staging_events", return_value=fake)
        resp = mock_archiver_client.post("/flush/staging/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zero_total(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_staging_events",
            return_value={"total_flushed": 0, "tables": [], "error": None},
        )
        resp = mock_archiver_client.post("/flush/staging/run")
        assert resp.json()["total_flushed"] == 0

    def test_error_propagated_in_response(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_staging_events",
            return_value={"total_flushed": 0, "tables": [], "error": "db down"},
        )
        resp = mock_archiver_client.post("/flush/staging/run")
        assert resp.status_code == 200
        assert resp.json()["error"] == "db down"


# ---------------------------------------------------------------------------
# POST /flush/silver/run
# ---------------------------------------------------------------------------

class TestFlushSilverRunEndpoint:
    def test_delegates_to_flush_silver_observations(self, mock_archiver_client, mocker):
        fake = {"flushed": 100, "error": None}
        mocker.patch("archiver.app._flush_silver_observations", return_value=fake)
        resp = mock_archiver_client.post("/flush/silver/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_no_work_returns_zero(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_silver_observations",
            return_value={"flushed": 0, "error": None},
        )
        resp = mock_archiver_client.post("/flush/silver/run")
        assert resp.json()["flushed"] == 0

    def test_error_propagated_in_response(self, mock_archiver_client, mocker):
        mocker.patch(
            "archiver.app._flush_silver_observations",
            return_value={"flushed": 0, "error": "minio unreachable"},
        )
        resp = mock_archiver_client.post("/flush/silver/run")
        assert resp.status_code == 200
        assert resp.json()["error"] == "minio unreachable"


# ---------------------------------------------------------------------------
# GET /ready
# ---------------------------------------------------------------------------

class TestReady:
    def test_ready_true_when_idle(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app.is_idle", return_value=True)
        resp = mock_archiver_client.get("/ready")
        assert resp.status_code == 200
        assert resp.json() == {"ready": True}

    def test_ready_false_when_busy(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app.is_idle", return_value=False)
        resp = mock_archiver_client.get("/ready")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ready"] is False
        assert "reason" in data
