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
# POST /compact/silver/run
# ---------------------------------------------------------------------------

class TestCompactSilverRunEndpoint:
    def test_delegates_to_compact_silver(self, mock_archiver_client, mocker):
        fake = {
            "scanned": 10, "compacted": 3, "incremental": 1, "skipped": 5, "failed": 0,
            "size_before_mb": 8.0, "size_after_mb": 2.6, "error": None, "partitions": [],
        }
        mocker.patch("archiver.app._compact_silver", return_value=fake)
        resp = mock_archiver_client.post("/compact/silver/run")
        assert resp.status_code == 200
        assert resp.json() == fake

    def test_ready_returns_503_while_compact_active(self, mock_archiver_client, mocker):
        """GET /ready returns 503 while active_job() counter is non-zero."""
        mocker.patch("archiver.app.is_idle", return_value=False)
        resp = mock_archiver_client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["detail"]["ready"] is False


# ---------------------------------------------------------------------------
# POST /snapshots/adaptive-refresh/run  (Plan 120)
# ---------------------------------------------------------------------------

class TestSnapshotExportRunEndpoint:
    def test_dry_run_calls_processor_and_returns_json(self, mock_archiver_client, mocker):
        fake_result = {
            "snapshot_id": "adaptive-refresh-2026-07-07-000000",
            "tier": "ci",
            "status": "planned",
            "source_window_start": None,
            "source_window_end": None,
            "seed_vin_count": None,
            "closed_vin_count": None,
            "listing_count": None,
            "artifact_count": None,
            "archive_bytes": None,
            "manifest_key": None,
            "archive_key": None,
            "coverage_failures": [],
        }
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = fake_result
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        assert resp.status_code == 200
        assert resp.json() == fake_result
        assert mock_fn.called

    def test_missing_body_defaults_to_empty_payload(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        # dry_run=True: this test is about payload defaulting, not the
        # non-dry-run sync-cohort guard.
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"dry_run": True}
        )
        assert resp.status_code == 200
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.tier is None

    def test_invalid_tier_returns_400(self, mock_archiver_client):
        # dry_run=True: keeps this test scoped to tier validation rather than
        # the non-dry-run sync-cohort guard.
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "bogus", "dry_run": True}
        )
        assert resp.status_code == 400

    def test_non_dry_run_rejected_by_default_sync_cohort_guard(self, mock_archiver_client):
        """Gate D: a real (non-dry-run) export always runs the same heavy
        planning as build_cohort=True, so it must be blocked by the same
        production sync-cohort guard by default."""
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": False}
        )
        assert resp.status_code == 409

    def test_non_dry_run_allowed_when_override_enabled(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SYNC_SNAPSHOT_COHORT", True)
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "exported"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": False}
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_tier_defaults_flow_through_to_dry_run_result(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "edge", "dry_run": True}
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["tier"] == "edge"
        assert data["status"] == "planned"

    def test_malformed_source_window_start_returns_400(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "ci", "source_window_start": "not-a-date", "dry_run": True},
        )
        assert resp.status_code == 400

    def test_non_numeric_limit_returns_400(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "ci", "target_vins": "five-thousand", "dry_run": True},
        )
        assert resp.status_code == 400

    def test_audit_sources_forwarded_to_request_and_response(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SOURCE_BASE_PATH", True)
        fake_result = {
            "snapshot_id": "adaptive-refresh-2026-07-07-000000",
            "tier": "ci",
            "status": "audited",
            "source_audit": {"tables": {}, "window": {"start": None, "end": None},
                              "errors": [], "ok": True},
        }
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = fake_result
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True, "audit_sources": True,
                "source_base_path": "/tmp/lake",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["source_audit"]["ok"] is True
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.audit_sources is True
        assert request_arg.source_base_path == "/tmp/lake"

    def test_audit_sources_defaults_to_false(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.audit_sources is False
        assert request_arg.source_base_path is None

    def test_run_selectors_defaults_to_false(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.run_selectors is False

    def test_run_selectors_forwarded_to_request_and_response(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SOURCE_BASE_PATH", True)
        fake_result = {
            "snapshot_id": "adaptive-refresh-2026-07-07-000000",
            "tier": "ci",
            "status": "planned",
            "selector_diagnostics": {"selectors": {}, "errors": [], "ok": True},
        }
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = fake_result
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True, "run_selectors": True,
                "source_base_path": "/tmp/lake",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["selector_diagnostics"]["ok"] is True
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.run_selectors is True

    def test_source_base_path_rejected_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True, "run_selectors": True,
                "source_base_path": "/tmp/lake",
            },
        )
        assert resp.status_code == 400
        assert not mock_fn.called

    def test_source_base_path_allowed_when_flag_enabled(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SOURCE_BASE_PATH", True)
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "ci", "dry_run": True, "source_base_path": "/tmp/lake"},
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_malformed_payload_with_audit_sources_still_returns_400(self, mock_archiver_client):
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "bogus", "audit_sources": True},
        )
        assert resp.status_code == 400

    # -----------------------------------------------------------------------
    # build_cohort sync safeguard (Plan 120 Gate C.5)
    # -----------------------------------------------------------------------

    def test_build_cohort_rejected_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "edge", "dry_run": True, "run_selectors": True,
                "build_cohort": True,
            },
        )
        assert resp.status_code == 409
        assert not mock_fn.called
        assert "snapshot-worker" in resp.json()["detail"]

    def test_build_cohort_allowed_when_override_enabled(self, mock_archiver_client, mocker):
        mocker.patch("archiver.app._ALLOW_SYNC_SNAPSHOT_COHORT", True)
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "edge", "dry_run": True, "run_selectors": True,
                "build_cohort": True,
            },
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_dry_run_without_build_cohort_still_allowed_by_default(
        self, mock_archiver_client, mocker
    ):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "edge", "dry_run": True}
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_audit_sources_still_allowed_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "audited"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "edge", "audit_sources": True}
        )
        assert resp.status_code == 200
        assert mock_fn.called

    def test_build_cohort_false_not_blocked_by_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={"tier": "edge", "dry_run": True, "build_cohort": False},
        )
        assert resp.status_code == 200
        assert mock_fn.called

    # -----------------------------------------------------------------------
    # Planning cache fields (Plan 120 Gate C.75)
    # -----------------------------------------------------------------------

    def test_planning_cache_fields_default(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run", json={"tier": "ci", "dry_run": True}
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.reuse_planning_cache is False
        assert request_arg.refresh_planning_cache is False
        assert request_arg.planning_cache_bucket_grain == "week"
        assert request_arg.planning_cache_prefix == "snapshot_planning_cache"

    def test_planning_cache_fields_forwarded(self, mock_archiver_client, mocker):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        mock_fn.return_value.to_dict.return_value = {"status": "planned"}
        mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "ci", "dry_run": True,
                "reuse_planning_cache": True,
                "planning_cache_bucket_grain": "day",
                "planning_cache_prefix": "custom_prefix",
            },
        )
        request_arg = mock_fn.call_args[0][0]
        assert request_arg.reuse_planning_cache is True
        assert request_arg.refresh_planning_cache is False
        assert request_arg.planning_cache_bucket_grain == "day"
        assert request_arg.planning_cache_prefix == "custom_prefix"

    def test_build_cohort_guard_still_intact_with_planning_cache_fields(
        self, mock_archiver_client, mocker
    ):
        mock_fn = mocker.patch("archiver.app._export_ci_lake_snapshot")
        resp = mock_archiver_client.post(
            "/snapshots/adaptive-refresh/run",
            json={
                "tier": "edge", "dry_run": True, "run_selectors": True,
                "build_cohort": True, "reuse_planning_cache": True,
            },
        )
        assert resp.status_code == 409
        assert not mock_fn.called


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
        assert resp.status_code == 503
        assert resp.json()["detail"]["ready"] is False
        assert "reason" in resp.json()["detail"]
