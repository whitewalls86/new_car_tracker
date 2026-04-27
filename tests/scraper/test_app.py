"""Unit tests for scraper/app.py — all 11 HTTP endpoints.

Import strategy:
  mock_scraper_client fixture (from conftest.py) provides a TestClient whose
  async lifespan hook is intercepted so no real DB pool is created.
"""
from unittest.mock import AsyncMock, MagicMock, mock_open

import scraper.app as scraper_app

N8N_ARTIFACT_KEYS = {
    "source", "artifact_type", "search_key", "search_scope", "page_num",
    "url", "fetched_at", "http_status", "content_type", "content_bytes",
    "sha256", "filepath", "error",
}


# ---------------------------------------------------------------------------
# Helper: inject a pre-built job into _jobs
# ---------------------------------------------------------------------------
def _inject_job(job_id, status="queued", artifacts=None):
    scraper_app._jobs[job_id] = {
        "job_id": job_id,
        "run_id": "run-0000-0000-0000-000000000001",
        "search_key": "toyota_rav4",
        "scope": "national",
        "status": status,
        "artifacts": artifacts or [],
        "artifact_count": len(artifacts or []),
        "page_1_blocked": False,
        "attempt": 1,
        "error": None,
        "started_at": None,
    }


def _sample_artifact():
    return {k: None for k in N8N_ARTIFACT_KEYS}


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
class TestHealth:
    def test_health_returns_ok(self, mock_scraper_client):
        resp = mock_scraper_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


# ---------------------------------------------------------------------------
# GET /logs
# ---------------------------------------------------------------------------
class TestLogs:
    def test_logs_default_200_lines(self, mock_scraper_client, mocker):
        fake_lines = [f"line {i}\n" for i in range(300)]
        m = mock_open()
        m.return_value.__enter__.return_value.readlines.return_value = fake_lines
        mocker.patch("builtins.open", m)
        resp = mock_scraper_client.get("/logs")
        assert resp.status_code == 200
        assert len(resp.json()["lines"]) == 200

    def test_logs_custom_line_count(self, mock_scraper_client, mocker):
        fake_lines = [f"line {i}\n" for i in range(300)]
        m = mock_open()
        m.return_value.__enter__.return_value.readlines.return_value = fake_lines
        mocker.patch("builtins.open", m)
        resp = mock_scraper_client.get("/logs?lines=50")
        assert resp.status_code == 200
        assert len(resp.json()["lines"]) == 50

    def test_logs_file_not_found_returns_empty(self, mock_scraper_client, mocker):
        mocker.patch("builtins.open", side_effect=FileNotFoundError)
        resp = mock_scraper_client.get("/logs")
        assert resp.status_code == 200
        assert resp.json() == {"lines": []}


# ---------------------------------------------------------------------------
# POST /scrape_results
# ---------------------------------------------------------------------------
class TestPostScrapeResults:
    def test_queues_job_returns_job_id(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results?run_id=r1&search_key=sk1&scope=national",
            json={"params": {"makes": ["Toyota"], "models": ["RAV4"]}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "job_id" in data
        assert data["status"] == "queued"

    def test_job_appears_in_jobs_list(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}},
        )
        job_id = resp.json()["job_id"]
        list_resp = mock_scraper_client.get("/scrape_results/jobs")
        job_ids = [j["job_id"] for j in list_resp.json()]
        assert job_id in job_ids

    def test_job_stored_with_correct_metadata(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results?run_id=myrun&search_key=honda_crv&scope=local",
            json={"params": {}},
        )
        job_id = resp.json()["job_id"]
        assert scraper_app._jobs[job_id]["search_key"] == "honda_crv"
        assert scraper_app._jobs[job_id]["scope"] == "local"


# ---------------------------------------------------------------------------
# GET /scrape_results/jobs/completed
# ---------------------------------------------------------------------------
class TestGetCompletedJobs:
    def test_empty_when_no_jobs(self, mock_scraper_client):
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_filters_out_queued_jobs(self, mock_scraper_client):
        _inject_job("q1", status="queued")
        _inject_job("c1", status="completed")
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        ids = [j["job_id"] for j in resp.json()]
        assert "c1" in ids
        assert "q1" not in ids

    def test_includes_failed_jobs(self, mock_scraper_client):
        _inject_job("f1", status="failed")
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        ids = [j["job_id"] for j in resp.json()]
        assert "f1" in ids

    def test_artifact_schema_present(self, mock_scraper_client):
        art = _sample_artifact()
        _inject_job("c2", status="completed", artifacts=[art])
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        job = next(j for j in resp.json() if j["job_id"] == "c2")
        assert "artifacts" in job
        missing = N8N_ARTIFACT_KEYS - job["artifacts"][0].keys()
        assert missing == set(), f"Artifact missing n8n keys: {missing}"


# ---------------------------------------------------------------------------
# POST /scrape_results/jobs/{job_id}/fetched
# ---------------------------------------------------------------------------
class TestMarkJobFetched:
    def test_removes_job_returns_200(self, mock_scraper_client):
        _inject_job("j1", status="completed")
        resp = mock_scraper_client.post("/scrape_results/jobs/j1/fetched")
        assert resp.status_code == 200
        assert "j1" not in scraper_app._jobs

    def test_unknown_job_returns_404(self, mock_scraper_client):
        resp = mock_scraper_client.post("/scrape_results/jobs/nonexistent/fetched")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /scrape_results/jobs
# ---------------------------------------------------------------------------
class TestListAllJobs:
    def test_lists_all_jobs(self, mock_scraper_client):
        _inject_job("j1")
        _inject_job("j2")
        resp = mock_scraper_client.get("/scrape_results/jobs")
        assert resp.status_code == 200
        ids = [j["job_id"] for j in resp.json()]
        assert "j1" in ids and "j2" in ids

    def test_excludes_artifacts_key(self, mock_scraper_client):
        _inject_job("j1", artifacts=[_sample_artifact()])
        resp = mock_scraper_client.get("/scrape_results/jobs")
        job = next(j for j in resp.json() if j["job_id"] == "j1")
        assert "artifacts" not in job


# ---------------------------------------------------------------------------
# GET /search_configs/{search_key}/known_vins
# ---------------------------------------------------------------------------
class TestGetKnownVins:
    def test_returns_vins_from_db(self, mock_scraper_client, mocker):
        mock_pool, mock_conn = MagicMock(), AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_conn.fetch = AsyncMock(return_value=[{"vin": "VIN1"}, {"vin": "VIN2"}])
        # app.py does `from db import get_pool` so patch the name in app module
        mocker.patch("scraper.app.get_pool", new_callable=AsyncMock, return_value=mock_pool)

        resp = mock_scraper_client.get("/search_configs/toyota_rav4/known_vins")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        assert "VIN1" in data["vins"]
        assert data["search_key"] == "toyota_rav4"


# ---------------------------------------------------------------------------
# GET /ready
# ---------------------------------------------------------------------------
class TestReady:
    def test_ready_when_no_jobs(self, mock_scraper_client):
        scraper_app._jobs.clear()
        resp = mock_scraper_client.get("/ready")
        assert resp.status_code == 200
        assert resp.json() == {"ready": True, "active_jobs": 0}

    def test_not_ready_when_job_queued(self, mock_scraper_client):
        _inject_job("rdy1", status="queued")
        resp = mock_scraper_client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["detail"]["ready"] is False
        assert resp.json()["detail"]["active_jobs"] == 1

    def test_not_ready_when_job_running(self, mock_scraper_client):
        _inject_job("rdy2", status="running")
        resp = mock_scraper_client.get("/ready")
        assert resp.status_code == 503

    def test_ready_when_only_completed_jobs(self, mock_scraper_client):
        scraper_app._jobs.clear()
        _inject_job("rdy3", status="completed")
        _inject_job("rdy4", status="failed")
        resp = mock_scraper_client.get("/ready")
        assert resp.status_code == 200
        assert resp.json()["ready"] is True


# ---------------------------------------------------------------------------
# POST /process/results_pages
# ---------------------------------------------------------------------------
class TestProcessResultsPages:
    def test_neither_minio_path_nor_filepath_returns_failed(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={"processor": "cars_results_page__listings_v3", "artifact": {"artifact_id": 1}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert "minio_path" in data["message"] or "filepath" in data["message"]

    def test_file_not_found_returns_failed(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=False)
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {"artifact_id": 1, "filepath": "/data/missing.html"},
            },
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "not found" in data["message"]

    def test_minio_path_preferred_over_filepath(self, mock_scraper_client, mocker):
        """When minio_path is present the MinIO reader is used; filepath is ignored."""
        mock_read = mocker.patch(
            "shared.minio.read_html",
            return_value=b"<html></html>",
        )
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {
                    "artifact_id": 1,
                    "minio_path": "s3://bronze/html/test.html.zst",
                    "filepath": "/data/file.html",
                },
            },
        )
        mock_read.assert_called_once_with("s3://bronze/html/test.html.zst")
        assert resp.json()["status"] == "ok"

    def test_minio_read_failure_returns_failed(self, mock_scraper_client, mocker):
        mocker.patch(
            "shared.minio.read_html",
            side_effect=Exception("connection refused"),
        )
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {
                    "artifact_id": 1,
                    "minio_path": "s3://bronze/html/test.html.zst",
                },
            },
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "MinIO" in data["message"]
        assert data["meta"]["minio_path"] == "s3://bronze/html/test.html.zst"

    def test_filepath_fallback_when_no_minio_path(self, mock_scraper_client, mocker):
        """filepath is used when minio_path is absent."""
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch(
            "builtins.open", 
            __import__(
                "unittest.mock", 
                fromlist=["mock_open"]
                ).mock_open(read_data=b"<html></html>")
            )
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {"artifact_id": 1, "filepath": "/data/file.html"},
            },
        )
        assert resp.json()["status"] == "ok"

    def test_invalid_artifact_id_returns_failed(self, mock_scraper_client):
        payload = {
            "processor": "cars_results_page__listings_v3",
            "artifact": {"artifact_id": "bad"},
        }
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json=payload,
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "artifact_id" in data["message"]

    def test_v3_success(self, mock_scraper_client, mocker):
        import json as _json
        vd = _json.dumps({
            "listingId": "aaa-0000-0000-0000-000000000001",
            "make": "Toyota",
            "seller": {"zip": "77002"},
            "metadata": {"page_number": 1},
        })
        html_content = f"<spark-card data-vehicle-details='{vd}'></spark-card>"
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=html_content.encode()))
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {"artifact_id": 42, "filepath": "/data/file.html"},
            },
        )
        data = resp.json()
        assert data["status"] == "ok"
        assert data["artifact_id"] == 42
        assert isinstance(data["listings"], list)

    def test_invalid_processor_returns_failed(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html></html>"))
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v99",
                "artifact": {"artifact_id": 1, "filepath": "/data/file.html"},
            },
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "Invalid Processor" in data["meta"]["error"]

    def test_force_status_skipped(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {"artifact_id": 1, "filepath": "/x"},
                "options": {"force_status": "skipped"},
            },
        )
        data = resp.json()
        assert data["status"] == "skipped"
        assert data["listings"] == []

    def test_response_schema(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html></html>"))
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={
                "processor": "cars_results_page__listings_v3",
                "artifact": {"artifact_id": 7, "filepath": "/data/file.html"},
            },
        )
        data = resp.json()
        for key in ("artifact_id", "status", "message", "meta", "processor", "listings"):
            assert key in data, f"Response missing key: {key}"


# ---------------------------------------------------------------------------
# POST /scrape_detail
# ---------------------------------------------------------------------------
class TestScrapeDetail:
    def test_mode_dummy_calls_dummy_fn(self, mock_scraper_client, mocker):
        mock_dummy = mocker.patch(
            "scraper.app.scrape_detail_dummy",
            return_value={"error": None, "artifacts": [], "meta": {"listing_id": "x"}},
        )
        mock_scraper_client.post(
            "/scrape_detail?run_id=r1",
            json={"listing_id": "x", "mode": "dummy"},
        )
        mock_dummy.assert_called_once()

    def test_mode_fetch_calls_fetch_fn(self, mock_scraper_client, mocker):
        mock_fetch = mocker.patch(
            "scraper.app.scrape_detail_fetch",
            return_value={"error": None, "artifacts": [], "meta": {"listing_id": "x"}},
        )
        mock_scraper_client.post(
            "/scrape_detail?run_id=r1",
            json={"listing_id": "x", "mode": "fetch"},
        )
        mock_fetch.assert_called_once()

    def test_invalid_mode_returns_error(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/scrape_detail?run_id=r1",
            json={"listing_id": "x", "mode": "nonexistent"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "unsupported mode" in data["error"]

    def test_response_has_artifacts_and_meta(self, mock_scraper_client, mocker):
        mocker.patch(
            "scraper.app.scrape_detail_dummy",
            return_value={"error": None, "artifacts": [], "meta": {"listing_id": "x"}},
        )
        resp = mock_scraper_client.post(
            "/scrape_detail?run_id=r1",
            json={"listing_id": "x", "mode": "dummy"},
        )
        data = resp.json()
        assert "artifacts" in data
        assert "meta" in data


# ---------------------------------------------------------------------------
# POST /process/detail_pages
# ---------------------------------------------------------------------------
class TestProcessDetailPages:
    DETAIL_HTML = (
        '<script id="initial-activity-data" type="application/json">'
        '{"listing_id": "dd-0000-0000-0000-000000000001", "vin": "1HGCM82633A123456"}'
        "</script>"
    )

    def test_neither_minio_path_nor_filepath_returns_failed(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={"processor": "cars_detail_page__v1", "artifact": {"artifact_id": 1}},
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "minio_path" in data["message"] or "filepath" in data["message"]

    def test_file_not_found_returns_failed(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=False)
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v1",
                "artifact": {"artifact_id": 1, "filepath": "/data/nope.html"},
            },
        )
        data = resp.json()
        assert data["status"] == "failed"

    def test_minio_path_preferred_over_filepath(self, mock_scraper_client, mocker):
        mocker.patch(
            "shared.minio.read_html",
            return_value=self.DETAIL_HTML.encode(),
        )
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v1",
                "artifact": {
                    "artifact_id": 1,
                    "minio_path": "s3://bronze/html/detail.html.zst",
                    "filepath": "/data/detail.html",
                },
            },
        )
        assert resp.json()["status"] == "ok"

    def test_minio_read_failure_returns_failed(self, mock_scraper_client, mocker):
        mocker.patch(
            "shared.minio.read_html",
            side_effect=Exception("bucket not found"),
        )
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v1",
                "artifact": {
                    "artifact_id": 1,
                    "minio_path": "s3://bronze/html/detail.html.zst",
                },
            },
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "MinIO" in data["message"]

    def test_v1_success(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch(
            "builtins.open",
            mock_open(read_data=self.DETAIL_HTML.encode()),
        )
        payload = {
            "processor": "cars_detail_page__v1",
            "artifact": {
                "artifact_id": 10,
                "filepath": "/data/detail.html",
                "search_key": "sk1",
            },
        }
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json=payload,
        )
        data = resp.json()
        assert data["status"] == "ok"
        assert data["artifact_id"] == 10
        assert isinstance(data["primary"], dict)
        assert isinstance(data["carousel"], list)

    def test_invalid_processor_returns_failed(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=b"<html></html>"))
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v99",
                "artifact": {"artifact_id": 1, "filepath": "/data/x.html"},
            },
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "Invalid Processor" in data["meta"]["error"]

    def test_force_status_retry(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v1",
                "artifact": {"artifact_id": 1, "filepath": "/x"},
                "options": {"force_status": "retry"},
            },
        )
        data = resp.json()
        assert data["status"] == "retry"

    def test_response_schema(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=self.DETAIL_HTML.encode()))
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v1",
                "artifact": {"artifact_id": 5, "filepath": "/data/x.html", "search_key": "sk"},
            },
        )
        data = resp.json()
        required_keys = (
            "artifact_id",
            "status",
            "message",
            "processor",
            "search_key",
            "meta",
            "primary",
            "carousel",
        )
        for key in required_keys:
            assert key in data, f"Response missing key: {key}"


# ---------------------------------------------------------------------------
# POST /scrape_detail/batch
# ---------------------------------------------------------------------------
class TestScrapeDetailBatch:
    def test_queues_job_returns_job_id(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_detail/batch?run_id=r1",
            json={"listings": [{"listing_id": "l1"}, {"listing_id": "l2"}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "job_id" in data
        assert data["status"] == "queued"
        assert data["listing_count"] == 2

    def test_empty_listings_returns_400(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_detail/batch?run_id=r1",
            json={"listings": []},
        )
        assert resp.status_code == 400

    def test_job_stored_with_correct_metadata(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_detail/batch?run_id=myrun",
            json={"listings": [{"listing_id": "l1"}]},
        )
        job_id = resp.json()["job_id"]
        job = scraper_app._jobs[job_id]
        assert job["job_type"] == "detail_batch"
        assert job["run_id"] == "myrun"
        assert job["listing_count"] == 1
        assert job["status"] == "queued"

    def test_completed_job_surfaces_in_jobs_endpoint(self, mock_scraper_client):
        _inject_job("db1", status="completed")
        scraper_app._jobs["db1"]["job_type"] = "detail_batch"
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        ids = [j["job_id"] for j in resp.json()]
        assert "db1" in ids

    def test_job_can_be_fetched(self, mock_scraper_client):
        _inject_job("db2", status="completed")
        scraper_app._jobs["db2"]["job_type"] = "detail_batch"
        resp = mock_scraper_client.post("/scrape_results/jobs/db2/fetched")
        assert resp.status_code == 200
        assert "db2" not in scraper_app._jobs


# ---------------------------------------------------------------------------
# POST /scrape_results/retry
# ---------------------------------------------------------------------------
class TestPostScrapeResultsRetry:
    def test_queues_job_returns_job_id(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results/retry?run_id=r1&search_key=sk1&scope=national",
            json={"params": {"makes": ["Toyota"], "models": ["RAV4"]}, "attempt": 2},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "job_id" in data
        assert data["status"] == "queued"

    def test_attempt_stored_on_job(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results/retry?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}, "attempt": 2},
        )
        job_id = resp.json()["job_id"]
        assert scraper_app._jobs[job_id]["attempt"] == 2

    def test_attempt_defaults_to_1_when_absent(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results/retry?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}},
        )
        job_id = resp.json()["job_id"]
        assert scraper_app._jobs[job_id]["attempt"] == 1

    def test_page_1_blocked_initialized_false(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results/retry?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}},
        )
        job_id = resp.json()["job_id"]
        assert scraper_app._jobs[job_id]["page_1_blocked"] is False

    def test_job_appears_in_jobs_list(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results/retry?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}},
        )
        job_id = resp.json()["job_id"]
        list_resp = mock_scraper_client.get("/scrape_results/jobs")
        ids = [j["job_id"] for j in list_resp.json()]
        assert job_id in ids


# ---------------------------------------------------------------------------
# page_1_blocked + attempt on /scrape_results jobs
# ---------------------------------------------------------------------------
class TestJobFields:
    def test_scrape_results_job_has_page_1_blocked(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}},
        )
        job_id = resp.json()["job_id"]
        assert "page_1_blocked" in scraper_app._jobs[job_id]
        assert scraper_app._jobs[job_id]["page_1_blocked"] is False

    def test_scrape_results_job_has_attempt(self, mock_scraper_client, mocker):
        mocker.patch.object(scraper_app._executor, "submit")
        resp = mock_scraper_client.post(
            "/scrape_results?run_id=r1&search_key=sk1&scope=national",
            json={"params": {}, "attempt": 3},
        )
        job_id = resp.json()["job_id"]
        assert scraper_app._jobs[job_id]["attempt"] == 3

    def test_completed_job_exposes_page_1_blocked(self, mock_scraper_client):
        _inject_job("c3", status="completed")
        scraper_app._jobs["c3"]["page_1_blocked"] = True
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        job = next(j for j in resp.json() if j["job_id"] == "c3")
        assert job["page_1_blocked"] is True

    def test_completed_job_exposes_attempt(self, mock_scraper_client):
        _inject_job("c4", status="completed")
        scraper_app._jobs["c4"]["attempt"] = 2
        resp = mock_scraper_client.get("/scrape_results/jobs/completed")
        job = next(j for j in resp.json() if j["job_id"] == "c4")
        assert job["attempt"] == 2
