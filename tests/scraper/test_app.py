"""Unit tests for scraper/app.py — all 11 HTTP endpoints.

Import strategy:
  mock_scraper_client fixture (from conftest.py) provides a TestClient whose
  async lifespan hook is intercepted so no real DB pool is created.
"""
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, mock_open
import app as scraper_app

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
        mocker.patch("app.get_pool", new_callable=AsyncMock, return_value=mock_pool)

        resp = mock_scraper_client.get("/search_configs/toyota_rav4/known_vins")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        assert "VIN1" in data["vins"]
        assert data["search_key"] == "toyota_rav4"


# ---------------------------------------------------------------------------
# POST /search_configs/advance_rotation
# ---------------------------------------------------------------------------
def _make_rotation_pool(mocker, fetchrow_side_effect):
    """
    Build a mock pool/conn for advance_rotation tests.
    conn.transaction() must return a sync context manager (not a coroutine)
    because app.py uses `async with conn.transaction()`.
    """
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=None)
    mock_transaction.__aexit__ = AsyncMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.transaction.return_value = mock_transaction
    mock_conn.fetchrow = AsyncMock(side_effect=fetchrow_side_effect)
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.execute = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_pool, mock_conn


class TestAdvanceRotation:
    def test_too_soon_returns_null_slot(self, mock_scraper_client, mocker):
        import datetime
        last_run_row = MagicMock()
        last_run_row.__getitem__ = lambda s, k: datetime.datetime.now(datetime.timezone.utc)
        mock_pool, _ = _make_rotation_pool(mocker, [last_run_row])
        mocker.patch("app.get_pool", new_callable=AsyncMock, return_value=mock_pool)

        resp = mock_scraper_client.post("/search_configs/advance_rotation?min_idle_minutes=1439&min_gap_minutes=230")
        assert resp.status_code == 200
        data = resp.json()
        assert data["slot"] is None
        assert data["reason"] == "too_soon"

    def test_no_slot_due_returns_empty_configs(self, mock_scraper_client, mocker):
        # last_run=None, slot_row=None, legacy_row=None
        mock_pool, _ = _make_rotation_pool(mocker, [None, None, None])
        mocker.patch("app.get_pool", new_callable=AsyncMock, return_value=mock_pool)

        resp = mock_scraper_client.post("/search_configs/advance_rotation")
        assert resp.status_code == 200
        data = resp.json()
        assert data["slot"] is None
        assert data["configs"] == []

    def test_response_has_slot_and_configs_keys(self, mock_scraper_client, mocker):
        mock_pool, _ = _make_rotation_pool(mocker, [None, None, None])
        mocker.patch("app.get_pool", new_callable=AsyncMock, return_value=mock_pool)

        resp = mock_scraper_client.post("/search_configs/advance_rotation")
        data = resp.json()
        assert "slot" in data
        assert "configs" in data


# ---------------------------------------------------------------------------
# POST /process/results_pages
# ---------------------------------------------------------------------------
class TestProcessResultsPages:
    def test_missing_filepath_returns_failed(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={"processor": "cars_results_page__listings_v3", "artifact": {"artifact_id": 1}},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert "filepath" in data["message"]

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

    def test_invalid_artifact_id_returns_failed(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/results_pages",
            json={"processor": "cars_results_page__listings_v3", "artifact": {"artifact_id": "bad"}},
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
            "app.scrape_detail_dummy",
            return_value={"error": None, "artifacts": [], "meta": {"listing_id": "x"}},
        )
        mock_scraper_client.post(
            "/scrape_detail?run_id=r1",
            json={"listing_id": "x", "mode": "dummy"},
        )
        mock_dummy.assert_called_once()

    def test_mode_fetch_calls_fetch_fn(self, mock_scraper_client, mocker):
        mock_fetch = mocker.patch(
            "app.scrape_detail_fetch",
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
            "app.scrape_detail_dummy",
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

    def test_missing_filepath_returns_failed(self, mock_scraper_client):
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={"processor": "cars_detail_page__v1", "artifact": {"artifact_id": 1}},
        )
        data = resp.json()
        assert data["status"] == "failed"
        assert "filepath" in data["message"]

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

    def test_v1_success(self, mock_scraper_client, mocker):
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=self.DETAIL_HTML.encode()))
        resp = mock_scraper_client.post(
            "/process/detail_pages",
            json={
                "processor": "cars_detail_page__v1",
                "artifact": {"artifact_id": 10, "filepath": "/data/detail.html", "search_key": "sk1"},
            },
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
        for key in ("artifact_id", "status", "message", "processor", "search_key", "meta", "primary", "carousel"):
            assert key in data, f"Response missing key: {key}"


# ---------------------------------------------------------------------------
# POST /cleanup/artifacts
# ---------------------------------------------------------------------------
class TestCleanupArtifacts:
    def test_success_count(self, mock_scraper_client, mocker):
        mocker.patch("os.remove")
        resp = mock_scraper_client.post(
            "/cleanup/artifacts",
            json={"artifacts": [
                {"artifact_id": 1, "filepath": "/a.html"},
                {"artifact_id": 2, "filepath": "/b.html"},
            ]},
        )
        data = resp.json()
        assert data["total"] == 2
        assert data["deleted"] == 2
        assert data["failed"] == 0

    def test_partial_failure(self, mock_scraper_client, mocker):
        mocker.patch("os.remove", side_effect=[None, PermissionError("no")])
        resp = mock_scraper_client.post(
            "/cleanup/artifacts",
            json={"artifacts": [
                {"artifact_id": 1, "filepath": "/a.html"},
                {"artifact_id": 2, "filepath": "/b.html"},
            ]},
        )
        data = resp.json()
        assert data["deleted"] == 1
        assert data["failed"] == 1

    def test_empty_list(self, mock_scraper_client):
        resp = mock_scraper_client.post("/cleanup/artifacts", json={"artifacts": []})
        data = resp.json()
        assert data == {"total": 0, "deleted": 0, "failed": 0, "results": []}

    def test_response_schema(self, mock_scraper_client, mocker):
        mocker.patch("os.remove")
        resp = mock_scraper_client.post(
            "/cleanup/artifacts",
            json={"artifacts": [{"artifact_id": 99, "filepath": "/x.html"}]},
        )
        data = resp.json()
        assert "total" in data
        assert "deleted" in data
        assert "failed" in data
        assert "results" in data
        result = data["results"][0]
        assert "artifact_id" in result
        assert "deleted" in result
