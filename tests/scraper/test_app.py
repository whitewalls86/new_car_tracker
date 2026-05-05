"""Unit tests for scraper/app.py HTTP endpoints."""
import scraper.app as scraper_app

ARTIFACT_KEYS = {
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
    return {k: None for k in ARTIFACT_KEYS}


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
class TestHealth:
    def test_health_returns_ok(self, mock_scraper_client):
        resp = mock_scraper_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


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
        missing = ARTIFACT_KEYS - job["artifacts"][0].keys()
        assert missing == set(), f"Artifact missing keys: {missing}"


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
