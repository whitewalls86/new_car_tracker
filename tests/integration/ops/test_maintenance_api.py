"""
Layer 3 — ops maintenance endpoint integration tests.

Validates the HTTP layer (routing, response shape, DB side effect) for all five
orphan/stale expiry endpoints. SQL logic edge cases are covered by the Layer 1
tests in test_maintenance.py.

All tests seed via autocommit verify_cur so rows are visible to the TestClient's
in-process app, and clean up after themselves.
"""
import uuid

import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Local seed helpers
# ---------------------------------------------------------------------------

def _run_id():
    return str(uuid.uuid4())


def _insert_run(cur, run_id, *, status="running", started_at_offset="0 seconds"):
    cur.execute(
        """
        INSERT INTO runs (run_id, status, trigger, started_at)
        VALUES (%s, %s, 'l3_maintenance_test', now() - %s::interval)
        """,
        (run_id, status, started_at_offset),
    )


def _insert_processing_run(cur, run_id, *, status="processing", started_at_offset="0 seconds"):
    cur.execute(
        """
        INSERT INTO processing_runs (run_id, status, started_at)
        VALUES (%s, %s, now() - %s::interval)
        """,
        (run_id, status, started_at_offset),
    )


def _insert_artifact(cur, run_id):
    cur.execute(
        """
        INSERT INTO raw_artifacts (run_id, source, artifact_type, url, filepath)
        VALUES (%s, 'cars.com', 'srp', 'http://test/' || gen_random_uuid(), '/tmp/test.html')
        RETURNING artifact_id
        """,
        (run_id,),
    )
    return cur.fetchone()["artifact_id"]


def _insert_artifact_processing(
        cur, artifact_id, *, status="processing", processed_at_offset="0 seconds"
    ):
    cur.execute(
        """
        INSERT INTO artifact_processing (artifact_id, processor, status, processed_at)
        VALUES (%s, 'srp', %s, now() - %s::interval)
        """,
        (artifact_id, status, processed_at_offset),
    )


def _insert_detail_claim(cur, run_id):
    listing_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO detail_scrape_claims (listing_id, claimed_by, status)
        VALUES (%s, %s, 'running')
        """,
        (listing_id, run_id),
    )
    return listing_id


def _insert_search_config(cur, search_key):
    cur.execute(
        """
        INSERT INTO search_configs (search_key, params, enabled)
        VALUES (%s, '{"makes": ["honda"]}'::jsonb, true)
        ON CONFLICT (search_key) DO NOTHING
        """,
        (search_key,),
    )


def _insert_scrape_job(cur, run_id, search_key, *, status="queued"):
    job_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO scrape_jobs (job_id, run_id, search_key, scope, status)
        VALUES (%s, %s, %s, 'national', %s)
        """,
        (job_id, run_id, search_key, status),
    )
    return job_id


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-runs
# ---------------------------------------------------------------------------

class TestExpireOrphanRunsApi:

    def test_stale_run_is_terminated(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_run(verify_cur, run_id, status="running", started_at_offset="61 minutes")

        resp = api_client.post("/maintenance/expire-orphan-runs")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute("SELECT status FROM runs WHERE run_id = %s", (run_id,))
        assert verify_cur.fetchone()["status"] == "terminated"

        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))

    def test_fresh_run_is_not_affected(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_run(verify_cur, run_id, status="running", started_at_offset="5 minutes")

        resp = api_client.post("/maintenance/expire-orphan-runs")

        assert resp.status_code == 200
        verify_cur.execute("SELECT status FROM runs WHERE run_id = %s", (run_id,))
        assert verify_cur.fetchone()["status"] == "running"

        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-processing-runs
# ---------------------------------------------------------------------------

class TestExpireOrphanProcessingRunsApi:

    def test_stale_processing_run_is_terminated(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_processing_run(verify_cur, run_id, started_at_offset="61 minutes")

        resp = api_client.post("/maintenance/expire-orphan-processing-runs")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute("SELECT status FROM processing_runs WHERE run_id = %s", (run_id,))
        assert verify_cur.fetchone()["status"] == "terminated"

        verify_cur.execute("DELETE FROM processing_runs WHERE run_id = %s", (run_id,))

    def test_fresh_processing_run_is_not_affected(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_processing_run(verify_cur, run_id, started_at_offset="5 minutes")

        resp = api_client.post("/maintenance/expire-orphan-processing-runs")

        assert resp.status_code == 200
        verify_cur.execute("SELECT status FROM processing_runs WHERE run_id = %s", (run_id,))
        assert verify_cur.fetchone()["status"] == "processing"

        verify_cur.execute("DELETE FROM processing_runs WHERE run_id = %s", (run_id,))


# ---------------------------------------------------------------------------
# POST /maintenance/reset-stale-artifact-processing
# ---------------------------------------------------------------------------

class TestResetStaleArtifactProcessingApi:

    def test_stale_processing_record_is_reset(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_run(verify_cur, run_id)
        artifact_id = _insert_artifact(verify_cur, run_id)
        _insert_artifact_processing(verify_cur, artifact_id, processed_at_offset="61 minutes")

        resp = api_client.post("/maintenance/reset-stale-artifact-processing")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute(
            "SELECT status FROM artifact_processing WHERE artifact_id = %s AND processor = 'srp'",
            (artifact_id,),
        )
        assert verify_cur.fetchone()["status"] == "retry"

        verify_cur.execute("DELETE FROM artifact_processing WHERE artifact_id = %s", (artifact_id,))
        verify_cur.execute("DELETE FROM raw_artifacts WHERE artifact_id = %s", (artifact_id,))
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))

    def test_fresh_processing_record_is_not_reset(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_run(verify_cur, run_id)
        artifact_id = _insert_artifact(verify_cur, run_id)
        _insert_artifact_processing(verify_cur, artifact_id, processed_at_offset="5 minutes")

        resp = api_client.post("/maintenance/reset-stale-artifact-processing")

        assert resp.status_code == 200
        verify_cur.execute(
            "SELECT status FROM artifact_processing WHERE artifact_id = %s AND processor = 'srp'",
            (artifact_id,),
        )
        assert verify_cur.fetchone()["status"] == "processing"

        verify_cur.execute("DELETE FROM artifact_processing WHERE artifact_id = %s", (artifact_id,))
        verify_cur.execute("DELETE FROM raw_artifacts WHERE artifact_id = %s", (artifact_id,))
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-detail-claims
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaimsApi:

    def test_claim_with_terminated_run_is_deleted(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_run(verify_cur, run_id, status="terminated")
        listing_id = _insert_detail_claim(verify_cur, run_id)

        resp = api_client.post("/maintenance/expire-orphan-detail-claims")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute(
            "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
        )
        assert verify_cur.fetchone() is None

        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))

    def test_claim_with_active_run_is_not_deleted(self, api_client, verify_cur):
        run_id = _run_id()
        _insert_run(verify_cur, run_id, status="running")
        listing_id = _insert_detail_claim(verify_cur, run_id)

        resp = api_client.post("/maintenance/expire-orphan-detail-claims")

        assert resp.status_code == 200
        verify_cur.execute(
            "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
        )
        assert verify_cur.fetchone() is not None

        verify_cur.execute(
            "DELETE FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
        )
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-scrape-jobs
# ---------------------------------------------------------------------------

class TestExpireOrphanScrapeJobsApi:

    def test_queued_job_with_terminated_run_is_failed(self, api_client, verify_cur):
        run_id = _run_id()
        search_key = f"l3-maint-{uuid.uuid4().hex[:8]}"
        _insert_run(verify_cur, run_id, status="terminated")
        _insert_search_config(verify_cur, search_key)
        job_id = _insert_scrape_job(verify_cur, run_id, search_key, status="queued")

        resp = api_client.post("/maintenance/expire-orphan-scrape-jobs")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute("SELECT status FROM scrape_jobs WHERE job_id = %s", (job_id,))
        assert verify_cur.fetchone()["status"] == "failed"

        verify_cur.execute("DELETE FROM scrape_jobs WHERE job_id = %s", (job_id,))
        verify_cur.execute("DELETE FROM search_configs WHERE search_key = %s", (search_key,))
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))

    def test_queued_job_with_active_run_is_not_affected(self, api_client, verify_cur):
        run_id = _run_id()
        search_key = f"l3-maint-{uuid.uuid4().hex[:8]}"
        _insert_run(verify_cur, run_id, status="running")
        _insert_search_config(verify_cur, search_key)
        job_id = _insert_scrape_job(verify_cur, run_id, search_key, status="queued")

        resp = api_client.post("/maintenance/expire-orphan-scrape-jobs")

        assert resp.status_code == 200
        verify_cur.execute("SELECT status FROM scrape_jobs WHERE job_id = %s", (job_id,))
        assert verify_cur.fetchone()["status"] == "queued"

        verify_cur.execute("DELETE FROM scrape_jobs WHERE job_id = %s", (job_id,))
        verify_cur.execute("DELETE FROM search_configs WHERE search_key = %s", (search_key,))
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))
