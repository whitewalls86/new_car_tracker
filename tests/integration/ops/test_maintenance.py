"""
Integration tests for ops maintenance SQL.

Validates all five orphan-expiry queries against a real DB.
Each class exercises the business logic for one endpoint's SQL:
  - threshold-based: only records older than N minutes are affected
  - state-based: only records whose parent run is no longer active are affected

All tests run inside a rolled-back transaction — no persistent state.
"""
import uuid

import pytest

from ops.queries import (
    EXPIRE_ORPHAN_DETAIL_CLAIMS,
    EXPIRE_ORPHAN_PROCESSING_RUNS,
    EXPIRE_ORPHAN_RUNS,
    EXPIRE_ORPHAN_SCRAPE_JOBS,
    RESET_STALE_ARTIFACT_PROCESSING,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Local seed helpers
# ---------------------------------------------------------------------------

def _insert_run(cur, *, status="running", started_at_offset="0 seconds"):
    run_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO runs (run_id, status, trigger, started_at)
        VALUES (%s, %s, 'integration_test', now() - %s::interval)
        """,
        (run_id, status, started_at_offset),
    )
    return run_id


def _insert_processing_run(cur, *, status="processing", started_at_offset="0 seconds"):
    run_id = uuid.uuid4()
    cur.execute(
        """
        INSERT INTO processing_runs (run_id, status, started_at)
        VALUES (%s, %s, now() - %s::interval)
        """,
        (run_id, status, started_at_offset),
    )
    return str(run_id)


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


def _insert_detail_claim(cur, run_id, *, status="running"):
    listing_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO detail_scrape_claims (listing_id, claimed_by, status)
        VALUES (%s, %s, %s)
        """,
        (listing_id, run_id, status),
    )
    return listing_id


def _insert_scrape_job(cur, run_id, search_key, *, status="queued"):
    job_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO scrape_jobs (job_id, run_id, search_key, scope, status)
        VALUES (%s, %s, %s, 'local', %s)
        """,
        (job_id, run_id, search_key, status),
    )
    return job_id


# ---------------------------------------------------------------------------
# EXPIRE_ORPHAN_RUNS
# ---------------------------------------------------------------------------

class TestExpireOrphanRuns:
    def _run_query(self, cur, threshold=60):
        cur.execute(EXPIRE_ORPHAN_RUNS, (threshold, threshold))
        return {r["run_id"] for r in cur.fetchall()}

    def test_stale_run_is_terminated(self, cur):
        run_id = _insert_run(cur, started_at_offset="61 minutes")
        assert run_id in self._run_query(cur)

    def test_fresh_run_is_not_terminated(self, cur):
        run_id = _insert_run(cur, started_at_offset="30 minutes")
        assert run_id not in self._run_query(cur)

    def test_already_terminated_run_not_affected(self, cur):
        run_id = _insert_run(cur, status="terminated", started_at_offset="120 minutes")
        assert run_id not in self._run_query(cur)

    def test_custom_threshold_respected(self, cur):
        run_id = _insert_run(cur, started_at_offset="35 minutes")
        assert run_id in self._run_query(cur, threshold=30)
        assert run_id not in self._run_query(cur, threshold=60)

    def test_terminated_run_has_error_recorded(self, cur):
        run_id = _insert_run(cur, started_at_offset="61 minutes")
        self._run_query(cur)
        cur.execute("SELECT status, error_count, last_error FROM runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
        assert row["status"] == "terminated"
        assert row["error_count"] >= 1
        assert "timeout" in row["last_error"].lower()


# ---------------------------------------------------------------------------
# EXPIRE_ORPHAN_PROCESSING_RUNS
# ---------------------------------------------------------------------------

class TestExpireOrphanProcessingRuns:
    def _run_query(self, cur, threshold=60):
        cur.execute(EXPIRE_ORPHAN_PROCESSING_RUNS, (threshold, threshold))
        return {str(r["run_id"]) for r in cur.fetchall()}

    def test_stale_processing_run_is_terminated(self, cur):
        run_id = _insert_processing_run(cur, started_at_offset="61 minutes")
        assert run_id in self._run_query(cur)

    def test_fresh_processing_run_is_not_terminated(self, cur):
        run_id = _insert_processing_run(cur, started_at_offset="30 minutes")
        assert run_id not in self._run_query(cur)

    def test_already_terminated_not_affected(self, cur):
        run_id = _insert_processing_run(cur, status="terminated", started_at_offset="120 minutes")
        assert run_id not in self._run_query(cur)

    def test_custom_threshold_respected(self, cur):
        run_id = _insert_processing_run(cur, started_at_offset="35 minutes")
        assert run_id in self._run_query(cur, threshold=30)
        assert run_id not in self._run_query(cur, threshold=60)

    def test_terminated_run_has_error_recorded(self, cur):
        run_id = _insert_processing_run(cur, started_at_offset="61 minutes")
        self._run_query(cur)
        cur.execute(
            """
            SELECT status, error_count, last_error 
            FROM processing_runs 
            WHERE run_id = %s
            """, (run_id,)
        )
        row = cur.fetchone()
        assert row["status"] == "terminated"
        assert row["error_count"] >= 1
        assert "timeout" in row["last_error"].lower()


# ---------------------------------------------------------------------------
# RESET_STALE_ARTIFACT_PROCESSING
# ---------------------------------------------------------------------------

class TestResetStaleArtifactProcessing:
    def _run_query(self, cur, threshold=60):
        cur.execute(RESET_STALE_ARTIFACT_PROCESSING, (threshold, threshold))
        return {r["artifact_id"] for r in cur.fetchall()}

    def test_stale_processing_record_reset_to_retry(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run)
        _insert_artifact_processing(cur, artifact_id, processed_at_offset="61 minutes")
        assert artifact_id in self._run_query(cur)

    def test_fresh_processing_record_not_reset(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run)
        _insert_artifact_processing(cur, artifact_id, processed_at_offset="30 minutes")
        assert artifact_id not in self._run_query(cur)

    def test_already_ok_record_not_affected(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run)
        _insert_artifact_processing(
            cur, artifact_id, status="ok", processed_at_offset="120 minutes"
        )
        assert artifact_id not in self._run_query(cur)

    def test_reset_record_has_retry_status_and_message(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run)
        _insert_artifact_processing(cur, artifact_id, processed_at_offset="61 minutes")
        self._run_query(cur)
        cur.execute(
            """
            SELECT status, message 
            FROM artifact_processing 
            WHERE artifact_id = %s AND processor = 'srp'
            """, (artifact_id,),
        )
        row = cur.fetchone()
        assert row["status"] == "retry"
        assert "timeout" in row["message"].lower()

    def test_custom_threshold_respected(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run)
        _insert_artifact_processing(cur, artifact_id, processed_at_offset="35 minutes")
        assert artifact_id in self._run_query(cur, threshold=30)
        assert artifact_id not in self._run_query(cur, threshold=60)


# ---------------------------------------------------------------------------
# EXPIRE_ORPHAN_DETAIL_CLAIMS
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaims:
    def _run_query(self, cur):
        cur.execute(EXPIRE_ORPHAN_DETAIL_CLAIMS)
        return {r["listing_id"] for r in cur.fetchall()}

    def test_claim_with_inactive_run_is_deleted(self, cur):
        run_id = _insert_run(cur, status="terminated")
        listing_id = _insert_detail_claim(cur, run_id)
        assert listing_id in self._run_query(cur)

    def test_claim_with_active_run_is_not_deleted(self, cur):
        run_id = _insert_run(cur, status="running")
        listing_id = _insert_detail_claim(cur, run_id)
        assert listing_id not in self._run_query(cur)

    def test_non_running_claim_not_deleted(self, cur):
        run_id = _insert_run(cur, status="terminated")
        listing_id = _insert_detail_claim(cur, run_id, status="terminated")
        assert listing_id not in self._run_query(cur)

    def test_claim_actually_removed_from_table(self, cur):
        run_id = _insert_run(cur, status="terminated")
        listing_id = _insert_detail_claim(cur, run_id)
        self._run_query(cur)
        cur.execute("SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s", (listing_id,))
        assert cur.fetchone() is None


# ---------------------------------------------------------------------------
# EXPIRE_ORPHAN_SCRAPE_JOBS
# ---------------------------------------------------------------------------

class TestExpireOrphanScrapeJobs:
    def _run_query(self, cur):
        cur.execute(EXPIRE_ORPHAN_SCRAPE_JOBS)
        return {r["job_id"] for r in cur.fetchall()}

    def test_queued_job_with_inactive_run_is_failed(self, cur, seed_search_config):
        run_id = _insert_run(cur, status="terminated")
        job_id = _insert_scrape_job(cur, run_id, seed_search_config, status="queued")
        assert job_id in self._run_query(cur)

    def test_running_job_with_inactive_run_is_failed(self, cur, seed_search_config):
        run_id = _insert_run(cur, status="terminated")
        job_id = _insert_scrape_job(cur, run_id, seed_search_config, status="running")
        assert job_id in self._run_query(cur)

    def test_queued_job_with_active_run_is_not_failed(self, cur, seed_search_config):
        run_id = _insert_run(cur, status="running")
        job_id = _insert_scrape_job(cur, run_id, seed_search_config, status="queued")
        assert job_id not in self._run_query(cur)

    def test_completed_job_not_affected(self, cur, seed_search_config):
        run_id = _insert_run(cur, status="terminated")
        job_id = _insert_scrape_job(cur, run_id, seed_search_config, status="completed")
        assert job_id not in self._run_query(cur)

    def test_failed_job_has_error_message(self, cur, seed_search_config):
        run_id = _insert_run(cur, status="terminated")
        job_id = _insert_scrape_job(cur, run_id, seed_search_config, status="queued")
        self._run_query(cur)
        cur.execute("SELECT status, error FROM scrape_jobs WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
        assert row["status"] == "failed"
        assert row["error"] is not None
