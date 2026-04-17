"""
Integration tests for archiver artifact cleanup SQL.

Validates GET_CLEANUP_CANDIDATES and MARK_ARTIFACTS_DELETED against a real DB.

The three deletion eligibility rules:
  - ok + >48h       → eligible
  - skip (no ok)    → eligible immediately
  - retry (no ok) + >7d → eligible

Exclusions:
  - no processing record → not eligible
  - deleted_at already set → not eligible
  - filepath IS NULL → not eligible
  - ok but <48h → not eligible
  - retry (no ok) but <7d → not eligible
"""
from datetime import datetime, timezone

import pytest

from archiver.queries import GET_CLEANUP_CANDIDATES, MARK_ARTIFACTS_DELETED

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_artifact(cur, seed_run, *, filepath="/tmp/test.html", fetched_at_offset="0 seconds",
                     deleted_at=None):
    """Insert a raw_artifacts row. Returns artifact_id."""
    cur.execute(
        """
        INSERT INTO raw_artifacts
            (run_id, source, artifact_type, url, filepath, fetched_at, deleted_at)
        VALUES
            (%s, 'cars.com', 'srp', 'http://test/' || gen_random_uuid(),
             %s, now() - %s::interval, %s)
        RETURNING artifact_id
        """,
        (seed_run, filepath, fetched_at_offset, deleted_at),
    )
    return cur.fetchone()["artifact_id"]


_NOW = datetime.now(timezone.utc)


def _add_processing(cur, artifact_id, status, processor="srp"):
    cur.execute(
        """
        INSERT INTO artifact_processing (artifact_id, processor, status)
        VALUES (%s, %s, %s)
        """,
        (artifact_id, processor, status),
    )


# ---------------------------------------------------------------------------
# GET_CLEANUP_CANDIDATES
# ---------------------------------------------------------------------------

class TestGetCleanupCandidates:
    def _candidate_ids(self, cur):
        cur.execute(GET_CLEANUP_CANDIDATES)
        return {r["artifact_id"] for r in cur.fetchall()}

    # --- ok rule ---

    def test_ok_older_than_48h_is_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="49 hours")
        _add_processing(cur, artifact_id, "ok")
        assert artifact_id in self._candidate_ids(cur)

    def test_ok_within_48h_is_not_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="1 hour")
        _add_processing(cur, artifact_id, "ok")
        assert artifact_id not in self._candidate_ids(cur)

    # --- skip rule ---

    def test_skip_with_no_ok_is_eligible_immediately(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="0 seconds")
        _add_processing(cur, artifact_id, "skip")
        assert artifact_id in self._candidate_ids(cur)

    def test_skip_alongside_ok_defers_to_ok_rule(self, cur, seed_run):
        """When both skip and ok exist, the artifact follows the ok rule (>48h)."""
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="1 hour")
        _add_processing(cur, artifact_id, "skip", processor="srp")
        _add_processing(cur, artifact_id, "ok", processor="detail")
        # <48h so ok rule blocks it
        assert artifact_id not in self._candidate_ids(cur)

    def test_skip_alongside_ok_older_than_48h_is_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="49 hours")
        _add_processing(cur, artifact_id, "skip", processor="srp")
        _add_processing(cur, artifact_id, "ok", processor="detail")
        assert artifact_id in self._candidate_ids(cur)

    # --- retry rule ---

    def test_retry_with_no_ok_older_than_7d_is_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="8 days")
        _add_processing(cur, artifact_id, "retry")
        assert artifact_id in self._candidate_ids(cur)

    def test_retry_with_no_ok_within_7d_is_not_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="1 day")
        _add_processing(cur, artifact_id, "retry")
        assert artifact_id not in self._candidate_ids(cur)

    def test_retry_alongside_ok_defers_to_ok_rule(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="1 hour")
        _add_processing(cur, artifact_id, "retry", processor="srp")
        _add_processing(cur, artifact_id, "ok", processor="detail")
        assert artifact_id not in self._candidate_ids(cur)

    # --- exclusions ---

    def test_no_processing_record_not_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run, fetched_at_offset="10 days")
        assert artifact_id not in self._candidate_ids(cur)

    def test_already_deleted_not_eligible(self, cur, seed_run):
        artifact_id = _insert_artifact(
            cur, seed_run, fetched_at_offset="49 hours", deleted_at=_NOW
        )
        _add_processing(cur, artifact_id, "ok")
        assert artifact_id not in self._candidate_ids(cur)

    def test_result_includes_filepath(self, cur, seed_run):
        artifact_id = _insert_artifact(
            cur, seed_run, filepath="/tmp/specific.html", fetched_at_offset="49 hours"
        )
        _add_processing(cur, artifact_id, "ok")
        cur.execute(GET_CLEANUP_CANDIDATES)
        rows = {r["artifact_id"]: r for r in cur.fetchall()}
        assert rows[artifact_id]["filepath"] == "/tmp/specific.html"


# ---------------------------------------------------------------------------
# MARK_ARTIFACTS_DELETED
# ---------------------------------------------------------------------------

class TestMarkArtifactsDeleted:
    def test_marks_deleted_at_for_given_ids(self, cur, seed_run):
        a1 = _insert_artifact(cur, seed_run)
        a2 = _insert_artifact(cur, seed_run)
        cur.execute(MARK_ARTIFACTS_DELETED, ([a1, a2],))
        cur.execute(
            "SELECT artifact_id, deleted_at FROM raw_artifacts WHERE artifact_id = ANY(%s)",
            ([a1, a2],),
        )
        rows = {r["artifact_id"]: r for r in cur.fetchall()}
        assert rows[a1]["deleted_at"] is not None
        assert rows[a2]["deleted_at"] is not None

    def test_does_not_touch_other_artifacts(self, cur, seed_run):
        target = _insert_artifact(cur, seed_run)
        bystander = _insert_artifact(cur, seed_run)
        cur.execute(MARK_ARTIFACTS_DELETED, ([target],))
        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s", (bystander,)
        )
        assert cur.fetchone()["deleted_at"] is None

    def test_empty_id_list_is_no_op(self, cur, seed_run):
        artifact_id = _insert_artifact(cur, seed_run)
        cur.execute(MARK_ARTIFACTS_DELETED, ([],))
        cur.execute(
            "SELECT deleted_at FROM raw_artifacts WHERE artifact_id = %s", (artifact_id,)
        )
        assert cur.fetchone()["deleted_at"] is None
