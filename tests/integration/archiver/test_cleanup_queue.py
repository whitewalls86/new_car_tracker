"""
Integration tests for artifacts_queue cleanup SQL (Plan 97).

Validates GET_QUEUE_CLEANUP_CANDIDATES against a real DB with Flyway migrations
applied. All tests run inside a rolled-back transaction so no data persists.

Status eligibility rules:
  - complete → eligible for deletion
  - skip     → eligible for deletion
  - pending  → NOT eligible
  - processing → NOT eligible
  - retry    → NOT eligible
"""
import uuid

import pytest

from archiver.queries import GET_QUEUE_CLEANUP_CANDIDATES

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_queue_row(cur, status: str = "pending") -> int:
    """Insert an artifacts_queue row with the given status. Returns artifact_id."""
    minio_path = (
        f"s3://bronze/html/year=2026/month=4/"
        f"artifact_type=results_page/{uuid.uuid4()}.html.zst"
    )
    cur.execute(
        """INSERT INTO artifacts_queue (minio_path, artifact_type, fetched_at, status)
           VALUES (%s, 'results_page', now(), %s) RETURNING artifact_id""",
        (minio_path, status),
    )
    return cur.fetchone()["artifact_id"]


def _candidate_ids(cur) -> set:
    """Run GET_QUEUE_CLEANUP_CANDIDATES and return the artifact_id set."""
    cur.execute(GET_QUEUE_CLEANUP_CANDIDATES)
    return {row["artifact_id"] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# GET_QUEUE_CLEANUP_CANDIDATES — status eligibility
# ---------------------------------------------------------------------------

class TestGetQueueCleanupCandidates:
    def test_complete_row_is_eligible(self, cur):
        aid = _insert_queue_row(cur, status="complete")
        assert aid in _candidate_ids(cur)

    def test_skip_row_is_eligible(self, cur):
        aid = _insert_queue_row(cur, status="skip")
        assert aid in _candidate_ids(cur)

    def test_pending_row_is_not_eligible(self, cur):
        aid = _insert_queue_row(cur, status="pending")
        assert aid not in _candidate_ids(cur)

    def test_processing_row_is_not_eligible(self, cur):
        aid = _insert_queue_row(cur, status="processing")
        assert aid not in _candidate_ids(cur)

    def test_retry_row_is_not_eligible(self, cur):
        aid = _insert_queue_row(cur, status="retry")
        assert aid not in _candidate_ids(cur)

    def test_result_includes_expected_columns(self, cur):
        _insert_queue_row(cur, status="complete")
        cur.execute(GET_QUEUE_CLEANUP_CANDIDATES)
        row = cur.fetchone()
        assert row is not None
        for col in ("artifact_id", "minio_path", "artifact_type", "status"):
            assert col in row, f"GET_QUEUE_CLEANUP_CANDIDATES missing column: {col}"

    def test_multiple_statuses_mixed(self, cur):
        aid_complete = _insert_queue_row(cur, status="complete")
        aid_skip = _insert_queue_row(cur, status="skip")
        aid_pending = _insert_queue_row(cur, status="pending")
        aid_retry = _insert_queue_row(cur, status="retry")

        candidates = _candidate_ids(cur)

        assert aid_complete in candidates
        assert aid_skip in candidates
        assert aid_pending not in candidates
        assert aid_retry not in candidates


# ---------------------------------------------------------------------------
# DELETE statement — validates the DELETE logic used by cleanup_queue()
# ---------------------------------------------------------------------------

class TestQueueDeleteSql:
    """
    Tests the DELETE ... WHERE status IN ('complete', 'skip') RETURNING artifact_id
    SQL used by cleanup_queue(). Runs within the shared rolled-back transaction.
    """

    def test_delete_complete_row_returns_id(self, cur):
        aid = _insert_queue_row(cur, status="complete")
        cur.execute(
            """DELETE FROM artifacts_queue
               WHERE artifact_id = ANY(%s) AND status IN ('complete', 'skip')
               RETURNING artifact_id""",
            ([aid],),
        )
        returned = {row["artifact_id"] for row in cur.fetchall()}
        assert aid in returned

    def test_delete_skip_row_returns_id(self, cur):
        aid = _insert_queue_row(cur, status="skip")
        cur.execute(
            """DELETE FROM artifacts_queue
               WHERE artifact_id = ANY(%s) AND status IN ('complete', 'skip')
               RETURNING artifact_id""",
            ([aid],),
        )
        returned = {row["artifact_id"] for row in cur.fetchall()}
        assert aid in returned

    def test_delete_pending_row_returns_nothing(self, cur):
        aid = _insert_queue_row(cur, status="pending")
        cur.execute(
            """DELETE FROM artifacts_queue
               WHERE artifact_id = ANY(%s) AND status IN ('complete', 'skip')
               RETURNING artifact_id""",
            ([aid],),
        )
        returned = {row["artifact_id"] for row in cur.fetchall()}
        assert aid not in returned

    def test_delete_retry_row_returns_nothing(self, cur):
        aid = _insert_queue_row(cur, status="retry")
        cur.execute(
            """DELETE FROM artifacts_queue
               WHERE artifact_id = ANY(%s) AND status IN ('complete', 'skip')
               RETURNING artifact_id""",
            ([aid],),
        )
        returned = {row["artifact_id"] for row in cur.fetchall()}
        assert aid not in returned

    def test_partial_delete_mixed_statuses(self, cur):
        aid_complete = _insert_queue_row(cur, status="complete")
        aid_pending = _insert_queue_row(cur, status="pending")

        cur.execute(
            """DELETE FROM artifacts_queue
               WHERE artifact_id = ANY(%s) AND status IN ('complete', 'skip')
               RETURNING artifact_id""",
            ([aid_complete, aid_pending],),
        )
        returned = {row["artifact_id"] for row in cur.fetchall()}

        assert aid_complete in returned
        assert aid_pending not in returned
