"""
Integration tests for ops maintenance SQL.

Validates the expire-orphan-detail-claims query against a real DB.
"""
import uuid

import pytest

from ops.queries import EXPIRE_ORPHAN_DETAIL_CLAIMS, SELECT_STUCK_PROCESSING_ARTIFACTS
from tests.sql_loader import queries

SQL = queries(__file__)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Local seed helpers
# ---------------------------------------------------------------------------

def _insert_detail_claim(cur, *, stale=False):
    listing_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    claimed_at = "now() - interval '3 hours'" if stale else "now()"
    cur.execute(
        SQL("insert_detail_scrape_claims").format(claimed_at=claimed_at),
        (listing_id, run_id),
    )
    return listing_id


# ---------------------------------------------------------------------------
# EXPIRE_ORPHAN_DETAIL_CLAIMS
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaims:
    def _run_query(self, cur):
        cur.execute(EXPIRE_ORPHAN_DETAIL_CLAIMS)
        return {str(r["listing_id"]) for r in cur.fetchall()}

    def test_stale_claim_is_deleted(self, cur):
        listing_id = _insert_detail_claim(cur, stale=True)
        assert listing_id in self._run_query(cur)

    def test_fresh_claim_is_not_deleted(self, cur):
        listing_id = _insert_detail_claim(cur, stale=False)
        assert listing_id not in self._run_query(cur)

    def test_claim_actually_removed_from_table(self, cur):
        listing_id = _insert_detail_claim(cur, stale=True)
        self._run_query(cur)
        cur.execute(SQL("select_1_from_detail_scrape_claims"), (listing_id,))
        assert cur.fetchone() is None


# ---------------------------------------------------------------------------
# SELECT_STUCK_PROCESSING_ARTIFACTS
# ---------------------------------------------------------------------------

def _insert_artifact(cur, *, status="processing", created_hours_ago=0, proc_event_hours_ago=None):
    """Insert an artifacts_queue row; optionally a 'processing' event row."""
    cur.execute(
        SQL("insert_ops_artifacts_queue").format(created_hours_ago=created_hours_ago),
        (f"s3://bronze/x/{uuid.uuid4()}.zst", status),
    )
    artifact_id = cur.fetchone()["artifact_id"]
    if proc_event_hours_ago is not None:
        cur.execute(
            SQL("insert_staging_artifacts_queue_events").format(proc_event_hours_ago=proc_event_hours_ago),
            (artifact_id,),
        )
    return artifact_id


class TestSelectStuckProcessing:
    def _run(self, cur):
        cur.execute(SELECT_STUCK_PROCESSING_ARTIFACTS)
        return {r["artifact_id"] for r in cur.fetchall()}

    def test_old_processing_with_flushed_event_is_selected(self, cur):
        # The production bug: event flushed away, so only created_at remains.
        aid = _insert_artifact(cur, created_hours_ago=48, proc_event_hours_ago=None)
        assert aid in self._run(cur)

    def test_recent_processing_not_selected(self, cur):
        aid = _insert_artifact(cur, created_hours_ago=0, proc_event_hours_ago=0)
        assert aid not in self._run(cur)

    def test_old_pending_just_claimed_not_selected(self, cur):
        # Old row (created long ago) but a fresh 'processing' event → still working.
        aid = _insert_artifact(cur, created_hours_ago=48, proc_event_hours_ago=0)
        assert aid not in self._run(cur)

    def test_non_processing_status_not_selected(self, cur):
        aid = _insert_artifact(cur, status="complete", created_hours_ago=48)
        assert aid not in self._run(cur)
