"""
Integration tests for ops maintenance SQL.

Validates the expire-orphan-detail-claims query against a real DB.
"""
import uuid

import pytest

from ops.queries import EXPIRE_ORPHAN_DETAIL_CLAIMS

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Local seed helpers
# ---------------------------------------------------------------------------

def _insert_detail_claim(cur, *, stale=False):
    listing_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    claimed_at = "now() - interval '3 hours'" if stale else "now()"
    cur.execute(
        f"""
        INSERT INTO detail_scrape_claims (listing_id, claimed_by, status, claimed_at)
        VALUES (%s, %s, 'running', {claimed_at})
        """,
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
        cur.execute("SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,))
        assert cur.fetchone() is None
