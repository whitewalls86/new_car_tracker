"""
Layer 3 — ops maintenance endpoint integration tests.

Validates the HTTP layer (routing, response shape, DB side effect) for the
expire-orphan-detail-claims endpoint. SQL logic edge cases are covered by
the Layer 1 tests in test_maintenance.py.
"""
import uuid

import pytest

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
# POST /maintenance/expire-orphan-detail-claims
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaimsApi:

    def test_stale_claim_is_deleted(self, api_client, verify_cur):
        listing_id = _insert_detail_claim(verify_cur, stale=True)

        resp = api_client.post("/maintenance/expire-orphan-detail-claims")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute(
            "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
        )
        assert verify_cur.fetchone() is None

    def test_fresh_claim_is_not_deleted(self, api_client, verify_cur):
        listing_id = _insert_detail_claim(verify_cur, stale=False)

        resp = api_client.post("/maintenance/expire-orphan-detail-claims")

        assert resp.status_code == 200
        verify_cur.execute(
            "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
        )
        assert verify_cur.fetchone() is not None

        verify_cur.execute(
            "DELETE FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
        )
