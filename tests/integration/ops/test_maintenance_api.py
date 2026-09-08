"""
Layer 4 — ops maintenance endpoint integration tests.

Validates the HTTP layer (routing, response shape, DB side effect) for the
expire-orphan-detail-claims endpoint. SQL logic edge cases are covered by
test_maintenance.py beside it.
"""
import pytest

from tests.sql_loader import queries

SQL = queries(__file__)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Local seed helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-detail-claims
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaimsApi:

    def test_stale_claim_is_deleted(self, api_client, verify_cur, insert_detail_claim):
        listing_id = insert_detail_claim(verify_cur, stale=True)

        resp = api_client.post("/maintenance/expire-orphan-detail-claims")

        assert resp.status_code == 200
        assert resp.json()["affected"] >= 1

        verify_cur.execute(
            SQL("select_1_from_detail_scrape_claims"), (listing_id,)
        )
        assert verify_cur.fetchone() is None

    def test_fresh_claim_is_not_deleted(self, api_client, verify_cur, insert_detail_claim):
        listing_id = insert_detail_claim(verify_cur, stale=False)

        resp = api_client.post("/maintenance/expire-orphan-detail-claims")

        assert resp.status_code == 200
        verify_cur.execute(
            SQL("select_1_from_detail_scrape_claims"), (listing_id,)
        )
        assert verify_cur.fetchone() is not None

        verify_cur.execute(
            SQL("delete_detail_scrape_claims"), (listing_id,)
        )
