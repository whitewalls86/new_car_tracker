"""Unit tests for ops/routers/maintenance.py — orphan expiry endpoints."""


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-detail-claims
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaims:
    def test_returns_affected_count(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [("listing-abc",), ("listing-def",)]
        resp = mock_client.post("/maintenance/expire-orphan-detail-claims")
        assert resp.status_code == 200
        assert resp.json() == {"affected": 2}

    def test_no_orphans_returns_zero(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/expire-orphan-detail-claims")
        assert resp.json() == {"affected": 0}
