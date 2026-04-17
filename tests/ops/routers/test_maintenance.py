"""Unit tests for ops/routers/maintenance.py — orphan expiry endpoints."""


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-runs
# ---------------------------------------------------------------------------

class TestExpireOrphanRuns:
    def test_returns_affected_count(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [(1,), (2,)]
        resp = mock_client.post("/maintenance/expire-orphan-runs")
        assert resp.status_code == 200
        assert resp.json() == {"affected": 2}

    def test_no_orphans_returns_zero(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/expire-orphan-runs")
        assert resp.json() == {"affected": 0}

    def test_custom_threshold_accepted(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [(1,)]
        resp = mock_client.post("/maintenance/expire-orphan-runs?threshold_minutes=30")
        assert resp.status_code == 200

    def test_threshold_passed_to_query(self, mock_client, mock_cursor_context, mocker):
        mock_fn = mocker.patch(
            "ops.routers.maintenance._run_maintenance_query",
            return_value={"affected": 0},
        )
        mock_client.post("/maintenance/expire-orphan-runs?threshold_minutes=45")
        _, params = mock_fn.call_args[0]
        assert params == (45, 45)


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-processing-runs
# ---------------------------------------------------------------------------

class TestExpireOrphanProcessingRuns:
    def test_returns_affected_count(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [(10,)]
        resp = mock_client.post("/maintenance/expire-orphan-processing-runs")
        assert resp.status_code == 200
        assert resp.json() == {"affected": 1}

    def test_no_orphans_returns_zero(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/expire-orphan-processing-runs")
        assert resp.json() == {"affected": 0}

    def test_custom_threshold_accepted(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/expire-orphan-processing-runs?threshold_minutes=120")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /maintenance/reset-stale-artifact-processing
# ---------------------------------------------------------------------------

class TestResetStaleArtifactProcessing:
    def test_returns_affected_count(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [(5,), (6,), (7,)]
        resp = mock_client.post("/maintenance/reset-stale-artifact-processing")
        assert resp.status_code == 200
        assert resp.json() == {"affected": 3}

    def test_no_stale_returns_zero(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/reset-stale-artifact-processing")
        assert resp.json() == {"affected": 0}

    def test_custom_threshold_accepted(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/reset-stale-artifact-processing?threshold_minutes=90")
        assert resp.status_code == 200


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


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-scrape-jobs
# ---------------------------------------------------------------------------

class TestExpireOrphanScrapeJobs:
    def test_returns_affected_count(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [(100,), (101,), (102,)]
        resp = mock_client.post("/maintenance/expire-orphan-scrape-jobs")
        assert resp.status_code == 200
        assert resp.json() == {"affected": 3}

    def test_no_orphans_returns_zero(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/expire-orphan-scrape-jobs")
        assert resp.json() == {"affected": 0}
