"""Unit tests for processing/app.py — health and ready endpoints.

The /process/batch and /process/artifact endpoints are tested at the
router level in test_batch_router.py.
"""


class TestHealth:
    def test_returns_ok(self, mock_processing_client):
        resp = mock_processing_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}


class TestReady:
    def test_ready_when_idle(self, mock_processing_client, mocker):
        mocker.patch("processing.app.is_idle", return_value=True)
        resp = mock_processing_client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ready"] is True
        assert "reason" not in body

    def test_not_ready_when_batch_running(self, mock_processing_client, mocker):
        mocker.patch("processing.app.is_idle", return_value=False)
        resp = mock_processing_client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ready"] is False
        assert body["reason"] == "batch in progress"
