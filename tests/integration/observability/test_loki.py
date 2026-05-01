"""Integration tests for Loki + Promtail log aggregation (Plan 104, Track 1).

Note: Loki tests run in CI. Promtail tests require docker-compose with mounted
config and log volumes, so they are marked to skip in CI environments.
"""
import time

import pytest
import requests


@pytest.mark.integration
class TestLokiHealth:
    """Verify Loki service is running and ready."""

    LOKI_URL = "http://loki:3100"

    def test_loki_ready_endpoint(self):
        """Loki /ready endpoint should return 'ready'."""
        resp = requests.get(f"{self.LOKI_URL}/ready", timeout=5)
        assert resp.status_code == 200
        assert resp.text == "ready"

    def test_loki_buildinfo(self):
        """Loki /loki/api/v1/status/buildinfo should return version."""
        resp = requests.get(f"{self.LOKI_URL}/loki/api/v1/status/buildinfo", timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        assert "version" in data
        # Expect version 2.9.x
        assert data["version"].startswith("2.9")


@pytest.mark.integration
class TestLokiLogIngestion:
    """Verify logs can be pushed to Loki and queried back."""

    LOKI_URL = "http://loki:3100"

    def test_push_and_query_log_entry(self):
        """Push a test log line and query it back with Loki API."""
        test_entry = {
            "streams": [
                {
                    "stream": {
                        "job": "integration-test",
                        "level": "INFO",
                        "logger": "test_handler",
                    },
                    "values": [
                        [
                            str(int(time.time() * 1e9)),
                            "2026-05-01 12:00:00,000 INFO test_handler: integration test log"
                        ]
                    ],
                }
            ]
        }

        # Push log entry
        resp = requests.post(
            f"{self.LOKI_URL}/loki/api/v1/push",
            json=test_entry,
            timeout=5,
        )
        assert resp.status_code == 204, f"Push failed: {resp.text}"

        # Wait for Loki to ingest
        time.sleep(0.5)

        # Query the log back
        resp = requests.get(
            f"{self.LOKI_URL}/loki/api/v1/query_range",
            params={
                "query": '{job="integration-test"}',
                "start": int((time.time() - 10) * 1e9),
                "end": int((time.time() + 10) * 1e9),
            },
            timeout=5,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert len(data["data"]["result"]) > 0

        # Verify the log entry is present
        logs = data["data"]["result"][0]["values"]
        assert any("integration test log" in log[1] for log in logs)

    def test_loki_querying_with_labels(self):
        """Verify label-based queries work (e.g., filter by level)."""
        # This is a smoke test — verify the query syntax is accepted
        resp = requests.get(
            f"{self.LOKI_URL}/loki/api/v1/query_range",
            params={
                "query": '{level="ERROR"}',
                "start": int((time.time() - 60) * 1e9),
                "end": int((time.time()) * 1e9),
            },
            timeout=5,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        # May return 0 results if no ERROR logs, that's fine
        assert isinstance(data["data"]["result"], list)
