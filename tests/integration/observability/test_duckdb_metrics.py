"""Integration tests for DuckDB mart metrics exposed via Prometheus (Plan 104, Track 2).

These tests verify that ops service correctly queries DuckDB marts and
exposes the data health metrics as Prometheus gauges.
"""
import os

import duckdb
import pytest
import requests


@pytest.mark.integration
class TestDuckDBMetricsEndpoint:
    """Verify ops /metrics endpoint exposes DuckDB data health metrics."""

    OPS_URL = "http://localhost:8060"
    DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/analytics/analytics.duckdb")

    def test_metrics_endpoint_exposes_all_duckdb_gauges(self):
        """Verify all 6 DuckDB data health metrics are present in /metrics."""
        resp = requests.get(f"{self.OPS_URL}/metrics", timeout=5)
        assert resp.status_code == 200

        expected_metrics = {
            "cartracker_observation_count_last_hour",
            "cartracker_artifact_count_last_hour",
            "cartracker_block_events_last_hour",
            "cartracker_extraction_yield_last_day",
            "cartracker_stale_listings_pct",
            "cartracker_cooldown_backlog_high",
        }

        for metric in expected_metrics:
            assert metric in resp.text, f"Metric {metric} not found in /metrics"

    def test_metrics_values_are_valid_numbers(self):
        """Verify DuckDB metrics have valid numeric values (not NaN or Inf)."""
        resp = requests.get(f"{self.OPS_URL}/metrics", timeout=5)
        assert resp.status_code == 200

        # Parse Prometheus text format to extract metric values
        lines = resp.text.split("\n")
        for line in lines:
            if line.startswith("cartracker_"):
                # Format: cartracker_metric_name VALUE
                parts = line.rsplit(maxsplit=1)
                if len(parts) == 2:
                    metric_name, value = parts
                    # Value should be a valid number (including 0.0)
                    try:
                        float_value = float(value)
                        # Verify it's not NaN or Inf
                        assert not (float_value != float_value), f"{metric_name} is NaN"
                        assert float_value != float("inf"), f"{metric_name} is Inf"
                        assert float_value != float("-inf"), f"{metric_name} is -Inf"
                    except ValueError:
                        pytest.fail(f"Metric {metric_name} has invalid value: {value}")

    def test_observation_count_metric_queries_scrape_volume(self):
        """Integration: verify observation_count metric matches DuckDB data."""
        try:
            with duckdb.connect(self.DUCKDB_PATH, read_only=True) as con:
                # Get expected value from DuckDB
                row = con.execute(
                    "SELECT COALESCE(observation_count, 0) FROM main.mart_scrape_volume "
                    "ORDER BY hour DESC LIMIT 1"
                ).fetchone()
                if row:
                    expected_value = row[0]

                    # Get metric value from Prometheus endpoint
                    resp = requests.get(f"{self.OPS_URL}/metrics", timeout=5)
                    assert resp.status_code == 200

                    # Extract the metric value
                    for line in resp.text.split("\n"):
                        if line.startswith("cartracker_observation_count_last_hour"):
                            metric_value = float(line.rsplit(maxsplit=1)[1])
                            assert metric_value == expected_value, (
                                f"Metric value {metric_value} doesn't match DuckDB "
                                f"value {expected_value}"
                            )
        except FileNotFoundError:
            pytest.skip(f"DuckDB file not found at {self.DUCKDB_PATH}")

    def test_extraction_yield_metric_queries_detail_outcomes(self):
        """Integration: verify extraction_yield metric matches DuckDB data."""
        try:
            with duckdb.connect(self.DUCKDB_PATH, read_only=True) as con:
                # Get expected value from DuckDB
                row = con.execute(
                    "SELECT COALESCE(extraction_yield, 0) FROM main.mart_detail_batch_outcomes "
                    "ORDER BY obs_date DESC LIMIT 1"
                ).fetchone()
                if row:
                    expected_value = row[0]

                    # Get metric value from Prometheus endpoint
                    resp = requests.get(f"{self.OPS_URL}/metrics", timeout=5)
                    assert resp.status_code == 200

                    # Extract the metric value
                    for line in resp.text.split("\n"):
                        if line.startswith("cartracker_extraction_yield_last_day"):
                            metric_value = float(line.rsplit(maxsplit=1)[1])
                            assert metric_value == expected_value, (
                                f"Metric value {metric_value} doesn't match DuckDB "
                                f"value {expected_value}"
                            )
        except FileNotFoundError:
            pytest.skip(f"DuckDB file not found at {self.DUCKDB_PATH}")

    def test_cooldown_backlog_metric_queries_cohorts(self):
        """Integration: verify cooldown_backlog metric matches DuckDB data."""
        try:
            with duckdb.connect(self.DUCKDB_PATH, read_only=True) as con:
                # Get expected value from DuckDB
                row = con.execute(
                    "SELECT COALESCE(listing_count, 0) FROM main.mart_cooldown_cohorts "
                    "WHERE attempt_bucket = '11+'"
                ).fetchone()
                if row:
                    expected_value = row[0]

                    # Get metric value from Prometheus endpoint
                    resp = requests.get(f"{self.OPS_URL}/metrics", timeout=5)
                    assert resp.status_code == 200

                    # Extract the metric value
                    for line in resp.text.split("\n"):
                        if line.startswith("cartracker_cooldown_backlog_high"):
                            metric_value = float(line.rsplit(maxsplit=1)[1])
                            assert metric_value == expected_value, (
                                f"Metric value {metric_value} doesn't match DuckDB "
                                f"value {expected_value}"
                            )
        except FileNotFoundError:
            pytest.skip(f"DuckDB file not found at {self.DUCKDB_PATH}")
