"""Unit tests for scripts/audit_adaptive_refresh_features.py

Groups:
  A - low-level check helpers (row_count, distinct_count, duplicate_group_count,
      null_counts, min_max_timestamp, value_distribution, negative_duration_counts)
  B - audit_table() against a real tiny in-memory/on-disk DuckDB table
  C - audit_table() missing-table behavior
  D - run_audit()/main() end-to-end against a fixture DuckDB file
"""
from __future__ import annotations

import json

import duckdb
import pytest

from scripts.audit_adaptive_refresh_features import (
    TableSpec,
    audit_table,
    distinct_count,
    duplicate_group_count,
    format_markdown,
    main,
    min_max_timestamp,
    negative_duration_counts,
    null_counts,
    row_count,
    run_audit,
    value_distribution,
)


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def _make_fingerprints_table(con):
    con.execute(
        """
        CREATE TABLE int_listing_state_fingerprints (
            vin17 VARCHAR,
            listing_id VARCHAR,
            artifact_id VARCHAR,
            fetched_at TIMESTAMP,
            parsed_fingerprint VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO int_listing_state_fingerprints VALUES
            ('VIN0000000000001', 'L1', 'A1', '2026-01-01 00:00:00', 'fp1'),
            ('VIN0000000000001', 'L1', 'A2', '2026-01-02 00:00:00', 'fp2'),
            ('VIN0000000000002', 'L2', 'A3', '2026-01-03 00:00:00', 'fp3'),
            (NULL,               'L3', 'A4', '2026-01-04 00:00:00', 'fp4')
        """
    )


# ── Group A: low-level check helpers ─────────────────────────────────────────

class TestCheckHelpers:
    def test_row_count(self, con):
        _make_fingerprints_table(con)
        assert row_count(con, "int_listing_state_fingerprints") == 4

    def test_distinct_count_single_column(self, con):
        _make_fingerprints_table(con)
        # NULL vin17 counts as its own distinct group
        assert distinct_count(con, "int_listing_state_fingerprints", ["vin17"]) == 3

    def test_distinct_count_composite_key(self, con):
        _make_fingerprints_table(con)
        assert distinct_count(con, "int_listing_state_fingerprints", ["artifact_id"]) == 4

    def test_duplicate_group_count_none(self, con):
        _make_fingerprints_table(con)
        assert duplicate_group_count(con, "int_listing_state_fingerprints", ["artifact_id"]) == 0

    def test_duplicate_group_count_detects_repeats(self, con):
        _make_fingerprints_table(con)
        con.execute(
            "INSERT INTO int_listing_state_fingerprints VALUES "
            "('VIN0000000000001', 'L1', 'A1', '2026-01-05 00:00:00', 'fp5')"
        )
        assert duplicate_group_count(con, "int_listing_state_fingerprints", ["artifact_id"]) == 1

    def test_null_counts(self, con):
        _make_fingerprints_table(con)
        counts = null_counts(con, "int_listing_state_fingerprints", ["vin17", "artifact_id"])
        assert counts == {"vin17": 1, "artifact_id": 0}

    def test_min_max_timestamp(self, con):
        _make_fingerprints_table(con)
        result = min_max_timestamp(con, "int_listing_state_fingerprints", "fetched_at")
        assert result["min"] == "2026-01-01 00:00:00"
        assert result["max"] == "2026-01-04 00:00:00"

    def test_value_distribution(self, con):
        _make_fingerprints_table(con)
        dist = value_distribution(con, "int_listing_state_fingerprints", "listing_id")
        assert dist == {"L1": 2, "L2": 1, "L3": 1}

    def test_negative_duration_counts(self, con):
        con.execute("CREATE TABLE int_listing_state_runs (run_duration_hours INTEGER)")
        con.execute("INSERT INTO int_listing_state_runs VALUES (5), (-1), (0)")
        result = negative_duration_counts(con, "int_listing_state_runs", ["run_duration_hours"])
        assert result == {"run_duration_hours": 1}


# ── Group B: audit_table against a real table ────────────────────────────────

class TestAuditTablePresent:
    def test_reports_all_checks_for_present_table(self, con):
        _make_fingerprints_table(con)
        spec = TableSpec(
            name="int_listing_state_fingerprints",
            grain="artifact_id (detail-only)",
            grain_keys=["artifact_id"],
            not_null_columns=["vin17", "artifact_id"],
            timestamp_column="fetched_at",
            vin_column="vin17",
            listing_column="listing_id",
        )
        result = audit_table(con, spec)

        assert result["missing"] is False
        checks = result["checks"]
        assert checks["row_count"] == 4
        assert checks["grain_distinct_count"] == 4
        assert checks["duplicate_group_count"] == 0
        assert checks["null_counts"]["vin17"] == 1
        assert checks["timestamp_range"]["min"] == "2026-01-01 00:00:00"
        assert checks["vin_listing_coverage"]["distinct_vin_count"] == 3
        assert checks["vin_listing_coverage"]["null_vin_count"] == 1
        assert checks["vin_listing_coverage"]["distinct_listing_count"] == 3

    def test_source_distribution_included_when_configured(self, con):
        con.execute("CREATE TABLE int_listing_observation_fingerprints (source VARCHAR)")
        con.execute(
            "INSERT INTO int_listing_observation_fingerprints "
            "VALUES ('detail'), ('srp'), ('detail')"
        )
        spec = TableSpec(
            name="int_listing_observation_fingerprints",
            grain="observation_id",
            grain_keys=["source"],
            not_null_columns=[],
            source_column="source",
        )
        result = audit_table(con, spec)
        assert result["checks"]["source_distribution"] == {"detail": 2, "srp": 1}

    def test_negative_durations_included_when_configured(self, con):
        con.execute("CREATE TABLE int_listing_state_runs (run_duration_hours INTEGER)")
        con.execute("INSERT INTO int_listing_state_runs VALUES (5), (-2)")
        spec = TableSpec(
            name="int_listing_state_runs",
            grain="vin17/run",
            grain_keys=["run_duration_hours"],
            not_null_columns=[],
            duration_columns=["run_duration_hours"],
        )
        result = audit_table(con, spec)
        assert result["checks"]["negative_durations"] == {"run_duration_hours": 1}


# ── Group C: missing-table behavior ──────────────────────────────────────────

class TestAuditTableMissing:
    def test_missing_table_reports_missing_and_skipped_checks(self, con):
        spec = TableSpec(
            name="int_does_not_exist",
            grain="n/a",
            grain_keys=["id"],
            not_null_columns=["id"],
        )
        result = audit_table(con, spec)
        assert result["missing"] is True
        assert "error" in result
        assert "row_count" in result["checks_skipped"]
        assert result["checks"] == {}


# ── Group D: run_audit()/main() end-to-end ───────────────────────────────────

class TestEndToEnd:
    def test_run_audit_covers_all_specs(self, con):
        _make_fingerprints_table(con)
        results = run_audit(con)
        names = [r["table"] for r in results]
        assert "int_listing_state_fingerprints" in names
        assert "int_listing_volatility_features" in names
        # tables not created in this fixture db report missing, not raise
        missing = {r["table"]: r["missing"] for r in results}
        assert missing["int_listing_state_fingerprints"] is False
        assert missing["int_listing_volatility_features"] is True

    def test_main_writes_json_out_and_returns_nonzero_when_tables_missing(self, tmp_path):
        db_path = tmp_path / "analytics.duckdb"
        connection = duckdb.connect(str(db_path))
        _make_fingerprints_table(connection)
        connection.close()

        json_out = tmp_path / "audit.json"
        rc = main(["--db-path", str(db_path), "--json-out", str(json_out)])

        assert rc == 1  # other spec tables are missing from this fixture db
        written = json.loads(json_out.read_text())
        present = {r["table"]: r["missing"] for r in written}
        assert present["int_listing_state_fingerprints"] is False
        assert any(r["missing"] for r in written)

    def test_format_markdown_includes_not_yet_built_section(self):
        results = [
            {"table": "int_listing_state_fingerprints", "grain": "artifact_id", "missing": False,
             "checks": {"row_count": 1, "grain_distinct_count": 1, "duplicate_group_count": 0,
                        "null_counts": {}}},
        ]
        markdown = format_markdown(results)
        assert "int_listing_state_fingerprints" in markdown
        assert "mart_detail_refresh_priority" in markdown
