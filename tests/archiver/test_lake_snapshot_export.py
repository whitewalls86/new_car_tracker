"""Unit tests for archiver/processors/lake_snapshot_export.py (Plan 120 Gate D).

Exercises the filter-predicate/writer logic against small local Parquet
fixtures (via DuckDB's local-fixture `base_path` mode — no MinIO required),
mirroring the "unit tests touch fixtures directly, integration tests touch
real MinIO" split used by the sibling cohort/selector test suites.

The central assertion here is the artifact fan-out non-regression: a
silver_observations row sharing an artifact_id with an in-cohort row, but
belonging to an unrelated VIN/listing, must never appear in the materialized
output — that's exactly the pollution the Plan 120 closure fix removed, and
Gate D's writer must not reintroduce it via a naive `artifact_id IN (...)`
table filter.
"""
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from archiver.processors.lake_snapshot_export import materialize_filtered_tables

UTC = timezone.utc

_SILVER_SCHEMA = pa.schema([
    pa.field("vin", pa.string()),
    pa.field("listing_id", pa.string()),
    pa.field("artifact_id", pa.int64()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
])

_PRICE_SCHEMA = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("vin", pa.string()),
    pa.field("listing_id", pa.string()),
    pa.field("artifact_id", pa.int64()),
    pa.field("event_at", pa.timestamp("us", tz="UTC")),
])

_VIN_TO_LISTING_SCHEMA = _PRICE_SCHEMA

_COOLDOWN_SCHEMA = pa.schema([
    pa.field("event_id", pa.int64()),
    pa.field("listing_id", pa.string()),
    pa.field("event_at", pa.timestamp("us", tz="UTC")),
])


def _write_silver(tmp_path, rows):
    for row in rows:
        row.setdefault("source", "detail")
        row.setdefault("obs_year", 2026)
        row.setdefault("obs_month", 7)
    table = pa.Table.from_pylist(rows, schema=pa.schema(
        list(_SILVER_SCHEMA) + [
            pa.field("source", pa.string()),
            pa.field("obs_year", pa.int32()),
            pa.field("obs_month", pa.int32()),
        ]
    ))
    pq.write_to_dataset(
        table, root_path=str(tmp_path / "silver_normalized/observations"),
        partition_cols=["source", "obs_year", "obs_month"],
    )


def _write_ops(tmp_path, relative_prefix, schema, rows):
    for row in rows:
        row.setdefault("year", 2026)
        row.setdefault("month", 7)
    table = pa.Table.from_pylist(rows, schema=pa.schema(
        list(schema) + [
            pa.field("year", pa.int32()),
            pa.field("month", pa.int32()),
        ]
    ))
    pq.write_to_dataset(
        table, root_path=str(tmp_path / relative_prefix),
        partition_cols=["year", "month"],
    )


def _seed_fixture_lake(tmp_path):
    """Seed a small local lake with:
      - VIN_A/LA (in-cohort via vin membership)
      - VIN_B/LB (co-occurs with VIN_A's artifact_id=150 in silver_observations,
        but is otherwise unrelated — must NOT appear in the export)
      - vin=None/LC (matched only via an explicit artifact_row_key, artifact_id=200)
      - VIN_D/LD (unrelated, must not appear anywhere)
    """
    ts = datetime(2026, 7, 1, tzinfo=UTC)
    _write_silver(tmp_path, [
        {"vin": "VIN_A", "listing_id": "LA", "artifact_id": 150, "fetched_at": ts},
        {"vin": "VIN_B", "listing_id": "LB", "artifact_id": 150, "fetched_at": ts},
        {"vin": None, "listing_id": "LC", "artifact_id": 200, "fetched_at": ts},
        {"vin": "VIN_D", "listing_id": "LD", "artifact_id": 300, "fetched_at": ts},
    ])
    _write_ops(tmp_path, "ops_normalized/price_observation_events", _PRICE_SCHEMA, [
        {"event_id": 1, "vin": "VIN_A", "listing_id": "LA", "artifact_id": 150, "event_at": ts},
        {"event_id": 2, "vin": "VIN_B", "listing_id": "LB", "artifact_id": 150, "event_at": ts},
    ])
    _write_ops(tmp_path, "ops_normalized/vin_to_listing_events", _VIN_TO_LISTING_SCHEMA, [
        {"event_id": 1, "vin": "VIN_A", "listing_id": "LA", "artifact_id": 150, "event_at": ts},
        {"event_id": 2, "vin": "VIN_B", "listing_id": "LB", "artifact_id": 150, "event_at": ts},
    ])
    _write_ops(tmp_path, "ops_normalized/blocked_cooldown_events", _COOLDOWN_SCHEMA, [
        {"event_id": 1, "listing_id": "LA", "event_at": ts},
        {"event_id": 2, "listing_id": "LB", "event_at": ts},
    ])


class TestMaterializeFilteredTables:
    def test_artifact_cooccurrence_does_not_leak_unrelated_rows(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            tables = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset({(200, None, "LC")}),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()

        # VIN_A (vin match) + the null-vin row (exact artifact row key) = 2.
        # VIN_B (artifact co-occurrence only) and VIN_D (unrelated) excluded.
        assert tables["silver_observations"]["rows"] == 2
        assert tables["silver_observations"]["error"] is None

        export_data_dir = (
            tmp_path / "snapshot_exports/fingerprints/testfp/data"
            / "silver_normalized/observations"
        )
        written = pq.read_table(str(export_data_dir)).to_pylist()
        listing_ids = {row["listing_id"] for row in written}
        assert listing_ids == {"LA", "LC"}
        assert "LB" not in listing_ids
        assert "LD" not in listing_ids

    def test_vin_listing_membership_filters_price_and_vin_to_listing_events(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            tables = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert tables["price_observation_events"]["rows"] == 1
        assert tables["vin_to_listing_events"]["rows"] == 1

    def test_blocked_cooldown_events_filtered_by_listing_id_only(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            tables = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset(), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert tables["blocked_cooldown_events"]["rows"] == 1

    def test_empty_cohort_writes_zero_rows_everywhere(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            tables = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset(), listing_ids=frozenset(), artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        for name, entry in tables.items():
            assert entry["rows"] == 0, name
            assert entry["files"] == 0, name

    def test_missing_table_captured_as_error_not_crash(self, tmp_path):
        con = duckdb.connect()
        try:
            tables = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset(),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert tables["silver_observations"]["error"] is not None
        assert tables["silver_observations"]["rows"] == 0
