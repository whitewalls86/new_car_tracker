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


def _read_data(tmp_path, data_path, relative_prefix):
    export_data_dir = tmp_path / data_path / relative_prefix
    return pq.read_table(str(export_data_dir)).to_pylist()


class TestMaterializeFilteredTables:
    def test_artifact_cooccurrence_does_not_leak_unrelated_rows(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset({(200, None, "LC")}),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()

        # VIN_A (vin match) + the null-vin row (exact artifact row key) = 2.
        # VIN_B (artifact co-occurrence only) and VIN_D (unrelated) excluded.
        assert result.ok
        assert result.tables["silver_observations"]["rows"] == 2
        assert result.tables["silver_observations"]["error"] is None

        written = _read_data(tmp_path, result.data_path, "silver_normalized/observations")
        listing_ids = {row["listing_id"] for row in written}
        assert listing_ids == {"LA", "LC"}
        assert "LB" not in listing_ids
        assert "LD" not in listing_ids

    def test_vin_listing_membership_filters_price_and_vin_to_listing_events(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert result.tables["price_observation_events"]["rows"] == 1
        assert result.tables["vin_to_listing_events"]["rows"] == 1

    def test_blocked_cooldown_events_filtered_by_listing_id_only(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset(), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert result.tables["blocked_cooldown_events"]["rows"] == 1

    def test_empty_cohort_writes_zero_rows_everywhere(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset(), listing_ids=frozenset(), artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert result.ok
        for name, entry in result.tables.items():
            assert entry["rows"] == 0, name
            assert entry["files"] == 0, name

    def test_missing_table_captured_as_error_not_crash(self, tmp_path):
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset(),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        assert not result.ok
        assert result.tables["silver_observations"]["error"] is not None
        assert result.tables["silver_observations"]["rows"] == 0
        assert result.data_path is None

    def test_table_error_leaves_no_generation_directory(self, tmp_path):
        """A failed table read must never leave a materialized generation
        directory behind — a caller that checks `result.ok` before
        publishing a manifest must find nothing to accidentally reference."""
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset(),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        generations_root = tmp_path / "snapshot_exports/fingerprints/testfp/generations"
        assert not generations_root.exists() or not any(generations_root.iterdir())
        assert result.generation_id is not None  # generated even on failure, for logging

    def test_successful_export_writes_success_marker(self, tmp_path):
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        generation_root = (
            tmp_path / "snapshot_exports/fingerprints/testfp/generations" / result.generation_id
        )
        assert (generation_root / "_SUCCESS").exists()

    def test_repeated_calls_produce_independent_untouched_generations(self, tmp_path):
        """Two materialize_filtered_tables calls for the same export_fingerprint
        (e.g. a refresh) must never mutate or delete each other's output —
        each writes to its own immutable generation directory. It's the
        caller's manifest update that decides which generation is "current";
        this function itself never replaces anything in place."""
        _seed_fixture_lake(tmp_path)
        con = duckdb.connect()
        try:
            first = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_A"}), listing_ids=frozenset({"LA"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
            second = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_D"}), listing_ids=frozenset({"LD"}),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()

        assert first.generation_id != second.generation_id

        first_rows = _read_data(tmp_path, first.data_path, "silver_normalized/observations")
        assert {row["listing_id"] for row in first_rows} == {"LA"}

        second_rows = _read_data(tmp_path, second.data_path, "silver_normalized/observations")
        assert {row["listing_id"] for row in second_rows} == {"LD"}

    def test_silver_observations_rows_are_deterministically_sorted(self, tmp_path):
        # Seed all four tables (via the shared fixture) so this test isolates
        # sort-order behavior rather than also exercising the table-error
        # abort path — a VIN absent from the ops tables is fine (0 rows
        # there, not an error), but a *missing table file* is a hard error.
        _seed_fixture_lake(tmp_path)
        ts_early = datetime(2026, 7, 1, tzinfo=UTC)
        ts_late = datetime(2026, 7, 5, tzinfo=UTC)
        _write_silver(tmp_path, [
            {"vin": "VIN_SORT", "listing_id": "LB", "artifact_id": 2, "fetched_at": ts_late},
            {"vin": "VIN_SORT", "listing_id": "LA", "artifact_id": 1, "fetched_at": ts_early},
        ])
        con = duckdb.connect()
        try:
            result = materialize_filtered_tables(
                con, base_path=str(tmp_path), window_start=None, window_end=None,
                vins=frozenset({"VIN_SORT"}), listing_ids=frozenset(),
                artifact_row_keys=frozenset(),
                export_fingerprint="testfp", export_prefix="snapshot_exports",
            )
        finally:
            con.close()
        written = _read_data(tmp_path, result.data_path, "silver_normalized/observations")
        assert [row["fetched_at"] for row in written] == sorted(
            row["fetched_at"] for row in written
        )
        assert written[0]["listing_id"] == "LA"
        assert written[1]["listing_id"] == "LB"
