"""
Plan 112 Gate A3: unit tests for
scripts/export_volatility_features_to_iceberg.py's metadata-capture,
source/Iceberg validation, and cleanup-prefix-guard logic. No live Spark/
Lakekeeper/MinIO/DuckDB required -- pyspark and duckdb are only imported
inside functions that need a live connection, never at module import time.
"""
import pytest

from scripts.export_volatility_features_to_iceberg import (
    capture_metadata,
    cleanup_keys,
    validate_iceberg_matches_source,
    validate_source_counts,
)
from shared.iceberg_catalog import CATALOG_NAME, WAREHOUSE_NAME, UnsafePrefixError


class TestCaptureMetadata:
    def test_returns_all_required_keys(self):
        snapshots = [{"snapshot_id": 111}, {"snapshot_id": 222}]
        metadata = capture_metadata(
            "volatility_features_snapshot",
            snapshots,
            row_count=250790,
            distinct_vin17=250790,
            max_latest_fetched_at="2026-07-14 23:00:00",
            location="s3://bronze/lakehouse_spike/warehouse/x",
        )

        assert metadata["catalog"] == CATALOG_NAME
        assert metadata["table"] == f"{WAREHOUSE_NAME}.volatility_features_snapshot"
        assert metadata["current_snapshot_id"] == 222
        assert metadata["snapshots"] == [111, 222]
        assert metadata["row_count"] == 250790
        assert metadata["distinct_vin17"] == 250790
        assert metadata["max_latest_fetched_at"] == "2026-07-14 23:00:00"
        assert metadata["location"] == "s3://bronze/lakehouse_spike/warehouse/x"

    def test_max_latest_fetched_at_none_stays_none(self):
        metadata = capture_metadata(
            "volatility_features_snapshot",
            [{"snapshot_id": 1}],
            row_count=0,
            distinct_vin17=0,
            max_latest_fetched_at=None,
            location="s3://bronze/x",
        )
        assert metadata["max_latest_fetched_at"] is None

    def test_raises_on_empty_snapshots(self):
        with pytest.raises(ValueError):
            capture_metadata(
                "volatility_features_snapshot",
                [],
                row_count=0,
                distinct_vin17=0,
                max_latest_fetched_at=None,
                location="s3://bronze/x",
            )


class TestValidateSourceCounts:
    def test_passes_when_one_row_per_vin_and_no_nulls(self):
        validate_source_counts(row_count=100, distinct_vin17=100, null_vin17_count=0)  # no raise

    def test_raises_on_null_vin17(self):
        with pytest.raises(ValueError, match="null vin17"):
            validate_source_counts(row_count=100, distinct_vin17=99, null_vin17_count=1)

    def test_raises_on_duplicate_vin17(self):
        with pytest.raises(ValueError, match="distinct vin17"):
            validate_source_counts(row_count=100, distinct_vin17=98, null_vin17_count=0)


class TestValidateIcebergMatchesSource:
    def test_passes_when_counts_match(self):
        validate_iceberg_matches_source(iceberg_row_count=250790, source_row_count=250790)

    def test_raises_when_counts_differ(self):
        with pytest.raises(ValueError, match="does not match"):
            validate_iceberg_matches_source(iceberg_row_count=250789, source_row_count=250790)


class TestCleanupKeys:
    """Reuses scripts.spike_iceberg_lakehouse.cleanup_keys -- same guard
    behavior applies here: only the table's actual, Lakekeeper-allocated
    prefix is ever deleted, never a guessed <namespace>/<table_name> path,
    and every matching key is re-verified against the shared spike-prefix
    guard as defense in depth."""

    def test_filters_to_given_prefix_only(self):
        prefix = "lakehouse_spike/warehouse/019f65f6-b861-7363-bc46-0dd926f68638/"
        all_keys = [
            f"{prefix}data/f1.parquet",
            f"{prefix}metadata/v1.metadata.json",
            "lakehouse_spike/warehouse/some-other-table-uuid/data/f2.parquet",
            "silver_normalized/observations/f3.parquet",
        ]

        matching = cleanup_keys(all_keys, prefix)

        assert matching == [f"{prefix}data/f1.parquet", f"{prefix}metadata/v1.metadata.json"]

    def test_rejects_non_spike_prefix(self, monkeypatch):
        import scripts.spike_iceberg_lakehouse as spike_module

        def _always_unsafe(key):
            raise UnsafePrefixError(key)

        monkeypatch.setattr(spike_module, "require_spike_prefix", _always_unsafe)
        prefix = "silver_normalized/observations/"
        with pytest.raises(UnsafePrefixError):
            cleanup_keys([f"{prefix}f1.parquet"], prefix)
