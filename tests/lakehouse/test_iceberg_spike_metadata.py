"""
Plan 112 Gate A2: unit tests for scripts/spike_iceberg_lakehouse.py's
metadata-capture and cleanup-prefix-guard logic. No live Spark/Lakekeeper/
MinIO required -- pyspark is only imported inside `_get_spark`, never at
module import time.
"""
import pytest

from scripts.spike_iceberg_lakehouse import capture_metadata, cleanup_keys
from shared.iceberg_catalog import CATALOG_NAME, WAREHOUSE_NAME, UnsafePrefixError


class TestCaptureMetadata:
    def test_returns_all_required_keys(self):
        snapshots = [{"snapshot_id": 111}, {"snapshot_id": 222}]
        metadata = capture_metadata("spike_fixture", snapshots, row_count=10, location="s3://bronze/x")

        assert metadata["catalog"] == CATALOG_NAME
        assert metadata["table"] == f"{WAREHOUSE_NAME}.spike_fixture"
        assert metadata["current_snapshot_id"] == 222
        assert metadata["snapshots"] == [111, 222]
        assert metadata["row_count"] == 10
        assert metadata["location"] == "s3://bronze/x"

    def test_raises_on_empty_snapshots(self):
        with pytest.raises(ValueError):
            capture_metadata("spike_fixture", [], row_count=0, location="s3://bronze/x")


class TestCleanupKeys:
    """cleanup_keys() takes the table's actual, Lakekeeper-allocated prefix
    (see shared.iceberg_catalog.key_prefix_from_location) -- not a
    reconstructed <namespace>/<table_name> guess, since Lakekeeper uses
    UUID-based object paths."""

    def test_filters_to_given_prefix_only(self):
        prefix = "lakehouse_spike/warehouse/019f65f6-b861-7363-bc46-0dd926f68637/"
        all_keys = [
            f"{prefix}data/f1.parquet",
            f"{prefix}metadata/v1.metadata.json",
            "lakehouse_spike/warehouse/some-other-table-uuid/data/f2.parquet",
            "silver_normalized/observations/f3.parquet",
        ]

        matching = cleanup_keys(all_keys, prefix)

        assert matching == [f"{prefix}data/f1.parquet", f"{prefix}metadata/v1.metadata.json"]

    def test_empty_when_no_keys_match(self):
        prefix = "lakehouse_spike/warehouse/019f65f6-b861-7363-bc46-0dd926f68637/"
        assert cleanup_keys(["silver_normalized/observations/f3.parquet"], prefix) == []

    def test_raises_if_a_matching_key_somehow_escapes_the_spike_prefix(self, monkeypatch):
        # Defense in depth: even a key that happens to start with the given
        # prefix must still pass the shared spike-prefix guard.
        import scripts.spike_iceberg_lakehouse as module

        def _always_unsafe(key):
            raise UnsafePrefixError(key)

        monkeypatch.setattr(module, "require_spike_prefix", _always_unsafe)
        prefix = "lakehouse_spike/warehouse/019f65f6-b861-7363-bc46-0dd926f68637/"
        with pytest.raises(UnsafePrefixError):
            cleanup_keys([f"{prefix}data/f1.parquet"], prefix)
