"""
Integration tests for compact_silver.

Seeds MinIO with known Parquet data, runs compact_silver, asserts post-state.
Requires real MinIO (MINIO_ENDPOINT must be set).

Skipped automatically in environments without MinIO.
"""
import os
import uuid
from datetime import date, timedelta, timezone, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import s3fs

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — MinIO not available",
    ),
]

_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")
_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "")
_ACCESS = os.environ.get("MINIO_ROOT_USER", "cartracker")
_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "")

# Use a date that is always within the 2-day watermark
_OBS_DATE = date(2026, 1, 15)
_PREFIX = (
    f"silver/observations/source=detail"
    f"/obs_year={_OBS_DATE.year}/obs_month={_OBS_DATE.month}/obs_day={_OBS_DATE.day}"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def s3_client():
    from botocore.client import Config
    return boto3.client(
        "s3",
        endpoint_url=_ENDPOINT,
        aws_access_key_id=_ACCESS,
        aws_secret_access_key=_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@pytest.fixture(scope="module")
def test_fs():
    return s3fs.S3FileSystem(
        key=_ACCESS,
        secret=_SECRET,
        use_ssl=False,
        client_kwargs={"endpoint_url": _ENDPOINT},
    )


def _list_parquet_keys(s3_client, prefix: str) -> list[str]:
    """List all keys under prefix (not matching .tmp)."""
    resp = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if not o["Key"].endswith(".tmp")]


def _list_all_keys(s3_client, prefix: str) -> list[str]:
    """List all keys under prefix including .tmp."""
    resp = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", [])]


def _delete_all_keys(s3_client, prefix: str) -> None:
    """Delete all objects under prefix (teardown helper)."""
    resp = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    for obj in resp.get("Contents", []):
        s3_client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


def _sample_table(n: int = 5, make: str = "Ford") -> pa.Table:
    return pa.table({
        "listing_id": [f"L{uuid.uuid4().hex[:6]}" for _ in range(n)],
        "make": [make] * n,
        "model": ["F-150"] * n,
        "dealer_state": ["CA"] * n,
        "dealer_name": ["Test Dealer"] * n,
        "year": pa.array([2024] * n, type=pa.int16()),
        "trim": ["XLT"] * n,
        "price": pa.array([30000 + i * 100 for i in range(n)], type=pa.int32()),
        "source": ["detail"] * n,
        "obs_year": pa.array([_OBS_DATE.year] * n, type=pa.int32()),
        "obs_month": pa.array([_OBS_DATE.month] * n, type=pa.int32()),
        "obs_day": pa.array([_OBS_DATE.day] * n, type=pa.int32()),
    })


def _seed_part_file(test_fs, table: pa.Table, suffix: str = "") -> str:
    """Write a part file to MinIO and return its key."""
    fname = f"part-{uuid.uuid4().hex[:8]}-0{suffix}.parquet"
    key = f"{_PREFIX}/{fname}"
    path = f"{_BUCKET}/{key}"
    pq.write_table(table, path, filesystem=test_fs, compression="zstd")
    return key


@pytest.fixture(autouse=True)
def _cleanup(s3_client):
    """Delete all test objects before and after each test."""
    _delete_all_keys(s3_client, _PREFIX)
    yield
    _delete_all_keys(s3_client, _PREFIX)


# ---------------------------------------------------------------------------
# test_end_to_end_compaction
# ---------------------------------------------------------------------------

class TestEndToEndCompaction:
    def test_seeds_compact_asserts_single_file(self, s3_client, test_fs):
        """Seed N part files → compact → only one compacted-*.parquet remains, row count matches."""
        table_a = _sample_table(5, make="Ford")
        table_b = _sample_table(3, make="Toyota")
        _seed_part_file(test_fs, table_a)
        _seed_part_file(test_fs, table_b)

        from archiver.processors.compact_silver import compact_silver
        result = compact_silver(max_partitions=10)

        assert result["error"] is None
        assert result["compacted"] >= 1

        keys = _list_parquet_keys(s3_client, _PREFIX)
        assert len(keys) == 1, f"Expected 1 compacted file, got {len(keys)}: {keys}"
        assert keys[0].endswith(".parquet") and "compacted-" in keys[0]

        # Row count in the compacted file matches the seeded total
        compacted_path = f"{_BUCKET}/{keys[0]}"
        final_table = pq.read_table(compacted_path, filesystem=test_fs)
        assert len(final_table) == 8  # 5 + 3

        # No .tmp files left behind
        all_keys = _list_all_keys(s3_client, _PREFIX)
        assert not any(k.endswith(".tmp") for k in all_keys), f"Stale .tmp file: {all_keys}"


# ---------------------------------------------------------------------------
# test_idempotent_second_run
# ---------------------------------------------------------------------------

class TestIdempotentSecondRun:
    def test_second_run_sees_done_and_skips(self, s3_client, test_fs):
        """Run twice on the same partition → second run sees Done, no re-write."""
        _seed_part_file(test_fs, _sample_table(3))

        from archiver.processors.compact_silver import compact_silver

        result1 = compact_silver(max_partitions=10)
        assert result1["compacted"] >= 1

        keys_after_first = _list_parquet_keys(s3_client, _PREFIX)
        assert len(keys_after_first) == 1

        result2 = compact_silver(max_partitions=10)
        assert result2["compacted"] == 0
        assert result2["skipped"] >= 1

        # File unchanged
        keys_after_second = _list_parquet_keys(s3_client, _PREFIX)
        assert keys_after_second == keys_after_first


# ---------------------------------------------------------------------------
# test_incremental_compaction_end_to_end
# ---------------------------------------------------------------------------

class TestIncrementalCompactionEndToEnd:
    def test_late_part_files_merged_no_duplicates(self, s3_client, test_fs):
        """
        Seed batch 1 → compact → seed batch 2 (simulates late flush) →
        compact again → all rows from both batches present, no duplicates.
        """
        table_batch1 = _sample_table(4, make="Honda")
        _seed_part_file(test_fs, table_batch1)

        from archiver.processors.compact_silver import compact_silver

        result1 = compact_silver(max_partitions=10)
        assert result1["compacted"] >= 1

        # Simulate a late-arriving part file landing after first compaction
        table_batch2 = _sample_table(2, make="Toyota")
        _seed_part_file(test_fs, table_batch2)

        result2 = compact_silver(max_partitions=10)
        assert result2["incremental"] >= 1

        keys = _list_parquet_keys(s3_client, _PREFIX)
        assert len(keys) == 1, f"Expected exactly 1 compacted file after incremental, got {len(keys)}"

        final_table = pq.read_table(f"{_BUCKET}/{keys[0]}", filesystem=test_fs)
        assert len(final_table) == 6  # 4 + 2

        # No duplicates: all listing_ids should be unique
        listing_ids = final_table.column("listing_id").to_pylist()
        assert len(listing_ids) == len(set(listing_ids)), "Duplicate rows detected after incremental compaction"


# ---------------------------------------------------------------------------
# test_tmp_file_not_visible_to_parquet_glob
# ---------------------------------------------------------------------------

class TestTmpFileNotVisibleToParquetGlob:
    def test_tmp_extension_excluded_from_parquet_glob(self, s3_client, test_fs):
        """A .parquet.tmp file in the partition directory is not returned by *.parquet glob."""
        # Manually upload a .tmp file to MinIO
        tmp_key = f"{_PREFIX}/compacted-2026-01-15.parquet.tmp"
        table = _sample_table(2)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        s3_client.put_object(Bucket=_BUCKET, Key=tmp_key, Body=buf.getvalue().to_pybytes())

        # Also upload a real part file so the partition is non-empty
        _seed_part_file(test_fs, _sample_table(2))

        # s3fs glob for *.parquet should NOT return the .tmp file
        import fnmatch
        all_entries = test_fs.ls(f"{_BUCKET}/{_PREFIX}", detail=False)
        parquet_entries = [e for e in all_entries if fnmatch.fnmatch(e.split("/")[-1], "*.parquet")]

        tmp_entries = [e for e in parquet_entries if e.endswith(".tmp")]
        assert tmp_entries == [], f".parquet.tmp file appeared in *.parquet listing: {tmp_entries}"
