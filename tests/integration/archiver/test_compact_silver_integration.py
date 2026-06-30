"""
Integration tests for compact_silver.

Seeds MinIO with known Parquet data, runs compact_silver, asserts post-state.
Requires real MinIO (MINIO_ENDPOINT must be set).

Each test patches compact_silver._MINIO_PREFIX to a unique test namespace so
the processor only scans test data and cannot compact unrelated production
partitions or have its results displaced by them.
"""
import os
import uuid
from datetime import date

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

# A fixed obs date well within the 2-day watermark
_OBS_DATE = date(2026, 1, 15)


# ---------------------------------------------------------------------------
# Session-scoped clients
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


# ---------------------------------------------------------------------------
# Per-test isolated namespace
# ---------------------------------------------------------------------------

@pytest.fixture()
def ns(s3_client, monkeypatch):
    """
    Returns a dict with 'minio_prefix' and 'obs_prefix' scoped to a unique
    test run. Patches compact_silver._MINIO_PREFIX so the processor only
    scans this namespace, preventing interference with real data or other tests.
    Cleans up all objects after the test.
    """
    run_id = uuid.uuid4().hex[:8]
    minio_prefix = f"silver/test-compact-{run_id}/observations"
    obs_prefix = (
        f"{minio_prefix}/source=detail"
        f"/obs_year={_OBS_DATE.year}/obs_month={_OBS_DATE.month}/obs_day={_OBS_DATE.day}"
    )
    monkeypatch.setattr("archiver.processors.compact_silver._MINIO_PREFIX", minio_prefix)

    yield {"minio_prefix": minio_prefix, "obs_prefix": obs_prefix}

    # Teardown: delete all objects under the test prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=_BUCKET, Prefix=minio_prefix):
        for obj in page.get("Contents", []):
            s3_client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_parquet_keys(s3_client, prefix: str) -> list[str]:
    resp = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if not o["Key"].endswith(".tmp")]


def _list_all_keys(s3_client, prefix: str) -> list[str]:
    resp = s3_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", [])]


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


def _seed_part_file(test_fs, table: pa.Table, obs_prefix: str) -> str:
    """Write a part file to MinIO and return its key."""
    fname = f"part-{uuid.uuid4().hex[:8]}-0.parquet"
    key = f"{obs_prefix}/{fname}"
    path = f"{_BUCKET}/{key}"
    pq.write_table(table, path, filesystem=test_fs, compression="zstd")
    return key


# ---------------------------------------------------------------------------
# test_end_to_end_compaction
# ---------------------------------------------------------------------------

class TestEndToEndCompaction:
    def test_seeds_compact_asserts_single_file(self, ns, s3_client, test_fs):
        """Seed N part files → compact → only one compacted-*.parquet remains, row count matches."""
        obs_prefix = ns["obs_prefix"]
        _seed_part_file(test_fs, _sample_table(5, make="Ford"), obs_prefix)
        _seed_part_file(test_fs, _sample_table(3, make="Toyota"), obs_prefix)

        from archiver.processors.compact_silver import compact_silver
        result = compact_silver(max_partitions=10)

        assert result["error"] is None
        assert result["compacted"] >= 1

        keys = _list_parquet_keys(s3_client, obs_prefix)
        assert len(keys) == 1, f"Expected 1 compacted file, got {len(keys)}: {keys}"
        assert "compacted-" in keys[0] and keys[0].endswith(".parquet")

        final_table = pq.read_table(f"{_BUCKET}/{keys[0]}", filesystem=test_fs)
        assert len(final_table) == 8  # 5 + 3

        all_keys = _list_all_keys(s3_client, obs_prefix)
        assert not any(k.endswith(".tmp") for k in all_keys), f"Stale .tmp file: {all_keys}"


# ---------------------------------------------------------------------------
# test_idempotent_second_run
# ---------------------------------------------------------------------------

class TestIdempotentSecondRun:
    def test_second_run_sees_done_and_skips(self, ns, s3_client, test_fs):
        """Run twice on the same partition → second run sees Done, no re-write."""
        obs_prefix = ns["obs_prefix"]
        _seed_part_file(test_fs, _sample_table(3), obs_prefix)

        from archiver.processors.compact_silver import compact_silver

        result1 = compact_silver(max_partitions=10)
        assert result1["compacted"] >= 1

        keys_after_first = _list_parquet_keys(s3_client, obs_prefix)
        assert len(keys_after_first) == 1

        result2 = compact_silver(max_partitions=10)
        assert result2["compacted"] == 0
        assert result2["skipped"] >= 1

        keys_after_second = _list_parquet_keys(s3_client, obs_prefix)
        assert keys_after_second == keys_after_first


# ---------------------------------------------------------------------------
# test_incremental_compaction_end_to_end
# ---------------------------------------------------------------------------

class TestIncrementalCompactionEndToEnd:
    def test_late_part_files_merged_no_duplicates(self, ns, s3_client, test_fs):
        """
        Seed batch 1 → compact → seed batch 2 (simulates late flush) →
        compact again → all rows from both batches present, no duplicates.
        """
        obs_prefix = ns["obs_prefix"]
        _seed_part_file(test_fs, _sample_table(4, make="Honda"), obs_prefix)

        from archiver.processors.compact_silver import compact_silver

        result1 = compact_silver(max_partitions=10)
        assert result1["compacted"] >= 1

        # Simulate a late-arriving part file
        _seed_part_file(test_fs, _sample_table(2, make="Toyota"), obs_prefix)

        result2 = compact_silver(max_partitions=10)
        assert result2["incremental"] >= 1

        keys = _list_parquet_keys(s3_client, obs_prefix)
        assert len(keys) == 1, f"Expected exactly 1 compacted file after incremental, got {len(keys)}"

        final_table = pq.read_table(f"{_BUCKET}/{keys[0]}", filesystem=test_fs)
        assert len(final_table) == 6  # 4 + 2

        listing_ids = final_table.column("listing_id").to_pylist()
        assert len(listing_ids) == len(set(listing_ids)), "Duplicate rows after incremental compaction"


# ---------------------------------------------------------------------------
# test_tmp_file_not_visible_to_parquet_glob
# ---------------------------------------------------------------------------

class TestTmpFileNotVisibleToParquetGlob:
    def test_tmp_extension_excluded_from_parquet_glob(self, ns, s3_client, test_fs):
        """A .parquet.tmp file is not matched by a *.parquet filename filter."""
        import fnmatch

        obs_prefix = ns["obs_prefix"]

        # Manually upload a .tmp file
        tmp_key = f"{obs_prefix}/compacted-2026-01-15.parquet.tmp"
        table = _sample_table(2)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        s3_client.put_object(Bucket=_BUCKET, Key=tmp_key, Body=buf.getvalue().to_pybytes())

        # Also seed a real part file so the partition is non-empty
        _seed_part_file(test_fs, _sample_table(2), obs_prefix)

        entries = test_fs.ls(f"{_BUCKET}/{obs_prefix}", detail=False)
        parquet_matches = [e for e in entries if fnmatch.fnmatch(e.split("/")[-1], "*.parquet")]

        tmp_matches = [e for e in parquet_matches if e.endswith(".tmp")]
        assert tmp_matches == [], f".parquet.tmp appeared in *.parquet listing: {tmp_matches}"
