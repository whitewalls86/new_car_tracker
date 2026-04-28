"""
End-to-end test for flush_silver_observations.

Seeds staging.silver_observations in Postgres, calls the real flush function,
and verifies parquet files land in MinIO. Requires both Postgres and MinIO
services (available in CI).

Skipped if MINIO_ENDPOINT is not set (local dev without MinIO).
"""
import os
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("MINIO_ENDPOINT"),
        reason="MINIO_ENDPOINT not set — MinIO not available",
    ),
]

_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")


@pytest.fixture()
def _seed_silver_observation(db_conn_factory):
    """
    Seed a staging.silver_observations row using autocommit so the flush
    function (which opens its own connection) can see it.

    Yields the inserted row id. Cleans up on teardown.
    """
    conn = db_conn_factory()
    conn.autocommit = True
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """INSERT INTO staging.silver_observations
                   (artifact_id, listing_id, source, listing_state, fetched_at, vin,
                    price, make, model)
               VALUES (999999, %s, 'srp', 'active', '2026-01-10 12:00:00+00',
                       'E2EFLUSHTEST00001', 25000, 'Test-Make', 'Test-Model')
               RETURNING id""",
            (f"e2e-flush-{uuid.uuid4().hex[:8]}",),
        )
        row_id = cur.fetchone()["id"]

    yield row_id

    # Cleanup: delete any leftover rows (flush should have deleted, but be safe)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM staging.silver_observations WHERE id = %s", (row_id,)
        )
    conn.close()


@pytest.fixture()
def _cleanup_minio_parquet():
    """Remove test parquet files from MinIO after the test."""
    yield

    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "cartracker"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", ""),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    prefix = "silver/observations/source=srp/obs_year=2026/obs_month=1/obs_day=10/"
    resp = s3.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    for obj in resp.get("Contents", []):
        s3.delete_object(Bucket=_BUCKET, Key=obj["Key"])


class TestFlushSilverE2E:
    def test_flush_writes_parquet_and_deletes_rows(
        self, _seed_silver_observation, _cleanup_minio_parquet, db_conn_factory
    ):
        """
        Full round-trip: seed → flush → verify parquet in MinIO + rows deleted.
        """
        row_id = _seed_silver_observation

        from archiver.processors.flush_silver_observations import (
            flush_silver_observations,
        )

        result = flush_silver_observations()

        # Flush should succeed
        assert result["error"] is None, f"flush failed: {result['error']}"
        assert result["flushed"] >= 1

        # Verify parquet exists in MinIO
        import boto3
        from botocore.client import Config

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "cartracker"),
            aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", ""),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        prefix = "silver/observations/source=srp/obs_year=2026/obs_month=1/obs_day=10/"
        resp = s3.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert len(keys) >= 1, f"No parquet files found at {prefix}"
        assert any(k.endswith(".parquet") for k in keys), f"No .parquet files in {keys}"

        # Verify staging rows were deleted
        conn = db_conn_factory()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM staging.silver_observations WHERE id = %s", (row_id,)
            )
            assert cur.fetchone() is None, "Flushed row should have been deleted"
        conn.close()
