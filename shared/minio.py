"""
Shared MinIO / S3-compatible object store helpers.

Provides two client factories (one per access pattern) and the HTML-layer
convenience functions used by the scraper and processing service:

  get_boto3_client() — singleton boto3 S3 client; thread-safe.
                       Used for raw-object get/put (bronze HTML).

  get_s3fs()         — s3fs.S3FileSystem instance.
                       Used for hive-partitioned Parquet dataset writes (silver).

Both factory functions lazy-import their library so each service only needs
to install the dependency it actually calls.  Configuration is read once from
environment variables, mirroring the pattern in shared/db.py.
"""
import logging
import os
import threading
import uuid

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENDPOINT = os.environ.get("MINIO_ENDPOINT",      "http://minio:9000")
ACCESS   = os.environ.get("MINIO_ROOT_USER",     "cartracker")
SECRET   = os.environ.get("MINIO_ROOT_PASSWORD", "")
BUCKET   = os.environ.get("MINIO_BUCKET",        "bronze")

ZSTD_LEVEL = 3  # fast compression; good enough for HTML

# ---------------------------------------------------------------------------
# boto3 singleton client (raw object get/put)
# ---------------------------------------------------------------------------

_boto3_lock   = threading.Lock()
_boto3_client = None


def get_boto3_client():
    """Return the process-wide boto3 S3 client (thread-safe singleton)."""
    global _boto3_client
    if _boto3_client is None:
        with _boto3_lock:
            if _boto3_client is None:
                import boto3
                from botocore.client import Config
                _boto3_client = boto3.client(
                    "s3",
                    endpoint_url=ENDPOINT,
                    aws_access_key_id=ACCESS,
                    aws_secret_access_key=SECRET,
                    config=Config(signature_version="s3v4"),
                    region_name="us-east-1",  # required by boto3; value ignored by MinIO
                )
    return _boto3_client


# ---------------------------------------------------------------------------
# s3fs filesystem factory (Parquet dataset writes)
# ---------------------------------------------------------------------------

def get_s3fs():
    """Return a new s3fs.S3FileSystem configured for MinIO."""
    import s3fs
    return s3fs.S3FileSystem(
        endpoint_url=ENDPOINT,
        key=ACCESS,
        secret=SECRET,
        use_ssl=False,
    )


# ---------------------------------------------------------------------------
# Bucket bootstrap (boto3 path — checked once per process)
# ---------------------------------------------------------------------------

_bucket_checked    = False
_bucket_check_lock = threading.Lock()


def ensure_bucket() -> None:
    """Ensure BUCKET exists. Uses the boto3 client. Checked at most once per process."""
    global _bucket_checked
    if _bucket_checked:
        return
    with _bucket_check_lock:
        if _bucket_checked:
            return
        from botocore.exceptions import ClientError
        client = get_boto3_client()
        try:
            client.head_bucket(Bucket=BUCKET)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code not in ("404", "NoSuchBucket"):
                raise
            client.create_bucket(Bucket=BUCKET)
            logger.info("Created MinIO bucket: %s", BUCKET)
        _bucket_checked = True


# ---------------------------------------------------------------------------
# HTML object helpers (bronze layer)
# ---------------------------------------------------------------------------

def make_key(artifact_type: str, fetched_at, file_id: str | None = None) -> str:
    """
    Build the object key (without bucket prefix).

    fetched_at: datetime or ISO string — used for the hive partition.
    file_id:    UUID string to use as the filename stem; generated if omitted.
    """
    from datetime import datetime

    if isinstance(fetched_at, str):
        fetched_at = datetime.fromisoformat(fetched_at)

    fid = file_id or str(uuid.uuid4())
    return (
        f"html/year={fetched_at.year}/month={fetched_at.month}"
        f"/artifact_type={artifact_type}/{fid}.html.zst"
    )


def write_html(key: str, content: bytes) -> str:
    """
    Compress *content* with zstd and upload to MinIO at *key*.
    Returns the full S3 URI: ``s3://<bucket>/<key>``.
    """
    import zstandard as zstd

    cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
    compressed = cctx.compress(content)

    client = get_boto3_client()
    ensure_bucket()
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=compressed,
        ContentEncoding="zstd",
        ContentType="text/html",
    )
    uri = f"s3://{BUCKET}/{key}"
    logger.debug("Uploaded %d bytes (compressed %d) → %s", len(content), len(compressed), uri)
    return uri


def read_html(minio_path: str) -> bytes:
    """
    Fetch and decompress a zstd-compressed HTML object.

    *minio_path* may be a full S3 URI (``s3://bucket/key``) or a bare key.
    Returns the raw (uncompressed) HTML bytes.
    """
    import zstandard as zstd

    if minio_path.startswith("s3://"):
        remainder = minio_path[len("s3://"):]
        bucket, key = remainder.split("/", 1)
    else:
        bucket = BUCKET
        key = minio_path

    client = get_boto3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()

    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(compressed)
