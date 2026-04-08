import logging, os
from typing import Any, Dict, List
import s3fs

logger = logging.getLogger("archiver")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "cartracker")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")


def cleanup_parquet(parquet_paths: List[str]) -> List[Dict[str, Any]]:
    """
    Delete Parquet partition directories from MinIO by path.

    parquet_paths: list of partition prefixes (e.g. "bronze/html/year=2026/month=03/")
    Returns: [{"path": str, "deleted": bool, "reason": str|None}]
    """
    fs = s3fs.S3FileSystem(
        endpoint_url=MINIO_ENDPOINT,
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        use_ssl=False,
    )
    results = []
    for path in parquet_paths:
        try:
            fs.rm(path, recursive=True)
            results.append({"path": path, "deleted": True, "reason": None})
        except FileNotFoundError:
            results.append({"path": path, "deleted": True, "reason": "already_deleted"})
        except Exception as e:
            results.append({"path": path, "deleted": False, "reason": str(e)})
    return results
