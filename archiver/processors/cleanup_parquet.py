import logging
import os
from typing import Any, Dict, List

import s3fs

from archiver.queries import GET_EXPIRED_PARQUET_MONTHS, MARK_PARQUET_DELETED
from shared.db import db_cursor

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


def run_cleanup_parquet() -> Dict[str, Any]:
    """
    Full cleanup cycle for the Airflow DAG (POST /cleanup/parquet/run).

    Queries expired months from the DB, deletes the corresponding Parquet
    partitions from MinIO, then marks those artifacts as deleted_at in the DB.
    The legacy POST /cleanup/parquet endpoint (called by n8n) remains unchanged.
    """
    with db_cursor(error_context="run_cleanup_parquet: get expired months") as cur:
        cur.execute(GET_EXPIRED_PARQUET_MONTHS)
        months = cur.fetchall()

    if not months:
        return {"total": 0, "deleted": 0, "failed": 0, "results": []}

    paths = [f"{MINIO_BUCKET}/html/year={y}/month={m}/" for y, m in months]
    results = cleanup_parquet(paths)

    with db_cursor(error_context="run_cleanup_parquet: mark deleted") as cur:
        cur.execute(MARK_PARQUET_DELETED)

    deleted_count = sum(1 for r in results if r.get("deleted"))
    return {
        "total": len(results),
        "deleted": deleted_count,
        "failed": len(results) - deleted_count,
        "results": results,
    }
