import logging, os, uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

logger = logging.getLogger("archiver")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "cartracker")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")

_SCHEMA = pa.schema([
    pa.field("artifact_id", pa.int64()),
    pa.field("run_id", pa.string()),
    pa.field("source", pa.string()),
    pa.field("artifact_type", pa.string()),
    pa.field("search_key", pa.string()),
    pa.field("search_scope", pa.string()),
    pa.field("url", pa.string()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("http_status", pa.int32()),
    pa.field("content_bytes", pa.int64()),
    pa.field("sha256", pa.string()),
    pa.field("error", pa.string()),
    pa.field("page_num", pa.int32()),
    pa.field("year", pa.int32()),       # hive partition col
    pa.field("month", pa.int32()),      # hive partition col
    pa.field("html", pa.large_binary()),
])


def _get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        endpoint_url=MINIO_ENDPOINT,
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        use_ssl=False,
    )


def _ensure_bucket(fs: s3fs.S3FileSystem) -> None:
    if not fs.exists(MINIO_BUCKET):
        fs.mkdir(MINIO_BUCKET)
        logger.info("Created MinIO bucket: %s", MINIO_BUCKET)


def archive_artifacts(
    artifacts: List[Dict[str, Any]],
    db_kwargs: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Archive a batch of raw HTML artifacts to MinIO as hive-partitioned Parquet.

    artifacts: [{"artifact_id": int, "filepath": str}, ...]
    db_kwargs: psycopg2 connection kwargs

    Returns: [{"artifact_id": int, "archived": bool, "reason": str|None}]
    """
    import psycopg2

    if not artifacts:
        return []

    artifact_ids = [a["artifact_id"] for a in artifacts]
    filepath_by_id = {a["artifact_id"]: a.get("filepath") for a in artifacts}

    meta_by_id: Dict[int, Dict] = {}
    try:
        conn = psycopg2.connect(**db_kwargs)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT artifact_id, run_id, source, artifact_type, search_key,
                       search_scope, url, fetched_at, http_status, content_bytes,
                       sha256, error, page_num
                FROM raw_artifacts WHERE artifact_id = ANY(%s)
            """, (artifact_ids,))
            for row in cur.fetchall():
                meta_by_id[row[0]] = {
                    "artifact_id": row[0], "run_id": str(row[1]),
                    "source": row[2], "artifact_type": row[3],
                    "search_key": row[4], "search_scope": row[5],
                    "url": row[6], "fetched_at": row[7],
                    "http_status": row[8], "content_bytes": row[9],
                    "sha256": row[10], "error": row[11], "page_num": row[12],
                }
        conn.close()
    except Exception as e:
        logger.error("DB fetch failed: %s", e)
        return [{"artifact_id": a, "archived": False, "reason": f"db_error: {e}"}
                for a in artifact_ids]

    rows = []
    results = []
    for artifact_id in artifact_ids:
        meta = meta_by_id.get(artifact_id)
        filepath = filepath_by_id.get(artifact_id)
        if not meta:
            results.append({"artifact_id": artifact_id, "archived": False, "reason": "not_found_in_db"})
            continue
        html = b""
        if filepath and os.path.exists(filepath):
            try:
                with open(filepath, "rb") as f:
                    html = f.read()
            except Exception as e:
                results.append({"artifact_id": artifact_id, "archived": False, "reason": f"read_error: {e}"})
                continue
        fetched_at = meta["fetched_at"] or datetime.now(timezone.utc)
        rows.append({**meta, "html": html, "year": fetched_at.year, "month": fetched_at.month})
        results.append({"artifact_id": artifact_id, "archived": True, "reason": None})

    if not rows:
        return results

    try:
        table = pa.Table.from_pylist(rows, schema=_SCHEMA)
        fs = _get_fs()
        _ensure_bucket(fs)
        pq.write_to_dataset(
            table,
            root_path=f"s3://{MINIO_BUCKET}/html",
            partition_cols=["year", "month", "artifact_type"],
            filesystem=fs,
            compression="zstd",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{uuid.uuid4()}-{{i}}.parquet",
        )
        logger.info("Archived %d artifacts to MinIO Parquet", len(rows))
        # Mark archived_at in DB for successfully written artifacts
        archived_ids = [r["artifact_id"] for r in results if r["archived"]]
        try:
            conn2 = psycopg2.connect(**db_kwargs)
            with conn2.cursor() as cur:
                cur.execute(
                    "UPDATE raw_artifacts SET archived_at = now() WHERE artifact_id = ANY(%s)",
                    (archived_ids,),
                )
            conn2.commit()
            conn2.close()
        except Exception as db_e:
            logger.error("archived_at update failed: %s", db_e)
    except Exception as e:
        logger.error("Parquet write failed: %s", e)
        for r in results:
            if r["archived"]:
                r["archived"] = False
                r["reason"] = f"parquet_write_error: {e}"

    return results
