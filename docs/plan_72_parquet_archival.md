# Plan 72: Parquet Archival Layer (MinIO + pyarrow + DuckDB)

## Context

Raw HTML artifacts accumulate at ~15GB/day uncompressed (50k detail pages × ~300KB), reaching ~300GB on disk despite a 15/30-day cleanup window. The Parquet archive addresses this by:

1. Compressing HTML into Parquet (zstd, ~5-8x) in MinIO before disk deletion
2. Providing a 28-day re-parse safety window at far lower storage cost
3. Enabling historical re-parsing (new parser → re-process existing HTML without re-scraping)
4. Demonstrating medallion architecture, object storage, and DuckDB for portfolio

**Architectural principle:** File management (archive + cleanup) is separated from scraping into its own `archiver` container. The scraper fetches and parses. The archiver manages what happens to files afterward.

---

## Two-Stage Artifact Lifecycle

```
Day 0      HTML fetched → saved to /data/raw/  (~300KB per file)
Day 2      Cleanup job: archive to MinIO Parquet → delete HTML from disk
Day 28     Parquet cleanup job: delete Parquet from MinIO
```

**Steady-state storage at scale (50k detail pages/day):**

| Layer | Retention | Size | Was |
|-------|-----------|------|-----|
| HTML on disk | 2 days (ok) / 7 days (retry) | ~30GB | 300GB |
| Parquet in MinIO | 28 days | ~56GB | — |
| **Total** | | **~86GB** | **300GB** |

Fits comfortably within Oracle VM's 200GB block storage (~114GB headroom) — no external cloud storage needed. MinIO is self-hosted on the same machine.

---

## Architecture

```
n8n: Cleanup Artifacts (daily 2:30 AM)
  → Get Candidates SQL                          ← updated retention windows
  → POST archiver:8001/archive/artifacts        ← NEW: HTML → MinIO Parquet
  → POST archiver:8001/cleanup/artifacts        ← MOVED from scraper
  → UPDATE raw_artifacts SET deleted_at         ← existing Postgres node

n8n: Cleanup Parquet (new, daily)
  → SELECT months from raw_artifacts WHERE deleted_at < now() - interval '28 days'
  → POST archiver:8001/cleanup/parquet          ← NEW: delete Parquet partitions from MinIO

MinIO (local, bronze bucket)
  └── html/
      └── year=2026/month=04/artifact_type=detail_page/
          └── part-{uuid}.parquet
```

DuckDB queries MinIO via S3 API. MinIO → AWS S3 / Cloudflare R2 is a credential/endpoint swap only — same code.

---

## Files to Create / Modify

| File | Change |
|------|--------|
| `archiver/app.py` | New FastAPI service |
| `archiver/processors/archive_artifacts.py` | New: reads HTML, writes Parquet to MinIO |
| `archiver/processors/cleanup_artifacts.py` | Moved verbatim from `scraper/processors/` |
| `archiver/processors/cleanup_parquet.py` | New: deletes Parquet partitions from MinIO |
| `archiver/requirements.txt` | fastapi, uvicorn, psycopg2-binary, pyarrow, s3fs |
| `archiver/Dockerfile` | Lightweight Python image |
| `docker-compose.yml` | Add `archiver` and `minio` containers + `parquet_data` volume |
| `scraper/processors/cleanup_artifacts.py` | Delete (moved to archiver) |
| `scraper/app.py` | Remove `/cleanup/artifacts` endpoint and import |
| `n8n/workflows/Cleanup Artifacts.json` | Add archive node; reroute cleanup to archiver; update retention SQL |
| `n8n/workflows/Cleanup Parquet.json` | New workflow: deletes old Parquet partitions from MinIO |

---

## Implementation

### 1. archiver/Dockerfile

```dockerfile
FROM python:3.13-slim
WORKDIR /usr/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]
```

### 2. archiver/requirements.txt

```
fastapi>=0.115
uvicorn>=0.30
psycopg2-binary>=2.9
pyarrow>=16.0
s3fs>=2024.3.1
```

### 3. docker-compose.yml additions

```yaml
minio:
  image: minio/minio:latest
  command: server /data --console-address ":9001"
  ports:
    - "9000:9000"   # S3 API
    - "9001:9001"   # MinIO web console
  volumes:
    - parquet_data:/data
  environment:
    MINIO_ROOT_USER: ${MINIO_ROOT_USER:-cartracker}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
  restart: unless-stopped

archiver:
  build: ./archiver
  volumes:
    - raw_data:/data/raw:ro          # read-only access to HTML files
  environment:
    DATABASE_URL: ${DATABASE_URL}
    MINIO_ENDPOINT: http://minio:9000
    MINIO_ROOT_USER: ${MINIO_ROOT_USER:-cartracker}
    MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    MINIO_BUCKET: bronze
  depends_on:
    - postgres
    - minio
  restart: unless-stopped
```

Add `parquet_data` to top-level `volumes:`.

### 4. archiver/processors/archive_artifacts.py

```python
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
    except Exception as e:
        logger.error("Parquet write failed: %s", e)
        for r in results:
            if r["archived"]:
                r["archived"] = False
                r["reason"] = f"parquet_write_error: {e}"

    return results
```

### 5. archiver/processors/cleanup_parquet.py

```python
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
```

### 6. archiver/processors/cleanup_artifacts.py

Moved verbatim from `scraper/processors/cleanup_artifacts.py` — no changes to the logic.

### 7. archiver/app.py

```python
from fastapi import FastAPI, Body
from typing import Any, Dict, List
import logging, os
from urllib.parse import urlparse

from processors.archive_artifacts import archive_artifacts as _archive_artifacts
from processors.cleanup_artifacts import cleanup_artifacts
from processors.cleanup_parquet import cleanup_parquet as _cleanup_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("archiver")

_DATABASE_URL = os.environ.get("DATABASE_URL", "")
if _DATABASE_URL:
    _p = urlparse(_DATABASE_URL)
    _SYNC_DB_KWARGS = {
        "host": _p.hostname or "postgres", "port": _p.port or 5432,
        "dbname": _p.path.lstrip("/") or "cartracker",
        "user": _p.username or "cartracker", "password": _p.password or "",
    }
else:
    _SYNC_DB_KWARGS = {
        "host": "postgres", "dbname": "cartracker", "user": "cartracker",
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
    }

app = FastAPI()


@app.post("/archive/artifacts")
def run_archive_artifacts(payload: dict = Body(...)) -> Dict[str, Any]:
    artifacts = (payload or {}).get("artifacts", [])
    results = _archive_artifacts(artifacts, _SYNC_DB_KWARGS)
    archived_count = sum(1 for r in results if r.get("archived"))
    return {"total": len(results), "archived": archived_count,
            "failed": len(results) - archived_count, "results": results}


@app.post("/cleanup/artifacts")
def run_cleanup_artifacts(payload: dict = Body(...)) -> Dict[str, Any]:
    artifacts = (payload or {}).get("artifacts", [])
    results = cleanup_artifacts(artifacts)
    deleted_count = sum(1 for r in results if r.get("deleted"))
    return {"total": len(results), "deleted": deleted_count,
            "failed": len(results) - deleted_count, "results": results}


@app.post("/cleanup/parquet")
def run_cleanup_parquet(payload: dict = Body(...)) -> Dict[str, Any]:
    paths = (payload or {}).get("paths", [])
    results = _cleanup_parquet(paths)
    deleted_count = sum(1 for r in results if r.get("deleted"))
    return {"total": len(results), "deleted": deleted_count,
            "failed": len(results) - deleted_count, "results": results}


@app.get("/health")
def health():
    return {"ok": True}
```

### 8. scraper/app.py — removals

- Remove `from processors.cleanup_artifacts import cleanup_artifacts`
- Remove the `POST /cleanup/artifacts` endpoint

### 9. n8n workflow changes

**Cleanup Artifacts.json — three changes:**

A. Update retention SQL in `Get Candidates1`:
```sql
-- ok: 48 hours (was 15 days)
AND ra.fetched_at < now() - interval '48 hours'
-- retry (no ok): 7 days (was 30 days)
AND ra.fetched_at < now() - interval '7 days'
```

B. Add `Call Archive API` HTTP node before `Call Cleanup API1`:
- POST `http://archiver:8001/archive/artifacts` — same body as cleanup node
- Best-effort: failure warns but does not block cleanup (preserve scraper disk hygiene)

C. Update `Call Cleanup API1` URL:
- `http://cartracker-scraper:8000/cleanup/artifacts` → `http://archiver:8001/cleanup/artifacts`

**Cleanup Parquet.json — new workflow (daily, offset from Cleanup Artifacts):**
- Postgres node: `SELECT DISTINCT date_trunc('month', fetched_at) as month FROM raw_artifacts WHERE deleted_at < now() - interval '28 days'`
- Build MinIO partition paths from result (e.g. `bronze/html/year=2026/month=03/`)
- POST `http://archiver:8001/cleanup/parquet` with the paths list

---

## Querying the Archive (DuckDB)

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='cartracker';
SET s3_secret_access_key='<MINIO_ROOT_PASSWORD>';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- 403 rate by day and artifact type
SELECT year, month, artifact_type, http_status, count(*)
FROM read_parquet('s3://bronze/html/**/*.parquet', hive_partitioning=true)
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;
```

---

## Verification

1. `docker compose build archiver && docker compose up -d archiver minio`
2. Force a small test batch eligible for cleanup (set `fetched_at` back in DB)
3. Run Cleanup Artifacts workflow manually
4. Confirm Parquet files in MinIO console (`localhost:9001`) at correct partition path
5. Verify roundtrip: DuckDB `sha256` column matches `raw_artifacts.sha256` for archived rows
6. Confirm `deleted_at` set; HTML files deleted from disk
7. Confirm `POST http://cartracker-scraper:8000/cleanup/artifacts` now returns 404
8. Run Cleanup Parquet workflow; confirm old partition directories removed from MinIO
9. Monitor disk usage over 48h — target: HTML on disk drops from ~300GB to ~30GB
