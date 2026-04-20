# Plan 97: MinIO-First Artifact Store

**Status:** Required prerequisite
**Priority:** 1 — must ship before Plan 93 and Plan 71 processing service work

## Overview

Establishes MinIO as the primary artifact store and replaces the `raw_artifacts` + `artifact_processing` two-table work queue with a single `artifacts_queue` table. This is a core architectural requirement — not an optional optimization — because the processing service (Plan 93) reads from MinIO and manages work via `artifacts_queue`.

This work was previously scoped as part of Plan 79 (multi-instance scraping, on hold). It is extracted here because it is required regardless of whether multi-instance deployment ever happens.

---

## What Changes

### Scraper
Currently writes raw HTML to local disk and inserts a `raw_artifacts` row with a `filepath` column. After this plan:
- Writes HTML directly to MinIO at `bronze/html/year=.../month=.../artifact_type=.../`
- Inserts `artifacts_queue` row with `minio_path` (S3 URI) instead of a local `filepath`
- No local disk writes

### Archiver
Currently archives local HTML files to MinIO bronze, then deletes local files. After this plan:
- HTML is already in MinIO — no archiving step needed for new artifacts
- Archiver cleans up `artifacts_queue` rows where `status IN ('complete', 'skip')`, leaving `retry` rows in place until resolved

### Processing Service (bridging step)
The existing processing service reads `raw_artifacts.filepath` from disk. As a bridging step before the full Plan 93 rewrite, update it to read `artifacts_queue.minio_path` from MinIO. This confirms the end-to-end MinIO read path before the larger rewrite lands.

---

## Schema

### New: `artifacts_queue`

```sql
CREATE TABLE artifacts_queue (
    artifact_id   bigserial    PRIMARY KEY,
    minio_path    text         NOT NULL,
    artifact_type text         NOT NULL,  -- 'results_page' | 'detail_page'
    listing_id    text,
    run_id        bigint,
    fetched_at    timestamptz  NOT NULL,
    status        text         NOT NULL DEFAULT 'pending',
    created_at    timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX ON artifacts_queue (status)
    WHERE status IN ('pending', 'retry');
```

`status` values: `pending` | `processing` | `complete` | `retry` | `skip`

### Deprecated: `raw_artifacts`, `artifact_processing`
Both tables stay in place during the transition. Writes to them stop when the scraper is updated. They are dropped in Plan 90 after the processing service is fully migrated and shadow period is complete.

---

## MinIO Path Convention

```
bronze/
  html/
    year=2026/month=4/artifact_type=detail_page/
      {artifact_id}_{listing_id}.html.zst
    year=2026/month=4/artifact_type=results_page/
      {artifact_id}_{run_id}.html.zst
```

Compression at write time (zstd) keeps storage costs low. The processing service decompresses on read.

---

## Rollout Order

1. **Flyway migration** — create `artifacts_queue` with indexes
2. **Scraper update** — write HTML to MinIO; insert `artifacts_queue` row; keep `raw_artifacts` write in parallel for the shadow period
3. **Validate** — confirm MinIO objects appear and `artifacts_queue` rows are created correctly on each download
4. **Bridging update to processing service** — read from `artifacts_queue.minio_path` via MinIO instead of `raw_artifacts.filepath` from disk; run both paths briefly to confirm parity
5. **Archiver update** — replace local-file archive step with `artifacts_queue` row deletion for completed/skip rows
6. **Remove `raw_artifacts` writes from scraper** — once bridging validation passes
7. **Remove local disk writes from scraper**

`raw_artifacts` and `artifact_processing` tables are not dropped here — dropped in Plan 90 after Plan 93 is validated.

---

## What Does Not Change

- MinIO bronze partition structure (files were going here via archiver — now they go directly)
- `detail_scrape_claims` concurrency model
- Scraper browser stack and fetch logic
- Processing service core parse/write logic — only claim and read path changes in the bridging step
