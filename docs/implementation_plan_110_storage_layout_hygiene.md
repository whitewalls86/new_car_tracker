# Implementation Plan 110: Storage Layout Hygiene + Iceberg Readiness

**Status:** Draft  
**Branch:** feature/storage-refresh-implementation-plans  
**Plan doc:** [docs/plan_110_html_storage_optimization.md](plan_110_html_storage_optimization.md)  
**Roadmap:** [docs/plan_117_storage_and_adaptive_refresh_roadmap.md](plan_117_storage_and_adaptive_refresh_roadmap.md)

---

## Context

Plan 117 defines a six-plan arc: normalize storage → build refresh features → backtest →
deploy. Plan 110 is the required first step. Nothing else in the arc should start until the
storage contracts defined here are settled.

Plan 110 has two independent cleanup tracks:

1. **Bronze HTML** — new writes adopt zstd level 9; historical recompression is explicitly
   deprioritized (Plan 116 measured 8% savings on ~5.8M objects — not worth the operational
   cost); add write-time metrics so future storage decisions do not require retrospective
   MinIO scans.

2. **Silver/ops Parquet** — inventory the current layout, define a canonical pre-Iceberg
   layout, rewrite into it, switch readers, and add guarded cleanup for the old layout. This
   is the higher-stakes track: it changes the contract that dbt, DuckDB, and future Iceberg
   registration depend on.

### What Plan 116 changed

Plan 116 ran the recompression estimate script (Track C measurement tool) against
production and found stable 8–8.1% savings on detail pages and 9.4% on results pages.
The decision recorded in Plan 116: **skip the historical recompression pass**. 8% over
~5.8M objects does not justify the operational risk. Focus on level-9 for new writes
only (Track A), and defer any retroactive pass until storage pressure becomes acute.

This implementation plan reflects that decision. Phase 2 (recompression script) is
designed and documented but **not scheduled for execution**.

---

## Non-Goals

- No production adaptive refresh (Plans 111–113).
- No Apache Iceberg table registration (Plan 112).
- No MLflow tracking server setup (Plan 112).
- No automatic 30-day raw HTML deletion (blocked on Plan 114).
- No whole-file HTML dedup (Plan 114).
- No sectioned/recomposable HTML storage (Plan 114).
- No destructive cleanup before row counts, schema checks, and query checks pass.
- No Airflow scheduling for the recompression script.

---

## Current State Summary

Established from reading source code before drafting:

### Bronze HTML

- Key layout: `html/year={year}/month={month}/artifact_type={artifact_type}/{uuid}.html.zst`
- Write path: `shared/minio.py:write_html()` uses `ZSTD_LEVEL = 3`
- Read path: `shared/minio.py:read_html()` — decompresses with `ZstdDecompressor()`;
  format-agnostic (zstd frames are self-describing, any level readable by any level)
- No day partition in the bronze HTML key

### Silver Observations

- Flush path: `archiver/processors/flush_silver_observations.py`
- MinIO prefix: `silver/observations/`
- Partition columns: `source`, `obs_year`, `obs_month`, `obs_day`
- File naming: `part-{uuid}-0.parquet` (pre-compaction) or `compacted-YYYY-MM-DD.parquet`
  (post Plan 109)
- dbt source glob: `silver/observations/**/*.parquet` with `hive_partitioning=true`
  ([dbt/models/sources.yml:17](../dbt/models/sources.yml))

### Ops Event Tables

- Flush path: `archiver/processors/flush_staging_events.py`
- Tables: `artifacts_queue_events`, `detail_scrape_claim_events`,
  `blocked_cooldown_events`, `price_observation_events`, `vin_to_listing_events`
- MinIO prefix pattern: `ops/{table_name}/year={year}/month={month}/`
- Partition columns: `year`, `month` only (no day)
- File naming: `part-{uuid}-0.parquet`
- dbt source globs: `ops/{table}/**/*.parquet` with `hive_partitioning=true`
- Note: `detail_scrape_claim_events` and `vin_to_listing_events` are not yet registered
  in dbt sources.yml (not currently read by dbt models)

### Existing Cleanup Processor

- `archiver/processors/cleanup_parquet.py:cleanup_parquet(parquet_paths)` — takes a list
  of prefixes, deletes each recursively via `fs.rm(path, recursive=True)`
- `run_cleanup_parquet()` is DB-driven: queries expired months, builds paths, calls
  `cleanup_parquet()`. This is for HTML retention, not Parquet layout cleanup.
- This processor is **not** the guarded cleanup target — Phase 7 adds a separate
  operator-controlled cleanup for old Parquet layouts.

---

## Phase 0: Inventory Current State

**Objective:** Establish exact current contracts before touching anything.
No code changes. Produces a living checklist that each subsequent phase marks off.

### Tasks

1. List all source partition directories under `silver/observations/`:
   ```bash
   # On production server via processing container
   docker exec -it processing python -c "
   from shared.minio import get_s3fs, BUCKET
   fs = get_s3fs()
   dirs = fs.ls(f'{BUCKET}/silver/observations', detail=False)
   for d in dirs: print(d)
   "
   ```

2. List all ops event table prefixes and sample the partition depth for two:
   ```bash
   docker exec -it processing python -c "
   from shared.minio import get_s3fs, BUCKET
   fs = get_s3fs()
   for t in ['price_observation_events', 'detail_scrape_claim_events',
             'blocked_cooldown_events', 'vin_to_listing_events',
             'artifacts_queue_events']:
       entries = fs.ls(f'{BUCKET}/ops/{t}', detail=False)
       print(f'{t}: {len(entries)} entries, first={entries[0] if entries else None}')
   "
   ```

3. Verify the Plan 109 compaction state — confirm `done` (compacted-only) vs
   `incremental` (mixed) partition counts across sources.

4. Run one dbt model that reads silver and confirm it works before making changes:
   ```bash
   docker exec -it dbt_runner dbt run --select stg_detail_observations --full-refresh
   ```

5. Record all current dbt source external_location globs from
   [dbt/models/sources.yml](../dbt/models/sources.yml) — these are the read contracts
   that must remain valid until Phase 6 switches them.

### Deliverable

A checklist comment in this document (or a scratch note) recording:
- Confirmed source partition names and file counts
- Confirmed ops event partition depth and naming
- Confirmed dbt glob expressions currently in use
- Confirmed all dbt models pass before any changes

**Files changed:** None.

---

## Phase 1: Bronze New-Write Standard

**Objective:** Bump `ZSTD_LEVEL` from 3 to 9 and add structured write-time metrics.
Forward-compatible: `read_html()` requires no change.

### Exact behavior

#### `shared/minio.py`

```python
# Before
ZSTD_LEVEL = 3  # fast compression; good enough for HTML

# After
ZSTD_LEVEL = 9  # ~8-10% smaller than level 3 (Plan 116 evidence)
```

Add write-time structured logging inside `write_html()`:

```python
def write_html(key: str, content: bytes) -> str:
    import hashlib
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

    # Structured metrics for future storage analysis (replaces bare debug log)
    artifact_type = key.split("/artifact_type=")[1].split("/")[0] if "/artifact_type=" in key else "unknown"
    logger.info(
        "write_html: artifact_type=%s raw_bytes=%d compressed_bytes=%d "
        "raw_sha256=%.12s minio_path=%s key=%s",
        artifact_type, len(content), len(compressed),
        hashlib.sha256(content).hexdigest(), uri, key,
    )
    return uri
```

**Return contract:** unchanged — still returns `s3://<bucket>/<key>`.  
**Read contract:** unchanged — `read_html()` uses `ZstdDecompressor()` which is
level-agnostic.

### Safety constraints

- Do NOT change `read_html()`.
- Do NOT change the return value of `write_html()`.
- Do NOT change `make_key()`.
- The sha256 is logged as a prefix only — 12 chars. It is not stored or indexed.
  Do not add a permanent dedup index (blocked on Plan 114).
- Level 9 is slower than level 3. Expected impact: negligible — HTML objects are
  small (~15 KB compressed) relative to network fetch time (~300–500 ms). Watch
  scraper throughput metric in Grafana after rollout for unexpected regression.

### Tests

**File:** `tests/shared/test_minio.py`

| Test | Assert |
|------|--------|
| `test_write_html_uses_level_9` | Mock `ZstdCompressor`, assert it is called with `level=9` |
| `test_write_html_returns_s3_uri` | Return value starts with `s3://` and contains key |
| `test_write_html_logs_metrics` | Caplog contains `artifact_type`, `raw_bytes`, `compressed_bytes`, `raw_sha256`, `minio_path`, `key` |
| `test_write_html_logs_unknown_artifact_type` | Key without `/artifact_type=` logs `unknown` |
| `test_read_html_roundtrip_level9` | Write with level 9, read back — bytes match original |
| `test_read_html_level3_still_readable` | Write with level 3 manually, read back — still works |

No real MinIO required for unit tests — mock boto3 and s3fs.

### Validation commands

```bash
# Check current level before deploy
docker exec -it scraper python -c "from shared.minio import ZSTD_LEVEL; print(ZSTD_LEVEL)"

# After deploy — confirm scraper sees level 9
docker exec -it scraper python -c "from shared.minio import ZSTD_LEVEL; print(ZSTD_LEVEL)"

# Watch scraper logs for write_html metrics lines
docker logs scraper --follow 2>&1 | grep "write_html:"
```

### Deploy impact

- **Services to rebuild:** `scraper`, `processing` (both import `shared/minio.py`)
- **Flyway migration needed:** No
- **Airflow DAG changes:** No
- **dbt changes:** No
- **Quiet window:** Preferred but not required. The change is safe mid-run — any
  in-flight write completes at level 3; next write uses level 9. Mixed levels in
  bronze are permanently fine.

### Rollback

Revert `ZSTD_LEVEL = 9` to `ZSTD_LEVEL = 3` in `shared/minio.py` and rebuild
scraper and processing images. No data to roll back — existing objects are unaffected.

---

## Phase 2: Manual Historical Bronze Recompression Script

**Status: DEPRIORITIZED** — Plan 116 found 8% savings on ~5.8M objects.
The script is designed here for completeness but is not scheduled for execution.
Do not run against production without explicit confirmation.

**Objective:** Provide an operator tool to recompress existing `.html.zst` objects
to the new level-9 standard on demand, should storage pressure warrant it later.

### Script: `scripts/recompress_bronze_html.py`

#### CLI

```
python scripts/recompress_bronze_html.py [OPTIONS]

Selectors (mutually exclusive):
  --prefix PREFIX            Exact MinIO prefix
  --year YEAR [--month M] [--artifact-type TYPE]

Safety/performance:
  --limit N                  Stop after N objects scanned
  --max-bytes BYTES          Stop after N compressed bytes downloaded
  --progress-every N         Print progress line every N objects (default: 500)
  --checkpoint PATH          JSON file to read/write processed keys (resume support)
  --json-out PATH            Write final summary JSON to PATH

Apply mode (default is dry-run):
  --apply                    Actually write recompressed objects to MinIO
  --force                    In apply mode, overwrite even if recompressed is larger

Other:
  --bucket BUCKET            [default: $MINIO_BUCKET or 'bronze']
  --log-level LEVEL          DEBUG|INFO|WARNING [default: INFO]
```

#### Exact behavior

1. **Default is dry-run.** `--apply` is required to write anything.
2. Download → decompress → recompress at level 9 → compare sizes in memory.
3. In apply mode: overwrite the object **only if** `len(new_compressed) < len(old_compressed)`
   unless `--force` is also supplied.
4. **Never delete objects.** Never call `delete_object`.
5. On any failure for a single object: log at WARNING, increment `failed` counter,
   continue to next object.
6. `--checkpoint PATH`: on start, load a set of already-processed keys from JSON;
   skip them. On each successful apply write, append the key to the checkpoint file.
   Safe to interrupt — restart resumes from checkpoint.
7. Final summary: `scanned`, `recompressed`, `skipped`, `failed`, `bytes_before`,
   `bytes_after`, `bytes_saved`, `savings_pct`.

#### Safety constraints

- Never call `put_object` in dry-run mode.
- Never call `delete_object` in any mode.
- The `--apply` flag must be explicitly supplied — no implicit writes.
- Overwrite only smaller objects (unless `--force`).
- Continue on corrupt/unreadable objects.

#### Tests

**File:** `tests/scripts/test_recompress_bronze_html.py`

| Test | Assert |
|------|--------|
| `test_dry_run_no_writes` | Mock boto3; assert `put_object` never called without `--apply` |
| `test_apply_only_smaller` | old=100 bytes, new=90 bytes → writes; old=100, new=110 → skips |
| `test_apply_force_writes_larger` | With `--force`: writes even if new > old |
| `test_corrupt_object_continues` | `get_object` raises `ClientError` → failed++, loop continues |
| `test_corrupt_zstd_continues` | `decompress()` raises `ZstdError` → failed++, loop continues |
| `test_checkpoint_resume_skips_done` | Load checkpoint with key K → assert K not downloaded |
| `test_checkpoint_written_on_apply` | Apply one object → checkpoint file contains its key |
| `test_never_deletes` | Mock boto3; assert `delete_object` never called in any mode |
| `test_summary_counts` | 10 objects: 7 recompressed, 2 skipped, 1 failed → correct counts |

#### Production runbook

```bash
# Dry-run a single month prefix to preview expected savings
docker exec -it processing python scripts/recompress_bronze_html.py \
  --year 2026 --month 6 --artifact-type detail_page \
  --limit 1000 --progress-every 100

# Apply with checkpoint (restartable)
docker exec -it processing python scripts/recompress_bronze_html.py \
  --year 2026 --month 6 --artifact-type detail_page \
  --apply --checkpoint /tmp/recompress_2026_06.json \
  --progress-every 500 --json-out /tmp/result_2026_06.json
```

- Run outside business hours or during a low-scraper-activity window.
- Use `--max-bytes` to limit bandwidth if running alongside active scraping.
- The scraper can continue writing new objects concurrently — no locking needed.

---

## Phase 3: Parquet Lake Layout Audit

**Objective:** Produce a complete, read-only inventory of the current MinIO Parquet
layout before any normalization. This report becomes the baseline verification target
for Phase 5 (rewrite) row-count checks.

### Script: `scripts/audit_parquet_layout.py`

#### CLI

```
python scripts/audit_parquet_layout.py [OPTIONS]

Selectors:
  --datasets NAMES...        Datasets to audit (default: all)
                             silver_observations price_observation_events
                             vin_to_listing_events blocked_cooldown_events
                             detail_scrape_claim_events artifacts_queue_events
  --json-out PATH            Write JSON report to PATH (default: audit_report.json)
  --md-out PATH              Write Markdown report to PATH (default: audit_report.md)

Other:
  --bucket BUCKET            [default: $MINIO_BUCKET or 'bronze']
  --log-level LEVEL          DEBUG|INFO|WARNING [default: INFO]
```

#### Exact behavior

For each dataset, report:
- **Partition columns** inferred from path structure
- **Object count** (per partition and total)
- **Total bytes** (from listing metadata — no download needed)
- **Small-file count** (files < 1 MB)
- **Schema variants** (read Parquet metadata from a sample of files — not full scan)
- **Row counts** (read Parquet metadata `num_rows` — no data scan)
- **Min/max timestamps** from Parquet row group statistics if available; otherwise
  mark as `null` (do not download data to compute them)
- **Legacy or unexpected prefixes** (objects not matching the expected pattern)

**No mutation of any kind.** Never call `put_object`, `delete_object`, `copy_object`,
or `rename`. Read listing metadata and Parquet file metadata only.

#### Output formats

JSON (machine-readable for Phase 5 row-count verification):
```json
{
  "generated_at": "2026-07-01T10:00:00Z",
  "datasets": {
    "silver_observations": {
      "prefix": "silver/observations/",
      "total_objects": 12345,
      "total_bytes": 987654321,
      "small_files": 234,
      "partitions": [
        {
          "path": "silver/observations/source=detail/obs_year=2026/obs_month=6/obs_day=15/",
          "objects": 1,
          "bytes": 8234567,
          "rows": 37412,
          "schema_fingerprint": "abc123",
          "ts_min": null,
          "ts_max": null
        }
      ],
      "schema_variants": 1,
      "unexpected_paths": []
    }
  }
}
```

Markdown: human-readable table per dataset, plus a summary table.

#### Tests

**File:** `tests/scripts/test_audit_parquet_layout.py`

| Test | Assert |
|------|--------|
| `test_schema_variant_detection` | Two Parquet fixtures with different columns → schema_variants=2 |
| `test_row_count_from_metadata` | Fixture with known row count → row count matches without reading data |
| `test_small_file_detection` | File < 1 MB → counted in small_files |
| `test_unexpected_path_detection` | Object outside expected partition pattern → in unexpected_paths |
| `test_never_writes` | Mock s3fs; assert `put`, `delete`, `rename` never called |
| `test_json_output_valid` | Output JSON is valid and contains all expected top-level keys |
| `test_markdown_output_has_tables` | Markdown output contains at least one `|` row |

#### Validation

```bash
docker exec -it processing python scripts/audit_parquet_layout.py \
  --json-out /tmp/audit_before_normalize.json \
  --md-out /tmp/audit_before_normalize.md

# Save the JSON report — it becomes the baseline for Phase 5 verification
```

---

## Phase 4: Canonical Pre-Iceberg Layout Decision

**Objective:** Define the physical layout that Iceberg will register. This decision
must be written down and reviewed before Phase 5 begins any rewrites.

This phase produces a layout decision record (an update to this document) — not code.

### Current layout summary

| Dataset | Current MinIO prefix | Partition columns |
|---------|----------------------|-------------------|
| silver observations | `silver/observations/source=X/obs_year=Y/obs_month=M/obs_day=D/` | source, obs_year, obs_month, obs_day |
| price_observation_events | `ops/price_observation_events/year=Y/month=M/` | year, month |
| vin_to_listing_events | `ops/vin_to_listing_events/year=Y/month=M/` | year, month |
| blocked_cooldown_events | `ops/blocked_cooldown_events/year=Y/month=M/` | year, month |
| detail_scrape_claim_events | `ops/detail_scrape_claim_events/year=Y/month=M/` | year, month |
| artifacts_queue_events | `ops/artifacts_queue_events/year=Y/month=M/` | year, month |

### Proposed canonical layout (recommended)

```
silver_normalized/observations/
    source={source}/
        data-{YYYY-MM-DD}-{uuid}.parquet

ops_normalized/
    {table_name}/
        data-{YYYY-MM-DD}-{uuid}.parquet
```

**Rationale:**

- **Source partition on silver is load-bearing.** Every dbt model and analytics
  query filters on source (`detail`, `carousel`, `srp`). Removing it would force
  full scans. Keep it.
- **Remove day/month partitions from Parquet path.** Iceberg manages time-based
  partition evolution and snapshot metadata. DuckDB glob-based time filtering should
  use the `fetched_at` / `event_at` column inside the file, not the path. Day
  partitions were essential for append-only Parquet growth; Iceberg removes that need.
- **Event timestamps stay as data columns** (`fetched_at`, `event_at`, `obs_year`,
  etc.) — they are not removed from the schema. Only the *path partition* on time
  is removed.
- **`data-{YYYY-MM-DD}-{uuid}.parquet` filename** encodes the write date for
  human-readable MinIO browsing and manual recovery, without making the date a
  partition column.
- **Separate `silver_normalized/` and `ops_normalized/` roots** allow dual-read
  validation: old paths remain active for dbt while new paths are being verified.
  Iceberg registration targets only the normalized roots.

### Alternative A: month/source partitions

```
silver_normalized/observations/
    source={source}/year={YYYY}/month={MM}/
        data-{uuid}.parquet
```

**Tradeoffs vs recommended:**
- + DuckDB partition pruning on year/month works without Iceberg
- + Smaller per-file size during compaction window
- - More path levels = more Iceberg manifest entries during registration
- - Partition evolution to "flat per source" still needed before long-term Iceberg use

### Alternative B: source-only, no change to ops

Normalize silver to source-only, leave ops events at year/month.

**Tradeoffs vs recommended:**
- + Half the migration scope
- - Ops events remain on an inconsistent layout requiring a second migration later
- - Iceberg registration needs different strategies per dataset

### Layout decision criteria

The recommended layout should be chosen **only after** Phase 3 audit confirms:

1. Silver per-source file counts post-Plan 109 compaction are ≤ ~180 files per source
   (manageable as a flat set under `source=X/`)
2. Ops event tables are small enough (< 500 objects each) that flat layout is practical
3. No dbt model currently filters silver by `obs_year` or `obs_month` as path-pruning
   (verify by checking explain output for a silver-reading model)

**Open question:** Is there a period where dbt/DuckDB needs to read both old and new
paths simultaneously? If yes, use a dual-read source in `sources.yml` temporarily
(Phase 6 covers this). Confirm before starting Phase 5.

---

## Phase 5: Safe Parquet Rewrite

**Objective:** Rewrite silver and ops Parquet into the canonical layout without
deleting the original layout. Apply Plan 109's safety lessons: write first, verify
before switching readers, no destructive step until row counts pass.

### Script: `scripts/rewrite_parquet_layout.py`

#### CLI

```
python scripts/rewrite_parquet_layout.py [OPTIONS]

Selectors:
  --dataset DATASET          One of: silver_observations, price_observation_events,
                             vin_to_listing_events, blocked_cooldown_events,
                             detail_scrape_claim_events, artifacts_queue_events
  --source SOURCE            For silver_observations: limit to one source (detail|carousel|srp)
  --month YYYY-MM            Limit to one calendar month (for scoped testing)
  --limit-partitions N       Max source partitions to rewrite per run

Safety:
  --dry-run                  Default. List what would be rewritten; no writes.
  --apply                    Actually write to the normalized prefix.
  --baseline-audit PATH      JSON from Phase 3 audit to verify against (recommended)
  --json-out PATH            Write verification report to PATH

Other:
  --bucket BUCKET            [default: $MINIO_BUCKET or 'bronze']
```

#### Exact behavior

1. **Default is dry-run.** `--apply` is required for any writes.
2. Read all Parquet files from one source partition (using PyArrow — same as compact_silver).
3. Normalize: validate schema, drop path-derived partition columns (`obs_year`, `obs_month`,
   `obs_day`, `year`, `month`) from the file if they are redundant with the data columns
   (`fetched_at`, `event_at`). These columns stay in the data if they are in the schema;
   do not drop them from the data — only stop using them as partition columns in the new path.
4. Write a single sorted file (or a small set of files for very large partitions) to the
   normalized prefix. Filename: `data-{source_month}-{uuid}.parquet`.
5. **Verify before marking done:**
   - Row count of rewritten file(s) == row count of source partition(s)
   - Min/max `fetched_at` / `event_at` within ±1 second of expected range
   - Schema fingerprint matches expected normalized schema
6. Produce a per-partition verification report (JSON).
7. **Never overwrite the old prefix.** Old paths remain untouched until Phase 7.
8. If `--baseline-audit` is provided, cross-check rewritten row counts against
   the Phase 3 audit JSON — flag any discrepancy.

#### Write sequence (per source partition)

Mirrors Plan 109 to prevent double-counting during any concurrent dbt read:

```
1. Read all Parquet files from old partition into memory
2. Sort by the same SORT_COLS as compact_silver
3. Write to normalized_prefix/source={source}/data-{month}-{uuid}.parquet.tmp
   (invisible to *.parquet glob readers)
4. Assert written rows == source rows  ← pre-rename safety check
5. fs.rename(tmp → data-{month}-{uuid}.parquet)
6. Append to verification report
```

#### Tests

**File:** `tests/scripts/test_rewrite_parquet_layout.py`

| Test | Assert |
|------|--------|
| `test_dry_run_no_writes` | Mock s3fs; assert no write calls without `--apply` |
| `test_row_count_preserved` | Fixture with known N rows → rewritten file has N rows |
| `test_schema_normalized` | Output schema matches expected normalized schema |
| `test_old_prefix_untouched` | Source files still exist after rewrite |
| `test_tmp_not_visible_to_glob` | `.parquet.tmp` file does not match `*.parquet` glob |
| `test_row_count_mismatch_aborts_rename` | Corrupt tmp write → rename not called |
| `test_baseline_cross_check` | Rewritten count differs from audit JSON → flagged in report |
| `test_verification_report_produced` | JSON report contains rows, schema fingerprint, ts range |

#### Validation commands

```bash
# Dry-run one source for one month
docker exec -it processing python scripts/rewrite_parquet_layout.py \
  --dataset silver_observations --source detail --month 2026-06 --dry-run

# Apply one month, cross-check against Phase 3 audit
docker exec -it processing python scripts/rewrite_parquet_layout.py \
  --dataset silver_observations --source detail --month 2026-06 \
  --apply \
  --baseline-audit /tmp/audit_before_normalize.json \
  --json-out /tmp/rewrite_verification_detail_2026_06.json

# Verify the normalized prefix is readable by DuckDB
docker exec -it dbt_runner python -c "
import duckdb
conn = duckdb.connect()
conn.execute(\"INSTALL httpfs; LOAD httpfs;\")
result = conn.execute(\"SELECT count(*) FROM read_parquet('s3://bronze/silver_normalized/observations/source=detail/**/*.parquet')\").fetchone()
print(f'Normalized detail rows: {result[0]}')
"
```

---

## Phase 6: Reader Switch / Compatibility

**Objective:** Move dbt/DuckDB source globs from the current layout to the normalized
layout, with validation gates before each switch and a clear rollback path.

### Switch sequence

**Gate 1:** Phase 5 verification report confirms row counts match for all sources.

**Gate 2:** Run a dual-read validation — query both old and new paths in DuckDB
and compare row counts:

```sql
-- Run against dbt_runner container
WITH old AS (
    SELECT source, count(*) as n FROM read_parquet(
        's3://bronze/silver/observations/**/*.parquet', hive_partitioning=true)
    GROUP BY source
),
new AS (
    SELECT source, count(*) as n FROM read_parquet(
        's3://bronze/silver_normalized/observations/**/*.parquet', hive_partitioning=true)
    GROUP BY source
)
SELECT o.source, o.n as old_rows, n.n as new_rows, o.n - n.n as diff
FROM old o JOIN new n USING (source)
WHERE o.n != n.n;
-- Must return 0 rows before switch
```

**Gate 3:** Run full dbt build against the normalized prefix (using a test source):

```bash
# Temporarily point sources.yml at normalized prefix for one model
docker exec -it dbt_runner dbt run --select stg_detail_observations \
  --vars '{"silver_prefix": "silver_normalized/observations"}'
# Adjust once source parameterization is confirmed (see below)
```

**Switch:**

Update `dbt/models/sources.yml` — change silver observations `external_location`:

```yaml
# Before
read_parquet(
  's3://{{ env_var("MINIO_BUCKET", "bronze") }}/silver/observations/**/*.parquet',
  hive_partitioning=true
)

# After
read_parquet(
  's3://{{ env_var("MINIO_BUCKET", "bronze") }}/silver_normalized/observations/**/*.parquet',
  hive_partitioning=true
)
```

Repeat for each ops event table that was rewritten in Phase 5.

**Feature flag option:** If the validation gate is uncertain, add an env var
`SILVER_PREFIX` that switches between old and normalized paths in sources.yml
via `env_var()`. This allows rollback without a code change. Only add this if
the validation gate does not give clear confidence.

### Rollback path

Revert `sources.yml` to the old glob. Old paths remain in MinIO until Phase 7,
so rollback is instant.

### Tests before switch

In addition to the dual-read SQL above:
- `dbt build --full-refresh` passes on all silver-reading models
- Integration test `test_storage_layout_integration.py` reads from normalized prefix
  and asserts row counts match audit baseline

### Files changed

- `dbt/models/sources.yml` — update external_location globs

**No Flyway migration needed.** DuckDB sources are not Postgres schema objects.

---

## Phase 7: Guarded Cleanup

**Objective:** Provide a safe, explicit, operator-controlled tool for deleting old
Parquet layout objects after Phase 6 has been running stably for an agreed period
(suggested: at least 7 days with no rollback).

This is **not** the existing `cleanup_parquet.py` (which is DB-driven for HTML retention).
This is a separate operator tool, not an Airflow DAG.

### Script: `scripts/cleanup_old_parquet_layout.py`

#### CLI

```
python scripts/cleanup_old_parquet_layout.py [OPTIONS]

Targets (at least one required):
  --prefix PREFIX            MinIO prefix to clean (can be repeated)
  --prefix-file PATH         File containing one prefix per line

Safety:
  --dry-run                  Default. List candidates; no deletion.
  --apply                    Actually delete.

Other:
  --bucket BUCKET            [default: $MINIO_BUCKET or 'bronze']
  --progress-every N         [default: 100]
  --json-out PATH            Write deletion report to PATH
```

#### Exact behavior

1. **Default is dry-run.** `--apply` required to delete anything.
2. For each prefix: list all objects under it. Print total count and bytes.
3. In dry-run mode: print what would be deleted, return.
4. In apply mode: delete each object individually (not `fs.rm(recursive=True)` on a
   prefix — prefer per-object to control partial failures).
5. **Missing objects are non-fatal** (already deleted — treat as success).
6. Partial failures: report `deleted`, `failed`, `already_absent` counts. Do not stop
   on a failure.
7. Final report: `total`, `deleted`, `already_absent`, `failed`, `bytes_freed`.

#### Safety constraints

- Never infer prefixes from the database or from any automatic detection.
  Prefixes must be explicitly supplied by the operator.
- Always print candidate count and bytes before any deletion.
- The `--apply` flag must be explicitly supplied.
- Do not use `fs.rm(path, recursive=True)` — per-object deletes give finer control.

#### Tests

**File:** `tests/scripts/test_cleanup_old_parquet_layout.py`

| Test | Assert |
|------|--------|
| `test_dry_run_no_deletes` | Mock s3fs; assert delete never called without `--apply` |
| `test_candidate_count_printed` | Dry-run output includes object count and bytes |
| `test_apply_deletes_supplied_prefix` | Apply deletes objects under supplied prefix only |
| `test_missing_object_non_fatal` | `FileNotFoundError` on delete → already_absent++, loop continues |
| `test_partial_failure_reported` | One delete raises Exception → failed++, others continue |
| `test_no_unsupplied_prefix_touched` | Only supplied prefix deleted, not adjacent prefix |

#### Integration test

**File:** `tests/integration/archiver/test_storage_layout_integration.py`

- Seed test MinIO with objects under `test_old_layout/` and `test_normalized/`
- Run cleanup on `test_old_layout/` only
- Assert `test_normalized/` objects are untouched
- Assert `test_old_layout/` objects are gone

#### Production runbook

```bash
# Preview what would be deleted
docker exec -it processing python scripts/cleanup_old_parquet_layout.py \
  --prefix silver/observations/ \
  --dry-run

# After at least 7 days of stable normalized reads:
docker exec -it processing python scripts/cleanup_old_parquet_layout.py \
  --prefix silver/observations/ \
  --apply \
  --json-out /tmp/cleanup_silver_old_layout.json

# Verify old prefix is empty
docker exec -it processing python -c "
from shared.minio import get_s3fs, BUCKET
fs = get_s3fs()
remaining = fs.ls(f'{BUCKET}/silver/observations', detail=False)
print(f'Objects remaining in old prefix: {len(remaining)}')
"
```

---

## Phase 8: Deployment and Operations

### Per-phase service impact

| Phase | Services to rebuild | Flyway needed | Scraper impact | dbt impact |
|-------|---------------------|---------------|----------------|------------|
| 1 | `scraper`, `processing` | No | Yes — level 9 | No |
| 2 | `processing` (script run) | No | No | No |
| 3 | `processing` (script run) | No | No | No |
| 4 | No code | No | No | No |
| 5 | `processing` (script run) | No | No | No |
| 6 | `dbt_runner` (config only) | No | No | Yes — source switch |
| 7 | `processing` (script run) | No | No | No |

### Monitoring during Phase 1 rollout

After deploying Phase 1 (`ZSTD_LEVEL = 9`):

```bash
# Confirm scraper write logs show compressed_bytes are smaller than raw_bytes
docker logs scraper 2>&1 | grep "write_html:" | head -20

# Watch for any scraper throughput regression
# Check Grafana: scraper artifacts/min panel — should remain stable
```

### Phase 5/6 deploy sequencing

Phase 5 (rewrite) can run in the background from the processing container without
affecting live traffic. It writes to `silver_normalized/` which nothing reads yet.

Phase 6 (reader switch) is a config change to `dbt/models/sources.yml`. It takes
effect on the next `dbt_runner` deploy. Coordinate with the dbt build schedule:

- `dbt_build` runs at `0 * * * *` (hourly)
- Deploy the sources.yml change, then monitor the next dbt run for failures
- If the next dbt run fails, revert sources.yml and investigate

### Rollback steps (per phase)

| Phase | Rollback |
|-------|---------|
| 1 | Revert `ZSTD_LEVEL = 9 → 3`, rebuild scraper + processing |
| 5 | Delete `silver_normalized/` prefix (no readers have switched yet) |
| 6 | Revert `sources.yml` glob to old prefix; old data still in MinIO |
| 7 | Cannot undo deletion. This is why Phase 7 runs only after ≥7 days stable. |

### Quiet window recommendations

| Phase | Recommendation |
|-------|----------------|
| 1 | Preferred off-peak; not required |
| 5 | Off-peak; processing container is the only writer; concurrent scraper is fine |
| 6 | Immediately after a successful dbt run to maximize lead time before next run |
| 7 | Off-peak; irreversible step |

---

## Phase 9: Implementation Order

Safe PR/commit sequence:

| PR | Contents | Can start when |
|----|----------|---------------|
| 1 | This implementation plan (docs only) | Now |
| 2 | Phase 1: `shared/minio.py` + `tests/shared/test_minio.py` | Phase 0 checklist complete |
| 3 | Phase 3: `scripts/audit_parquet_layout.py` + tests | Phase 1 deployed and stable |
| 4 | Phase 5: `scripts/rewrite_parquet_layout.py` + tests | Phase 3 audit report reviewed and layout decision confirmed |
| 5 | Phase 6: `dbt/models/sources.yml` reader switch | Phase 5 verification report passes all gates |
| 6 | Phase 7: `scripts/cleanup_old_parquet_layout.py` + tests | Phase 6 stable for ≥7 days |
| 7 | Phase 2 (optional): `scripts/recompress_bronze_html.py` + tests | Any time; not on critical path |

Note: Phase 4 (layout decision) is resolved as part of PR 3 review — the audit
report informs the final decision, and the decision must be recorded in this document
before PR 4 is written.

---

## Testing Matrix

| Phase | Unit tests | Integration tests | Manual validation |
|-------|-----------|-------------------|-------------------|
| 1 | `tests/shared/test_minio.py` | Roundtrip read after level-9 write | `docker logs scraper \| grep write_html` |
| 2 | `tests/scripts/test_recompress_bronze_html.py` | Seed MinIO, dry-run asserts no change | Preview command on one month prefix |
| 3 | `tests/scripts/test_audit_parquet_layout.py` | Seed MinIO, check report counts | Run against production, review Markdown report |
| 5 | `tests/scripts/test_rewrite_parquet_layout.py` | Seed old-layout MinIO, rewrite, check rows | DuckDB row count cross-check |
| 6 | (existing dbt model tests) | Dual-read SQL row count check | `dbt build --full-refresh` passes |
| 7 | `tests/scripts/test_cleanup_old_parquet_layout.py` | `test_storage_layout_integration.py` | Dry-run candidate count reviewed before apply |

All unit tests: no real MinIO required. All integration tests: require a MinIO
test instance (already used by existing integration tests).

---

## Open Questions

1. **Exact canonical normalized Parquet layout for silver.** The plan recommends
   `silver_normalized/observations/source={source}/data-*.parquet` (source-only partition,
   no time partition). This must be confirmed after Phase 3 audit reveals actual per-source
   file counts post-compaction. If a single source has >500 files, a month-level partition
   may still be needed before Iceberg.

2. **Canonical layout for ops events.** Recommendation is flat
   `ops_normalized/{table}/data-*.parquet`. Confirm `price_observation_events` total
   object count (highest volume ops table) supports flat layout before Phase 5.

3. **Do normalized prefixes live beside current prefixes or under a new root?**
   Current plan: `silver_normalized/` beside `silver/`. This keeps old and new clearly
   separated and avoids any reader confusion. Alternative: move directly to
   `silver/observations/` (in-place) requires more careful orchestration to avoid
   double-count during transition. Recommend keeping separate roots.

4. **How much of Plan 109 compaction applies to the rewritten layout?** The
   `compact_silver` DAG currently targets `silver/observations/`. After Phase 6 switches
   readers to `silver_normalized/observations/`, the compaction DAG must be updated to
   target the new prefix. This update is not in the current phase breakdown — it should
   be added to PR 5 (reader switch) or as its own PR immediately after.

5. **Which container runs historical recompression and rewrite scripts?** Both use
   `shared/minio.py` and `boto3`/`s3fs`. The processing container has all dependencies
   and is the natural host. Confirm `processing` has adequate memory for rewriting
   one full-source monthly partition in-memory (carousel is the largest: ~5.4 MB
   compacted post-Plan 109, safe).

6. **Checkpoint state for Phase 2 (recompression): local JSON or MinIO object?**
   Local JSON is simpler; MinIO object survives container restarts. Since
   Phase 2 is deprioritized and run manually, local JSON is sufficient. If it is ever
   operationalized, move checkpoint to a well-known MinIO path.

7. **When to update the `compact_silver` DAG's target prefix.** After Phase 6 switches
   `dbt/models/sources.yml` to `silver_normalized/`, new silver flushes will continue
   writing to the old `silver/observations/` prefix until `flush_silver_observations.py`
   is also updated. This means Phase 5 and Phase 6 must include updating the flush
   prefix too — or the transition must be done atomically (stop flush, rewrite all
   remaining data, start flush at new prefix, switch sources.yml). This is the highest-
   risk coordination point. It should be resolved during Phase 4 layout review.
