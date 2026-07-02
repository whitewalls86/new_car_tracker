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
   docker exec -it cartracker-processing python -c "
   from shared.minio import get_s3fs, BUCKET
   fs = get_s3fs()
   dirs = fs.ls(f'{BUCKET}/silver/observations', detail=False)
   for d in dirs: print(d)
   "
   ```

2. List all ops event table prefixes and sample the partition depth for two:
   ```bash
   docker exec -it cartracker-processing python -c "
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
   docker exec -it cartracker-dbt-runner dbt run --select stg_detail_observations --full-refresh
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
docker exec -it cartracker-scraper python -c "from shared.minio import ZSTD_LEVEL; print(ZSTD_LEVEL)"

# After deploy — confirm scraper sees level 9
docker exec -it cartracker-scraper python -c "from shared.minio import ZSTD_LEVEL; print(ZSTD_LEVEL)"

# Watch scraper logs for write_html metrics lines
docker logs cartracker-scraper --follow 2>&1 | grep "write_html:"
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
docker exec -it cartracker-processing python scripts/recompress_bronze_html.py \
  --year 2026 --month 6 --artifact-type detail_page \
  --limit 1000 --progress-every 100

# Apply with checkpoint (restartable)
docker exec -it cartracker-processing python scripts/recompress_bronze_html.py \
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
docker exec -it cartracker-processing python scripts/audit_parquet_layout.py \
  --json-out /tmp/audit_before_normalize.json \
  --md-out /tmp/audit_before_normalize.md

# Save the JSON report — it becomes the baseline for Phase 5 verification
```

---

## Phase 4: Canonical Pre-Iceberg Layout + Flush Cadence Decision

**Objective:** Define the physical layout that Iceberg will register and stop
creating unnecessary small files before dbt can expose the data. This decision
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

### Phase 3 audit baseline

Production audit before normalization:

| Dataset | Objects | Small files | Partitions | Rows | Schema variants | Metadata failures |
|---------|---------|-------------|------------|------|-----------------|-------------------|
| silver_observations | 1,056 | 73.3% | 456 | 35,741,942 | 1 | 0 |
| price_observation_events | 6,820 | 99.8% | 7 | 32,765,506 | 1 | 0 |
| vin_to_listing_events | 6,812 | 100.0% | 7 | 3,014,947 | 1 | 0 |
| blocked_cooldown_events | 1,145 | 100.0% | 4 | 53,816 | 1 | 0 |
| detail_scrape_claim_events | 6,809 | 100.0% | 4 | 2,704,353 | 1 | 0 |
| artifacts_queue_events | 6,824 | 99.9% | 7 | 12,092,159 | 1 | 0 |

Interpretation:

- Schema/path correctness is not the problem. Every dataset has one schema variant,
  no unexpected paths, and no metadata read failures.
- The problem is file cardinality. Across audited datasets there are 29,466 Parquet
  objects, and 99.0% are small files.
- Ops event tables are the worst offenders because frequent flushes create many
  tiny files in a small number of monthly partitions.

### Decision: Month-level normalized layout

```text
silver_normalized/observations/
    source={source}/obs_year={year}/obs_month={month}/
        part-{uuid}-0.parquet
        compacted-through-{YYYY-MM-DD}.parquet
```

For ops events:
```text
ops_normalized/
    {table_name}/year={year}/month={month}/
        part-{uuid}-0.parquet
        compacted-through-{YYYY-MM-DD}.parquet
```

Why month-level:

- It collapses silver's day-level partition explosion while keeping useful
  pre-Iceberg DuckDB pruning on `source`, `obs_year`, and `obs_month`.
- Sorting by `fetched_at`/`event_at` inside files gives Parquet row group min/max
  statistics for intra-month filters.
- Ops tables already use year/month partitions, so this keeps ops stable while
  fixing file count through cadence and compaction.
- Iceberg does not make Parquet files mutable, but it later provides snapshot
  commits, manifest-based reads, hidden partitioning, partition evolution, and
  safer compaction over this cleaner physical base.

**Separate new roots (`silver_normalized/`, `ops_normalized/`) allow dual-read
validation:** old paths stay live for dbt while Phase 5 rewrites and Phase 6 verifies.
Iceberg registration in Plan 112 targets only the normalized roots.

### Decision: Align Parquet flush cadence with dbt visibility

Current schedules:

| DAG | Current cadence |
|-----|-----------------|
| `results_processing` | every 5 minutes |
| `flush_silver_observations` | every 5 minutes |
| `flush_staging_events` | every 15 minutes |
| `dbt_build` | hourly at `:00` |

Current flush cadence creates files faster than analytics usually exposes them.
The target state is an orchestrated hourly analytics refresh:

```text
results_processing continues every 5 minutes

hourly_analytics_refresh:
    deploy_intent_sensor
    archiver health
    dbt_runner health
    flush_silver_observations
    flush_staging_events
    dbt_build

daily:
    compact active month / closed months
```

This shifts the tradeoff from MinIO small-file churn to Postgres staging buffer
size. That is acceptable if we add guardrails:

- alert on staging row counts
- alert on oldest unflushed staging row age
- alert on flush failures
- keep manual flush endpoints
- only delete staging rows after successful Parquet writes
- cap with "flush if oldest row > X minutes or row count > N" if hourly alone
  proves too coarse

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
2. Read all Parquet files for one target rewrite unit:
   - silver: one `source` + calendar month
   - ops: one event table + calendar month
3. Normalize: validate schema, drop path-derived partition columns (`obs_year`, `obs_month`,
   `obs_day`, `year`, `month`) from the file if they are redundant with the data columns
   (`fetched_at`, `event_at`). These columns stay in the data if they are in the schema;
   do not drop them from the data unless the reader contract is updated and tested. Stop
   using `obs_day` as a path partition in the new layout.
4. Write a single sorted file (or a small set of files for very large partitions) to the
   normalized month prefix. Filename: `part-{uuid}-0.parquet` for the initial rewrite,
   or `compacted-through-{YYYY-MM-DD}.parquet` for compaction outputs.
5. **Verify before marking done:**
   - Row count of rewritten file(s) == row count of source month(s)
   - Min/max `fetched_at` / `event_at` within ±1 second of expected range
   - Schema fingerprint matches expected normalized schema
6. Produce a per-month verification report (JSON).
7. **Never overwrite the old prefix.** Old paths remain untouched until Phase 7.
8. If `--baseline-audit` is provided, cross-check rewritten row counts against
   the Phase 3 audit JSON — flag any discrepancy.

#### Write sequence (per source/month or table/month)

Mirrors Plan 109 to prevent double-counting during any concurrent dbt read:

```
1. Read all Parquet files from old source/month or table/month into memory
2. Sort by fetched_at/event_at first, then stable identifiers
3. Write to normalized month prefix as part-{uuid}-0.parquet.tmp
   (invisible to *.parquet glob readers)
4. Assert written rows == source rows  ← pre-rename safety check
5. fs.rename(tmp → part-{uuid}-0.parquet)
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
docker exec -it cartracker-processing python scripts/rewrite_parquet_layout.py \
  --dataset silver_observations --source detail --month 2026-06 --dry-run

# Apply one month, cross-check against Phase 3 audit
docker exec -it cartracker-processing python scripts/rewrite_parquet_layout.py \
  --dataset silver_observations --source detail --month 2026-06 \
  --apply \
  --baseline-audit /tmp/audit_before_normalize.json \
  --json-out /tmp/rewrite_verification_detail_2026_06.json

# Verify the normalized prefix is readable by DuckDB
docker exec -it cartracker-dbt-runner python -c "
import duckdb
conn = duckdb.connect()
conn.execute(\"INSTALL httpfs; LOAD httpfs;\")
result = conn.execute(\"SELECT count(*) FROM read_parquet('s3://bronze/silver_normalized/observations/source=detail/**/*.parquet', hive_partitioning=true)\").fetchone()
print(f'Normalized detail rows: {result[0]}')
"
```

---

## Phase 6: Writer + Reader Cutover

**Objective:** Atomically switch all writers (archiver flush + compaction) and all
readers (dbt/DuckDB) from the old layout to the normalized layout. This is the
highest-risk phase because live production services write to `silver/observations/`
and `ops/*/` continuously. Switching readers alone (dbt sources.yml) while writers
still target the old prefix would immediately create a diverging dataset.

### Files changed in this phase

| File | Change |
|------|--------|
| `archiver/processors/flush_silver_observations.py` | `_MINIO_PREFIX = "silver_normalized/observations"` |
| `archiver/processors/compact_silver.py` | Retarget to `silver_normalized/observations`; compact month-level layout |
| `archiver/processors/flush_staging_events.py` | Each `minio_prefix` in `_TABLE_CONFIGS`: `ops/{table}` → `ops_normalized/{table}` |
| `dbt/models/sources.yml` | All silver and ops_events `external_location` globs |
| `airflow/dags/hourly_analytics_refresh.py` | New orchestrated hourly flush + dbt DAG |
| `airflow/dags/flush_silver_observations.py` | Unschedule or disable standalone frequent flush |
| `airflow/dags/flush_staging_events.py` | Unschedule or disable standalone frequent flush |
| `airflow/dags/dbt_build.py` | Unschedule hourly standalone build or keep manual-only |

**Services to rebuild:** `cartracker-archiver`, `cartracker-dbt-runner`, Airflow
scheduler/webserver images or DAG volume, depending on deploy process.

**Flyway migration needed:** No, unless staging observability tables/views are added
in the same PR. Prefer Prometheus/dbt/SQL checks first and keep migrations out of the
cutover if possible.

### Pre-cutover validation gates

All three must pass before starting the cutover sequence.

**Gate 1:** Phase 5 verification report shows 0 row-count discrepancies for all
silver sources and all ops event tables.

**Gate 2:** Dual-read SQL returns 0 rows (run from cartracker-dbt-runner):

```sql
WITH old_silver AS (
    SELECT source, count(*) AS n
    FROM read_parquet(
        's3://bronze/silver/observations/**/*.parquet',
        hive_partitioning=true)
    GROUP BY source
),
new_silver AS (
    SELECT source, count(*) AS n
    FROM read_parquet(
        's3://bronze/silver_normalized/observations/**/*.parquet',
        hive_partitioning=true)
    GROUP BY source
)
SELECT o.source, o.n AS old_rows, n.n AS new_rows, o.n - n.n AS diff
FROM old_silver o JOIN new_silver n USING (source)
WHERE o.n != n.n;
-- Must return 0 rows.
```

**Gate 3:** Full dbt build passes against the normalized prefix. Set
`external_location` to `silver_normalized/` in a throwaway copy of sources.yml
and run `dbt build --full-refresh` to confirm all models compile and pass.

### Cutover sequence

This is a ~20-minute operator window. Each step is listed with the failure mode
and its recovery action.

#### Step 1 — Set deploy intent

```bash
curl -sf -X POST http://localhost:8060/deploy/start
# Returns true on success; 409 if another intent is already set.

curl -s http://localhost:8060/deploy/status
# Confirm: "intent": "pending", and number_running is low (in-flight claims draining).
```

Do not proceed until `/deploy/status` returns `"intent": "pending"`. The ops
service will begin returning 503 on claim endpoints, preventing new detail scrape
claims from starting while the deploy is in progress.

#### Step 2 — Verify no active archiver jobs

```bash
# /ready returns 503 while an active_job() is in progress
curl -s http://localhost:8001/ready
# Must return 200 before proceeding
```

If `/ready` returns 503, wait for the in-progress job to finish (typically < 60s).

#### Step 3 — Pause Airflow DAGs that write silver/ops

```bash
# Run from cartracker-airflow-apiserver or any container with airflow CLI
docker exec -it cartracker-airflow-apiserver airflow dags pause flush_silver_observations
docker exec -it cartracker-airflow-apiserver airflow dags pause compact_silver
docker exec -it cartracker-airflow-apiserver airflow dags pause flush_staging_events
```

Verify no DAG run is currently active for these in the Airflow UI before continuing.

#### Step 4 — Final flush: drain staging.silver_observations → old prefix

```bash
curl -s -X POST http://localhost:8001/flush/silver/run \
  | python -c "import sys,json; r=json.load(sys.stdin); print(f'flushed={r[\"flushed\"]} error={r[\"error\"]}')"
# Expect: flushed=N error=None
```

This writes any remaining buffered rows to `silver/observations/` (old prefix, intentionally).

#### Step 5 — Final compact: clean up any uncompacted day partitions in old prefix

```bash
curl -s -X POST http://localhost:8001/compact/silver/run \
  | python -c "import sys,json; r=json.load(sys.stdin); print(f'compacted={r[\"compacted\"]} failed={r[\"failed\"]}')"
# Expect: failed=0
```

Skip this step if compact_silver ran successfully within the last 24 hours and no
new part files have appeared since.

#### Step 6 — Final delta rewrite

Rewrite any partitions written to the old layout since the Phase 5 rewrite ran.
This is the last data movement before the cutover.

```bash
# Rewrite any new silver partitions
docker exec -it cartracker-processing python scripts/rewrite_parquet_layout.py \
  --dataset silver_observations --apply \
  --baseline-audit /tmp/audit_before_normalize.json \
  --json-out /tmp/final_delta_rewrite_silver.json

# Rewrite any new ops partitions
for table in price_observation_events vin_to_listing_events \
             blocked_cooldown_events detail_scrape_claim_events artifacts_queue_events; do
  docker exec -it cartracker-processing python scripts/rewrite_parquet_layout.py \
    --dataset $table --apply \
    --json-out /tmp/final_delta_rewrite_${table}.json
done
```

#### Step 7 — Final dual-read check

Run Gate 2 SQL again. Must still return 0 rows. If it returns rows, investigate
the delta rewrite output before proceeding.

#### Step 8 — Deploy archiver with updated writer prefixes

```bash
# On the production server, in the repo root:
git pull
docker compose build archiver
docker compose up -d --no-deps archiver
```

Wait for the archiver to pass `/health`:
```bash
curl -s http://localhost:8001/health
# Expect: {"status": "ok"}
```

**From this point**, new flush runs write to `silver_normalized/` and `ops_normalized/`.
Old prefixes receive no new writes. The window between Step 5 and this step is the
longest gap where staging might accumulate; it is bounded by the deploy time (~60s).

#### Step 9 — Deploy dbt_runner with updated sources.yml

```bash
docker compose build dbt_runner
docker compose up -d --no-deps dbt_runner
```

Verify the dbt container started cleanly:
```bash
docker logs cartracker-dbt-runner --tail 20
```

#### Step 10 — Validate with a controlled flush (deploy intent still pending)

While deploy intent is still set, DAG sensors block scheduled flushes. Use that
window to trigger a single manual flush and verify it writes to the new prefix
before any normal scheduling resumes.

```bash
# Manually trigger one flush — bypasses DAG scheduler, does not require intent release
curl -s -X POST http://localhost:8001/flush/silver/run \
  | python -c "import sys,json; r=json.load(sys.stdin); print(f'flushed={r[\"flushed\"]} error={r[\"error\"]}')"
# Expect: flushed=N error=None

# Confirm the flush wrote to the normalized prefix (or logged nothing to write)
docker logs cartracker-archiver --tail 30 2>&1 | grep "flush_silver:"
# Expect: "flush_silver: wrote N rows → silver_normalized/observations"
#
# Note: if flushed=0, this does NOT prove the writer targets the right prefix.
# It only proves the endpoint responds without error. If staging was empty,
# the log line may say "nothing to flush" rather than confirming the destination.
# In that case, the dbt build below is the primary validation signal. If you
# need a stronger writer test, insert a synthetic staging row or wait for a
# natural flush after staging accumulates from scraper traffic.

# Run dbt build against the new source globs
docker exec -it cartracker-dbt-runner dbt build
# Expect: all models pass

# Also flush staging events — validates ops_normalized/ writer
curl -s -X POST http://localhost:8001/flush/staging/run \
  | python -c "import sys,json; r=json.load(sys.stdin); print(r)"
# Expect: total_flushed=N (may be 0) error=None

# Confirm ops event tables wrote to the normalized prefix (or logged nothing to write)
docker logs cartracker-archiver --tail 50 2>&1 | grep "flush_staging:"
# Expect any non-empty tables to log ops_normalized/<table>
#
# Note: same flushed=0 caveat applies — if all staging tables were empty,
# this only proves the endpoint responds. dbt build below is the primary signal.

# Run dbt build against the new source globs
docker exec -it cartracker-dbt-runner dbt build
# Expect: all models pass

# Confirm old prefixes received no new objects during this window
docker exec -it cartracker-processing python -c "
from shared.minio import get_s3fs, BUCKET
fs = get_s3fs()
prefixes = [
    'silver/observations',
    'ops/price_observation_events',
    'ops/vin_to_listing_events',
    'ops/artifacts_queue_events',
    'ops/detail_scrape_claim_events',
    'ops/blocked_cooldown_events',
]
for prefix in prefixes:
    try:
        count = len(fs.ls(f'{BUCKET}/{prefix}', detail=False))
        print(f'{prefix}: {count} objects (should not have grown since Step 5)')
    except FileNotFoundError:
        print(f'{prefix}: not found')
"
```

#### Step 11 — Re-enable Airflow DAGs and release deploy intent

All validation passed. Re-enable scheduling, then release the deploy intent so the
ops service resumes accepting claim requests.

```bash
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_silver_observations
docker exec -it cartracker-airflow-apiserver airflow dags unpause compact_silver
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_staging_events

curl -sf -X POST http://localhost:8060/deploy/complete
# Returns true. Claim traffic resumes. Next scheduled flush will also write to silver_normalized/.
```

### Rollback paths

#### Before Step 8 (archiver not yet deployed)

```bash
# Re-enable DAGs — no code was deployed, no data was changed
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_silver_observations
docker exec -it cartracker-airflow-apiserver airflow dags unpause compact_silver
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_staging_events

# Release deploy intent
curl -sf -X POST http://localhost:8060/deploy/complete
# Done — old prefix resumes writes, dbt still reads old prefix
```

#### After Step 8, before Step 9 (archiver deployed, dbt not yet switched)

```bash
# Restore previous processor files and rebuild archiver
git checkout HEAD~ -- archiver/processors/flush_silver_observations.py \
                      archiver/processors/compact_silver.py \
                      archiver/processors/flush_staging_events.py
docker compose build archiver
docker compose up -d --no-deps archiver

# Re-enable DAGs
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_silver_observations
docker exec -it cartracker-airflow-apiserver airflow dags unpause compact_silver
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_staging_events

# Release deploy intent
curl -sf -X POST http://localhost:8060/deploy/complete
# dbt still reads old prefix — no dbt action needed
```

Note: any rows flushed to `silver_normalized/` between Steps 8 and the rollback
will not appear in dbt queries (dbt still reads `silver/`). These rows are not
lost — they are in MinIO. Run `rewrite_parquet_layout.py --dry-run` after rollback
to confirm no gap exists, then schedule a follow-up delta rewrite if needed.

#### After Step 9, before Step 10 (both deployed, no flush triggered yet)

No rows have been written to `silver_normalized/` yet. Old prefix is complete.

```bash
# Restore previous sources.yml and rebuild dbt_runner
git checkout HEAD~ -- dbt/models/sources.yml
docker compose build dbt_runner
docker compose up -d --no-deps dbt_runner

# Restore previous processor files and rebuild archiver
git checkout HEAD~ -- archiver/processors/flush_silver_observations.py \
                      archiver/processors/compact_silver.py \
                      archiver/processors/flush_staging_events.py
docker compose build archiver
docker compose up -d --no-deps archiver

# Re-enable DAGs
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_silver_observations
docker exec -it cartracker-airflow-apiserver airflow dags unpause compact_silver
docker exec -it cartracker-airflow-apiserver airflow dags unpause flush_staging_events

# Release deploy intent
curl -sf -X POST http://localhost:8060/deploy/complete
# Old prefix has complete data — nothing was written to normalized prefix
```

#### After Step 10 with flushed=0 and total_flushed=0

Both flushes ran but wrote nothing (silver staging and all ops staging tables
were empty). Old prefixes are still complete. Rollback procedure is identical
to "before Step 10" above — nothing was written to any normalized prefix.

#### After Step 10 with flushed>0 (rows written to silver_normalized/ or ops_normalized/)

**Do not revert dbt to old globs and claim the old prefixes are complete.** Any
rows flushed by `/flush/silver/run` or `/flush/staging/run` were deleted from
staging and written only to `silver_normalized/` or `ops_normalized/<table>`.
Reverting dbt to `silver/` and `ops/` would silently drop those rows from all
queries.

**Option A — Roll forward (preferred if the problem is isolated):**

Keep dbt on `silver_normalized/`. Diagnose and fix the specific failure in the
new archiver or sources.yml, redeploy only the broken component, and re-run
Step 10 validation. The normalized data already written is valid and queryable.

```bash
# dbt is already on normalized globs — leave it there
# Fix the specific issue, rebuild only the affected service
docker compose build <service>
docker compose up -d --no-deps <service>
# Re-run Step 10 validation
```

**Option B — Emergency delta rewrite (only if roll-forward is not viable):**

Copy the normalized-only delta back into the old layout before reverting dbt.
`rewrite_parquet_layout.py` does not support this direction — it only writes
forward to the normalized prefix. This requires a one-off recovery script or
a future `--from-prefix / --to-prefix / --delta-since` mode that does not
exist yet.

Steps at a conceptual level:
1. Identify all Parquet files written to `silver_normalized/observations/` and
   each `ops_normalized/<table>/` after Step 5's final compact (use the Step 5
   log timestamp as the lower bound on object `LastModified`).
2. For each dataset, read the delta files with DuckDB or PyArrow, repartition
   into the old layout, and write back: silver → `silver/observations/` with
   `source=X/obs_year=Y/obs_month=M/obs_day=D/` partitioning; ops tables →
   `ops/<table>/` with their original partition scheme.
3. Verify row counts for each dataset match the pre-cutover baseline plus the
   delta row count from the flush logs.
4. Only after verification, revert archiver and dbt, rebuild, re-enable DAGs,
   and release deploy intent.

**Do not attempt Option B under time pressure.** If the situation requires
fast recovery, keep dbt on `silver_normalized/` (Option A) and treat the
old-prefix revert as a scheduled follow-up once a proper recovery script
exists.

### Tests

**Archiver unit tests** (existing test files, add coverage for prefix change):

| Test | Assert |
|------|--------|
| `test_flush_silver_writes_to_normalized_prefix` | Mock s3fs; assert `root_path` contains `silver_normalized/observations` |
| `test_compact_silver_targets_normalized_prefix` | Mock s3fs; assert `_MINIO_PREFIX` is `silver_normalized/observations` |
| `test_compact_silver_discovers_month_partitions` | Month-level silver partitions are discovered and day-level traversal is not required |
| `test_flush_staging_events_writes_to_normalized_prefix` | Mock s3fs; assert each table uses `ops_normalized/{table}` prefix |

**Airflow DAG integrity tests** (`tests/integration/airflow/test_dag_integrity.py`):

| Test | Assert |
|------|--------|
| `test_hourly_analytics_refresh_order` | DAG task order is `flush_silver` → `flush_staging_events` → `dbt_build` |
| `test_standalone_flush_dags_not_scheduled_frequently` | Standalone flush DAGs are manual-only or not scheduled at 5/15 minute cadence |
| `test_dbt_build_not_racing_flushes` | Hourly dbt build is not independently scheduled at the same time as the orchestrated refresh |

**Integration test** (`tests/integration/archiver/test_storage_layout_integration.py`):

- Seed MinIO with old-layout data; run full rewrite (Phase 5); run dual-read check;
  assert zero diff; update `_MINIO_PREFIX` configs; assert next flush writes to
  normalized prefix and produces readable rows in DuckDB.

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
docker exec -it cartracker-processing python scripts/cleanup_old_parquet_layout.py \
  --prefix silver/observations/ \
  --dry-run

# After at least 7 days of stable normalized reads:
docker exec -it cartracker-processing python scripts/cleanup_old_parquet_layout.py \
  --prefix silver/observations/ \
  --apply \
  --json-out /tmp/cleanup_silver_old_layout.json

# Verify old prefix is empty
docker exec -it cartracker-processing python -c "
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
| 1 | `cartracker-scraper`, `cartracker-processing` | No | Yes — level 9 | No |
| 2 | `cartracker-processing` (script run only) | No | No | No |
| 3 | `cartracker-processing` (script run only) | No | No | No |
| 4 | No code | No | No | No |
| 5 | `cartracker-processing` (script run only) | No | No | No |
| 6 | `cartracker-archiver`, `cartracker-dbt-runner`, Airflow DAG deploy | No | No | Yes — source switch + ordered refresh |
| 7 | `cartracker-processing` (script run only) | No | No | No |

Phase 6 rebuilds the archiver because `flush_silver_observations.py`,
`compact_silver.py`, and `flush_staging_events.py` all live inside it and
all change their target prefixes in that phase. It also deploys Airflow DAG
changes so Parquet flushes run as part of the hourly analytics refresh instead
of independent 5/15 minute schedules.

### Monitoring during Phase 1 rollout

After deploying Phase 1 (`ZSTD_LEVEL = 9`):

```bash
# Confirm scraper write logs show compressed_bytes are smaller than raw_bytes
docker logs cartracker-scraper 2>&1 | grep "write_html:" | head -20

# Watch for any scraper throughput regression
# Check Grafana: scraper artifacts/min panel — should remain stable
```

### Phase 5/6 deploy sequencing

Phase 5 (rewrite) runs from the processing container in the background. It writes
only to `silver_normalized/` which nothing reads yet — safe to run at any time.

Phase 6 (writer + reader cutover) is a ~20-minute coordinated window documented in
full in the Phase 6 cutover sequence above. Do not treat it as a simple config deploy.
The key constraints:

- All three pre-cutover gates must pass first.
- Airflow DAGs must be paused before the final flush runs.
- Archiver and dbt_runner must deploy within the same operator session.
- `dbt_build` runs at `0 * * * *` (hourly) — begin the cutover immediately after a
  successful dbt run to maximize lead time before the next scheduled run.

### Rollback steps (per phase)

| Phase | Rollback |
|-------|---------|
| 1 | Revert `ZSTD_LEVEL = 9 → 3`, rebuild scraper + processing |
| 5 | Delete `silver_normalized/` prefix (no readers have switched yet) |
| 6 (before archiver deploy) | Re-enable paused DAGs — no code to revert |
| 6 (after archiver, before dbt) | Revert archiver image, re-enable DAGs |
| 6 (after both deployed) | Revert sources.yml + archiver, redeploy both, re-enable DAGs |
| 7 | Cannot undo deletion. This is why Phase 7 runs only after ≥7 days stable. |

### Quiet window recommendations

| Phase | Recommendation |
|-------|----------------|
| 1 | Preferred off-peak; not required |
| 5 | Any time; concurrent scraping is fine |
| 6 | Off-peak window; ~20 min; require deploy intent; start after a clean dbt run |
| 7 | Off-peak; irreversible step |

---

## Phase 9: Implementation Order

Safe PR/commit sequence:

| PR | Contents | Can start when |
|----|----------|---------------|
| 1 | This implementation plan (docs only) | Now ✓ |
| 2 | Phase 1: `shared/minio.py` + `tests/shared/test_minio.py` | Phase 0 checklist complete |
| 3 | Phase 3: `scripts/audit_parquet_layout.py` + tests | Phase 1 deployed and stable |
| 4 | Phase 5: `scripts/rewrite_parquet_layout.py` + tests | Phase 3 audit reviewed; Phase 4 layout decision confirmed in this doc |
| 5 | Phase 6 code: flush/compact/sources changes + new archiver/dbt tests | Phase 5 verification report passes all gates |
| 6 | Phase 6 cutover: operator execution (not a PR — a deploy window) | PR 5 merged; all pre-cutover gates pass |
| 7 | Phase 7: `scripts/cleanup_old_parquet_layout.py` + tests | Phase 6 cutover stable for ≥7 days |
| 8 | Phase 2 (optional): `scripts/recompress_bronze_html.py` + tests | Any time; not on critical path |

Note: Phase 4 is resolved by this document: use month-level normalized layout
and align Parquet flushes with the hourly analytics refresh before PR 4 begins.

---

## Testing Matrix

| Phase | Unit tests | Integration tests | Manual validation |
|-------|-----------|-------------------|-------------------|
| 1 | `tests/shared/test_minio.py` | Roundtrip read after level-9 write | `docker logs cartracker-scraper \| grep write_html` |
| 2 | `tests/scripts/test_recompress_bronze_html.py` | Seed MinIO, dry-run asserts no change | Preview command on one month prefix |
| 3 | `tests/scripts/test_audit_parquet_layout.py` | Seed MinIO, check report counts | Run against production, review Markdown report |
| 5 | `tests/scripts/test_rewrite_parquet_layout.py` | Seed old-layout MinIO, rewrite, check rows | DuckDB row count cross-check |
| 6 | Archiver prefix tests; Airflow ordering tests; dbt model tests | Dual-read SQL; `test_storage_layout_integration.py` | Full cutover sequence; watch archiver logs; `hourly_analytics_refresh` passes |
| 7 | `tests/scripts/test_cleanup_old_parquet_layout.py` | `test_storage_layout_integration.py` | Dry-run candidate count reviewed before apply |

All unit tests: no real MinIO required. All integration tests: require a MinIO
test instance (already used by existing integration tests).

---

## Open Questions

1. **Memory ceiling for rewrite in cartracker-processing.** The rewrite script reads
   one full partition into memory. Carousel is the largest source (~5.4 MB compacted
   per day post-Plan 109). Monthly total for carousel is ~167 MB compacted; decompressed
   is ~310 MB. If rewriting month-sized chunks, confirm the processing container's
   memory limit accommodates this. If not, use `--month` selector to rewrite one month
   at a time (which is the default guidance in Phase 5 runbook).

2. **Hourly flush thresholds.** Hourly pre-dbt flush is the target, but production
   may need a safety valve: flush early if staging rows exceed N or oldest unflushed
   row age exceeds X minutes. Decide the initial thresholds after checking staging
   volume during one normal dbt interval.

3. **Staging observability implementation.** Minimum requirement is alerting on row
   count, oldest unflushed row age, and flush failures. Decide whether to expose this
   through existing Prometheus gauges, dbt freshness checks, or a small archiver/ops
   endpoint before Phase 6 deploy.

4. **Checkpoint state for Phase 2 (recompression): local JSON or MinIO object?**
   Phase 2 is deprioritized and run manually, so local JSON is sufficient. If it is
   ever operationalized as a scheduled job, move checkpoint to a well-known MinIO path
   (e.g., `bronze/checkpoints/recompress_bronze_html.json`).
