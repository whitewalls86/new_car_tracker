# Plan 109: Silver Parquet Compaction

**Status:** PLANNED  
**Date:** 2026-06-30  
**Branch:** (TBD)

## Background

The silver flush job runs every 5 minutes and creates one `part-<uuid>-0.parquet` per flush. As of June 2026, this produces ~150 files per source per day across 3 sources (detail, carousel, srp). DuckDB pays per-file overhead on every query, and small files compress worse because RLE runs can't span file boundaries.

June 15 2026 empirical analysis confirmed:
- **Detail** (37K rows/day): optimal sort saves 15% vs unsorted, 8 MB → 2.6 MB
- **Carousel** (280K rows/day): optimal sort saves 37% vs unsorted, 8.5 MB → 5.4 MB  
- **SRP** (7K rows/day): too small/uniform — sort is neutral; gains come from day-merging alone

**Optimal sort key (confirmed empirically):**
```
make → model → dealer_state → dealer_name → year → trim → listing_id
```
This interleaves the vehicle taxonomy hierarchy with the dealer geography hierarchy, then places the higher-conditional-cardinality columns last. SRP uses the same key (neutral effect, no harm).

## Goal

Add a `compact_silver` step to the archiver that merges all per-flush `part-*.parquet` files for completed days into a single sorted `compacted-<YYYY-MM-DD>.parquet` per partition. The step runs as an Airflow DAG on a daily cadence.

## Scope

**In scope:**
- `silver/observations` partitions only (all 3 sources)
- Completed days with a 2-day watermark (see below)
- New processor, endpoint, DAG, and tests

**Out of scope:**
- `silver/ops` event tables (different schema, different access patterns, not analyzed)
- Any change to the flush job or flush cadence
- CDC / content-defined chunking (assessed and deferred — requires CAS backend)

---

## Watermark

**Only compact partitions where `obs_day <= today_utc - 2` (two days ago and older).**

The flush job partitions by `fetched_at`, not by flush time. A listing that was scraped on day D but whose HTML was processed late can legitimately produce a new `part-*.parquet` in day D's partition well after midnight UTC. A 2-day buffer ensures any plausible backfill has landed before we compact. Compacting yesterday's data (1-day watermark) is too aggressive.

---

## Partition State Machine

A partition (one source × one calendar day) is classified on each run:

| State | Condition | Action |
|-------|-----------|--------|
| **Needs compaction** | Only `part-*.parquet` files present | Full compaction |
| **Incremental** | `compacted-*.parquet` AND `part-*.parquet` both present | New part files arrived after previous compaction (legitimate late write). Re-compact: read existing compacted file + new part files → write new compacted file → delete all originals. |
| **Done** | Only `compacted-*.parquet` present | Skip |
| **Empty** | No parquet files at all | Skip |

The "Incremental" state is the result of a late-arriving flush, not necessarily a failed compaction run. Treating it as "just delete the part files" would silently drop observations. Re-compacting is safe because it reads all data before deleting any of it.

---

## Reader-Safe Write Sequence

DuckDB reads all `*.parquet` in a partition directory. If we write `compacted-*.parquet` before deleting `part-*.parquet`, any concurrent dbt read sees both sets of files and double-counts every row. The fix is a staged write that avoids double-counting at the cost of a brief zero-row window (milliseconds):

**Full compaction (Needs compaction):**
```
1. Read all part-*.parquet into memory
2. Sort and write → compacted-<date>.parquet.tmp  (does NOT match *.parquet glob)
3. Assert tmp row count == sum of source row counts  ← pre-delete safety check
4. Delete all part-*.parquet  (partition is empty — no double-count possible)
5. fs.rename(compacted-<date>.parquet.tmp → compacted-<date>.parquet)
```

**Incremental compaction (Incremental state):**
```
1. Read existing compacted-<date>.parquet + all new part-*.parquet
2. Sort and write → compacted-<date>.parquet.tmp
3. Assert tmp row count == compacted rows + sum of new part rows  ← pre-delete safety check
4. Delete existing compacted-<date>.parquet
5. Delete all part-*.parquet  (partition is empty)
6. fs.rename(compacted-<date>.parquet.tmp → compacted-<date>.parquet)
```

Steps 4–6 (full) or 4–6 (incremental) leave the partition empty for the duration of the MinIO copy+delete that backs `fs.rename`. In practice this is O(milliseconds) for a <10 MB file. A query during this window returns zero rows for that partition, which is always preferable to double-counting.

**If `fs.rename` fails after originals are deleted (step 5/6):** the partition is empty and the data is not in MinIO. The `.tmp` file should still exist on MinIO. Recovery path:

1. The processor catches this exception and logs `ERROR` with the `.tmp` path.
2. On the next run the partition is in the "Empty" state (no `*.parquet`). The processor detects Empty and skips rather than erroring blindly.
3. Manual recovery: `fs.rename(compacted-<date>.parquet.tmp → compacted-<date>.parquet)` on the MinIO path logged in the error. This should be documented in the runbook.
4. If the `.tmp` file is also gone (double failure), the day's observations are unrecoverable from MinIO. The bronze HTML for those artifacts is the upstream source; re-triggering the processing pipeline for that date is the recovery path. This scenario requires two independent failures (write + rename) and is considered acceptable risk.

The pre-delete row count assertion (step 3) is the primary guard: if the `.tmp` write is corrupt or truncated, we never delete the originals.

Note: `s3fs.rename()` is a copy-then-delete internally — it is not atomic. The `.tmp` extension ensures the temp file is invisible to `*.parquet` glob readers throughout.

---

## Scheduling and dbt Collision

`dbt_build` runs at `schedule="0 * * * *"` (every hour, minute 0). The compaction DAG must not run at minute 0 to avoid dbt reading a partition mid-compaction.

**DAG schedule: `10 4 * * *` (4:10 AM UTC)**

This gives a 50-minute buffer before the 5:00 AM dbt run and a 70-minute buffer after the 3:00 AM `cleanup_parquet` run. Each compaction partition completes in seconds; 10 partitions finishes well within this window.

---

## Concurrency with the Flush Job

The flush job (`*/5 * * * *`) and compaction (`10 4 * * *`) can run simultaneously. The existing `active_job()` counter and `/ready` endpoint do NOT prevent this — `flush_silver_observations` uses `http_health_sensor` which polls `/health` (always 200), not `/ready`.

**Concurrent runs are safe under the 2-day watermark assumption.** The flush job partitions by `fetched_at`, which is typically today or yesterday, so `obs_day <= today - 2` partitions are usually disjoint from active flush targets. This is a practical guarantee, not a hard one — a deep backfill or reprocessing run could write to an older partition. When that happens, the incremental compaction state handles it correctly on the next run: the newly arrived part files are detected and merged into a fresh compacted file.

The only additional risk from concurrency is memory pressure: carousel compaction peaks at ~30 MB in-memory per partition; flush peaks at a few MB per batch. This is acceptable.

No locking is needed. Document the watermark assumption in the processor docstring so future maintainers understand the invariant they are relying on.

---

## Run-level Controls

### Backlog rate limit
`max_partitions_per_run` (default: **10**, configurable via env var `COMPACT_SILVER_MAX_PARTITIONS`).

Partitions are processed **oldest-first** so the backlog drains chronologically. The first run will find ~60 days × 3 sources = ~180 eligible partitions; at 10/run this clears in ~18 daily runs.

### Discovery order
1. List all `obs_year / obs_month / obs_day` directories across all 3 sources
2. Apply 2-day watermark filter
3. Classify state; skip Done and Empty
4. Sort ascending by (year, month, day, source)
5. Take first `max_partitions_per_run` items

---

## Files

### New
| File | Purpose |
|------|---------|
| `archiver/processors/compact_silver.py` | Processor: discovery, state classification, compaction loop |
| `airflow/dags/compact_silver.py` | Airflow DAG: daily at 04:10 UTC |
| `tests/archiver/test_compact_silver.py` | Unit tests |
| `tests/integration/archiver/test_compact_silver_integration.py` | MinIO integration tests |

### Modified
| File | Change |
|------|--------|
| `archiver/app.py` | Add `POST /compact/silver/run` endpoint |
| `tests/archiver/test_app.py` | Add endpoint smoke test |
| `tests/integration/airflow/test_dag_integrity.py` | Add `compact_silver` to DAG specs |

---

## Processor Interface

```python
# archiver/processors/compact_silver.py

MAX_PARTITIONS = int(os.environ.get("COMPACT_SILVER_MAX_PARTITIONS", "10"))

SORT_COLS = ["make", "model", "dealer_state", "dealer_name", "year", "trim", "listing_id"]

def compact_silver(max_partitions: int = MAX_PARTITIONS) -> dict:
    """
    Discover and compact up to max_partitions completed silver observation partitions.

    Only processes obs_day <= today_utc - 2 (2-day watermark against late-arriving flushes).
    Concurrent flush runs are safe under the 2-day watermark assumption; late backfills that
    land in an already-compacted partition are handled by incremental compaction on the next run.

    Returns:
    {
        "scanned": int,
        "compacted": int,         # full compaction (Needs compaction state)
        "incremental": int,       # re-compacted (Incremental state — late part files)
        "skipped": int,           # already Done
        "failed": int,
        "size_before_mb": float,
        "size_after_mb": float,
        "error": str | None,
        "partitions": [
            {
                "source": str,
                "date": str,              # YYYY-MM-DD
                "state": str,             # needs_compaction | incremental | done | empty
                "files_merged": int,
                "rows": int,
                "size_before_bytes": int,
                "size_after_bytes": int,
                "ok": bool,
                "error": str | None,
            }
        ]
    }
    """
```

---

## Endpoint

```
POST /compact/silver/run
```

No request body. Returns the processor dict. Wrapped in `active_job()`. Follows the same pattern as `/cleanup/parquet/run`.

---

## Airflow DAG

```python
dag_id:   compact_silver
schedule: "10 4 * * *"   # 4:10 AM UTC — avoids dbt at :00 and cleanup_parquet at 3:00
catchup:  False
tags:     ["maintenance"]

tasks: ready >> archiver_up >> compact_silver
```

---

## Logging

All log lines use `logger = logging.getLogger("archiver")`.

| Event | Level | Message |
|-------|-------|---------|
| Partition already done | DEBUG | `compact_silver: skip source=X date=Y (already compacted)` |
| Incremental compaction | INFO | `compact_silver: incremental source=X date=Y, N new part files since last compaction` |
| Compaction start | INFO | `compact_silver: compacting source=X date=Y, N files, M rows` |
| Compaction success | INFO | `compact_silver: done source=X date=Y, before=A bytes, after=B bytes (-C%)` |
| File delete failure | WARNING | `compact_silver: failed to delete path=X: error` |
| Partition failure | ERROR | `compact_silver: partition failed source=X date=Y: error` (exc_info=True) |
| Run summary | INFO | `compact_silver: run complete — compacted=N incremental=M failed=K` |

---

## Testing Plan

### Unit tests — `tests/archiver/test_compact_silver.py`

All use in-memory PyArrow tables + mocked s3fs. No real MinIO required.

| Test | What it verifies |
|------|-----------------|
| `test_full_compaction_happy_path` | Reads N part files, writes .tmp, deletes parts, renames to compacted; returns correct counts and sizes |
| `test_sort_order_applied` | Output rows are sorted by SORT_COLS; nulls last |
| `test_sort_cols_absent_from_schema_handled` | srp has cardinality-1 dealer columns — excluded cols don't error |
| `test_skips_done_partition` | Only `compacted-*.parquet` present → skipped, no reads or writes |
| `test_skips_empty_partition` | No parquet files → skipped gracefully |
| `test_incremental_compaction` | Compacted + part files both present → re-reads both, writes new compacted, deletes all originals |
| `test_watermark_excludes_yesterday` | `obs_day == today - 1` is NOT included in discovery |
| `test_watermark_includes_two_days_ago` | `obs_day == today - 2` IS included |
| `test_max_partitions_respected` | 20 eligible partitions → only 10 processed |
| `test_oldest_first_ordering` | Processing order is ascending by (year, month, day, source) |
| `test_failed_partition_does_not_abort_run` | One partition errors → others still processed; error count incremented |
| `test_tmp_preserved_for_manual_recovery` | If rename fails after originals are deleted, the .tmp file is left in place (not cleaned up) and its path is logged at ERROR so it can be manually renamed as the recovery path |

### Integration tests — `tests/integration/archiver/test_compact_silver_integration.py`

Require real MinIO (test fixture or live). Seed MinIO with known data before each test, assert post-state.

| Test | What it verifies |
|------|-----------------|
| `test_end_to_end_compaction` | Seed N part files → run compact_silver → assert only one `compacted-*.parquet` exists, row count matches, no `*.tmp` left |
| `test_idempotent_second_run` | Run twice on same partition → second run sees Done, no re-write |
| `test_incremental_compaction_end_to_end` | Seed part files → compact → seed new part files → compact again → assert rows from both batches in final file, no duplicates |
| `test_no_double_count_during_compaction` | (If feasible) concurrent read during compaction sees all-original rows, zero rows (during the delete→rename window), or all-compacted rows — never a mix of original and compacted that would double-count |
| `test_tmp_file_not_visible_to_parquet_glob` | During write, assert `*.parquet` glob does not return the `.tmp` file |

### Endpoint tests — `tests/archiver/test_app.py` (addition)

| Test | What it verifies |
|------|-----------------|
| `test_post_compact_silver_run_returns_200` | `POST /compact/silver/run` returns 200 with expected JSON keys |
| `test_ready_returns_503_while_compact_active` | `GET /ready` returns 503 while `active_job()` counter is non-zero (i.e., during a compaction run); returns 200 once it completes. Note: `active_job()` is a counter, not a mutex — two concurrent endpoint calls are not prevented, only reflected in the `/ready` state. |

### DAG integrity — `tests/integration/airflow/test_dag_integrity.py` (addition)

Add `compact_silver` to the existing `DAG_SPECS` list with `schedule="10 4 * * *"` and expected task count.

---

## dbt Source Compatibility

The dbt silver source reads `silver/observations/**/*.parquet` ([dbt/models/sources.yml line 17](../dbt/models/sources.yml)). After compaction, a partition contains only `compacted-<date>.parquet`, which this glob correctly finds. No dbt config change is needed.

The integration test `test_no_double_count_during_compaction` (above) is the guard against a regression here — if the `.tmp` staging ever leaks into the glob, this test fails.

---

## Open Questions (pre-implementation)

1. **Metrics follow-up**: `size_before_mb` / `size_after_mb` per run are good candidates for a Grafana panel. Defer to Plan 110 (Grafana alerting).
