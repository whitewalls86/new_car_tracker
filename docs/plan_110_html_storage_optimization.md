# Plan 110: Storage Layout Hygiene + Iceberg Readiness

## Goal

Normalize the storage layer before adaptive-refresh experiments depend on it.

This plan is no longer just "make HTML smaller." Plan 116 showed that zstd
level 9 saves a consistent but moderate 8-10% on existing bronze HTML. That is
worth doing as hygiene, but the larger goal is to make the object store clean
enough to support Apache Iceberg snapshots and MLflow-tracked refresh-policy
experiments in Plans 111-112.

This plan has two independent cleanup tracks:

1. Bring bronze HTML forward to the new compression/observability standard.
2. Audit and normalize silver/ops Parquet layout before Iceberg is introduced.

It does **not** introduce automatic 30-day raw HTML expiry.

---

## Evidence

### Semantic Duplication Is High

A DuckDB query over `silver/observations` grouped detail-page artifacts by a
parsed-state fingerprint:

| Metric | Value |
|--------|-------|
| Total detail artifacts | 5,804,559 |
| Unique parsed listing states | 804,870 |
| Semantically duplicate artifacts | 4,999,689 |
| Semantic duplicate rate | 86.13% |

Repeated detail fetches often produce the same business state.

### Whole-File HTML Hashing Is Weak

A targeted audit sampled five high-duplicate semantic groups and hashed the
stored compressed HTML blobs:

| Metric | Value |
|--------|-------|
| Sampled groups | 5 |
| Sampled artifacts | 25 |
| Groups with identical sampled HTML | 0 |
| Repeat compressed-hash matches | 0 |

The parsed vehicle state was identical, but full-file byte hashes differed.

### Diff Evidence Points To Volatile Page Regions

Follow-up diffs showed unchanged listings with only small volatile regions:

- `trip_id`
- CSRF tokens
- `dni_correlation_id`
- `facebook_event_id`
- `timestamp_utc`
- `page_instance_id`
- Cloudflare challenge params
- bot/edge-provider analytics metadata
- JSON key ordering

This means whole-file dedup is the wrong first storage optimization, but
section-level reuse may still be valuable. Plan 114 tests that.

### Plan 116 Recompression Evidence

Plan 116 sampled existing bronze HTML and recompressed in memory from the
current zstd level to level 9:

| Prefix | Savings |
|--------|---------|
| detail_page, June | 8.1% |
| results_page, June | 9.4% |
| detail_page, May | 8.0% |

All sampled runs completed with 0 failures.

Decision: level 9 is worth adopting as the new write standard. Historical
recompression is also reasonable as a bounded normalization step, not as the
primary storage strategy.

---

## Track A - New Bronze HTML Write Standard

**File:** `shared/minio.py`

```python
# before
ZSTD_LEVEL = 3

# after
ZSTD_LEVEL = 9
```

Expected gain: 8-10% further size reduction on new HTML writes based on Plan
116 production samples. Existing objects remain readable because zstd frames
are self-describing.

**Tradeoff:** Compression happens in the scraper fetch path. Level 9 is slower
than level 3, but HTML objects are small relative to network fetch time. Watch
scraper throughput after rollout.

---

## Track B - Bronze HTML Write Observability

Add lightweight metrics at HTML write time so future storage decisions do not
require expensive retrospective MinIO scans.

Suggested fields to emit as structured logs:

| Field | Purpose |
|-------|---------|
| `artifact_type` | Separate `detail_page` and `results_page` behavior |
| `raw_bytes` | Pre-compression size |
| `compressed_bytes` | Actual storage cost |
| `raw_sha256` | Whole-file duplicate measurement |
| `minio_path` | Traceability |
| `key` | Bare object key |

Do not add a permanent dedup index until Plan 114 proves a storage strategy
worth indexing.

---

## Track C - Historical Bronze HTML Recompression

Add a manual operator script that recompresses existing `.html.zst` objects to
the new level-9 standard.

This is explicitly **not** an Airflow job and not automatic retention cleanup.
It is a one-object-at-a-time normalization tool.

Required behavior:

1. Default to dry-run.
2. Accept explicit selectors: `--prefix` or `--year --month --artifact-type`.
3. Support `--limit`, `--max-bytes`, `--progress-every`, `--checkpoint`, and
   `--json-out`.
4. Download, decompress, recompress, and compare in memory.
5. In apply mode, overwrite only when the recompressed bytes are smaller unless
   `--force` is supplied.
6. Never delete objects.
7. Log progress and final totals: scanned, recompressed, skipped, failed, old
   bytes, new bytes, saved bytes, saved percent.
8. Keep a failure list and continue on corrupt/unreadable objects.
9. Be safe to interrupt; every object is rewritten independently.

Plan 116's estimate script remains the measurement tool. This track is the
apply tool.

---

## Track D - Parquet Lake Layout Audit

Before Iceberg is introduced, inventory the current MinIO Parquet layout:

| Dataset | Current area |
|---------|--------------|
| Silver observations | `silver/observations/**` |
| Price observation events | `ops/price_observation_events/**` |
| VIN mapping events | `ops/vin_to_listing_events/**` |
| Blocked cooldown events | `ops/blocked_cooldown_events/**` |
| Detail claim events | `ops/detail_scrape_claim_events/**` |
| Artifacts queue events | `ops/artifacts_queue_events/**` |

For each dataset, report:

- partition columns present in paths
- object count
- total bytes
- small-file distribution
- schema variants
- row counts
- minimum and maximum event/observation timestamps
- legacy or unexpected prefixes

This audit should produce a JSON/Markdown report and should be safe to run
without mutating MinIO.

---

## Track E - Canonical Pre-Iceberg Parquet Layout + Flush Cadence

Define the physical layout that Iceberg will be built on.

The intent is to stop treating day-partitioned object paths as the long-term
table contract. Day partitions were useful for append-only Parquet and DuckDB
globs, but Iceberg should own partition evolution and snapshot metadata.

Canonical layout:

```text
silver_normalized/observations/
    source=<source>/obs_year=<YYYY>/obs_month=<M>/
        part-*.parquet
        compacted-through-<YYYY-MM-DD>.parquet

ops_normalized/<event_table>/year=<YYYY>/month=<M>/
    part-*.parquet
    compacted-through-<YYYY-MM-DD>.parquet
```

Rationale:

- Month-level partitions reduce silver partition count without removing useful
  DuckDB/dbt pruning during the pre-Iceberg period.
- `source` remains a silver partition because SRP, detail, and carousel rows
  have different query patterns and row density.
- `fetched_at` / `event_at` ordering inside Parquet files provides row-group
  statistics for intra-month skipping.
- Iceberg still adds value later: snapshot commits, manifest-based file
  tracking, partition evolution, and safer compaction.

This track also changes the write cadence. Current DAGs flush silver every 5
minutes and ops events every 15 minutes, while `dbt_build` runs hourly. That
creates many small files before analytics can usually expose the data.

Target orchestration:

```text
processing continues every 5 minutes
hourly analytics refresh:
    flush_silver_observations
    flush_staging_events
    dbt_build
daily:
    compact active month / closed months
```

The key tradeoff is larger Postgres staging buffers and a larger delayed-data
window if a flush fails. Rows are not deleted from staging until Parquet writes
succeed, so the main risk is delayed analytics visibility, not normal-case data
loss. Add observability for staging row counts, oldest unflushed row age, and
flush failures.

---

## Track F - Safe Parquet Rewrite + Verification

Rewrite existing silver/ops Parquet into the canonical layout without deleting
the original layout initially.

Required sequence:

1. Read source dataset from the current layout.
2. Normalize schema and partition columns.
3. Write to a new normalized prefix.
4. Verify row counts by dataset/source/month.
5. Verify timestamp min/max by dataset/source/month.
6. Verify schema compatibility.
7. Run representative DuckDB/dbt reads against the normalized prefix.
8. Only after validation, update dbt sources or Iceberg registration to the new
   location.
9. Keep old layout until a manual cleanup decision is made.

This track should reuse the safety lessons from Plan 109 silver compaction:
write to new paths first, verify before switching readers, and avoid any window
where readers can double-count.

---

## Track G - Guarded Cleanup, Not Automatic Retention

After Track F verifies normalized Parquet, add guarded cleanup for old layouts:

1. Support explicitly supplied MinIO prefixes or object keys.
2. Report candidate object count and bytes before deletion.
3. Require an explicit `dry_run=false` style flag before deleting.
4. Treat missing objects as already deleted.
5. Report `total`, `deleted`, `failed`, and bytes affected.

This is for operator-controlled cleanup only. Do not introduce automatic raw
HTML expiry or automatic old-layout deletion.

---

## Deferred: Automatic Raw HTML Expiry

Do not implement a 30-day retention window until Plan 114 answers:

- Can section manifests reconstruct parser-equivalent HTML?
- How often does section extraction fail?
- How much storage would sectioned raw artifacts actually save?
- Which artifacts must keep full raw HTML indefinitely?

If Plan 114 succeeds, retention should become smarter than "delete after 30
days":

- Keep full raw HTML for recent artifacts and parse failures.
- Keep section manifests and content-addressed sections for longer windows.
- Delete full raw HTML only after successful manifest verification and a
  recovery grace period.

---

## Testing

### Unit Tests

- `write_html` uses the new zstd level and still returns `s3://bucket/key`.
- Storage metrics include raw bytes, compressed bytes, artifact type, and path.
- Recompression dry-run never calls write/delete APIs.
- Recompression apply overwrites only smaller recompressed objects.
- Recompression checkpoint/resume skips completed keys.
- Parquet layout audit reports files, bytes, schemas, and row counts.
- Guarded cleanup dry-run returns candidate counts without deleting objects.
- Explicit cleanup deletes supplied keys/prefixes only when deletion is enabled.
- Missing objects are treated as already deleted.
- MinIO delete failures are reported without hiding partial failure.

### Integration Tests

- Seed MinIO with HTML objects, run recompression dry-run, assert no objects
  changed.
- Run recompression apply for a test prefix, assert objects remain readable and
  only smaller objects were rewritten.
- Seed MinIO with current-layout Parquet, run layout audit, assert counts and
  schema report are correct.
- Rewrite a small silver fixture to normalized layout and verify row counts.
- DAG integrity verifies hourly analytics refresh runs flushes before dbt and
  disables/unschedules frequent standalone flushes.
- Run guarded cleanup for a test prefix, assert only that prefix is deleted.

---

## Files Changed

| File | Change |
|------|--------|
| `shared/minio.py` | `ZSTD_LEVEL` 3 -> 9 and storage metrics |
| `scripts/recompress_bronze_html.py` | New manual recompression apply tool |
| `scripts/audit_parquet_layout.py` | New read-only lake layout audit |
| `scripts/rewrite_parquet_layout.py` | New guarded normalized-layout rewrite |
| `airflow/dags/hourly_analytics_refresh.py` | New hourly flush-before-dbt orchestration |
| `airflow/dags/flush_silver_observations.py` | Remove frequent standalone schedule or make manual-only |
| `airflow/dags/flush_staging_events.py` | Remove frequent standalone schedule or make manual-only |
| `airflow/dags/dbt_build.py` | Avoid independent hourly run racing the orchestrated refresh |
| `archiver/processors/cleanup_parquet.py` | Guarded explicit old-layout cleanup |
| `tests/shared/test_minio.py` | Compression and metrics coverage |
| `tests/scripts/test_recompress_bronze_html.py` | Recompression coverage |
| `tests/scripts/test_audit_parquet_layout.py` | Parquet audit coverage |
| `tests/scripts/test_rewrite_parquet_layout.py` | Rewrite verification coverage |
| `tests/integration/airflow/test_dag_integrity.py` | Hourly refresh ordering and schedule coverage |
| `tests/archiver/test_cleanup_parquet.py` | Guarded cleanup coverage |
| `tests/integration/archiver/test_storage_layout_integration.py` | MinIO integration coverage |

---

## Out of Scope

- Automatic 30-day raw HTML expiry.
- Exact whole-file HTML dedup.
- Sectioned/recomposable HTML storage. See Plan 114.
- Apache Iceberg table registration. This plan prepares the layout; Plan 112
  introduces the experiment substrate.
- MLflow tracking server setup. See Plan 112.
- Adaptive detail fetch scheduling. See Plans 111-113.
