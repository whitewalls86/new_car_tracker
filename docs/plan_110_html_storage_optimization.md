# Plan 110: HTML Storage Baseline + Safe Hygiene

## Goal

Stabilize raw HTML storage growth without reducing the retention window yet.

Plan 114 may let us keep raw-page auditability for much longer by replacing
eligible full HTML objects with verified section manifests. Until that is
tested, this plan should avoid a hard 30-day deletion policy.

This plan does three conservative things:

1. Improve compression for newly written HTML.
2. Add observability for raw HTML storage cost.
3. Repair cleanup plumbing only enough to support manual/emergency deletion.

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

---

## Track A - Bump Compression Level

**File:** `shared/minio.py`

```python
# before
ZSTD_LEVEL = 3

# after
ZSTD_LEVEL = 9
```

Expected gain: 15-25% further size reduction on new HTML writes. Existing
objects remain readable because zstd frames are self-describing.

**Tradeoff:** Compression happens in the scraper fetch path. Level 9 is slower
than level 3, but HTML objects are small relative to network fetch time. Watch
scraper throughput after rollout.

---

## Track B - Storage Observability

Add lightweight metrics at HTML write time so future storage decisions do not
require expensive retrospective MinIO scans.

Suggested fields to emit as a staging event or structured log:

| Field | Purpose |
|-------|---------|
| `artifact_type` | Separate `detail_page` and `results_page` behavior |
| `listing_id` | Detail-page grouping key |
| `raw_bytes` | Pre-compression size |
| `compressed_bytes` | Actual storage cost |
| `raw_sha256` | Whole-file duplicate measurement |
| `minio_path` | Traceability |
| `fetched_at` | Time-window analysis |
| `http_status` | Separate successful pages from blocks/interstitials |

Prefer append-only events or metrics logs. Do not add a permanent dedup index
until Plan 114 proves a storage strategy worth indexing.

---

## Track C - Cleanup Plumbing, Not Automatic Retention

`cleanup_artifacts` may continue calling `/cleanup/parquet/run` for
compatibility, but the implementation should not automatically delete raw HTML
on a 30-day window yet.

Instead, implement cleanup as a guarded operation:

1. Support explicitly supplied MinIO prefixes or object keys.
2. Report candidate object count and bytes before deletion.
3. Require an explicit `dry_run=false` style flag before deleting.
4. Treat missing objects as already deleted.
5. Report `total`, `deleted`, `failed`, and bytes affected.

This gives us operational safety if storage pressure becomes urgent, while
preserving data for the Plan 114 section-level audit.

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
- Cleanup dry-run returns candidate counts without deleting objects.
- Explicit cleanup deletes supplied keys/prefixes only when deletion is enabled.
- Missing objects are treated as already deleted.
- MinIO delete failures are reported without hiding partial failure.

### Integration Tests

- Seed MinIO with HTML objects, run dry-run cleanup, assert no objects deleted.
- Run explicit cleanup for a test prefix, assert only that prefix is deleted.
- Verify cleanup response includes object counts, bytes, deleted, and failed.

---

## Files Changed

| File | Change |
|------|--------|
| `shared/minio.py` | `ZSTD_LEVEL` 3 -> 9 and optional storage metrics |
| `archiver/processors/cleanup_parquet.py` | Guarded explicit raw-object cleanup |
| `tests/shared/test_minio.py` | Compression and metrics coverage |
| `tests/archiver/test_cleanup_parquet.py` | Guarded cleanup coverage |
| `tests/integration/archiver/test_cleanup_html_integration.py` | MinIO cleanup integration coverage |

---

## Out of Scope

- Automatic 30-day raw HTML expiry.
- Exact whole-file HTML dedup.
- Sectioned/recomposable HTML storage. See Plan 114.
- Adaptive detail fetch scheduling. See Plans 111-113.
