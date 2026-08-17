# Plan 137: Legacy Bronze Parquet Recovery and Disposition

## Status

**DRAFT — read-only inventory complete; no production writes or deletions are
authorized.** Written 2026-08-17 after investigating the April 2026 Parquet
objects under the bronze HTML prefix.

This is a one-time recovery and disposition plan. It does not make legacy
Parquet part of the supported bronze read path, and it does not define the
long-term raw-HTML retention policy that Plans 114, 129, 130, and 131 still
need.

## Decision in one paragraph

Do not keep all 1,299 objects merely because they survived the migration, and
do not delete the prefix as a unit. First remove only files that contain no
information or whose every non-empty row is byte-identical to verified packed
content. Then reconcile every remaining successful page with the permanent
silver record, giving special attention to the April 20–27 dual-write and
migration boundary. Preserve or recover anything that adds observations,
captures a real parser failure, or is selected as a historical-layout sample.
Only then may the obsolete Parquet containers be deleted by an explicit,
reviewed key manifest.

---

## What exists

The original report described **1,172 objects** because it counted only the
`detail_page` partition. The complete April prefix contains **1,299 Parquet
objects**:

| Artifact type | Objects | Stored bytes |
|---|---:|---:|
| `detail_page` | 1,172 | 14,670,223,837 |
| `results_page` | 127 | 131,736,508 |
| **Total** | **1,299** | **14,801,960,345 (13.79 GiB)** |

Those files contain 954,817 rows. The read-only content census produced three
whole-file safety classes:

| Whole-file class | Files | Stored bytes | Rows | Present-day disposition |
|---|---:|---:|---:|---|
| Empty placeholders only | 57 | 3,094,857 | 43,019 | Eligible for a first deletion wave after manifest review |
| Strictly pack-recoverable | 483 | 6,200,168,281 | 422,904 | Eligible after donor validation and durable mapping |
| Mixed or unique content | 759 | 8,598,697,207 | 488,894 | **Must not be deleted as files yet** |

At row level:

| Class | Row occurrences | Meaning |
|---|---:|---|
| Empty HTML | 43,019 | The old writer archived `b""`; no original page body exists in the row |
| Exact packed content | 423,304 | SHA-256 is present in a current pack, sometimes under a different artifact/source key |
| Content absent from all current packs | 488,494 | The legacy page body is not represented in the current pack corpus |
| **Total** | **954,817** | Exact accounting invariant for subsequent runs |

The 400-row difference between 423,304 pack matches and 422,904 rows in
strictly recoverable files is intentional: those 400 matches share Parquet
files with unique rows, so the containing files cannot yet be deleted.

The content absent from packs comprises 90,579,757,490 uncompressed bytes:

| HTTP status | Rows | Uncompressed HTML bytes | Initial interpretation |
|---|---:|---:|---|
| 200 | 438,994 | 90,542,171,595 | Potentially useful historical pages; requires silver reconciliation |
| 403 | 49,494 | 37,559,044 | Mostly small challenge/denial payloads; retain only if they add diagnostic value |
| 502/503/504 | 6 | 26,851 | Error bodies; inspect and classify |

These counts and byte totals are the baseline fingerprint. Any drift at the
start of an implementation run stops the plan until the difference is
explained.

---

## Working hypothesis: an interrupted retention system, not a permanent lake

The repository history supports the following explanation.

1. Plan 72 introduced Parquet archival on 2026-04-07 as a temporary safety
   layer. It moved locally stored raw HTML into partitioned Parquet before
   deleting local files and specified a 28-day MinIO retention window.
2. The Airflow cleanup DAG ran hourly. The writer buffered roughly 1,000 rows,
   partitioned by artifact type/date/hour, and used random
   `part-<uuid>-0.parquet` names. The surviving files' naming, partitioning,
   row counts, and on-the-hour cadence match that writer.
3. Plan 97 introduced the present individual-object MinIO path through a
   shadow dual-write period. The old local/`raw_artifacts` path and the new
   MinIO/queue path overlapped, and a failed new-path write was non-fatal.
4. Plan 100 migrated historical observations while explicitly remapping two
   overlapping bigserial artifact-ID spaces. A legacy artifact ID therefore
   cannot safely be joined to a current artifact ID without the migration
   mapping or a natural-key/content check.
5. Plan 102 removed n8n, the raw tables, and the old archiver on 2026-04-29.
   The oldest April Parquet partitions had not yet reached their 28-day expiry,
   so the writer and purge mechanism disappeared before any of this cohort was
   old enough to be deleted.
6. The old writer initialized `html = b""` and still emitted a row when the
   local path was missing. That behavior explains the 43,019 empty rows without
   treating them as lost page bodies.

The row arithmetic reinforces, but does not prove, this history:

- April 9–19 fetched rows: 483,562
- Legacy unique, non-empty rows: 488,494
- Difference: **4,932**
- April 20–27 fetched rows: 471,255
- Pack-recoverable plus empty rows: 466,323
- Difference: **4,932**

The likely shape is pre-cutover originals, followed by a dual-write tail that
mostly duplicated the new object store, plus empty archive records and about
4,932 transition-only rows. Natural duplicate page bodies and imperfect
historical joins mean this arithmetic is a hypothesis, not a deletion rule.

### Why the transition cohort is the danger zone

Plan 100's historical cutoff and Plan 97's non-fatal shadow write overlap. A
page fetched after the migration cutoff could have succeeded through the old
n8n/local path while failing to enter the new MinIO queue. If so, its legacy
Parquet row may contain facts that never reached the current silver dataset.
The implementation must identify and audit the transition cohort directly; it
must not assume that the inferred 4,932 rows are harmless duplicates.

---

## Goals

1. Produce a reproducible, row-complete manifest for all 954,817 legacy rows.
2. Prove which raw bytes already exist in current packs and preserve a durable
   route from each deleted legacy row to its donor content.
3. Determine whether every unique successful page is represented semantically
   in silver, even when its raw bytes are not retained elsewhere.
4. Recover missing observations and preserve genuine failures or useful layout
   examples before deleting their containing Parquet files.
5. Reclaim obsolete storage through bounded, reviewable deletion waves with an
   exact receipt and no recursive prefix deletion.

## Non-goals

- Making `read_html()` understand the legacy Parquet format.
- Treating raw HTML and normalized silver observations as equivalent. The
  audit records both byte preservation and semantic preservation separately.
- Reusing `cleanup_parquet()` for this operation. Its recursive directory
  deletion is intentionally too broad for a mixed-content recovery job.
- Changing production MinIO, Postgres, silver, or pack indexes during the
  read-only audit stages.
- Establishing general raw-HTML expiry. That policy remains a separate decision
  and should be informed by the results of this plan.

---

## Safety invariants

These are release gates, not implementation suggestions.

1. **Read-only first.** Stages 0–2 perform no production writes or deletes.
2. **Exact-key deletion only.** No recursive prefix, partition, wildcard, or
   age-based delete is allowed.
3. **Whole-file awareness.** A Parquet object is not eligible while it contains
   even one row whose required information has not been preserved.
4. **No artifact-ID-only joins.** Legacy and current IDs overlap. Use the Plan
   100 mapping where available, stable natural keys, timestamps, and content
   hashes.
5. **Content-addressed proof.** A pack duplicate requires a recomputed legacy
   SHA-256, a matching pack-index hash, an existing donor pack, and valid frame
   bounds. The normal artifact-ID read path is not proof because 67,263
   same-ID comparisons had different hashes in the investigation.
6. **Durable evidence before mutation.** The reviewed manifest, checksums,
   census fingerprint, and recovery locations must exist outside the deletion
   prefix before the first delete.
7. **Approval per mutating phase.** Recovery writes, silver backfill, and each
   deletion wave are separately approved actions.
8. **Fail closed.** Count drift, missing donors, ambiguous silver coverage, or
   an incomplete receipt makes the affected file ineligible.

---

## Stage 0 — Freeze the evidence baseline

**Status: investigation complete; codify it before implementation.**

Add a read-only audit command, proposed as
`scripts/audit_legacy_bronze_parquet.py`, with unit tests under `tests/scripts/`.
It must enumerate the exact legacy prefix and write local outputs only by
default.

The run records:

- bucket, prefix, run timestamp, code commit, and configuration;
- every object key, size, ETag/version metadata when available, partition
  fields, schema, row count, and row-group count;
- aggregate counts and bytes matching the baseline above;
- the current pack-index and silver-snapshot identities used for comparison;
- a stable checksum over the sorted object census.

**Gate 0:** object count, exact stored bytes, row count, and all three
whole-file classes reproduce. If they do not, generate a delta report and stop.

---

## Stage 1 — Build the row-complete disposition manifest

For every row, record at least:

- legacy object key, row group, row offset, partition date/hour, and artifact
  type;
- legacy artifact ID, run ID, source, search key/scope, URL, fetched time, and
  HTTP status where present;
- stored hash, independently recomputed SHA-256, HTML length, and empty flag;
- classification: `empty`, `exact_pack_content`, or `unique_content`;
- for pack matches: donor source key, pack key, index key, frame offset/length,
  compression metadata, and donor content hash;
- for semantic audit: parser outcome, listing identity, extracted observation
  fingerprint, silver match evidence, and final retention reason;
- containing-file eligibility and the reason it remains blocked.

The manifest should be machine-readable Parquet or JSON Lines plus a concise
Markdown summary. It must be deterministic: two runs against unchanged inputs
produce the same sorted records and aggregate checksum.

### Pack-donor verification

For all 423,304 content matches:

1. Recompute the hash from the legacy row bytes rather than trusting the stored
   legacy hash.
2. Verify the referenced donor pack and index exist and that frame bounds fall
   within the pack object.
3. Verify the pack index advertises the same uncompressed content hash and
   length.
4. Perform decoded byte comparisons for all anomalies and a deterministic,
   stratified sample spanning source, date, artifact type, pack, and legacy
   file. The prior investigation's 250/250 donor sample is evidence, but the
   implementation run must generate its own receipt.
5. Preserve the donor mapping even when the donor uses a different source key.

**Gate 1:** all 483 strictly recoverable files have zero empty or unique rows,
and every row has a valid donor mapping. The 400 pack matches in mixed files
remain blocked with their containers.

---

## Stage 2 — Audit information value, not just byte duplication

### 2A. Empty and error responses

- Confirm every `empty` row has length zero after reading the actual Parquet
  value, not only metadata.
- Hash-cluster the 49,494 HTTP 403 bodies. Determine which are repeated
  Cloudflare/challenge templates and retain only representative examples if
  they remain useful for Plan 128 or parser tests.
- Inspect all six 5xx bodies individually.
- Record any page that is misclassified by status or contains usable listing
  data; move it into the successful-page audit.

### 2B. Reconcile all 438,994 unique HTTP 200 pages

Run the current parser offline against every page. This must not enqueue work,
write queue state, or alter silver. For each page, capture:

- parser success/failure and parser version;
- stable listing identity and source;
- normalized observation fields and a deterministic observation fingerprint;
- whether an equivalent observation already exists in silver;
- the join evidence: Plan 100 mapping where available, otherwise natural keys,
  source, fetched-time tolerance, and field-level comparison;
- whether the current parser extracts materially new fields from the old HTML;
- layout/failure cluster for pages that do not parse cleanly.

Sampling is sufficient to refine the implementation, but **not** to authorize
deleting a mixed file. Final disposition requires row-complete classification.

### 2C. Exhaust the migration-boundary cohort

Create explicit cohorts around:

- the Plan 97 dual-write start;
- Plan 100's historical migration cutoff;
- the Plan 102 decommission date;
- any interval where the new MinIO write failed but the old local/n8n path
  continued.

Audit every unique row in those cohorts. Reproduce or reject the inferred 4,932
transition-only rows using row-level evidence. Any successful page without an
equivalent silver observation becomes a recovery item, regardless of its age or
apparent similarity to other pages.

### Stage 2 output classes

Every non-empty unique row must end in exactly one class:

| Class | Required action |
|---|---|
| `represented_in_silver` | Eligible under the approved raw-retention decision |
| `missing_from_silver` | Recover observation before raw deletion |
| `new_parser_output` | Review and backfill if valuable; preserve raw until decided |
| `parser_failure` | Preserve raw until failure is understood or intentionally waived |
| `historical_layout_sample` | Preserve byte-identical raw page in a curated corpus |
| `error_or_challenge_sample` | Preserve only the selected diagnostic representative |
| `no_retention_value` | Eligible only after explicit policy approval |
| `ambiguous` | Ineligible for deletion |

**Gate 2:** 100% of the transition cohort and 100% of rows in any proposed
deletion file are non-ambiguous. Counts reconcile back to 954,817.

---

## Stage 3 — Preserve exceptions and repair missing history

This stage is mutating and requires a separate approval after the Stage 2
report is reviewed.

### Recovery actions

1. Write missing observations through an idempotent, provenance-preserving
   backfill path. Use backdated safety gates consistent with Plan 132: no
   refresh-policy regression, no accidental current-state overwrite, and a
   dry-run diff before commit.
2. Preserve byte-identical raw HTML for parser failures, newly extractable
   pages, transition anomalies, and the approved historical-layout sample.
3. Store retained raw pages in a supported, content-addressed packed format,
   not another special Parquet island. Record the legacy row coordinate and
   original hash in the recovery manifest.
4. Verify every recovery object through the production-compatible read path or
   a deliberately documented forensic read path before considering the source
   Parquet file eligible.

### Optional conservative bridge

If semantic reconciliation is inconclusive but keeping 759 obsolete Parquet
files is undesirable, losslessly repack all 488,494 unique rows into the current
packed format. This preserves future reparse value while removing the unsupported
format. It is a fallback, not evidence that all raw history merits indefinite
retention.

**Gate 3:** every retained row has two independent proofs: the destination hash
matches the legacy bytes, and the durable manifest resolves the legacy row to
the destination. Every silver repair has an idempotency and before/after
receipt.

---

## Stage 4 — Delete in bounded waves

Implement a dedicated command, proposed as
`scripts/prune_legacy_bronze_parquet.py`. It is dry-run by default, consumes
only a reviewed manifest, refuses keys outside the exact legacy prefix, applies
a small per-run cap, and never calls recursive delete.

Immediately before each key is deleted, compare its current size and
ETag/version metadata to the audited value. A mismatch skips that object and
fails the wave closed. After each batch, verify absence and append a deletion
receipt; do not overwrite prior receipts.

### Wave A — Empty-only files

- 57 files
- 3,094,857 bytes (2.95 MiB)
- 43,019 zero-length rows

This is the lowest-risk wave, but it still requires an explicit approved key
list and fresh precondition checks.

### Wave B — Strictly pack-recoverable files

- 483 files
- 6,200,168,281 bytes (5.77 GiB)
- 422,904 rows

Together, Waves A and B reclaim **6,203,263,138 bytes (5.78 GiB)** while leaving
all files that contain unique non-empty content untouched.

### Wave C — Reconciled mixed/unique files

- up to 759 files
- up to 8,598,697,207 bytes (8.01 GiB)

No Wave C file is eligible today. Each becomes eligible only after all of its
retained rows have been recovered and all other rows have a non-ambiguous,
approved disposition. Execute in small date/hour batches so an unexpected
result cannot affect the whole cohort.

### Abort conditions

Stop the active wave if any of the following occurs:

- baseline or manifest checksum drift;
- missing or unreadable donor/recovery content;
- content hash, size, ETag, pack bounds, or row-count mismatch;
- a candidate file contains an unclassified row;
- a silver backfill changes current listing state or refresh scheduling;
- the deletion tool sees a key outside the reviewed set;
- post-delete verification or receipt persistence fails.

Deletion is not the rollback mechanism. The rollback is to stop before the next
batch; required bytes must already exist in verified donor or recovery storage.

---

## Stage 5 — Closeout and prevent another orphan format

1. Re-run the complete legacy-prefix census and publish before/after objects and
   bytes.
2. Reconcile deleted, retained, skipped, and failed files to all 1,299 baseline
   objects.
3. Re-run donor/recovery reads and silver validation from the durable manifest.
4. Record the retained historical corpus, its owner, read path, and retention
   decision.
5. Update Plans 114, 131, and 132 with what this audit establishes about raw
   retention, pack identity, and migration-boundary risk.
6. Separately decide whether to remove or hard-disable the dormant recursive
   `cleanup_parquet()` path. It currently has a `WHERE FALSE` selector, but it
   should not be repurposed for this operation.

---

## Test and verification matrix

| Area | Required proof |
|---|---|
| Census | Fixture and production dry run reproduce object/row/byte totals and deterministic checksum |
| Parquet reading | Empty, null, malformed, multi-row-group, and schema-variant fixtures classify correctly |
| Hashing | Stored hash disagreement is surfaced; recomputed bytes are authoritative |
| Pack donors | Cross-source donor, missing pack, bad index bounds, corrupt frame, and hash mismatch all fail closed |
| Silver joins | Overlapping artifact IDs cannot create a match by themselves; natural-key and Plan 100 mapping cases are covered |
| Parser audit | Success, failure, challenge page, changed extraction, and ambiguous listing identity are distinct outcomes |
| Backfill | Dry-run diff, idempotent rerun, backdated observation, and no current-state regression |
| Pruning | Dry-run default, exact-prefix enforcement, manifest checksum, per-run cap, precondition mismatch, and append-only receipt |
| End to end | A mixed fixture file remains blocked until its unique row is recovered, then becomes eligible |

Production dry runs should emit summaries and bounded samples, not raw HTML or
credentials, into logs.

---

## Deliverables

- `scripts/audit_legacy_bronze_parquet.py` — read-only census, classification,
  donor validation, and semantic-audit orchestration.
- `scripts/prune_legacy_bronze_parquet.py` — manifest-driven, dry-run-first,
  exact-key deletion with preconditions and receipts.
- Unit and integration fixtures covering the matrix above.
- Baseline census, row manifest, donor/recovery map, semantic reconciliation
  report, approval record, and append-only deletion receipts.
- A closeout note with exact storage reclaimed and the intentionally retained
  corpus.

## Approval boundaries

| Approval | Authorizes | Does not authorize |
|---|---|---|
| Audit approval | Read-only production scans and local report generation | Production writes or deletes |
| Recovery approval | Named recovery-pack writes and/or reviewed silver backfill | Legacy deletion |
| Wave A approval | Exact 57-key empty-file manifest | Any prefix or Wave B/C deletion |
| Wave B approval | Exact verified duplicate-file manifest | Mixed-file deletion |
| Wave C approval | Exact per-batch reconciled-file manifest | Unlisted or ambiguous files |

## Expected outcome

The conservative outcome is **5.78 GiB reclaimed** with all unique page bodies
left untouched. The likely outcome is materially more after silver
reconciliation, potentially approaching the full 13.79 GiB minus recovery
packs and the curated historical sample. The plan deliberately makes no final
storage-savings promise until the 438,994 unique HTTP 200 pages and the
migration boundary have been audited row by row.

