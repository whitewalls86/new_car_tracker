# Implementation Plan 145: April Cutover Reconciliation

## Status

**Proposed 2026-08-26.** This document turns
[Plan 145](plan_145_april_cutover_reconciliation.md) into five executable
slices, one for each open Linear issue: CAR-13 and CAR-19 through CAR-22.

The canonical plan still owns the outcome and evidence. Linear owns the active
slice. This document owns the build sequence between them.

## Delivery map

| Order | Linear issue | Plan 145 scope | Deliverable | Production mutation |
|---:|---|---|---|---|
| 1 | [CAR-13](https://linear.app/cartracker/issue/CAR-13/plan-145-close-stage-0d-0e-build-the-one-backfill-write-path) | Stage 0 ledger and Stage 1 write path | A frozen row-complete ledger plus a fixture-tested, dry-run-first backfill driver | No |
| 2 | [CAR-19](https://linear.app/cartracker/issue/CAR-19/plan-145-stage-2-recover-the-36241-orphans-through-the-backfill-path) | Stage 2 | Recover the 37,715 orphan HTTP 200 captures; discard the 5,261 challenge pages | Yes: append-only historical writes |
| 3 | [CAR-20](https://linear.app/cartracker/issue/CAR-20/plan-145-stage-3-recover-the-11600-row-information-bearing-cohort) | Stage 3 | Recover only the three information-bearing cohorts | Yes: append-only historical writes |
| 4 | [CAR-21](https://linear.app/cartracker/issue/CAR-21/plan-145-stage-4-preserve-fixtures-and-edge-cases-before-deletion) | Stage 4 | Copy the bounded preservation set outside the deletion prefix and verify it | Yes: preservation writes only |
| 5 | [CAR-22](https://linear.app/cartracker/issue/CAR-22/plan-145-stage-5-delete-legacy-parquet-by-reviewed-manifest) | Stage 5 | Delete all 1,299 legacy objects in three approved, receipted waves | **Yes: irreversible deletion** |

The ticket boundary is intentional: **CAR-13 builds and proves the complete
machine; CAR-19 is the first time that machine writes recovered April data.**
CAR-19 through CAR-22 are operational execution slices, not continuations of
an unfinished implementation hidden inside CAR-13.

## Decisions that apply to all five slices

### The ledger is the control plane

One immutable ledger keyed by the legacy row's recomputed `sha256` carries the
plan from census through deletion. It is not a loose collection of per-stage
CSV files. At minimum each row records:

- legacy object key, row group and row offset;
- stored and recomputed `sha256`;
- legacy `artifact_id`, `run_id`, `source`, `search_key`, `search_scope`, URL,
  `fetched_at`, HTTP status, content length and legacy object metadata;
- matching pack key, sidecar key, sidecar row and `raw_sha256`, when present;
- queue-event and silver-observation matches using only valid content/time
  keys;
- the Stage 0f information-value cohort;
- the intended disposition: recover in CAR-19, recover in CAR-20, preserve in
  CAR-21, redundant, challenge page, empty, or unresolved;
- the execution receipt that ultimately satisfied that disposition.

The builder orders rows and columns deterministically and writes a content
fingerprint beside the Parquet ledger. Every later command takes that
fingerprint explicitly and refuses a different ledger.

`artifact_id` is metadata only. It is never a cross-system join key.

### Backfill writes are historical, not live processing

The driver parses bytes with `parse_cars_detail_page_html_v1` and reuses the
normal silver row construction, but it does not call `POST /process/batch` and
does not enqueue artifacts. A backfill write may only:

1. append a row to `staging.silver_observations`; and
2. append the corresponding row to `staging.price_observation_events`.

The price event's `event_at` is the recovered April `fetched_at`, not the date
the backfill happens. Without this rule, `int_price_history` would treat an
April recovery as the latest current price.

The backfill path must not mutate `ops.price_observations`,
`ops.vin_to_listing`, `ops.blocked_cooldown`, or
`ops.detail_scrape_claims`; must not emit VIN/price/listing messages; and must
not append claim, cooldown or VIN-mapping events.

### Dry-run, caps and approval

Every new mutating command is dry-run by default. Applying a recovery requires
an explicit reviewed manifest, its fingerprint, `--apply`, and a per-run cap.
CAR-19 and CAR-20 receive separate named approvals. CAR-22 receives a separate
approval for each deletion wave.

### Deferred hardening

Durable provenance embedded in every silver/event row and general-purpose
database-enforced backfill idempotency are **not blockers for this bounded
April recovery**. The immutable ledger, reviewed manifests and execution
receipts are the audit mechanism for these five tickets. A follow-on plan may
add first-class provenance columns and reusable idempotency constraints.

This deferral does not relax three immediate requirements:

- the historical event time must be correct;
- a manifest row may be attempted only once during an approved run; and
- observed counts and receipts must reconcile before the next ticket starts.

## Slice 1 — CAR-13: build and prove the complete recovery machine

### Outcome

CAR-13 leaves a frozen, row-complete April ledger and a runnable recovery
driver that has passed unit, fixture and real-Postgres safety tests. It performs
no recovery write against the production April population.

### 1A. Build the row-complete ledger

Create `scripts/build_april_ledger.py` as a read-only production scanner and a
fixture-capable local command.

The builder:

1. enumerates the exact 1,299-object legacy prefix and records object size,
   ETag and last-modified metadata;
2. streams Parquet row groups rather than loading 13.79 GiB into memory;
3. recomputes each non-empty row's HTML hash and records disagreement with the
   stored hash as unresolved;
4. reads the 32 April pack sidecars once and joins `sha256` to `raw_sha256`;
5. reaches queue events only through a **current pack-sidecar** `artifact_id`,
   when one exists, and joins silver observations by `(listing_id,
   fetched_at)`; the numerically overlapping legacy `artifact_id` is never
   traversed;
6. classifies the Stage 2 orphan population, the three Stage 3 cohorts, the
   Stage 4 preservation set and the redundant remainder; and
7. emits ledger Parquet, a concise JSON/Markdown count report, and a SHA-256
   fingerprint file.

The report must reconcile to the measured baseline or fail closed: 1,299
objects and the plan's row/status populations. A count drift is evidence to
investigate, not a tolerance to encode.

### 1B. Add the backfill-only writer branch

Add an explicit keyword-only `backfill` mode to `write_detail_active` and
`write_detail_unlisted`. Refactor silver-row construction only as far as needed
to keep normal and backfill rows identical.

In backfill mode:

- active writes one primary detail silver row and one `upserted` detail price
  event;
- unlisted writes one unlisted silver row and one `deleted` detail price event;
- carousel rows are excluded unless a later ticket explicitly supplies a
  manifest for them; Plan 145 recovers legacy detail observations;
- event time is explicitly the recovered `fetched_at`; and
- every live-state and message-emission branch is bypassed.

The silver row and price event are one database transaction. Do not call the
current non-fatal silver helper and then insert the event in a second
transaction: without general rerun idempotency, a partial pair would have no
safe automatic repair. Either add a cursor-taking silver insert primitive or a
dedicated transactional backfill writer; keep the normal writer's non-fatal
silver behavior unchanged.

Use a dedicated backfill event insert or an optional event-time parameter whose
normal-path default retains database `now()`. Do not silently change the event
time semantics of normal scrapes.

### 1C. Build the generic recovery driver

Create `archiver/processors/backfill_unrecorded_observations.py`. It consumes a
manifest derived from the frozen ledger; it does not rediscover or reinterpret
the cohort while writing.

For each manifest row it:

1. validates the ledger fingerprint and the row's allowed disposition;
2. rejects non-200 rows for the recovery modes in CAR-19/CAR-20;
3. reads the legacy source key through `shared.minio.read_html`, exercising the
   existing pack fallback and hash verification;
4. parses the bytes through the production detail parser;
5. compares the parsed listing ID to the ledger identity and refuses a
   mismatch;
6. reports the proposed silver/event rows in dry-run mode; or
7. calls the backfill writer branch when `--apply` is present.

The command has a hard cap, progress reporting, structured output and a
receipt containing the ledger/manifest fingerprints and attempted, written,
refused and failed counts. It never edits a `.zpack` or sidecar.

### 1D. Close Stage 0d with tests, not an argument

Unit tests prove the backfill branch executes only the two allowed inserts and
that the normal writer path is unchanged.

A real-Postgres integration test seeds a current active listing, VIN mapping,
blocked cooldown and detail claim; snapshots the relevant HOT rows and V040
views; performs both an older active and older unlisted backfill; and proves:

- every HOT row is byte-for-byte unchanged;
- V040 staleness/queue output is unchanged;
- exactly the expected silver and price-event rows were appended; and
- the appended event time is the supplied April `fetched_at`.

Fixture tests build a miniature legacy Parquet + pack/sidecar corpus, generate
the ledger, run the driver in dry-run mode and then against a test database,
and reconcile every input row to an output or explicit refusal.

### Expected files

- `scripts/build_april_ledger.py`
- `processing/writers/detail_writer.py`
- `processing/sql/insert_backfill_price_observation_event.sql` or the
  equivalent narrowly scoped query change
- `processing/queries.py`
- `archiver/processors/backfill_unrecorded_observations.py`
- `tests/scripts/test_build_april_ledger.py`
- `tests/processing/test_detail_writer_backfill.py`
- `tests/archiver/test_backfill_unrecorded_observations.py`
- `tests/integration/processing/test_detail_writer_backfill.py`

### CAR-13 exit gate

- Stage 0 ledger counts and fingerprint are recorded in Plan 145.
- Stage 0d's real-Postgres evidence is recorded in Plan 145.
- The fixture-tested driver can consume a CAR-19-shaped manifest end to end.
- No production recovery rows have been written.

That closes CAR-13. Building only the writer flag does not.

## Slice 2 — CAR-19: recover the orphan HTTP 200 captures

### Outcome

Recover the 37,715 orphan HTTP 200 captures whose bytes are in packs and whose
metadata is in legacy Parquet. The 5,261 orphan 403 captures are classified as
challenge pages and receive no observation.

### Execution

1. Derive an immutable CAR-19 manifest from the CAR-13 ledger: orphan,
   pack-resolvable, legacy HTTP 200, no existing silver observation.
2. Assert 37,715 recovery rows and 5,261 excluded 403 rows, or stop for drift.
3. Run the whole manifest in dry-run mode and require zero identity/hash/parser
   refusals before requesting write approval.
4. Select an approximately 500-row stratified canary across packs, capture
   days and parsed listing states. Compare its normalized output with controls
   where both legacy and existing silver records are available.
5. With named approval, apply the canary, flush both staging streams, and
   verify the April lake rows and unchanged HOT/V040 state.
6. Apply the remaining manifest in capped batches, stopping on any unexpected
   refusal or count drift.
7. Flush, verify and append execution receipts to the ledger evidence.

Measure sidecar GETs, pack ranged GETs, cache hits/misses, elapsed time and peak
memory with `PACK_INDEX_CACHE_PACKS=48`. This is the effectiveness measurement
Plan 133 deliberately left to this workload.

### CAR-19 exit gate

- 37,715 HTTP 200 orphan captures have corresponding historical silver/event
  rows.
- 5,261 HTTP 403 orphan captures are accounted for and unwritten.
- Canary comparison, pack-cache measurement and before/after HOT/V040 evidence
  are recorded in Plan 145.
- The CAR-19 manifest reconciles completely to written or explicitly refused
  rows; any refusal remains a blocker for CAR-20.

No preservation or deletion occurs in this ticket.

## Slice 3 — CAR-20: recover the information-bearing cohort

### Outcome

Recover only the legacy captures that add information silver lacks:

- 11,199 listings with no priced observation;
- 138 listings absent from silver entirely; and
- 270 captures bracketed by a price change.

The cohort manifest is deduplicated by recomputed `sha256`; overlapping cohort
labels remain on the ledger row so the final distinct count is explainable.

### Execution

1. Derive and fingerprint the CAR-20 manifest from the same frozen ledger,
   excluding anything satisfied by CAR-19.
2. Publish both per-cohort counts and the deduplicated distinct-row count.
3. Dry-run every row and investigate all parser, identity or hash refusals.
4. With a separate named approval, apply in capped batches through the CAR-13
   driver.
5. Flush staging, verify the historical lake rows, and re-run the information
   classification to prove the intended gaps closed.
6. Mark the remaining roughly 212,000 missing observations as measured
   redundant in the ledger; do not write them.

### CAR-20 exit gate

- Every distinct row in the three approved cohorts is written or has a named,
  unresolved refusal.
- The redundant remainder has a ledger disposition and zero recovery writes.
- HOT tables and V040 outputs remain unchanged.
- Counts and evidence are recorded in Plan 145.

An unresolved refusal blocks CAR-21 because preservation/deletion cannot yet
claim row-complete accounting.

## Slice 4 — CAR-21: preserve fixtures and edge cases

### Outcome

Copy the bounded material that must outlive the legacy Parquet into a dedicated,
non-legacy preservation prefix and verify every copied object before any delete
manifest is eligible for review.

### Implementation

Create `archiver/processors/preserve_legacy_samples.py`, dry-run by default and
driven only by a CAR-21 manifest derived from the frozen ledger. The manifest
contains:

- deterministic historical-layout parser fixtures selected across layout,
  date, parse outcome and content hash;
- all 138 listings absent from silver;
- all 11 HTTP 5xx bodies; and
- a bounded, deterministic 403 challenge-page sample.

Write preserved bytes outside the deletion prefix under a Plan 145-specific
namespace. Beside them write a manifest containing source coordinates,
recomputed SHA-256, destination key and reason for preservation. Read every
destination back and verify its hash before issuing a success receipt.

Parser fixtures intended for automated regression tests get a small,
reviewable checked-in fixture subset plus expected parser output. The durable
preservation copy remains in object storage; the repository should not absorb
an unbounded historical corpus.

### Tests

- deterministic selection and stable manifest fingerprint;
- dry-run performs no PUTs;
- destination prefix cannot overlap the legacy deletion prefix;
- source and destination hashes must agree;
- a failed PUT/readback leaves the source row ineligible for deletion; and
- all four preservation classes reconcile to the ledger.

### CAR-21 exit gate

- Every preservation-manifest row has a verified destination and receipt.
- Parser fixtures and expected outputs are versioned.
- The ledger has no preservation-required row without a verified copy.
- Evidence is recorded in Plan 145.

CAR-21 performs no legacy deletion.

## Slice 5 — CAR-22: delete by reviewed manifest

### Outcome

Delete exactly the 1,299 legacy Parquet objects, freeing approximately
13.79 GiB, only after every contained row has a satisfied ledger disposition.

### Implementation

Create `archiver/processors/delete_legacy_parquet.py`, borrowing the refusal-
first shape of `delete_packed_source_html.py` but consuming only an explicit
reviewed key manifest.

The deletion manifest records key, expected size, ETag, ledger fingerprint,
row count, disposition counts and wave. The command:

- is dry-run by default;
- requires `--apply`, a manifest fingerprint and a named approval record;
- refuses keys outside the exact legacy prefix;
- contains no prefix, wildcard or recursive-delete operation;
- rechecks key size and ETag immediately before deletion;
- refuses a file containing even one unresolved ledger row;
- deletes no more than the reviewed per-run cap;
- verifies absence after each delete; and
- appends a receipt outside the deletion prefix before continuing.

### Waves

1. **Wave 1:** the 57 empty-placeholder files / 43,019 empty rows.
2. **Wave 2:** files whose every non-empty row is recovered, already present in
   silver, measured redundant, or preserved by CAR-21.
3. **Wave 3:** every remaining file, with its row-level justification included
   in the review artifact.

Each wave gets its own dry-run report, manifest review and named approval.
Approval for one wave grants no authority for the next.

### Tests

- an unaccounted row refuses the whole file;
- manifest fingerprint, size or ETag drift refuses deletion;
- out-of-prefix and unlisted keys are refused;
- dry-run and missing approval delete nothing;
- cap enforcement stops exactly at the boundary;
- partial failure preserves an accurate receipt and safe resume point;
- post-delete absence is verified; and
- no code path invokes recursive/prefix deletion.

### CAR-22 exit gate

- All three waves have named approvals and durable receipts.
- A fresh census finds zero legacy objects and reports the measured bytes freed.
- Deleted, retained, skipped and failed counts reconcile to the original 1,299
  objects and every ledger row.
- Control reads from packs and CAR-21 preservation destinations still verify.
- Final evidence and Plan 145 success criteria are closed.

This closes Plan 145. Recovery in CAR-19 or CAR-20 is not the finish line.

## Cross-ticket verification matrix

| Invariant | CAR-13 | CAR-19 | CAR-20 | CAR-21 | CAR-22 |
|---|:---:|:---:|:---:|:---:|:---:|
| Ledger fingerprint fixed and checked | Build | Check | Check | Check | Check |
| No `artifact_id` cross-system join | Test | Verify | Verify | Verify | Verify |
| HOT tables and V040 unchanged | Prove | Recheck | Recheck | — | — |
| Dry-run is default | Prove | Use | Use | Prove/use | Prove/use |
| Separate mutation approval | — | Required | Required | Required | Per wave |
| Row-complete reconciliation | Fixture | Orphan cohort | Value cohort | Preservation set | Entire ledger |
| Exact-key-only deletion | — | — | — | — | Prove and use |

## Safe stopping points

- **After CAR-13:** no production recovery occurred; rerun fixtures or revise
  manifests freely.
- **After CAR-19:** orphan recovery is durable; no legacy bytes were deleted.
- **After CAR-20:** all approved history recovery is durable; no legacy bytes
  were deleted.
- **After CAR-21:** preservation is verified; deletion still requires new,
  explicit authority.
- **During CAR-22:** stop after any key or wave. Receipts identify exactly what
  was deleted; unapproved keys remain untouched.

## Documentation and ticket closeout

At each ticket boundary:

1. write counts, fingerprints, approvals and verification results into the
   corresponding Plan 145 stage;
2. link the implementation PR and evidence from the Linear issue;
3. close only that issue; and
4. do not advance the next issue merely because the code for it exists.

Before CAR-13 closes, reconcile Plan 145's current idempotency/provenance
language with the deferred-hardening decision recorded above so the canonical
plan and ticket exit criteria say the same thing.
