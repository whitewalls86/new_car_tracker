# Plan 145: Recover April Detail Artifacts and Delete the Legacy Parquet

## Status

**Revised draft — 2026-08-26.** The first implementation guide and two CAR-13
implementations were reverted before this rewrite. They remain in git history
and are not inputs to this design.

Production measurements taken read-only on 2026-08-21 remain valid evidence.
No recovery or deletion has run. This revision replaces selective recovery
with a simpler bounded migration: keep successful captures, discard failed
responses, recover every distinct successful observation missing from silver,
repack April and delete the obsolete detail-page Parquet.

**Supersedes [Plan 132](plan_132_unrecorded_artifact_recovery.md) and
[Plan 137](plan_137_legacy_bronze_parquet_disposition.md).**

---

## Goal

Delete the **1,172 April 2026 `detail_page` Parquet objects** containing
951,821 legacy row occurrences and occupying 14,670,223,837 bytes
(approximately 13.66 GiB), after every distinct successful capture is either:

1. already represented by a silver observation; or
2. reconstructed as a real current artifact, parsed and written to silver
   history.

The 127 legacy `results_page` Parquet objects are out of scope. So are all
current `results_page` objects and the broader raw-HTML retention policy.

This is a one-time cleanup, not a general recovery platform. Prefer slow use of
existing, proven storage machinery over new pack generations, lookup rules or
reusable workflow infrastructure.

There is no deadline. Nothing is deleted until the recovered artifacts read
back correctly, the historical writes reconcile and an exact deletion-key
manifest receives named approval.

---

## Recovery contract

### Keep successful captures; discard failed responses

The in-scope legacy population is:

| HTTP result | row occurrences | treatment |
|---|---:|---|
| 200 | 847,785 | reconcile, persist and recover when missing |
| 403 | 104,025 | discard; challenge pages correctly produce no observation |
| 5xx | 11 | discard; failed responses are not vehicle observations |
| **total** | **951,821** | exact census |

No 403, 5xx or empty body is copied into the recovered artifact population.
The original Parquet remains untouched until final deletion, so these rows are
not destroyed early merely because they have no recovery work.

### Identity and duplication

The three identifiers have separate jobs:

| identity | use |
|---|---|
| `(legacy_object_key, row_group, row_offset)` | account for every legacy occurrence |
| `(listing_id, fetched_at)` | identify one observation and decide whether silver already has it |
| recomputed `sha256` | prove byte identity and join legacy HTML to pack sidecars |

Legacy `artifact_id` is metadata only. It is never joined to a current artifact
ID. Recovered artifacts receive new current-system IDs from
`ops.artifacts_queue`.

The 847,785 successful row occurrences contain 837,061 distinct SHA values and
exactly 837,061 distinct `(listing_id, fetched_at)` pairs. The additional
approximately 10,700 rows are exact duplicate occurrences. They remain in the
census but collapse to one recovery candidate and at most one historical write.
If rows sharing `(listing_id, fetched_at)` disagree on recomputed SHA, the run
stops rather than selecting a donor.

### Reconstruct an artifact before parsing it

For a pack-backed capture:

```text
pack sidecar -> source key + surviving bytes + raw SHA
legacy row   -> listing ID + fetched_at + URL + run/search metadata
SHA join     -> reconstructed artifact
```

For content absent from packs, the legacy row already supplies both metadata
and HTML. It is materialized as a standard compressed April detail object and
then receives a new queue artifact record.

Both forms are real current artifacts before they are written as historical
observations. The current parser consumes their verified HTML; the authoritative
`fetched_at` comes from legacy Parquet, not from the parser or recovery run.

### Recover every missing successful observation

This revision deliberately drops the prior information-value cohorts. Their
measurement was useful—it proved that bulk recovery changes little—but
selectively deciding which missing successful captures are valuable introduced
more machinery than this bounded cleanup warrants.

Every distinct HTTP 200 capture with no exact silver observation at
`(listing_id, fetched_at)` is recovered. The previous window analysis measured
approximately 224,000 missing observations; Stage 3 records the exact distinct
count used for execution. Even that approximate population is small against the
roughly 40 million observations already held.

Equal parsed values at different capture times are not duplicates. A stable
price observed twice is still two observations. Only the same listing and
capture time collapses.

---

## Evidence carried forward

The implementation must reproduce these facts before mutating production.

### Silver matching is sound

For the 847,785 successful legacy rows, the nearest silver observation for the
same listing, with silver widened to March-May, was:

| window | matched | share |
|---|---:|---:|
| <= 5 s | 622,617 | 73.4% |
| <= 60 s | 623,155 | 73.5% |
| <= 600 s | 624,330 | 73.6% |
| <= 3,600 s | 688,579 | 81.2% |
| <= 86,400 s | 837,114 | 98.7% |
| listing present at all | 847,647 | 100.0% |

The curve is flat from five seconds to ten minutes. The unmatched captures are
not an ASOF-window artifact; their nearest observations are different scrapes
hours later. The implementation uses exact normalized `(listing_id,
fetched_at)` equality and records the same window profile as a diagnostic.

### The pack join is content-based and complete

April detail bronze has 32 packs and 32 sidecars containing 557,065 members.
The sidecars reproduce the orphan predicate exactly:

| sidecar predicate | members |
|---|---:|
| `artifact_id IS NULL` | 42,276 |
| >=50 KiB | 36,241 |
| 1-50 KiB | 294 |
| <1 KiB | 5,741 |

All 42,276 orphan sidecar members join by `raw_sha256 = recomputed legacy
sha256` to the legacy detail Parquet. The join reaches 42,976 legacy row
occurrences because exact duplicates exist. Those occurrences split 37,715
HTTP 200 and 5,261 HTTP 403.

Every sampled orphan read byte-identically through the pack fallback. Legacy
Parquet supplies authoritative `listing_id`, `fetched_at`, URL, status, run ID
and search metadata for the content match.

### Artifact IDs cannot cross the cutover

The legacy and current bigserial spaces overlap numerically but describe
different artifacts. A prior legacy-ID-to-silver join returned 4,672,074 rows
while agreeing on listing ID only 48 times and never agreeing on `fetched_at`
within 60 seconds. Therefore:

- legacy-to-pack uses SHA only;
- legacy-to-silver uses `(listing_id, fetched_at)` only; and
- recovered artifacts receive new current IDs.

---

## Delivery map

The five existing Linear issues are repurposed around the six stages below.
CAR-19 carries two stages because both are read-only joins over the same frozen
manifest and require no code deploy between them.

| Order | Linear issue | Stage | Outcome | Production mutation |
|---:|---|---|---|---|
| 1 | CAR-13 | Stage 1 | Freeze and prove the successful-capture manifest | No |
| 2 | CAR-19 | Stages 2 and 3 | Reconcile content to packs, then observations to silver | No |
| 3 | CAR-20 | Stage 4 | Persist missing captures as held recovery artifacts | Yes: HTML objects and artifact records |
| 4 | CAR-21 | Stage 5 | Parse recovery artifacts and append historical rows | Yes: staging history and artifact completion |
| 5 | CAR-22 | Stage 6 | Restore, repack, prune and delete legacy Parquet | **Yes: irreversible deletion** |

Ticket descriptions and acceptance criteria should be replaced with these
outcomes. The old slice descriptions are obsolete and must not be used as
implementation guidance.

---

## Stage 1 — Freeze the successful population (CAR-13)

Build one purpose-specific April reconciliation command. Its first mode is
read-only and scans only the exact legacy `detail_page` prefix.

It must:

1. enumerate exactly 1,172 Parquet objects and record their key, size, ETag and
   last-modified metadata;
2. stream all 951,821 row occurrences;
3. reproduce the 847,785 / 104,025 / 11 status census;
4. recompute every non-empty HTML SHA and refuse stored-hash disagreement;
5. emit one HTTP 200 occurrence manifest with legacy locator, metadata, HTML
   length, stored SHA and recomputed SHA;
6. collapse exact duplicates into a distinct observation manifest while
   retaining the occurrence-to-observation mapping; and
7. write deterministic manifests, a count report and fingerprints.

Stage 1 does not need a permanent database ledger. The immutable files and
fingerprints are the evidence for this one run.

### Gate

- All baseline counts reproduce exactly or the plan stops.
- Distinct SHA count equals distinct `(listing_id, fetched_at)` count.
- Duplicate pairs agree on SHA.
- No production object or database row is written.

---

## Stage 2 — Reconcile successful content to packs (CAR-19)

Join the frozen distinct-observation manifest to all 32 April detail sidecars
using recomputed SHA only.

For each observation record either:

- the verified current packed source key, pack key and sidecar row; or
- `legacy_only`, meaning its bytes must be materialized from its deterministic
  donor occurrence in Stage 4.

Reproduce the documented pack and orphan counts. Read and hash a bounded sample
from every pack through `shared.minio.read_html`.

### Gate

- All 42,276 orphan members are explained by legacy matches.
- No legacy artifact ID participates in the join.
- Exactly 32 pack indexes are read and the sampled read has zero hash failures.

---

## Stage 3 — Reconcile observations to silver (CAR-19)

Without another deploy, join the Stage 2 manifest to March-May silver by exact
normalized `(listing_id, fetched_at)`.

Write two immutable outputs:

1. `already_observed`: no recovery write is required; and
2. `recover`: every distinct successful capture absent from silver.

The recovery manifest retains its pack source or legacy donor locator and is
sorted deterministically. It also records the <=5 s, <=60 s, <=10 min and
listing-level profile so reviewers can compare it to the closed measurement.

### Gate

- Every distinct successful observation appears in exactly one output.
- The approximate 224,000-row gap is replaced by one exact reviewed count.
- Rerunning against the same inputs produces identical fingerprints.
- No production mutation has occurred through the end of CAR-19.

---

## Stage 4 — Persist held recovery artifacts (CAR-20)

Create real current artifacts for every row in the frozen recovery manifest,
without exposing them to normal processing.

The implementation adds an explicit non-claimable `recovery` artifact status.
For each recovery row:

1. pack-backed content keeps its existing source key and is read through the
   verified fallback path;
2. legacy-only content is read from its frozen Parquet donor, recompressed as a
   standard April detail object, written under a collision-free key and read
   back byte-identically;
3. insert an `ops.artifacts_queue` row with a new current artifact ID, legacy
   `listing_id`, `run_id` and `fetched_at`, the verified source key and status
   `recovery`; and
4. append the corresponding queue event and execution receipt.

The mode is dry-run by default, takes the Stage 3 manifest fingerprint
explicitly, runs in capped batches and never selects `recovery` rows through
the normal pending/retry claim query.

### Gate

- Every manifest row has exactly one new current artifact record.
- Every artifact reads back to the manifest SHA.
- No artifact entered normal processing.
- No silver, price-event or hot-state row was written.

---

## Stage 5 — Parse and write historical observations (CAR-21)

Process only the explicit Stage 4 recovery artifact IDs. Use
`parse_cars_detail_page_html_v1` unchanged.

For each artifact:

1. read and hash the HTML;
2. parse it with the current production parser;
3. require the parsed listing ID to equal the artifact/manifest listing ID;
4. append one primary detail row to `staging.silver_observations` with the
   artifact's legacy `fetched_at`;
5. append the corresponding price event with `event_at` equal to that same
   legacy `fetched_at`; and
6. mark the artifact `complete` and append its queue completion event.

The silver row, price event, artifact status and completion event commit in one
database transaction. The recovery path cannot mutate `ops.price_observations`,
`ops.vin_to_listing`, `ops.blocked_cooldown`, `ops.detail_scrape_claims`, or
emit live messages.

Before the full run, process an approximately 500-artifact canary. Compare
parsed output to normal-parser control captures and snapshot the relevant live
tables and V040 views before and after.

### Gate

- Every recovery artifact is complete or has an explicit reviewed failure.
- No duplicate `(listing_id, fetched_at)` historical row was written.
- Silver and price-event counts agree with successful artifact completions.
- Event time and silver time equal the legacy capture time.
- Live pricing, VIN, cooldown, claim and refresh state is unchanged.

---

## Stage 6 — Restore, repack, prune and delete (CAR-22)

Use the existing storage path even though it is slower. Do not introduce pack
generations, supplemental-pack lookup precedence or a new reader contract.

### 6A. Restore the retained April population

Use existing sidecars plus `read_html`/`write_html` to restore every successful
April detail member from the 32 current packs to its normal individual object
key. Newly materialized Stage 4 objects already join this population.

Do not restore verified challenge or other non-success bodies. Sidecar members
with a current artifact ID are classified from queue-event status; orphan
members are classified from the legacy SHA join. The restore report must account
for every old sidecar member as either retained-successful or intentionally
discarded-non-successful.

Every restored object is read back and checked against the sidecar SHA. Run a
disk-space and inode preflight before apply; the temporary individual-object
population is deliberately large.

### 6B. Retire the old packs and run the existing packer

Once every retained member exists independently and verifies, delete the 32 old
packs and 32 sidecars by exact reviewed manifest. At that point normal reads are
served from individual objects, so the packed representation is no longer
load-bearing.

Run the existing April month packer over the complete individual-object
population. It already:

- orders captures by listing and time;
- uses the trained dictionary and established frame sizing;
- writes the existing pack and sidecar format; and
- re-extracts and verifies every stored member.

After the new April packs verify, run the existing prune machinery to delete
the individual source objects. No new pack format, reader behavior or pruning
algorithm is part of this plan.

### 6C. Delete the legacy detail Parquet

Regenerate the exact 1,172-key deletion manifest from the frozen Stage 1 object
census. With named approval, delete those keys in capped batches and write
receipts.

Deletion is by exact key only, never by prefix. The 127 `results_page` Parquet
keys must not appear in the manifest.

### Gate

- The restore accounts for every old sidecar member.
- Every retained artifact reads byte-identically before old packs are removed.
- The existing packer verifies every member in the replacement April packs.
- The existing prune reports zero unexplained failures.
- Zero recovery artifacts remain in `recovery` status.
- Deleted, absent and failed legacy-key counts reconcile to exactly 1,172.
- The legacy `detail_page` prefix contains zero Parquet objects.
- The legacy `results_page` population is unchanged.

---

## Testing and operational evidence

Only tests that protect this bounded migration are required:

- fixture Parquet proves census, SHA recomputation and exact duplicate collapse;
- fixture sidecars prove SHA-only pack matching;
- fixture silver proves exact `(listing_id, fetched_at)` partitioning;
- `recovery` artifacts are invisible to normal claim SQL;
- current parser output uses the manifest identity and authoritative time;
- historical write plus artifact completion is atomic;
- a real-Postgres test proves live tables and V040 views are unchanged;
- restore refuses an unclassified sidecar member or a hash mismatch;
- existing pack/read/prune integration tests remain the Stage 6 storage proof;
- deletion refuses a results-page key, an unapproved run or an unverified
  recovery manifest.

Production evidence is recorded after each stage: input fingerprints, exact
counts, sampled or complete verification, execution receipts and the approving
name for irreversible deletion.

---

## Success criteria

| Metric | Required result |
|---|---|
| Legacy detail Parquet deleted | 1,172 objects / approximately 13.66 GiB |
| Legacy results Parquet deleted | 0 |
| Distinct successful captures unaccounted for | 0 |
| Recovery artifacts without verified bytes | 0 |
| Recovery artifacts left non-terminal | 0 |
| Duplicate `(listing_id, fetched_at)` writes | 0 |
| Legacy `artifact_id` used as a current join key | 0 |
| Hot-state mutations caused by recovery | 0 |
| Deletion without named approval | 0 |

The plan is complete only when the 1,172 legacy detail files are gone and all
retained April detail artifacts remain readable through the newly rebuilt
packs.

---

## Rollback and stopping points

- **After Stages 1-3:** discard generated manifests and rerun; production is
  unchanged.
- **After Stage 4:** recovery objects and non-claimable artifact records may be
  audited or removed by exact receipt; no observation exists yet.
- **During Stage 5:** stop between capped batches. Completed artifacts and
  their paired historical rows remain valid; `recovery` rows remain held.
- **During Stage 6 restore:** old packs remain authoritative until every
  retained source object is restored and verified.
- **After old packs are retired:** individual objects are authoritative and the
  existing packer can be rerun until its normal verification gate passes.
- **After legacy deletion:** rollback is restoration from MinIO versioning or
  backup only. This is why named approval and complete receipts are final gates.

---

## Effect on other plans

| Plan | Effect |
|---|---|
| 132 | Superseded; its orphan measurements and `ok`-is-success finding remain evidence here |
| 137 | Superseded; its exact detail census remains evidence, but deletion now follows successful-capture recovery |
| 131 | Its existing pack, verify, fallback and prune machinery carries Stage 6 |
| 133 | Its hardened pack read path carries artifact reconstruction and restore |
| 111/112/113 | Receive the recovered April observations; no live refresh state is changed by recovery |

---

## Out of scope

- All legacy and current `results_page` cleanup.
- Preserving 403 challenge pages or 5xx response bodies.
- Selecting only price-changing or otherwise information-bearing successful
  captures; this revision recovers every missing successful observation.
- Fixing the unrelated April `detail/active` null-price parser gap.
- A reusable historical-reprocessing framework.
- A new pack format, generation selector, reader or prune algorithm.
- The long-term raw-HTML retention policy owned by Plans 129-131.
