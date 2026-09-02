# Plan 145: Recover April Detail Artifacts and Delete the Legacy Parquet

## Status

**COMPLETE — 2026-08-30. All 1,172 legacy Parquet objects are deleted and every success criterion is met.** The goal and success criteria have survived
every revision unchanged. The *method* has now changed three times, each time
because a measurement contradicted an identity key the previous method relied
on. This revision therefore states the trust boundary once, up front, and
derives the method from it.

What changed, and why:

- The 2026-08-26 design reconciled the legacy Parquet to production through
  metadata joins. Disproved: the pack sidecar's `listing_id` is wrong for
  194,639 of 371,095 content matches.
- The 2026-08-27 second revision responded by refusing all metadata and
  re-deriving everything from bytes — parsing the union of two stores,
  deduplicated by content hash, at 24.8 core-hours. Correct but overbuilt: it
  carried a two-store union through every stage of the pipeline to avoid a
  question that unpacking answers for free.
- This revision **flattens the population first**. Materialize the legacy
  bodies, delete the ones the packs already hold, unpack the packs, and the
  result is one directory of distinct captures. Every later stage then reads
  one store instead of reconciling two.

Stages 1–5 are complete. Stage 1 (`3f6e6d4`), Stage 2, Stage 3a and Stage 3b
all landed 2026-08-27; Stage 4 parsed the flattened population 2026-08-28; Stage
5 committed all 701,375 `to_import` rows on 2026-08-29, and Stage 5b fixed the
packer before Stage 6 wrote anything.

**Stage 6 completed 2026-08-30.** The ordering trial ran and split, so the
incumbent clustering carried; the replacement packs were written and
`repack-verify` PASSED over all 983,043 members; the 32 superseded packs were
retired; the prune deleted 983,043 loose objects with 0 refused; and **all 1,172
legacy Parquet objects were deleted with named approval.**
`html/year=2026/month=4/artifact_type=detail_page/` is empty. April went from
24.48 GiB to 4.34 GiB — **20.14 GiB reclaimed** — and the 127 out-of-scope
results-page objects are untouched.

**Supersedes [Plan 132](plan_132_unrecorded_artifact_recovery.md) and
[Plan 137](plan_137_legacy_bronze_parquet_disposition.md).**

---

## Goal

Delete the **1,172 April 2026 `detail_page` Parquet objects** containing
951,821 legacy row occurrences and occupying 14,670,223,837 bytes
(approximately 13.66 GiB), after every distinct successful capture is either
already represented by a silver observation, or folded into production as a
real HTML artifact and parsed into silver.

The 127 legacy `results_page` Parquet objects are out of scope, as are all
current `results_page` objects and the broader raw-HTML retention policy.

This is a one-time migration, not a recovery platform.

---

## The trust boundary

Every previous revision died to an identity key that turned out to be
unusable. They are enumerated here once, with what proves each verdict, so no
later stage has to re-derive them.

| key | verdict | proof |
|---|---|---|
| **Content bytes** (both stores) | **Trustworthy** | 807,797 legacy bodies agree with their stored hash, zero mismatches (Stage 1). Every pack member is verified against `raw_sha256` on read. |
| **Legacy `listing_id`** (extracted from `url`) | **Trustworthy** | Where legacy and sidecar disagree, silver holds a `detail` observation for the *legacy* listing at that exact timestamp in 194,734 of 194,734 cases. |
| **Legacy `fetched_at`** | **Trustworthy** | Corroborated by the same 194,734 exact-timestamp matches. |
| **Sidecar `raw_sha256`** | **Trustworthy** | It is what `read_packed_html` verifies every read against. |
| **Sidecar `artifact_id`** | **Trustworthy where present** | Written from `artifacts_queue_events` by `minio_path`. NULL for 42,276 of 557,065 members. |
| **Sidecar `listing_id`** | **Unusable as identity; its NULL-ness is a reliable signal** | Correct for 31.4% of April members, 9.8% of June, 8.4% of July, measured against the scraper's own record. See below. |
| **Sidecar `fetched_at`** | **Trustworthy where present** | 100.00% exact against `artifacts_queue_events` for June's 1,124,122 members; 99.98% for April's named members. `detail_writer` stamps one capture time on the primary and every carousel row, so `min(fetched_at)` over the group returns it regardless of which listing was picked. NULL where the member has no silver row. |
| **Legacy `artifact_id`** | **Unusable** | `raw_artifacts` and `ops.artifacts_queue` are separate `bigserial` sequences. The same integer names two different artifacts across the cutover. |
| **Stored `sha256` of an empty body** | **Unusable** | The Plan 72 writer archived `b""` while copying the database hash. 43,014 rows. |

### What a NULL sidecar identity actually means

`archiver/processors/pack_bronze_html.py:431-456` builds sidecar identity by
taking `artifact_id` from `artifacts_queue_events` via `minio_path`, then
**LEFT JOINing silver observations on `artifact_id`** for `listing_id` and
`fetched_at`. The packer's own comment says an object with no silver row is
still packed — *"an object nobody can describe."*

So a NULL-identity pack member is not a defect and not a heuristic. It is,
definitionally, **an object silver has no observation for**. Measured
2026-08-27: 99,981 of 557,065 April members carry NULL `listing_id`, and a
453-member sample found 445 with no silver observation within 300 s — the
8 exceptions being carousel-only coverage.

This single fact replaces the entire metadata-join apparatus of the previous
two revisions. "What is missing from silver" is legible from the sidecar
without trusting a single sidecar identity *value*.

### Why the non-NULL values are wrong

The same `obs` CTE has **no `source` filter** and reduces with
`any_value(listing_id) GROUP BY artifact_id`. One detail artifact writes one
`source='detail'` row — the page's actual subject — plus ~5.7
`source='carousel'` rows for the other cars shown on that page, all sharing
that one `artifact_id`. `any_value` therefore returns one of ~6.7 listings, and
only one of them is the page's subject.

`artifact_id` is unaffected: it comes from `artifacts_queue_events` by
`minio_path`, before the join. `fetched_at` is unaffected: `detail_writer`
stamps the same capture time on the primary and every carousel row, so `min`
over the group returns it whichever listing was picked. **Only `listing_id` is
scrambled.**

Measured 2026-08-27 across all 144 packs — correct `listing_id`, and the share
of adjacent members that are captures of the same vehicle:

| month | members | `listing_id` correct | adjacency |
|---|---:|---:|---:|
| April | 557,065 | 31.4% | 32.7% |
| May | 1,021,266 | 59.5% | 57.6% |
| June | 1,124,122 | 9.8% | 16.7% |
| July | 909,654 | 8.4% | 17.1% |

This is **not a correctness defect in production**: artifact serving looks up
by `source_key` and verifies `raw_sha256`, and `check_index` validates only
member counts, frame ordinals and offset tiling. Nothing on the read path
consults `listing_id`. Its one real cost is that sidecar identity is a trap for
metadata joins — three revisions of this plan were lost to it.

### The compression question — answered 2026-08-29 by a split trial

`listing_id` is also the packer's sort key, and `PackWriter.add` seals a frame
at a listing boundary so a vehicle's repeat captures compress together. So a
naive `source = 'detail'` fix does not merely correct a column: it reorders
every pack. Whether that helps or hurts is **unresolved**, and two attempts to
resolve it were both biased.

The scrambled column is not random. It is a **coarser clustering key** than the
true listing, because one carousel car appears on many detail pages and
`any_value` collapses them together:

| July pack-00023 | scrambled id | true listing |
|---|---:|---:|
| distinct values | 2,591 | 6,078 |
| members per value (in-pack) | 9.98 | 4.25 |
| largest cluster | 1,955 | — |
| neighbours sharing it, stored order | 90.0% | 26.0% |

Two repack experiments, production dictionary, level 9, 16 MiB frame target,
control reproducing the stored size exactly:

| test | true-listing order vs current | why it is still biased |
|---|---:|---|
| whole pack, frame structure held constant | **19.4% worse** (July), 3.2% (May) | the member set was chosen *by* the scrambled sort, so true ordering got only 4.25 of each listing's ~10 captures |
| only listings wholly inside one pack | **8.4% worse** | listings that fit in one pack are the low-capture ones — median 2 captures — so the case that benefits most is excluded |

Production sorts the whole month and then slices it into packs, so a real fix
would gather **all** of a listing's captures. Month-wide for July: 90,832
listings, mean 10.0 captures each (median 7, max 980), today scattered over a
mean 2.2 packs with **4.56 captures of a listing per frame**. A global true sort
would roughly double that.

The deficit halved (19.4% → 8.4%) as true ordering was given more of its
cluster, so the trend points toward parity or better and the question cannot be
called from the evidence here.

**Stage 6 answered it with a bounded trial.** A fixed ~50,000-member subset was
packed both ways — one such subset drawn in each ordering — and the trial
**split**: true ordering came out 27.50% worse on the sample drawn in the
current order and 4.76% better on the sample drawn in its own. A 32-point swing
from sample selection alone, larger than the effect under test. The rule fixed
before the run gives the incumbent the pass, so **April was repacked in the
existing clustering order and `fetch_member_metadata` was not changed.**

The bias this section worried about is therefore real and was measured: holding
the population fixed is necessary but not sufficient, because a *contiguous*
sample also has to be drawn in some order, and that choice moves the answer
further than the ordering does. See *Evidence — the ordering trial* under
Stage 6.

**What is safe to conclude regardless:** the repair records the correct
`listing_id` in the sidecar and leaves ordering and frame sealing alone, and it
needs no repack — rewriting that one column while preserving `source_key`,
`frame_ordinal`, `offset_in_frame`, `length` and `raw_sha256` leaves the pack
bytes untouched and every read and index check passing. Changing the sort is a
separate, unproven optimization.

Fixed in **Stage 5b**, before Stage 6 writes anything: identity and placement
are split so the sidecar can be corrected without relaying out a pack, and the
ordering question stays open for Stage 6's trial to answer.

---

## The production lifecycle of a captured page

Reprocessing must reproduce the parts of this that belong to a historical
capture and none of the parts that belong to a live one. Traced from source
2026-08-27.

| # | Step | Where |
|---:|---|---|
| 1 | Scraper fetches the page | `scraper/processors/scrape_detail.py` |
| 2 | Body written zstd-compressed under a `make_key` path | `shared/minio.py:write_html` |
| 3 | `INSERT INTO ops.artifacts_queue ... RETURNING artifact_id` — the `bigserial` allocates identity here | `scrape_detail.py:186` |
| 4 | `staging.artifacts_queue_events` row, status `pending` | `scrape_detail.py:196` |
| 5 | Processing claims a batch, `FOR UPDATE SKIP LOCKED`, status → `processing` | `processing/sql/claim_artifacts.sql` |
| 6 | `read_html(minio_path).decode("utf-8", errors="replace")` | `processing/routers/batch.py:87` |
| 7 | `parse_cars_detail_page_html_v1(html, url)` → `(primary, carousel, meta)` | `processing/processors/parse_detail_page.py:292` |
| 8 | **Blocked** → status `skip`, no observation, cooldown left intact | `batch.py:165` |
| 9 | Batch VIN lookup for primary + carousel listing_ids | `detail_writer.py` |
| 10 | VIN collision check (relisting detection) | `detail_writer.py` |
| 11 | **Upsert `ops.price_observations`** for primary | `detail_writer.py` |
| 12 | **Upsert `ops.vin_to_listing`** | `detail_writer.py` |
| 13 | Carousel filtered by `search_configs`; matches upserted to `price_observations` | `detail_writer.py` |
| 14 | **Silver write: primary + all carousel rows** (~5.7/page), regardless of filter | `silver_writer.py` |
| 15 | **Clear `ops.blocked_cooldown`** + lifecycle event | `detail_writer.py:_clear_cooldown` |
| 16 | **Release `ops.detail_scrape_claims`** + event | `detail_writer.py` |
| 17 | Emit `price_updated` / `vin_mapped` / `listing_removed` | `processing/events.py` |
| 18 | Status → `complete` | `batch.py:_set_status` |
| 19 | `staging.silver_observations` flushed to lake Parquet, rows deleted | `flush_silver_observations.py` |
| 20 | Queue row **deleted** once `complete`/`skip` | `cleanup_queue.py:35` |
| 21 | dbt: `stg_observations` → intermediate → marts | `dbt/models/` |
| 22 | Packer packs the object, attributing it via steps 3 + 14 | `pack_bronze_html.py` |

Step 20 is why `ops.artifacts_queue` holds nothing for April, and step 22 is
why a capture that never reached step 14 becomes an unattributable pack member.

### What "full treatment" means for a recovered April artifact

A capture from April is a **historical observation**, not news. Steps that
record history must run; steps that mutate present-tense state must not, because
current price, VIN mapping, cooldown and claim state have four months of newer
truth on top of them.

| Lifecycle step | Recovered artifact | Why |
|---|---|---|
| 6 — production decode | **Yes, exactly** | A different decode changes parsed strings for encoding reasons and corrupts the comparison. |
| 7 — parser, unmodified | **Yes** | Any parser change invalidates comparison against what production wrote. |
| 9–10 — VIN lookup / collision | **Read-only** | Needed to populate `vin` on carousel rows; must not write. |
| 14 — silver, primary + carousel | **Yes** | This is the deliverable. |
| 17 — price events (historical) | **Yes, at the legacy capture time** | The historical price record is the point of the recovery. |
| 3 — `artifact_id` allocation | **Yes, from the sequence** | See below. |
| 11 — `price_observations` upsert | **No** | Would set current price from an April capture. |
| 12 — `vin_to_listing` upsert | **No** | Four months of newer mappings. |
| 13 — carousel → `price_observations` | **No** | Same. |
| 15 — clear `blocked_cooldown` | **No** | Would clear live backoff on stale evidence. |
| 16 — release `detail_scrape_claims` | **No** | Would release live claims. |
| 17 — live event emission | **No** | Downstream consumers would treat April as now. |
| 5 — queue claim | **No** | See `artifact_id`, below. |
| 22 — repack with attribution | **Yes** | Stage 6. |

---

## `artifact_id` for recovered artifacts

`artifact_id` reaches silver (`stg_observations`), price events, and the marts
(`mart_scrape_volume`, `mart_detail_batch_outcomes` both count distinct
`artifact_id`). It is also how the packer attributes an object. So recovered
artifacts need real ones.

**For the 557,065 existing pack members: preserve when one exists, never
re-derive.** Their `artifact_id` lives in
`ops_normalized/artifacts_queue_events` in the lake, keyed by `minio_path`. It
is durable there — unlike `ops.artifacts_queue`, which step 20 empties.
**Unpacking under each member's original `source_key` keeps that join intact
for free**, which is the decisive argument for original keys over
content-derived ones. The 42,276 members with no event row may remain
unattributed only if Stage 5 imports no observation from them; an import-bearing
member requires a real ID and receives a new sequence value.

**For every import-bearing artifact without that trusted ID, including newly
materialized legacy bodies: allocate from the sequence, never compute one.**
`ops.artifacts_queue_artifact_id_seq` is a `bigserial` sequence; it was at
7,732,177 during the 2026-08-27 design audit. `nextval` is concurrency-safe by
construction: it never returns a value twice, and it does not roll back or
reuse on abort. Concurrent production inserts are therefore safe **as long as
identity comes from the sequence** — a `max(artifact_id) + 1` would race and
collide, and is forbidden.

**Allocate without enqueueing.** Do *not* insert `ops.artifacts_queue` rows:
`claim_artifacts.sql` claims anything `pending` or `retry`, so an enqueued row
is picked up by live processing within seconds and runs the full hot-state
path this plan forbids. Instead:

1. Preserve the unique event-lake ID, or call
   `nextval('ops.artifacts_queue_artifact_id_seq')`, once per import-bearing
   artifact.
2. Write the silver rows carrying that `artifact_id`.
3. Write one `staging.artifacts_queue_events` row per artifact with status
   `recovered` — the table has no FK and no status CHECK, and it is exactly
   where the packer looks for attribution in step 22.

The result: recovered artifacts are attributable, countable in the marts, and
repackable, without ever becoming claimable work.

---

## The block-page defect

The parser treats any non-Cloudflare block page as a live listing.
`_detect_challenge` keys only on Cloudflare's `Just a moment...` title, so an
Akamai `Access Denied` body (430 bytes) parses to `listing_state="active"`
with every vehicle field NULL.

Measured 2026-08-27 — `source=detail` silver, rows that are active with price,
VIN and make all NULL:

| month | detail rows | all-NULL | share |
|---|---:|---:|---:|
| April | 1,272,617 | 87,003 | **6.84%** |
| June | 1,124,122 | 271 | 0.02% |
| July | 907,090 | 178 | 0.02% |
| August | 758,549 | 19 | 0.00% |

**This is not a live production emergency.** August's 19 rows fall inside a
single 15-second window on 2026-08-20 and are a Cloudflare *"Site Maintenance"*
page, not an Akamai block. The parser's blindness to non-Cloudflare blocks is
structural and still present, but its current volume is negligible; April's
cohort is an April phenomenon.

Two consequences:

- **This plan must filter them.** 54,341 April pack members are 256–511 bytes,
  and the sampled ones are Akamai blocks. Reprocessing without a filter would
  inject tens of thousands of junk active/NULL-price observations into silver.
  The filter is applied in the recovery pipeline, not in the parser, so the
  "parser runs unmodified" property that makes the comparison meaningful is
  preserved.
- **The parser fix is a separate ticket**, low priority on current volume. It
  is very likely the cause of the April `detail/active` null-price gap that
  earlier revisions listed as out of scope.

---

## Method

Flatten the population, then treat it as one thing.

1. **Materialize** every surviving successful legacy body as an ordinary
   `.html.zst` object.
2. **Delete** the materialized objects whose content the packs already hold.
3. **Unpack** all 32 April packs under each member's original key.
4. **Parse** the resulting single population.
5. **Compare** to silver and apply what is missing.
6. **Repack** with correct attribution, prune, delete the legacy Parquet.

Steps 2 and 3 are what buy the simplicity: after them, there is one directory
of distinct captures, and every later stage reads one store.

### Population arithmetic

| | count |
|---|---:|
| distinct successful legacy captures with bytes | 797,073 |
| of which content already in the packs, deleted in step 2 | 371,095 |
| materialized objects surviving step 2 | 425,978 |
| April pack members unpacked in step 3 | 557,065 |
| **flattened population** | **983,043** |

Reconciled 2026-08-28 against the Stage 3a receipts; the earlier projection of
~993,767 was taken before 3a ran. `EXPECTED_FLATTENED_INPUTS` in
`scripts/reconcile_april_detail.py` carries the reconciled figure, and the
Stage 5 slice-1 probe reproduces it (see *Evidence — slice 1*).

Peak object count never exceeds the end state, because the deletion precedes
the unpack.

---

## Stage 1 — Freeze the census (CAR-13) — **complete**

`scripts/reconcile_april_detail.py census`, read-only. 1,172 objects,
951,821 rows, 807,797 hashes verified, zero drift, zero mismatches.

Status census: 200 = 847,785; 403 = 104,025; 5xx = 11. Empty bodies: 43,014
(39,988 of them HTTP 200). Distinct stored SHA 837,061; distinct recomputed
SHA 797,073. The discrepancy is the *Empty bodies* finding, not a failure.

---

## Stage 2 — Materialize successful bodies (CAR-19) — **complete**

`scripts/reconcile_april_detail.py materialize`, dry-run by default.

Per legacy row: skip empty and non-success with a recorded disposition;
otherwise derive a content-based key, skip if the object exists, write with
`write_html`, read back through `read_html` and require a hash match, record a
manifest row. One Parquet manifest shard per source file under
`recovery/plan145/materialized/`.

### Evidence — 2026-08-27

`materialize --apply` ran under tmux `plan145` on the VM, ~4h10m at
~4.7 source-files/min, exit 0. All 1,172 manifest shards written under
`recovery/plan145/materialized/`. Disposition totals over the shard set:

| disposition | rows |
|---|---:|
| `written` | 796,430 |
| `exists` | 11,367 |
| `skipped_empty` | 43,014 |
| `skipped_non_success` | 101,010 |
| **sum** | **951,821** |

**807,797** materialized objects (`written` + `exists`), matching the Stage 1
projection. The 11,367 `exists` rows are the idempotency proof — identical
bytes recomputed the same content-derived key and were skipped rather than
rewritten. 403/5xx bodies recorded, never written.

### Gate — met

- Every legacy occurrence carries exactly one disposition; the four counts sum
  to 951,821. **Reproduced exactly.**
- Every materialized object reads back byte-identically; any mismatch stops the
  run. **Run completed clean.**
- Every manifest row retains its legacy locator, `listing_id` and `fetched_at`.
  **Consumed unchanged by Stage 3a.**
- Object keys are distinct and content-derived; a re-run writes nothing.
- No silver, artifact-queue or live-state row is written.

---

## Stage 3 — Flatten the population (CAR-20) — **complete**

Two mechanical steps, no parsing.

**3a — Delete the duplicates.** Both sides already record `raw_sha256`:
materialize manifests write it per object, sidecars carry it per member. So
the comparison is a join of two written-down columns — no bytes are re-hashed.
Delete every materialized object whose hash appears in an April sidecar. Safe
at every instant: the content remains in the pack, and `read_html` falls back
to the pack for a missing object.

**3b — Unpack.** Write every one of the 557,065 members back as a loose
`.html.zst` object **under its original `source_key`**, preserving the
`minio_path` → `artifact_id` join in `artifacts_queue_events`. Existing keys
are skipped, so the step is idempotent and resumable.

Implemented as the `dedupe` and `unpack` modes on
`scripts/reconcile_april_detail.py` (PR #260, merged to `master` as
`cd57a25`), dry-run by default with `--apply`, following the `census` /
`materialize` structure.

### Evidence — 3a, 2026-08-27

`dedupe --apply` ran 19:57–20:10 UTC, exit 0.

| | |
|---|---:|
| deletion candidates (`written` + `exists`) | 806,898 |
| delete operations / receipts | 371,495 |
| **distinct objects deleted** | **371,095** |
| rate over candidates | 46.0% (gate 45.6% ± 10%) |

371,095 distinct is the plan's 371,095 content matches, to the object. 1,172
deletion-manifest shards written before any delete, plus 1,172 receipt shards,
under `recovery/plan145/dedupe/`; every receipt `deleted`, zero errors.
Deletion was by explicit key list, ≤1,000 keys per `delete_objects` call,
never by prefix.

Post-run verification: **0 of 371,495** deletion-manifest rows carry a
`raw_sha256` absent from an April sidecar (checked against 557,063 distinct
packed hashes). The 400-row gap between operations and distinct objects is
idempotent re-deletes of identical content materialized from two source files;
0 key/hash conflicts. `ops.artifacts_queue` and silver untouched.

### Evidence — 3b, 2026-08-27

Dry run verified 2 packs / 34,751 members — every member ranged-read and
checked against its sidecar `raw_sha256`, no failure. `unpack --apply` ran
20:24–22:27 UTC under tmux `plan145-s3b` (log
`/home/ubuntu/plan145-unpack.log`), **2h03m**, writing via `write_html` under
each member's original `source_key` with production dictionary `1367127621`.

**All 557,065 members, across 32 manifest shards.** Three later runs re-derive
that figure independently rather than restating it: Stage 4's completeness gate
would not open without it, `EXPECTED_UNPACK_SHARDS`/`EXPECTED_UNPACK_MEMBERS`
assert it, and Stage 6's `repack-verify` reloaded the same manifests on
2026-08-29 and reported *"frozen Stage 3b baseline: 557065 members over 32
packs"* — then found every one of them present as a live loose object.

### Gate — met

- Deletion is by exact key from a written manifest with receipts, never by
  prefix; the count reconciles against the sidecar hash join. **3a: met.**
- No key is deleted whose content is not provably in a verified pack.
  **3a: verified, 0 exceptions.**
- Every unpacked member verifies against its `raw_sha256` on write.
  **3b: met** — 557,065 members written, and Stage 6 re-read all of them against
  the same hashes on 2026-08-29 with zero mismatches.
- The flattened population contains no two objects with identical content.
  **Met for materialized objects by construction** — their keys are derived from
  the content hash, so identical bytes cannot produce two objects — and **met
  across the two stores** by 3a, which deleted every materialized twin of a
  packed body. **Not met strictly within the packs:** 3a's own verification
  counted **557,063 distinct hashes across 557,065 members**, so two members
  duplicate another member's content under a different `source_key`. 3a never
  targeted packed↔packed duplicates and this plan does not remove them: the
  packer keys on `source_key`, both copies pack and verify, and the cost is two
  objects in 983,043.
- `ops.artifacts_queue` is not written; no silver row is written.
  **3a: held.**

---

## Stage 4 — Parse the flattened population (CAR-26) — **complete**

Run `parse_cars_detail_page_html_v1` **unmodified** over the flattened
population and write primary and carousel rows to `recovery/plan145/parsed/`.

- Decode exactly as step 6 does: `read_html(...).decode("utf-8", errors="replace")`.
- `fetched_at` is authoritative from the legacy manifest row, or from
  `artifacts_queue_events` for a pack-only object; never from the parser or
  the run.
- Parsed listing identity is compared to manifest identity; disagreements are
  reported, never silently resolved.
- Block pages are classified and excluded from the import set, per *The
  block-page defect*.
- Body-size distribution is measured up front, so the sub-1 KiB cohort
  (Plan 137 recorded 5,741; this plan measures 54,341 in the 256–511 band)
  arrives as a number rather than as thousands of parse failures.

Cost: 90.7 ms/page over ~993,767 pages ≈ 25 core-hours. A process pool is
required — bs4/lxml is GIL-bound, one process ≈ one core. The host has 4 and
production needs some.

### Evidence — 2026-08-28

The run completed on the VM 2026-08-28, `parse_report.json` reporting
**1,204 / 1,204 units** over **983,043 inputs**, emitting **5,738,532**
observation rows at **zero `failed`, zero `missing_object` and zero identity
disagreements**. Identity resolved by legacy manifest for 797,073 objects and
by `artifacts_queue_events` for 180,710, leaving the 5,260 characterized under
*The tier-3 population, measured*.

Those totals are reproduced by a second program rather than asserted here: the
authoritative Stage 5 `compare` (`cmp-6c7c90d807bbdf13`) refuses to start until
`parse_report.json` says all 1,204 units completed and its input and
observation counts match, and it ran.

**Where the two cohort counts actually come from.** Stage 4 classified 4,966
Akamai bodies as `blocked_other`; its `_detect_challenge` is structurally dead
for every object whose identity resolved, so a further 59,460 leaked pages were
recorded as `parsed` upstream. Those were measured and excluded by the Stage 5
block-page filter (PR #272, `c5c5ee2`) — 59,460 rows over 59,460 objects,
cross-tabbed at *Evidence — slice 1, the authoritative run*, which is also where
the size-band distribution lands: **59,455 in the 000000–000511 band**, against
the 54,341 this stage's design projected for 256–511. `parse_report.json`'s
`blocked_other: 4,966` stands as an accurate record of what Stage 4 itself
classified and is not rewritten.

### Gate — met

- Every input is parsed or carries an explicit recorded failure.
- Every parsed row carries the authoritative capture time.
- The block-page cohort is measured and excluded, with counts.
- No production mutation outside the recovery prefix.

---

## Stage 5 — Compare to silver, then apply (CAR-21) — **complete**

Compare parsed output to March–May silver on `(listing_id, fetched_at)` with a
**300 s** tolerance, counting observations from any source, because carousel
rows are real coverage produced by detail artifacts.

Produce three immutable outputs — already represented, to import, and
unclassifiable — then write the import set with the treatment table above: silver rows and
historical price events at the legacy capture time, preserved or
sequence-allocated `artifact_id`s, `recovered` queue events, and **no** mutation of
`ops.price_observations`, `ops.vin_to_listing`, `ops.blocked_cooldown`,
`ops.detail_scrape_claims`, or live message emission.

Run an approximately 500-observation canary against normal-parser controls,
with before/after snapshots of live tables and V040 views, before the full apply.

### Design findings — 2026-08-27

The Stage 4 run made enough real materialized output available to test the
comparison shape without waiting for the unpacked units. The probe was bounded
to one DuckDB thread and three completed row shards; two were legitimately
empty and the non-empty shard held **3,374 observations from 643 artifacts**.
Against the deployed March–May silver snapshot:

| probe result | count |
|---|---:|
| parsed observations | 3,374 |
| distinct artifacts | 643 |
| average / maximum silver fan-out | 5.25 / 9 |
| represented within 300 s | 2,898 |
| not represented | 476 |
| represented at the exact microsecond | 2,879 |
| more than one silver candidate inside the window | 614 |
| duplicate parsed `(listing_id, fetched_at)` groups | 0 |
| maximum nearest-match distance | 228.945 s |

This sample is deliberately **not** a population estimate. Materialized units
are selection-biased and the 32 large unpacked units had not started. It does
settle the algorithmic question: classification is an existence test, not a
join that chooses a silver row. Multiple candidates are normal. The evidence
row records `match_count`, nearest absolute distance and the nearest sources;
no arbitrary candidate supplies identity or values.

The production inputs are tractable without a database export. March–May
silver contains **20,681,645 observations** in nine compacted Parquet objects
(one object per source/month), 219,710,181 stored bytes. A one-thread scan on
the VM completed in seconds. The final comparison therefore reads those nine
named objects directly and records each key, size and ETag in its report; it
does not read a moving dbt view without freezing its backing objects.

The deployed Flyway and writer contracts add requirements absent from the
short design above:

- `staging.silver_observations.artifact_id` is NOT NULL, but has no FK or
  uniqueness constraint. `staging.price_observation_events.listing_id` is a
  UUID and `event_at` defaults to `now()`. Recovery must validate UUIDs and
  explicitly insert the legacy capture time; the normal insert helper cannot
  be reused unchanged.
- `staging.artifacts_queue_events` intentionally has no status CHECK or FK, so
  `status='recovered'` with no hot queue row is valid. Its `event_at` is the
  recovery action time; its separate `fetched_at` is the April capture time.
- The March–May artifact-event lake has 4,906,595 detail event rows reducing to
  **1,536,055 distinct object paths, with zero paths mapped to conflicting
  artifact IDs**. Existing identity is therefore a strict lookup by normalized
  `minio_path`, never by sidecar listing ID. The largest historical ID in that
  window is 4,902,473; the deployed sequence was at 7,732,177 during the audit.
- `ops.vin_to_listing` has an index on `listing_id`, so the production batch
  lookup can be used read-only. Parsed primary VIN wins; missing primary and
  carousel VINs may be filled from one frozen lookup snapshot. A parsed VIN
  colliding with current hot state is reported but never causes a delete or
  remap.

### The tier-3 population, measured — 2026-08-28

The Stage 4 run's identity census, over all 983,043 inputs, resolves every
object by legacy manifest (797,073) or queue events (180,710) and leaves
**5,260** with neither a listing nor a capture time, at **zero tier
disagreements**. Arithmetic places all 5,260 inside the unpacked members:
425,978 + 371,095 = 797,073, and 371,095 + 180,710 + 5,260 = 557,065. They are
pack members whose content is absent from the legacy Parquet and which have no
`artifacts_queue_events` row.

They were characterized directly rather than estimated. Every one of them is a
**block page**:

| band | objects | what it is |
|---|---:|---|
| 426–441 B | 4,966 | Akamai `Access Denied`, parsing to `active` with every field NULL |
| 5.4–16.4 KB | 294 | Cloudflare `Just a moment...`, which the parser flags correctly |

That explains the identity gap rather than merely measuring it: a blocked fetch
has no listing to record, so no silver row and no queue event, and its bytes
were never worth keeping. **The absence of identity is the block.**

**Consequence for Stage 5, correcting an expectation this plan carried.** Stage 4
excludes block pages before emitting observation rows, so all 5,260 contribute
**zero parsed rows** and never reach `compare`. Since the census shows these are
the only objects without a capture time, the `unclassifiable` family should be
**empty or near it, on both reasons** — not the ~760 the Stage 4 design
estimated, and not 5,260. The ~760 figure counted a different thing than the
`--max-unclassifiable` ceiling it inspired. Neither that ceiling nor
`--max-no-listing-id 0` is expected to trip.

The Akamai body embeds the listing UUID in its text, so identity is technically
recoverable from these pages. It is not worth recovering — there is no capture
time and they are blocks — but it is a note for the separate ticket that fixes
`_detect_challenge`.

### Final comparison contract

The exploratory probe may run against completed Stage 4 units and writes only
a disposable report. The authoritative `compare` run refuses to start until
`parse_report.json` says all 1,204 units completed and reproduces the Stage 4
input and observation totals. It freezes:

1. every parsed row/input object key, size and ETag;
2. the nine March–May silver object keys, sizes and ETags;
3. the March–May artifact-event objects used for identity; and
4. the read-only VIN lookup rows used to enrich the import set.

For each parsed observation, the silver predicate is:

```text
same listing_id AND abs(silver.fetched_at - parsed.fetched_at) <= 300 seconds
```

Source, VIN, `artifact_id` and parsed values are not match keys. An observation
with at least one candidate goes to
`recovery/plan145/compared/already_represented/`; otherwise it goes to
`recovery/plan145/compared/to_import/`. Both families are sharded, immutable,
carry the parsed provenance, and are covered by a report digest. Existing
objects are never overwritten; a changed input inventory requires a new run
identifier and a complete re-compare.

Parsed duplicates are resolved globally, not independently per Stage 4 shard.
For an otherwise-unrepresented `(listing_id, fetched_at)` group, an identical
business-row duplicate has one deterministic canonical winner (`detail`
before `carousel`, then `object_key` and content hash); the rest enter
`already_represented` with reason `recovery_duplicate`. Two rows with that key
but different business fingerprints stop the comparison for review. This is
what makes the no-duplicate-write gate enforceable across shard boundaries.

### Artifact identity and immutable assignment

Identity is one value per source HTML object, shared by its primary and every
carousel row:

- preserve the queue-event `artifact_id` when the normalized object path has
  exactly one;
- otherwise allocate with
  `nextval('ops.artifacts_queue_artifact_id_seq')`, including for an old pack
  member with no historical event ID; and
- never read or compare `legacy_artifact_id`, and never use `max()`.

This corrects one statement earlier in the plan: the 42,276 April pack members
with no historical event ID can stay unattributed only when they contribute no
row to the import set. A pack member with a row to import cannot stay that way:
the silver table requires a non-NULL ID and the repacker needs attribution, so
it receives a new sequence value just like a materialized legacy object.

Allocation is an explicit apply step after comparison. It writes a
create-if-absent assignment shard under
`recovery/plan145/assigned/<batch>.parquet` before database insertion. A
sequence value lost before that immutable write is a harmless gap; after the
write, every retry reuses the recorded value. Each assignment records whether
the ID was `preserved_queue_event` or `allocated_sequence`.

### Historical write set

A batch is ordered by `object_key`, never splits an artifact, and is capped at
5,000 artifacts and 50,000 silver rows. One transaction writes:

1. only the `to_import` rows to `staging.silver_observations`, enriched by the
   frozen read-only VIN lookup and carrying the assigned artifact ID;
2. one historical price event for each imported **detail** row — `upserted`
   for active and `deleted` for unlisted — with `event_at=fetched_at`; and
3. one `staging.artifacts_queue_events` row per artifact with
   `status='recovered'`, the full `s3://bronze/<object_key>` path, the primary
   listing ID, `artifact_type='detail_page'`, and the legacy `fetched_at`.

Recovery does not mint carousel price events. Production emits those only for
carousel hints passing the search configuration active at capture time; that
April configuration is not recoverable, and applying today's filter or writing
all carousel rows would manufacture history. Carousel remains first-class
silver coverage and participates fully in comparison.

The same transaction inserts a row into a plan-specific durable Flyway receipt
table keyed by batch name and assignment-manifest SHA-256. The receipt stores
the artifact, silver, price-event and queue-event counts. On retry, an equal
receipt skips the batch; the same batch name with a different digest stops.
This receipt is necessary because all three staging tables are asynchronously
flushed and deleted: checking PostgreSQL after an ambiguous client response
cannot otherwise distinguish “never committed” from “committed and flushed.”

### Canary and live-state proof

Parser controls and the write canary are separate checks:

- choose approximately 500 exact, same-source represented observations and
  compare every silver business field to the production row, ignoring only
  recovery provenance, `artifact_id`, `written_at`, and the intentional
  pre-Stage-5 carousel VIN gap;
- choose approximately 500 `to_import` observations, grouped by artifact and
  stratified across detail/carousel, active/unlisted, input kind and identity
  source, then run them through the real assignment and batch writer; and
- require the silver/event flushers to carry the canary through to their lake
  prefixes before approving the full apply.

The before/after V040 assertion must run in a short named maintenance window;
otherwise live production legitimately changes the same tables while the
canary runs and an equality claim is meaningless. Quiesce every service with
write access to the four protected hot tables, begin one verifier transaction
so PostgreSQL's `now()` is fixed for both V040 snapshots, capture the hot tables
and views, run the canary on a separate connection, capture them again, and
require byte-equivalent results before restarting the writers. Pausing or
resuming those services remains a manual, separately approved production
action.

### Evidence — slice 1, 2026-08-28

`compare` is implemented and merged (PR #265, `master` at `48987c1`). It adds a
third output family the short design above did not have: **`unclassifiable`**,
for a parsed row that cannot be windowed and cannot be imported. Two reasons
are counted apart — `no_capture_time`, the tier-3 pages the design already
expected at ~760, and `no_listing_id`, tier-2 rows that resolve a capture time
but no listing. `staging.silver_observations.listing_id` is NOT NULL, so the
second is no more importable than the first, and without the family those rows
would have reached `to_import`. The three families are asserted to sum to the
parsed row total, which is what makes *classified exactly once* enforceable.

Two dry-run probes against the completed Stage 4 units, on the VM. Both wrote
nothing and issued no VIN query.

| | 20 units | 315 units | 532 units |
|---|---:|---:|---:|
| parsed rows | 44,446 | 657,963 | **1,142,700** |
| source objects | 7,866 | 113,438 | **196,453** |
| already represented | 77.2% | 80.7% | **81.1%** |
| to import | 22.8% | 19.3% | **18.9%** |
| unclassifiable | 0 | 0 | **0** |
| more than one silver candidate | 21.04% | 21.1% | **20.6%** |
| carousel rows per object | 4.65 | 4.80 | **4.82** |
| recovery duplicates collapsed | 0 | 0 | **0** |
| unrepresented captures with a neighbour ≤300 s | 39.1% of to-import | 21.3% | **19.9%** |

Every ratio converges rather than drifting: the multiple-candidate share holds
near 21% across a 26× increase in sample and agrees with the 2026-08-27 design
probe's 18%, and the near-neighbour share settles from a small-sample 39% toward
~20%. That stability is the evidence that the existence test behaves
consistently at scale. Object counts extrapolate to ~978,000 against
`EXPECTED_FLATTENED_INPUTS`'s 983,043, so the arithmetic closes.

**Cost, for planning the authoritative run.** The 532-unit run took three
minutes wall: 39 s to build a silver index of 14,862,304 observations over
89,612 wanted listings, then 532 units classified in about two. DuckDB was
capped at one thread and 2 GB and stayed inside it; memory did not move on the
Grafana panels while CPU spiked. The full run indexes toward the whole
20,681,645 and classifies 1,204 units, so single-digit minutes, not hours.

**What these numbers are not.** Every completed unit is a materialized legacy
body, because Stage 4 walks a key-sorted inventory and the 32 unpacked shards
sort last. So the population behind both probes is tier-1 identity only, and:

- the `unclassifiable` cohort reads 0 because tier-1 resolves both halves of
  identity every time — the `no_listing_id` population lives on the pack side
  and remains unmeasured;
- `recovery duplicates 0` is likewise structural: materialized survivors are
  distinct-content by construction, so nothing can collapse;
- both gates, and the fingerprint-conflict stop, have therefore never fired on
  real data.

**One finding that needs a ruling before slice 2 applies.** 27,078 of 127,048
`to_import` rows — 21.3%, concentrated in 3,169 listings at ~8.5 each — are
unrepresented captures with another unrepresented capture within 300 s. This is
the asymmetry between the two windows: representation tests ±300 s, duplicate
collapse tests an exact `(listing_id, fetched_at)`, so these all survive as
distinct rows. They are most likely genuine burst re-scrapes and genuinely
importable, and collapsing them would discard real history. The plan records
the measurement; the decision is the maintainer's.

> **Superseded 2026-08-29.** The burst-re-scrape reading was wrong: measured on
> the authoritative run, that mechanism is 0.11% of the cohort and the rest is
> carousel fan-out in the time domain. See *The near-duplicate cohort,
> decomposed* below. The conclusion — do not collapse — survives; the reason
> does not.

### Evidence — slice 2, 2026-08-27

`assign` and `apply` are implemented, together with
`V047__plan145_recovery_batch_receipts.sql`. **No authoritative `compare` run
exists yet** — Stage 4 was at 315 of 1,204 units — so everything below is
proven against fixtures and a real-Postgres integration test, not against real
compare output. No `--apply` has been run against production.

Two modes on `scripts/reconcile_april_detail.py`, both defaulting to a dry run:

| mode | reads | writes |
|---|---|---|
| `assign` | `compared/<run_id>/to_import/`, `parsed/inputs/`, the March–May artifact-event objects | `recovery/plan145/assigned/<run_id>-bNNNNN.parquet`, plus one assign report; `nextval` is its only statement |
| `apply` | those shards, the `to_import` rows and the frozen VIN snapshot | three staging tables and the receipt, one transaction per batch |

**What the deployed contracts forced.** Neither production helper survives
contact with this problem: `shared.db.db_cursor` opens its own connection and
commits on exit, so three calls are three transactions, and
`write_silver_observations_postgres` catches every exception and returns 0, so
a half-written batch would be logged as a warning and the run would continue
believing it committed. The writer opens one connection, does all four writes
on it, commits once, and lets exceptions propagate; it reuses `_POSTGRES_COLS`
and `_INSERT_SQL` so a silver schema change cannot drift away from it silently.
The run uses the `april-processor` profile, which connects as `cartracker`,
because `scraper_user` has only `SELECT, DELETE` on
`staging.price_observation_events` and no `INSERT` grant anywhere in
`db/migrations/`; V047 therefore grants the receipt table to `cartracker` and
leaves that gap alone rather than half-enabling a second role. Because
`april-processor` builds from `processing/Dockerfile`, which has no duckdb,
both modes read Parquet with pyarrow.

**One thing the short design did not say, found while building.** A source
object's detail row and its carousel rows are classified independently, so an
object can contribute only carousel rows to `to_import` while its own detail
row is already represented. The recovered queue event still needs that object's
*page* listing id, which no carousel row carries. It comes from Stage 4's
frozen `parsed/inputs/` shards, which also supply `input_kind` — and therefore
the count of unattributed pack members that turn out to be import-bearing.

**Refusals, all scoped so a run that writes nothing is never stopped by one.**
A NULL or non-UUID `listing_id` anywhere in `to_import` stops `assign`, but only
after the whole population has been scanned and the cohort counted, so the
maintainer learns its size rather than the first offending row; only the printed
examples are capped, so a systematic upstream defect is described in constant
space rather than accumulating a dict per row. `apply` re-checks the same
invariant on every row it is about to write, because it re-reads the shards
independently and is the last thing standing before the INSERT — and because
`staging.silver_observations.listing_id` being `text NOT NULL` does *not* catch
it: `str(None)` is the four-character string `"None"`, which the column accepts.
One object path mapped to two queue-event artifact ids stops the run rather than
choosing. Re-assigning a run under different batch caps is refused, because the
caps decide membership and the batch names would not change.

**The canary gate is measured in rows, not batches.** `apply --apply` refuses a
selection over `--max-unapproved-rows` (default 1,000 silver rows) without
`--maintainer-approval <name>`. Counting batches would have been no gate at all
for the case that matters: one default-cap batch is 5,000 artifacts and up to
50,000 silver rows, two orders of magnitude past the ~500 observations this plan
sizes the canary at. The row budget refuses that and still lets a genuinely
canary-sized assignment through, however many batches it spans.

**Proven on real Postgres** (`tests/integration/scripts/`, 14 tests, now run in
CI): a committed batch re-run writes zero rows and does not advance the
sequence; the same batch name with a different digest stops and leaves the
first receipt intact; a failure on the third write rolls back all four, and the
same connection then commits the batch whole on retry; deleting the staging
rows the way the flusher does leaves the receipt behind and the retry still
skips; `nextval` never repeats across two connections and a rolled-back
allocation leaves a `bigserial` gap rather than a reuse; `ops.artifacts_queue`
gains no row and `ops.price_observations`, `ops.vin_to_listing`,
`ops.blocked_cooldown` and `ops.detail_scrape_claims` hash identically before
and after, over deliberately non-empty tables.

Slice 3 — the canary, the maintenance window and the quiesced-writer
live-state proof — is unstarted. It is a manual, separately approved
production action.

### Evidence — slice 1 & 2 probe against real compare output, 2026-08-28

`--probe` on `compare`, `assign` and `apply` (PRs #268, #269) exists so slice 2
can be exercised on real data while Stage 4 finishes. First real run, on the VM
2026-08-28 with Stage 4 at 1,186 → 1,188 of 1,204 parsed units. Everything below
routed to the `*_probe/` prefixes; nothing was committed to Postgres.

**Slice 1 — `compare --probe --apply`** (`cartracker-archiver`, `scraper_user`,
`--duckdb-threads 1`). run_id `cmp-e37723ede49fad4f`, inventory digest
`e37723ede49fad4f9efb02d663096eb1021f6e8cc68503bfd545a9f5984e1991`:

- 1,186 / 1,204 parsed row shards, 4,455,248 parsed rows; silver index 16,318,833
  observations over 111,915 wanted listings.
- `already_represented` 3,980,701 / `to_import` 474,547 / `unclassifiable` 0 —
  sum 4,455,248, the three families partition the parsed rows exactly.
- `unclassifiable` 0 confirms the reference-doc measurement: the ~760
  no-capture-time / no-listing-id pages are block pages and emit no parsed rows.
- carousel fan-out 5.6332 per object over 671,657 objects (max 8); multi-candidate
  share 0.3069 (1,367,256); recovery duplicates 0; VIN collisions 0.
- near-dup ≤ 300 s: 67,994 adjacent pairs, 95,168 captures with a neighbour, over
  7,483 listings — recorded for the maintainer's ruling before any authoritative
  apply, per the slice-1 gate.
- Wrote `compared_probe/cmp-e37723ede49fad4f/`,
  `inventory_probe/cmp-e37723ede49fad4f.json`,
  `vin_snapshot_probe/cmp-e37723ede49fad4f.parquet`.
- Nit found: slice 1's success line prints `wrote compared/…, inventory/….json`
  without the `_probe` suffix under `--probe --apply`
  (`_print_compare_report`, `scripts/reconcile_april_detail.py:3671`). Cosmetic —
  the data went to `*_probe/`, proven by the isolation check below — but
  misleading in exactly the line that gets screenshotted. Follow-up, not a slice-2
  regression.

**Slice 2 — `assign --probe --apply`** (`april-processor` /
`cartracker-processing`, `cartracker` role, pyarrow — no duckdb):

- 474,547 `to_import` rows → 294,325 artifacts, no validation stop (no NULL or
  non-UUID `listing_id`, no object split across units).
- Identity: 1,893 preserved from queue events, 292,432 allocated via real
  `nextval`; 0 of 42,276 unattributed pack members turned out import-bearing at
  this partial stage.
- 59 batches (58 bound by the 5,000-artifact cap, 1 by end). Wrote
  `assigned_probe/cmp-e37723ede49fad4f-b00001…b00059.parquet` and
  `assigned_probe/cmp-e37723ede49fad4f-assign_report.json`.
- `ops.artifacts_queue_artifact_id_seq`: 7,747,250 → 8,039,682, advanced by
  exactly 292,432. Permanent and sanctioned — a `bigserial` gap, not a reuse.
  This is the only durable effect of the whole probe.

**Slice 2 — `apply --probe`**:

- The first `--probe --apply` on batch `b00001` failed on its first statement:
  `psycopg2.errors.UndefinedTable: relation
  "public.plan145_recovery_batch_receipts" does not exist`. V047 had not been
  applied to the prod DB — it was one migration behind (V045/V046 dated
  2026-08-27). The probe surfaced the missing precondition rather than writing:
  `write_import_batch`'s `except` rolled back and re-raised, no partial state.
- `docker compose run --rm flyway` applied V047 at 2026-08-28 15:56:39; only V047
  was pending.
- Re-run of
  `apply --probe --apply --run-id cmp-e37723ede49fad4f --batch cmp-e37723ede49fad4f-b00001`:
  5,000 artifacts, 7,993 silver rows, 2,678 price events, 5,000 queue events. The
  full statement sequence — receipt `SELECT`, silver insert, price-event insert,
  queue-event insert, receipt `INSERT` — ran against production Postgres in one
  transaction and then `ROLLBACK`. No constraint or cast error: the `::uuid` cast
  on `price_observation_events.listing_id` (2,678 rows), the `NOT NULL` on
  `silver_observations.listing_id` (7,993 rows), both CHECK constraints and every
  coercion held on real rows. Report: `mode  PROBE (real transaction, rolled
  back)`, "No row was committed".

**Isolation, verified after the run:**

- `recovery/plan145/compared/` and `recovery/plan145/assigned/` (authoritative) —
  0 keys. Probe output never left the `*_probe/` prefixes.
- `staging.artifacts_queue_events WHERE status = 'recovered'` — 0;
  `public.plan145_recovery_batch_receipts` — 0 rows;
  `staging.silver_observations WHERE artifact_id >= 8000000` — 0. The rollback
  held; nothing committed.

**What this establishes.** The probe path works end to end on real April data:
slice 1 produces a complete real compare under `compared_probe/`, slice 2 assigns
identity from it and runs the full historical write set against production
Postgres and rolls it back, with every constraint and cast exercised on real
rows rather than fixtures. The near-dup cohort above is the one number a
maintainer still has to rule on before an authoritative apply. Durable footprint:
the sequence advanced 292,432, V047 is now applied on prod (a prerequisite the
authoritative run needs anyway), and the `*_probe/` prefixes hold disposable
output. The authoritative `compare --apply` still waits for all 1,204 units.

### Evidence — slice 3 Phase A, 2026-08-28

Slice 3 splits into two build phases
(`docs/plans/plan_145_stage_5_canary_handoff.md`). **Phase A is built and
unit-tested; nothing in it has been run against production, and no service was
paused.** Phase B — the write canary as a real commit, and the flush round trip
— is a follow-up once the authoritative `compare` lands.

Three checks, none of which decides the gate:

- **The parser control** — `control` mode on `scripts/reconcile_april_detail.py`.
  Draws exact, same-source represented observations from slice 1's
  `already_represented/` family (`nearest_distance_s == 0` and `match_sources`
  is the row's own source), reads the one deployed silver row at
  `(listing_id, fetched_at, source)`, and diffs every silver business field.
  Ignores exactly four things by name — recovery provenance columns,
  `artifact_id`, `written_at`, and carousel `vin` (on the raw parsed row);
  a fifth entry raises. Any other field disagreement is a finding and the mode
  exits non-zero. Read-only; runs in `cartracker-archiver` (DuckDB, read-only
  `scraper_user`); writes one JSON report under
  `recovery/plan145/control[_probe]/` only with `--apply`.
- **The canary stratified sampler** — `canary-sample` mode. Joins slice 1's
  `to_import/` rows to slice 2's `assigned/` shards on `object_key`, stratifies
  across `source` × `listing_state` (from the `to_import` row) and `input_kind`
  × `id_source` (from the assignment shard), and greedily selects **whole
  artifacts** — no object split across the sample boundary — until every
  non-empty stratum is covered and the ~500-row target is met. An `to_import`
  object with no assignment row is a stop. Writes a manifest +
  report under `recovery/plan145/canary[_probe]/` with `--apply`; this manifest
  is Phase B's input.
- **The V040 live-state verifier** — `scripts/verify_recovery_live_state.py`,
  a standalone script. **Refuses to run without `--window <name>`** and opens no
  connection until that check passes. Opens one `READ COMMITTED` transaction —
  **not** `REPEATABLE READ`, which would freeze the transaction's data snapshot
  at the first statement so the second snapshot could never see the canary's
  committed writes; `now()` is `transaction_timestamp()` and is fixed for the
  whole transaction at every isolation level, which is all the time-dependent
  views need. Snapshots the four protected hot tables and the two V040 views
  (`bit_xor(hashtextextended(t::text,0))` — order-independent, no giant
  intermediate) plus `txid_current()`, runs the canary on a **separate
  connection** (`--canary-cmd` subprocess, or an injected callable in tests),
  snapshots again in the same transaction, and requires byte-equivalence and
  one shared `txid`. It **does not and cannot** quiesce or resume any writer —
  that is the maintainer's manual, separately approved action, before and after.

Tests: `tests/scripts/test_reconcile_april_detail.py` section P (16 tests),
`tests/scripts/test_verify_recovery_live_state.py` (6 tests) and
`tests/integration/scripts/test_plan145_live_state_verifier.py` (2, real
Postgres) — the control samples only exact same-source matches by reservoir so
memory is bounded at `--sample-size`; the four ignores each drive a branch and
renaming one surfaces its column; a single differing field is reported and
exits non-zero; the sampler covers every stratum (proven on a 40-object / 3-row
budget) and cross-references slice 2's own per-object count both ways — a
missing assignment, an assigned object absent from the read (a dropped shard),
and a short read all stop it; the verifier refuses without a window; both
snapshots share one transaction; a write committed between them on another
connection is **seen** and fails the proof; a failing canary command fails the
check. `python -m pytest tests/scripts -q -m "not integration"` — 806 passed;
`ruff` clean.

**What remains unproven — every run.** The control and the canary sampler have
run only against fixtures and need the authoritative `compare`/`assign` output
on the VM. The V040 verifier's mechanism is covered by a real-Postgres
integration test, but the proof itself needs a maintainer-opened window with the
writers quiesced. The write canary itself
(a real commit) and the flush round trip into `silver_normalized/observations/`
and `ops_normalized/` are Phase B and unbuilt. The carousel fan-out review
(probe: 5.6332/object, max 8) and the near-duplicate cohort (probe: 67,994
adjacent pairs, 95,168 captures with a neighbour, 7,483 listings) are recorded
above and still need to be put in front of the maintainer, off the authoritative
`compare_report.json`, before any authoritative apply.

### Evidence — slice 1, the authoritative run, 2026-08-29

**Stage 4 completed 2026-08-28** — 1,204/1,204 units, 983,043 inputs, 5,738,532
observation rows, zero `failed`, zero `missing_object`, zero identity
disagreements — so the completeness gate that had blocked the authoritative
`compare` since 2026-08-27 passes. The run below is the first authoritative
Stage 5 output that exists; `recovery/plan145/compared/` was empty before it.

Run on the VM under `cartracker-archiver` (`scraper_user`, `--duckdb-threads 1
--duckdb-memory-limit 2GB`), on `master` at `c5c5ee2` — that is, **with the
block-page filter of PR #272**. The dry run and the `--apply` produced identical
counts and the same `run_id`, which is the inventory digest doing its job.

- `run_id` **`cmp-6c7c90d807bbdf13`**
- inventory digest
  `6c7c90d807bbdf137bf9b96c94d2a54c2cb9d94706a6a29afa36c209a08ea60d`
- dry run 04:30:34 → 04:38:57 UTC; `--apply` 04:40:49 → 04:54:08. About
  8½ and 13½ minutes, against the "single-digit minutes" the slice-1 probe
  projected.
- `refusals: []`. Neither `--max-unclassifiable` nor `--max-no-listing-id 0`
  fired, and no drift flag was used.

**The four families, and the sum that makes *classified exactly once*
enforceable:**

| family | rows | share |
|---|---:|---:|
| `already_represented` | 4,977,697 | 86.74% |
| `to_import` | **701,375** | 12.22% |
| `blocked_excluded` | 59,460 | 1.04% |
| `unclassifiable` | **0** | 0% |
| **sum** | **5,738,532** | matches the parsed row total exactly |

`unclassifiable` is 0 on **both** reasons — `no_capture_time` 0 against the
~760 the design expected, `no_listing_id` 0 against a ceiling of 0. That
confirms the 2026-08-28 reference measurement at full population rather than on
the materialized-only sample: the 5,260 objects with neither a listing nor a
capture time are all block pages and emit no parsed rows. The `no_listing_id`
cohort, unmeasured until now because it lives on the pack side, is genuinely
empty.

**The block-page filter, measured for the first time.** 59,460 rows over 59,460
objects, every one a `detail` row; the filter quarantines whole objects and
excluded nothing else:

| cross-tab | |
|---|---|
| `size_band` | 000000–000511 **59,455** · 000512–001023 3 · 004096–016383 2 |
| `input_kind` | unpacked 51,844 · materialized 7,616 |
| `listing_id_source` | `queue_events` 44,374 · `legacy_manifest` 15,086 |

Two numbers in that section answer questions the plan had left open rather than
merely restating the cohort:

- **`objects_that_emitted_carousel_rows` is 0.** The handoff flagged this as
  unmeasured and as the shape that defeats a row-level check — a block page
  contributing only carousel rows to `to_import` while its detail row sits in
  `already_represented`. It does not happen: a 439-byte `Access Denied` body has
  no carousel. The object-level filter and `assign`'s row-level check therefore
  agree, and the stale-run refusal added in `7410016`/`5802cb5` is guarding a
  gap that is empty **for this run** — it stays, because that was not knowable
  in advance and is not knowable for a future one.
- **`detail_rows_carrying_a_business_value` is 0.** The fail-closed precision
  gate — stop an `apply and not probe` run if any excluded row carries a
  non-NULL `price`, `vin` or `make` — did not fire. The predicate is exactly as
  targeted as it was specified to be.

The 44,374 `queue_events` and 15,086 `legacy_manifest` split is the defect the
filter exists for, confirmed at scale: Stage 4's classifier is structurally dead
for every object whose identity resolved, so all 59,460 of these were recorded
as `parsed` upstream. `parse_report.json`'s `blocked_other: 4,966` remains an
accurate record of what Stage 4 classified; it is not rewritten.

**The two maintainer rulings, now off the authoritative report.** Both moved
from their probe values, as the handoff predicted they would once block pages
left the population:

| | probe `cmp-e37723ede49fad4f` | authoritative `cmp-6c7c90d807bbdf13` |
|---|---:|---:|
| carousel fan-out per object | 5.6332 over 671,657 objects | **5.2 over 915,972 objects**, max 8 |
| near-dup adjacent pairs ≤ 300 s | 67,994 | **96,800** |
| captures with a neighbour | 95,168 | **142,397** |
| listings involved | 7,483 | **20,625** |
| multi-candidate share | 0.3069 | **0.3207** (1,821,048 of 5,679,072) |

The fan-out is now measured over importable objects only — blocked objects are
out of both numerator and denominator — so **5.2 is a sharper figure than the
probe's 5.6332, not drift**, and it sits just under production's ~5.7. The
near-duplicate cohort is the quantity to rule on: 142,397 unrepresented captures
have another unrepresented capture of the same listing within 300 s, across
20,625 listings. This is the deliberate asymmetry between the two windows —
representation tests ±300 s, duplicate collapse tests an exact
`(listing_id, fetched_at)` — and are decomposed immediately below.
Collapsing them would discard real history. **Neither ruling is made here.**

#### The near-duplicate cohort, decomposed — 2026-08-29

The cohort was characterized directly rather than reasoned about, by a
read-only DuckDB scan over `compared/cmp-6c7c90d807bbdf13/to_import/` (658
shards, 71.4 MB). **It corrects an explanation this plan and the slice-2
handoff both carried**: that these are "most likely genuine burst re-scrapes."
They are not.

All 96,800 adjacent pairs span **two different source objects** — not one pair
is two rows of the same page — and **zero** pairs have `gap == 0`, which
independently confirms that `groups_collapsed: 0` reflects an absence of
exact-key duplicates rather than a missed collapse.

| pair type | pairs | identical business values | what it is |
|---|---:|---:|---|
| carousel ↔ carousel | 82,280 | **82,249 (100.0%)** | one listing carried in two different pages' carousels |
| carousel ↔ detail | 14,415 | **0 (0.0%)** | a summary card and a full page — two different observations |
| detail ↔ detail | 105 | 105 (100.0%) | the burst re-scrape case the plan assumed |

**The burst-re-scrape population is 105 of 96,800 pairs — 0.11%.** What the
cohort actually measures is carousel fan-out in the time domain: one scrape pass
captures many detail pages seconds apart, and a listing popular enough to appear
in several of their carousels is then observed several times inside a 300 s
window. 47,042 pairs (48.6%) are ≤ 1 s apart, which is the signature of a single
pass rather than of re-scraping. By source, the 142,397 captures with a
neighbour are 132,002 carousel and 10,395 detail.

The carousel ↔ detail pairs are not duplicates under any definition — **none**
carries identical values, and the price differs in 7,651 of them. A carousel
card and a detail page are different views of the same listing, and collapsing
them would discard the more informative one.

That leaves exactly one genuinely duplicative population: the 82,249
identical-valued carousel ↔ carousel pairs, about **11.7% of `to_import` rows**
if each run were collapsed to a single winner.

**Why the recommendation is to import all of them.** Production writes this
shape today — one carousel row per hint per artifact, with no deduplication
anywhere in the live path. Verified 2026-08-29 across the whole chain:
`_INSERT_SQL` (`processing/writers/silver_writer.py:38`) is a plain
`INSERT … VALUES` with no `ON CONFLICT`; `staging.silver_observations` has only
a `bigserial` primary key and no uniqueness on `(listing_id, fetched_at)`
(`V025__silver_observations_staging.sql:12`);
`archiver/processors/flush_silver_observations.py` does not deduplicate; and
`dbt/models/staging/stg_observations.sql` has no `distinct`, `row_number`,
`qualify` or `group by`. Silver is a coverage record, not a deduplicated fact
table. Collapsing here would make April's silver uniquely deduplicated against
every other month — worse than the redundancy, because it breaks comparability
and deletes observations production itself would have written. These rows are in
`to_import` precisely because silver holds no record of them; that absence is
the loss being recovered.

Unexplained, and too small to gate anything: **31** carousel ↔ carousel pairs
whose values differ inside the window — a real price change within a second or
two, or a stale card on one of the two pages.

**Everything else the gate asks for.** Recovery duplicates 0 collapsed in 0
groups, and **0 conflicting-fingerprint groups** — the stop that has never fired
did not fire at full population either. VIN collisions 0. The match-count
distribution runs from 701,375 rows at zero candidates to a long tail (40
candidates at the top), which is the existence test behaving as designed:
multiple candidates are normal and none supplies identity or a value.

**The freeze.** The inventory names and fingerprints all four input families:
1,204 `parsed/rows/` objects (242,126,394 B), 1,204 `parsed/inputs/`
(87,411,145 B), the **nine** March–May silver objects (219,710,181 B, unchanged
from the frozen shape, 16,663,136 observations indexed over 119,445 wanted
listings), and 3 `artifacts_queue_events` objects (204,175,412 B). The
read-only VIN snapshot is 61,117 rows, 1,736,857 B, sha256
`085c2655…785c68`. Written to `compared/cmp-6c7c90d807bbdf13/`,
`inventory/cmp-6c7c90d807bbdf13.json` and
`vin_snapshot/cmp-6c7c90d807bbdf13.parquet`.

**What this supersedes.** The probe's 81.1% / 18.9% family split is retired:
rows a block page had put in `already_represented` now land in
`blocked_excluded`, and the authoritative split is 86.74% / 12.22% / 1.04% / 0%.
The probe run `cmp-e37723ede49fad4f` and its 59 `assigned_probe/` shards predate
the filter and are refused by `assign` and `apply` on sight — their
`compare_report.json` has no `blocked_excluded` section.

**What remains unproven.** The parser control has not been run against this
output, so nothing yet establishes that reprocessing reproduces what production
wrote. No identity has been allocated and the sequence is untouched by this run.
The two rulings above are recorded, not decided.

### Evidence — slice 3 Phase A, the parser control run, 2026-08-29

The parser control ran for the first time against real data, on the
authoritative compare run. **It reported `FINDINGS` and exited non-zero.** The
diagnosis below is what that verdict turned out to mean, and it changes the
check rather than the recovery.

```
control --apply --run-id cmp-6c7c90d807bbdf13 --sample-size 500 --seed 145
  exact same-source candidates   4,183,152
  sampled                              500   (carousel 425 / detail 75)
  compared                             498
  no silver row / multiple silver rows   0 / 2
  field disagreements                2,867
  result                          FINDINGS
```

The census decomposes exactly — 206 carousel rows × 13 fields + 27 detail rows
× 7 fields = 2,867 — so **233 of 498 compared rows, 46.8%**, disagreed. Not
noise, and far too large to wave through.

**The cause is the Plan 100 migration boundary, not a parse defect.**
[Plan 100](plan_100_historical_data_migration.md) migrated the legacy
`detail_observations`, `srp_observations` and `detail_carousel_hints` tables
into MinIO silver, and its *Cutoff Date* section fixes the boundary: the Airflow
processing service went live **2026-04-21**, and only rows with
`fetched_at < 2026-04-21` were migrated. April silver is therefore a **mix** —
migrated legacy rows before the 21st, live-written rows from the 21st — and
Plan 145's April population straddles it.

The legacy schema explains the exact field set that disagrees. Plan 100's
`silver/detail` mapping carries `dealer_name`, `dealer_zip` and `customer_id`
and **nothing else dealer-side**: no `dealer_street`, `dealer_city`,
`dealer_state`, `dealer_phone`, `dealer_website`, `dealer_cars_com_url`,
`dealer_rating`, no `seller_id`. Those seven columns did not exist in the old
pipeline, so no reparse can reproduce them — and they are precisely the fields
the control flagged.

**Measured directly.** A read-only scan over 19,872 exact-distance
`already_represented` rows (reservoir sample, seed 145), matched to silver on
the control's own key — same listing, same source, same microsecond — and split
on the cutoff:

| | rows | with a disagreement | mean fields |
|---|---:|---:|---:|
| `fetched_at >= 2026-04-21` | 11,665 | **4 (0.03%)** | 0.00 |
| `fetched_at < 2026-04-21` | 8,404 | **8,404 (100.0%)** | 12.19 |

By attribution — joining the silver row's `artifact_id` back to
`ops_normalized/artifacts_queue_events`:

| silver row | rows | with a disagreement |
|---|---:|---:|
| artifact **unmapped** (no queue event) | 8,120 | 8,120 (100.0%) |
| **same object** as the one being reparsed | 11,949 | 288 (2.4%) |
| different object | **0** | — |

**`different_object` is zero in every bucket.** A competing hypothesis — that
the control's `(listing_id, fetched_at, source)` key was silently matching a
carousel row written by a *different* page — is therefore **refuted**. The
control matches the right artifact; that artifact's silver row simply predates
the current pipeline in 42% of cases. Post-cutoff, 0 of 11,665 rows are
unmapped and 100% resolve to the same object.

**So the control passes on everything it can legitimately test.** Against silver
rows production actually wrote from the same artifact, recovery reproduces
production at **0.03% disagreement — 4 rows in 11,665**. That is the assertion
Stage 5 rests on, and it holds.

**The consequence for the check.** "Recovery must reproduce silver" is not
well-posed against a migrated legacy row: the dealer address fields were never
captured, so there is no page that would reparse to it. The control needs a
**scope predicate** — restrict the sample to `fetched_at >= 2026-04-21`, or
equivalently to silver rows whose `artifact_id` resolves in the queue-event
lake. The two agree here (8,120 of 8,404 pre-cutoff rows are unmapped; 0 of
11,665 post-cutoff rows are), and the date predicate is the cheaper of the two.
Until that lands, the control's non-zero exit should be read as *out of scope*,
not as a parse regression.

**The consequence for the recovery — this is upside, not damage.** On pre-cutoff
observations the reparse recovers a mean of **12.19 silver fields per row** that
the legacy pipeline never captured, chiefly the seven dealer-address columns.
That data exists only because the bronze HTML was kept; it is not recoverable
from any downstream table. It also means recovered April rows before 2026-04-21
will be **richer than their migrated neighbours**, which is a property to state
in the plan rather than discover in a dashboard.

**Residuals, none blocking.**

- **4 post-cutoff `detail` disagreements** in 1,815 (0.2%). Small enough to be
  individual pages rather than a class, but unexamined.
- **2 `multiple_silver_rows`.** The mode counts these as findings, so even a
  clean field census would not have returned `clean` as written.
- **Migrated carousel rows carry `make`/`model`, which Plan 100 says they should
  not.** Its `silver/carousel` mapping lists only `artifact_id`, `listing_id`,
  `source_listing_id`, `source`, `listing_state`, `fetched_at`, `price`,
  `mileage`, `year`, `body`, `condition` — no `make`, no `model`. No version of
  `detail_writer` sets them on a carousel row either, and `srp_writer` writes
  `source='srp'`. The migration implementation therefore diverged from its own
  plan document. Harmless here — those rows are out of scope once the predicate
  lands — but it means Plan 100's schema tables should not be trusted as a
  description of what is actually in the lake.

### Evidence — slice 2, the authoritative assign, 2026-08-29

`assign` ran against the authoritative compare run `cmp-6c7c90d807bbdf13` in
`april-processor` (`cartracker` role, pyarrow). The dry run and `--apply` agree.

```
to_import rows            701,375
artifacts                 341,903
  preserved_queue_event    13,253
  allocated_sequence      328,650
  pack members newly attributed 36,220 of 42,276
batches                        69   (68 bound by the artifact cap, 1 by end)
```

**All four refusals passed on real data.** No NULL or non-UUID `listing_id`, no
object path mapped to two queue-event artifact ids, no stale-compare-run
rejection, and no block-signature hit. Two of these had never fired or been
exercised against a population that could trip them — both probes ran
materialized-only, where the cohorts are structurally empty. This is the first
run with the pack side at full weight and they are genuinely clean.

**The finding: 36,220 of the 42,276 unattributed pack members are
import-bearing — 85.7%.** The 2026-08-28 probe measured **0 of 42,276**. That
is not drift; it is the probe's structural blind spot closing, because those
members live in the 18 unpacked shards the partial compare never reached. The
plan already rules on this case — a pack member with a row to import cannot
stay unattributed, because silver's `artifact_id` is NOT NULL and the Stage 6
repacker needs the attribution — so it is handled by design rather than
discovered. **Downstream consequence: 36,220 previously unattributable pack
members now carry artifact ids, which is material to Stage 6's repacker.**

**Arithmetic.** 13,253 + 328,650 = 341,903. 701,375 rows over 341,903 artifacts
is 2.05 rows per artifact. 341,903 ÷ 5,000 = 68 full batches plus one partial,
matching the reported 69. The **artifact cap binds every time**: the mean batch
is ~10,165 silver rows against a 50,000-row cap, so the row cap never engages.
Preservation rose to 3.9% from the probe's 0.6%, consistent with more
post-cutoff objects having queue events.

**`--apply` durable effect.** 70 objects under `recovery/plan145/assigned/` —
69 batch shards plus the assign report — and
`ops.artifacts_queue_artifact_id_seq` advanced **8,054,031 → 8,383,887**. The
329,856 advance is the 328,650 allocations plus ~1,200 from concurrent live
production traffic. `nextval` is called only for allocations; preserved ids do
not consume one. Permanent and sanctioned: a `bigserial` gap is not a reuse.

**`apply` dry run, batch `cmp-6c7c90d807bbdf13-b00001`:** 5,000 artifacts,
10,157 silver rows, 2,995 price events, 5,000 queue events; the write set was
built and validated and **no statement was issued**. Verified after:
`public.plan145_recovery_batch_receipts` 0 rows and
`staging.artifacts_queue_events WHERE status = 'recovered'` 0. Nothing has
committed.

Note that b00001's 10,157 silver rows are **ten times** the 1,000-row canary
budget. The slice-2 batch unit is therefore not the canary unit, which is what
Phase B has to reconcile.

### Evidence — slice 3 Phase A, the canary sample, 2026-08-29

`canary-sample --apply` against the authoritative run, seed 145, target 500
rows:

```
selected artifacts            234
silver rows                   505   (140 detail / 365 carousel)
strata in population            9
strata covered                  9   (every stratum covered: True)
no artifact split            True
```

| stratum | rows |
|---|---:|
| `carousel \| active \| materialized \| allocated_sequence` | 213 |
| `carousel \| active \| unpacked \| allocated_sequence` | 112 |
| `detail \| unlisted \| materialized \| allocated_sequence` | 105 |
| `carousel \| active \| unpacked \| preserved_queue_event` | 40 |
| `detail \| active \| unpacked \| allocated_sequence` | 14 |
| `detail \| unlisted \| unpacked \| allocated_sequence` | 11 |
| `detail \| unlisted \| unpacked \| preserved_queue_event` | 6 |
| `detail \| active \| materialized \| allocated_sequence` | 3 |
| `detail \| active \| unpacked \| preserved_queue_event` | 1 |

None of the three cross-checks fired — no `missing` object, no `absent`
assigned object (the dropped-shard blind spot closed in `5a5cce7`), and no
`split` artifact. The population holds nine non-empty strata rather than the
sixteen the cross-product allows: `materialized × preserved_queue_event` is
empty by construction, since materialized objects carry content-derived keys
with no queue event, and `carousel × unlisted` cannot exist because an unlisted
page emits no carousel rows.

505 rows slightly overshoots the 500 target because every non-empty stratum is
covered even when that costs a few rows — the documented behaviour of
`--target-rows`.

The manifest at
`recovery/plan145/canary/cmp-6c7c90d807bbdf13-canary_sample.parquet` is
**Phase B's input**.

### Evidence — slice 3 Phase B and the full apply, 2026-08-29

Phase B built, the window run, the canary committed and verified through to the
lake, and the whole `to_import` population applied — all on 2026-08-29 between
14:40 and 15:30 UTC. Deployed at `9ed0f45`.

#### The manifest migration

The frozen Phase A manifest predated `write_set_digest`, which `canary-commit`
requires. It was **migrated, not re-sampled**: re-running `canary-sample`
reselects, and determinism reproduces a selection only while every input is
unchanged — the assumption the digest exists to distrust. `canary-remanifest`
took the frozen object set as given and wrote a new object beside it.

```
object set digest  frozen   0cf77c1a31fb2d0149c12ac9da96b7d9726a3494f8929a3d42b913efc21349f9
                   migrated 0cf77c1a31fb2d0149c12ac9da96b7d9726a3494f8929a3d42b913efc21349f9
                   identical: True
carried forward    artifact_id, id_source, input_kind, batch_name,
                   page_listing_id, silver_rows, detail_rows
added              page_fetched_at, write_set_digest, vin_snapshot_sha256
```

The frozen `…-canary_sample.parquet` (`75c547dd…`) was neither deleted nor
overwritten; `…-canary_sample_digested.parquet` hashes to `d2b9d4d5…`, and every
consumer re-proves the promotion against the original before building a write
set.

#### The V040 live-state proof — two runs

**The window was an ordinary deploy-intent drain, not a container stop.**
`POST /deploy/start` with targets `scraper, processing, ops` holds every mutating
DAG at its `deploy_intent_sensor`; `number_running` — `ops.artifacts_queue` in
pending/processing plus `ops.detail_scrape_claims` in running — was drained to 0.
No container was stopped: those three services are reactive `uvicorn` apps with
no scheduler or background loop, and `shared/deploy_intent.py` has only two
consumers, neither of them a writer of the protected tables.

| window | txid | result | outcome |
|---|---|---|---|
| `p145-canary-2026-08-29` | 49539962 | **PASS** | rolled back deliberately, minutes later |
| `p145-canary-2026-08-29-b` | 49540717 | **PASS** | kept — committed 14:51:23.182919 UTC |

Both runs: `single transaction True`, and all six relations byte-identical
across the canary — `ops.price_observations` 50,920, `ops.vin_to_listing`
200,482, `ops.blocked_cooldown` 146, `ops.detail_scrape_claims` 1,869, and the
two V040 views `ops.ops_vehicle_staleness` 50,920 and
`ops.ops_detail_scrape_queue` 1,162.

**The rollback is evidence, not an accident.** It exercised the path the run
sheet documents — one transaction, scoped by `fetched_at < '2026-05-01'` because
13,253 of the run's artifacts carry preserved historical `artifact_id`s that
could otherwise match a live staging row — and deleting the receipt is what made
the canary re-runnable. It also proved the commit report repairs itself: the
second run rewrote `committed_at` from the receipt (`14:51:23.182919`), not from
the stale first-run value, which is what `canary-flush-verify` uses as its scan
lower bound.

#### The canary and its flush round trip

234 artifacts, **505 silver rows** (140 detail / 365 carousel), 140 price
events, 234 queue events, one receipt — one transaction. `canary-flush-verify`
then found every row **by key**, with staging already emptied by the flushers:

| table | rows | lake object |
|---|---:|---|
| `staging.silver_observations` | 505 | `silver_normalized/observations/source={carousel,detail}/obs_year=2026/obs_month=4/part-b9b51291-…-0.parquet` |
| `staging.price_observation_events` | 140 | `ops_normalized/price_observation_events/year=2026/month=4/part-1795523b-…-0.parquet` |
| `staging.artifacts_queue_events` | 234 | `ops_normalized/artifacts_queue_events/year=2026/month=8/part-2e8a9bde-…-0.parquet` |

0 missing, 0 duplicates. The partitions confirm two design decisions: a silver
row's `source` lives in the hive path and not in the file, and the queue events
land in the month the canary **ran** (August), because
`build_recovery_queue_event` leaves `event_at` to `now()` by design while the
price events carry April's capture time.

#### The canary exclusion, and the full apply

Receipts are keyed by batch name, so the canary's `<run>-canary` receipt could
not stop a full `apply` from rewriting those same 234 artifacts out of
`b00001`–`b00069`. `apply` now reads the canary's commit report, re-reads the
manifest through the digest that report recorded, and drops those object keys —
whole artifacts, computed connection-free so the exclusion shows in the dry run
and in the budget the gate measures, with the receipt confirmed afterwards on
the write connection. A report without a receipt, or a receipt without a report,
both stop.

Applied in 7 rounds of 10 batches, flushing between rounds — not for
correctness, but because `flush_silver_observations` fetches every pending row
into memory in one pass and the archiver container has no memory limit. Each
flush stayed near 100k rows; the VM held flat at 15 GB available throughout, so
the concern proved unfounded at this scale. Batches averaged **4.2 s**.

| | artifacts | silver | price | queue |
|---|---:|---:|---:|---:|
| 69 batches | **341,669** | **700,870** | | |
| canary | 234 | 505 | | |
| **70 receipts, total** | **341,903** | **701,375** | **200,599** | **341,903** |

**That arithmetic is the proof, in both directions.** The batches wrote exactly
234 artifacts and 505 rows short of the assign census, so the exclusion caught
every canary artifact — no duplicate. And the two rows sum to precisely the
census, so it caught nothing else — no gap. `staging.silver_observations` and
`staging.artifacts_queue_events WHERE status='recovered'` both drained to 0, and
the flusher deletes only after a successful Parquet write.

#### What this closes, and what it does not

Closed: the write canary as a real commit; the flush round trip by key; the V040
before/after equality in a named window; the duplicate-write interaction; and
the full apply of all 701,375 `to_import` rows.

Not closed: **Stage 6** — repack, prune, and the deletion of the 1,172 legacy
Parquet objects, which is the plan's actual goal and remains unbuilt. Not one
byte has been deleted. Stage 5b's compression trial is also still outstanding.

> *Overtaken the same day.* Stage 6 was built, the trial ran and split, the
> replacement packs were written and verified, and the superseded packs were
> retired — all on 2026-08-29. See the Stage 6 evidence below. The paragraph
> above is left as written because it was true when written.

The full-population lake verification was not run independently: `canary-flush-verify`
proves the round trip by key for the canary's 234 artifacts only, and the
evidence for the other 341,669 is the receipts plus the flushers' own
delete-on-success contract. A by-key check at full population would need a
mode that does not exist.

### Gate — met

- Every parsed observation is classified exactly once, into one of the **four**
  families — `already_represented`, `to_import`, `blocked_excluded`,
  `unclassifiable` — whose counts sum to the parsed row total. (Three when this
  gate was written; `blocked_excluded` was added by the block-page filter, and
  the four-way sum was met by `cmp-6c7c90d807bbdf13` on 2026-08-29.)
- The final comparison names and fingerprints every parsed, silver,
  artifact-event and VIN-lookup input it used.
- No duplicate `(listing_id, fetched_at)` observation is written.
- Silver times and every emitted price-event time equal the legacy capture
  time; recovered queue-event `fetched_at` does too.
- Every import-bearing artifact carries either its one trusted historical
  `artifact_id` or a sequence-allocated one; none is computed from `max()`.
- Every committed batch has one matching durable receipt and re-running it
  writes zero rows.
- No row is inserted into `ops.artifacts_queue`.
- Live pricing, VIN, cooldown, claim and refresh state is unchanged.
- The carousel fan-out is measured and reviewed before the full apply.

---

## Stage 5b — Fix the packer before it writes new packs — **complete**

Stage 6 does not merely read packs, it **writes** them. Left unfixed, it would
mint fresh April sidecars carrying the scrambled `listing_id` — turning a
historical defect into one this plan actively produces, immediately after
spending three revisions proving that column cannot be trusted.

It also blocks Stage 6's own trial. That trial decides which value the *sort
key* should be, and today a single column is both sort key and recorded
identity. They have to be separable before the question can even be asked.

So the packer is corrected first, and only in the way that is knowable now:

- **`shared/packfile.py`** — `PackMember` gains `cluster_key`. `listing_id`
  stays identity and is what reaches the sidecar; `cluster_key` is placement,
  the value frame boundaries are drawn on, and it defaults to `listing_id`, so
  every other caller is unchanged.
- **`archiver/processors/pack_bronze_html.py`** — the `obs` CTE now selects
  both: `any_value(listing_id) FILTER (WHERE source = 'detail')` as identity,
  and the historical unfiltered reduction as `cluster_key`. `ORDER BY` and
  frame sealing move to `cluster_key`, which is the value they already used —
  so **this change corrects the sidecar without relaying out a single pack.**

That last property is the point. Correcting identity and changing placement are
two decisions, and only the first is settled; bundling them would ship an
unproven rewrite of every pack under cover of a defect fix.

### The audit

Every other reducer over silver was checked for the same shape. The three
analysis scripts that group by `artifact_id` — `estimate_dictionary_savings`,
`estimate_pack_savings`, `diff_log_analysis` — already filter
`WHERE source ILIKE '%detail%'` ahead of the reduction, so carousel rows never
reach them. The remaining `any_value` calls reduce `minio_path`, which is
unambiguous per artifact. `pack_bronze_html` was the only affected reducer.

### Evidence — 2026-08-27

Shipped in `dd6aa26`, before Stage 6 writes anything.

- `shared/packfile.py` — `PackMember.cluster_key` added, defaulting to
  `listing_id`, so every other caller is unchanged. Identity and placement are
  now separable, which is what lets Stage 6's ordering trial ask its question.
- `archiver/processors/pack_bronze_html.py` — the `obs` CTE selects
  `any_value(listing_id) FILTER (WHERE source = 'detail')` as identity and keeps
  the historical unfiltered reduction as `cluster_key`. `ORDER BY` and frame
  sealing use `cluster_key`, the value they already used, so **no pack is
  relaid out**.
- `tests/archiver/test_pack_bronze_html.py` — both gate tests present.
  `test_sidecar_identity_is_the_detail_subject_not_a_carousel_hint` writes the
  six carousel hints *before* the subject, so a reducer ignoring `source`
  cannot pass by luck of scan order;
  `test_an_artifact_with_no_detail_row_has_no_sidecar_identity` asserts the
  carousel-only case yields NULL rather than a guess — the signal Stage 5
  depends on.

The audit of every other silver reducer stands as recorded above: the three
analysis scripts filter `source ILIKE '%detail%'` ahead of their reduction, and
the remaining `any_value` calls reduce `minio_path`, which is unambiguous per
artifact. `pack_bronze_html` was the only affected reducer.

Existing April packs still carry the scrambled column; this stage stops the
defect being reproduced, and Stage 6 rewrites the sidecars it replaces.

### Gate — met

- A regression test builds the production shape — one artifact with one
  `source='detail'` row and six `source='carousel'` rows — and asserts the
  sidecar names the subject. It fails on the pre-fix code.
- The fixture does **not** depend on scan order. `any_value` returns the first
  row it scans and `detail_writer` writes the primary first, so a fixture in
  write order lets the unfixed reducer pass by luck; the subject is written
  last.
- An artifact with only carousel rows yields a NULL identity rather than a
  guessed one — that NULL is the signal Plan 145 depends on.
- `fetched_at` is unchanged, which its own test asserts.
- Placement is byte-for-byte what it was: no pack is relaid out by this stage.
- Every other silver reducer is audited and the result recorded.

---

## Stage 6 — Repack, prune and delete (CAR-22) — **complete**

**Complete, 2026-08-30.** Machinery merged at `64631de`; the trial split so the
incumbent ordering carried; 68 replacement packs written and verified; 32
superseded packs retired; 983,043 loose objects pruned; **1,172 legacy Parquet
objects deleted.**

Run the existing packer over the flattened population, which by now is
complete and — for everything that reached silver — attributable. Verify every
member, retire the superseded packs and sidecars by exact reviewed manifest,
then run the existing prune.

Finally, regenerate the exact 1,172-key deletion manifest from the frozen
Stage 1 census and, with named approval, delete those keys in capped batches
with receipts. Deletion is by exact key, never by prefix, and the 127
`results_page` keys must not appear.

Keep the original April packs until the replacements verify.

No new pack format, generation selector, reader contract or prune algorithm is
part of this plan.

### The ordering trial, before the full pass

April is repacked regardless, so it is also where the open ordering
question gets a controlled answer — from a **bounded trial**, not a second full
pass.

Take ~50,000 contiguous members of the flattened population (about two packs'
worth), pack that same set twice, and compare total stored bytes:

| trial | members added in |
|---|---|
| **current** | the existing clustering key, as the packer orders today |
| **true** | `(listing_id, fetched_at)` on the corrected listing |

Same members, same dictionary, level and frame target; only the order differs.
Discard both trial packs, then run **one** full pass over the whole population
in the winning order. Fix the rule before running: smaller wins.

**Two samples, not one — revised 2026-08-29 when the trial was built.** A
contiguous sample keeps whole the clusters of whichever ordering drew it and
truncates the other's, so a single sample is biased toward the arm that drew
it — the same effect the *Caveats* below describe. A random sample cannot fix
it: 50,000 drawn from 983,043 holds about half a capture per listing and
destroys the clustering *both* arms depend on, measuring nothing. So the trial
draws **one contiguous sample per ordering** and packs each both ways — four
passes — and the rule becomes: true ordering carries only if it is smaller on
**both**. A split verdict leaves the incumbent in place and the question open.

**Cost measured at 18m02s** — the 1–1.5 hour estimate counted GETs and ignored
page cache on the re-read passes. Risk is nil and was confirmed: the trial packs
are built in memory and never stored, the April pack prefix still held exactly
32 packs and 32 sidecars afterwards, and the original packs stay authoritative
until the real replacement set verifies.

**Answered 2026-08-29: a split verdict, so `current` carries.** See *Evidence —
the ordering trial*. Each sample favoured whichever ordering drew it, across a
32-point swing, so the trial cannot separate ordering quality from selection
bias at this size. The repack proceeds in the existing clustering order.

Members with no subject listing are excluded. One cannot inform a question
about ordering by subject listing, and in the `true` arm they would collapse
into a single enormous false cluster under NULL.

#### Why a trial and not a cross-month comparison

Comparing April's finished bytes-per-page against other months cannot isolate
ordering, because the months already differ far more than the effect being
measured. Achieved ratios, from production metadata on 2026-08-27:

| month | members | raw GiB | stored GiB | ratio | `listing_id` correct |
|---|---:|---:|---:|---:|---:|
| April | 557,065 | 86.77 | 1.99 | 43.66x | 31.4% |
| May | 1,021,266 | 170.45 | 2.51 | 67.95x | 59.5% |
| June | 1,124,122 | 190.30 | 2.32 | 82.00x | 9.8% |
| July | 909,654 | 157.28 | 2.03 | 77.44x | 8.4% |

The ratio does not track clustering quality at all — June, the *worst*
true-listing clustered month, achieves the *best* ratio. Between-month spread
is nearly 2x, against an ordering effect of 8–19%. That is not evidence
ordering is irrelevant; it is evidence that cross-month comparison is far too
noisy to detect it. Holding the population fixed is the only way to attribute
the difference.

#### Caveats for the result

- **April is the least representative month.** Its sidecar is already 31.4%
  correct, so its current clustering is a hybrid; June and July are ~9%. The
  direction should generalize; the magnitude should not be assumed to.
- A 50,000-member trial cannot gather every capture of a listing the way a
  month-global sort would, so it still understates true-listing ordering — less
  than the two bench tests did, and on a population that is at least fixed.

If the true ordering wins, the size of the win is what justifies (or does not)
a separate plan to reorder May, June and July — 6.86 GiB and ~3M members that
this plan does not touch.

### What NULL identity will look like in the replacement packs — corrected 2026-08-29

This gate originally asked for NULL identity to fall from 99,981 to **42,276**.
Both numbers describe the **557,065-member** pack population. The replacement
packs hold the flattened **983,043**, and ~426k of those are materialized
objects whose content-derived keys no `artifacts_queue_events` row has ever
named. The original figure cannot be met, and is not the right question.

Derived from the recorded `assign` and full-apply censuses, and **since
confirmed exactly by `repack-verify` on 2026-08-29** — every cell below was
reproduced to the member by the sidecars the packer actually wrote:

| origin | members | with a queue event | **no queue event** |
|---|---:|---:|---:|
| old pack member | 557,065 | 551,009 | **6,056** |
| materialized | 425,978 | 292,430 | **133,548** |
| **total** | **983,043** | **843,439** | **139,604** |

- 42,276 pack members had no event when they were packed; `assign` attributed
  36,220 of them, leaving 6,056.
- Of the 341,903 import-bearing artifacts, 13,253 preserved an existing queue
  event and 328,650 were allocated one. A preserved event can only belong to a
  pack member — materialized keys are content-derived and production never saw
  them — so 49,473 import-bearing artifacts are pack members and **292,430 are
  materialized**. The remaining 133,548 materialized objects are
  `already_represented`, `blocked_excluded`, or emit no rows; they get no event
  and no silver row, and are NULL by construction, exactly as a carousel-only
  pack member is.

**NULL `listing_id` will be higher than 139,604**, because an artifact with a
queue event but only `source='carousel'` silver rows has an id and no subject
listing. That is Stage 5b behaving as designed — the NULL is the signal, not a
defect.

`repack-verify` measures and reports this decomposition by origin. The number
above is what the run is checked against; a large divergence is a finding to
understand before anything is retired.

### Evidence — the Stage 6 dry runs, 2026-08-29

Every mode run read-only against production, on `master` at `64631de`, in
`docker compose run --rm archiver`. **Nothing was packed, retired, pruned or
deleted**, and the only write any of them made is one verification report under
the recovery prefix. Logs on the VM under `~/plan145-s6-*.log`, tmux session
`plan145-stage-6`.

The archiver image had to be rebuilt first: it dated from 14:13 UTC, before the
16:42 merge, and carried none of the four modes. The archiver bakes code into
its image rather than mounting the source, so `compose run` alone would have run
Stage 5's binary and reported `invalid choice: 'pack-trial'`.

#### `pack-trial` — the metadata query, and a confirmed derivation

```
population members          843,439
eligible for the trial      657,629
  sample drawn in current        50,000 members
  sample drawn in true           50,000 members
--apply would read 200,000 objects and build 4 pack sets, writing none.
```

**843,439 is exactly the figure this plan derived for "members with a queue
event"** when the Stage 6 gate was corrected — 551,009 old pack members plus
292,430 materialized, reasoned from the assign and full-apply censuses before
anything was run. The packer's own `fetch_member_metadata` reproduces it to the
member. The derivation is confirmed, not merely plausible.

The dictionary resolved to `1367127621` from MinIO — the same production
dictionary Stage 3b unpacked with, so the trial will measure production's
packing rather than something adjacent to it.

#### Sidecar identity in the replacement packs, now measured

The corrected gate predicted the decomposition; a bounded re-run of the same
metadata query measures it exactly:

| | members | `listing_id` |
|---|---:|---|
| both `listing_id` and `cluster_key` set | 657,629 | recorded |
| `listing_id` NULL, `cluster_key` set | 137,209 | **NULL** — carousel-only artifacts |
| `listing_id` set, `cluster_key` NULL | **0** | impossible by construction |
| both NULL | 48,601 | **NULL** — queue event, no April silver row |
| **described by the lake** | **843,439** | |
| not described at all | 139,604 | **NULL** — no queue event |
| **flattened population** | **983,043** | |

So the replacement sidecars will carry **657,629 members with a subject listing
and 325,414 without** — and 657,629 + 325,414 = 983,043 exactly.

The zero in row three is worth keeping: `cluster_key` is the unfiltered
reduction and `listing_id` the `source='detail'` filter of the same group, so a
member can never have identity without placement. It is a structural invariant
of the Stage 5b split, and the population satisfies it with no exceptions.

**This retires the gate's original figure for good.** 42,276 was a property of
the 557,065-member pack population; the replacement packs hold 983,043, and
325,414 of them have no subject listing. The plan's corrected prediction — "NULL
`listing_id` will be higher than 139,604" — is right, and the margin is 2.3x.

#### `repack-verify` — refuses, on the two grounds it should

```
old packs (frozen 3b)                32          replacement packs        0
baseline members                557,065          live population    983,043
old member not replaced         557,065          live object not packed 983,043
old member bytes changed              0          member in two packs      0
REFUSED: no replacement sidecars — run the repack before verifying it
REFUSED: the replacement packs do not cover the old population
VERDICT                    FAIL
```

Exit 1. Every load path ran before the refusal, which is what makes the run
worth having:

- the frozen Stage 3b baseline resolved to **557,065 members over 32 packs**,
  from the 32 unpack manifests;
- the sidecar split found **32 old, 0 replacement**, against the frozen set
  rather than against sequence numbers;
- the population derived from the Stage 2/3a/3b manifests came to **983,043**,
  matching `EXPECTED_FLATTENED_INPUTS` and the count Stage 4 parsed, in 23
  seconds and ~2,376 Parquet reads.

**Peak memory 1.148 GiB** against 23.42 GiB on the VM — inside the 1–1.5 GiB
the handoff predicted, and not a constraint.

The report is durable at
`recovery/plan145/repack/repack-e3b0c44298fc1c14/verify_report.json`, naming all
32 old pack keys `pack-00000` through `pack-00031`.

One cosmetic note: with no replacement sidecars the run id hashes the empty
string, hence `e3b0c44298fc1c14`. Harmless — a run in that state always fails —
but two such runs would overwrite one report.

#### The two deletion gates refuse a *failed* report, not merely a missing one

Both stopped at exit 2, quoting the report and both of its refusals:

```
STOP: recovery/plan145/repack/repack-e3b0c44298fc1c14/verify_report.json did not
pass: no replacement sidecars — run the repack before verifying it; the
replacement packs do not cover the old population
```

This is a stronger test than the one planned. A missing report only proves the
lookup fails closed; a report that exists and says `passed: false` proves the
gate reads the verdict. `retire-packs` and `delete-legacy` both refuse to delete
anything on the strength of a verification that failed.

#### `pack_bronze_html --repack-bucket` — the flag, and the store cross-check

```
repacking            true          already_packed     557,065
objects_pending    983,043          next_seq                32
orphan_packs            []          read_failures            0
source_bytes  9,478,040,747         estimated_packs_upper_bound  142
```

Listed **983,043 objects in 1,643s** (27.4 min, mean 598 keys/s).

Three things this establishes that nothing else could:

- **The store agrees with the manifests.** The listing is an independent walk of
  `html/year=2026/month=4/artifact_type=detail_page/`, and it returns exactly
  the 983,043 the Stage 2/3a/3b manifests derive. Nothing unexpected has
  appeared in the prefix, and nothing the manifests claim has gone missing. This
  is the cross-check `repack-verify --list-population` exists for, obtained
  early and for free.
- **`--repack-bucket` does precisely what it claims.** `already_packed` is
  557,065 — the exact frozen baseline, every one of them still a live loose
  object — and `pending` is nonetheless the full 983,043. Without the flag,
  `pending` would have been 425,978 and the scrambled sidecars would have
  survived the stage. `next_seq` is 32, so replacements will be
  `pack-00032` onward and nothing overwrites `pack-00000`–`pack-00031`.
- **No orphan packs and no read failures**, so the 32 existing packs are all
  sidecar-complete and there is no interrupted prior run to reason about.

`source_bytes` is 8.83 GiB of individually-compressed loose objects. The 142-pack
upper bound assumes zero cross-member compression gain and is therefore loose:
April's current 32 packs hold 557,065 members in 1.99 GiB, so the replacement
set should land nearer ~3.5 GiB and ~56 packs. Free space is 105.5 GiB against a
5 GiB floor.

#### Timing, measured rather than budgeted

| phase | measured |
|---|---|
| archiver image rebuild | ~3 min |
| `pack-trial` dry run | 35 s |
| `repack-verify` | 26 s, peak RSS 1.148 GiB |
| `retire-packs` / `delete-legacy` gates | ~30 s each, dominated by container start |
| April prefix listing | **1,643 s (27.4 min)** |

The listing figure is the one that matters, because **step 3 pays it again** —
the repack lists before it reads. Nothing caches the enumeration between the two
commands, so ~27 minutes is spent twice across the stage.

**Keep other work off MinIO during a listing.** Two DuckDB queries run
concurrently here dropped the instantaneous rate from ~750 to 446 keys/s and
cost ~90 seconds. Per-chunk rates recovered to a flat ~600 as soon as they
finished, and the cumulative figure the log prints kept falling only because it
is a running average dragging the two slow chunks along — it looked like decay
and was not. During the real repack, which reads 983k bodies as well, contention
would be far more expensive.

### Evidence — the ordering trial, 2026-08-29

`pack-trial --apply`, run id `trial-5fbadb36972161fb`, 17:31:26–17:49:28 UTC,
**18m02s**. Production dictionary `1367127621`, level 9, 16 MiB frame target,
64 MiB pack roll — the packer's own values, resolved from it rather than
restated.

The run id is a hash of the sample composition and **reproduced the dry run's
exactly**, 45 minutes apart, so the selection is deterministic and the
population did not move between them.

| sample | arm | stored bytes | packs | frames | ratio |
|---|---|---:|---:|---:|---:|
| drawn in `current` | current | 170,640,788 | 3 | 407 | **54.39x** |
| | true | 217,572,014 | 4 | 486 | 42.65x |
| drawn in `true` | current | 176,817,588 | 3 | 374 | 46.70x |
| | true | 168,407,136 | 3 | 370 | **49.04x** |

```
drawn in current  true ordering is larger  by 46,931,226 B (27.50%)
drawn in true     true ordering is smaller by  8,410,452 B ( 4.76%)

WINNER                 current
  split verdict -- the incumbent carries and the question stays open
```

**The decision rule, fixed before the run, returns `current`.** The repack
therefore proceeds in the existing clustering order and
`fetch_member_metadata` is not changed. The compression question stays open,
which is the honest outcome rather than a null one.

#### The split is the finding

Each sample favours whichever ordering drew it, and **the swing is 32
percentage points** — from true being 27.50% worse to true being 4.76% better,
on the same population, the same dictionary, the same frame target, with only
the 50,000 members differing. That is far larger than the effect under test.

This is exactly the bias the plan's own *Caveats for the result* section
predicted and could not size. It can now be sized, and it is bigger than the
signal.

**A single sample would have produced a confident wrong answer in whichever
direction it was drawn.** The plan originally specified one — "the first
~50,000 members of the flattened population" — which, taken in stored order, is
the `current` draw: it would have reported *true ordering is 27.50% worse* and
closed the question on the strength of a selection artefact. Drawn the other
way it would have reported a 4.76% win. The two-sample rule is what turned an
answer into a refusal.

Why the asymmetry, mechanically: the scrambled `cluster_key` is a **coarser**
key than the true listing — the plan measured 9.98 members per scrambled value
against 4.25 per true listing in-pack. A contiguous draw in `current` order
therefore captures whole coarse clusters, and re-sorting that sample by true
listing shatters them into fragments (−27.50%). A contiguous draw in `true`
order captures whole fine clusters spread across many *partial* coarse ones, so
re-sorting by `cluster_key` costs much less (−4.76%). Coarse clustering is
simply more robust to sample truncation, which means a bounded trial measures
containment at least as much as it measures ordering.

#### What this says about reordering May, June and July

The plan asked whether a win would justify a separate plan for the other three
months — 6.86 GiB and ~3M members it does not touch.

In its **most favourable** condition here, with whole true-listing clusters
present, true ordering wins by **4.76%**. Applied to 6.86 GiB that is about
**0.33 GiB**, against a full repack of ~3M members. The earlier bench tests
showed true ordering 19.4% and 8.4% *worse* as it was given more of its
cluster, so the trend toward parity the plan identified does continue past
parity — but it arrives at a modest win, not a transformative one.

That is not a decision, and this trial cannot make it: 50,000 members is not a
month-global sort. It is the first evidence that the ceiling is low.

#### The trial wrote no pack

Verified after the run: the April pack prefix holds **32 `.zpack` and 32
`.idx.parquet`** — unchanged — and `recovery/plan145/pack_trial/` holds exactly
**one** object, the report. Both trial pack sets were built in memory and
discarded, which is a stronger form of "the trial packs are discarded" than
deleting them, and it is why no trial pack can ever be mistaken for a
replacement by `_pack_state`.

#### Timing, corrected again

18m02s, not the 1–1.5 hours this plan and the run sheet budgeted. Per pass, for
50,000 members each:

| pass | duration |
|---|---|
| `current` sample, `current` order | 5m31s |
| `current` sample, `true` order | 3m47s |
| `true` sample, `current` order | 4m29s |
| `true` sample, `true` order | 3m44s |

~151 objects/s on the first pass and faster after, the later passes benefiting
from page cache on re-read. The 1–1.5 hour figure was extrapolated from GET
counts alone and did not account for that.

### Evidence — the repack and its verification, 2026-08-29

`pack_bronze_html --year 2026 --month 4 --repack-bucket --max-packs 0 --apply`,
17:56:59–20:17:35 UTC, **2h20m** — 33 min listing, 1h47m packing. Nothing was
deleted.

```
packs_written        68        members_packed      983,043
members_verified 983,043       read_failures             0
pack_bytes   4,577,402,957     source_bytes  9,478,040,747
```

**Every one of the 983,043 members was verified from its stored pack.**
`_verify_stored_pack` re-extracts each member out of the object as it actually
landed in MinIO and checks its sha256 before the sidecar is written, so
`members_verified == members_packed` is a statement about the store, not about
what was in memory. Zero read failures across 983,043 reads.

The replacement set is `pack-00032`–`pack-00099`, 4.58 GB over 68 packs. The
original 32 are untouched at 2.13 GB, and both sets carry a sidecar: 100 packs,
100 sidecars.

#### Compression: 39.28x, below the original packs, and why

179.81 GB raw into 4.58 GB stored is **39.28x**, against the original April
packs' recorded **43.66x**. That is a property of the population, not of the
packing.

The head packs beat the old ratio comfortably — `pack-00036` holds 18,646
members, 3.59 GB raw in 67.4 MB, **53.2x**. The tail collapses:
`pack-00099` holds 6,326 members, 1.31 GB raw in 56.6 MB, **23.1x**.

`iter_ordered_keys` yields metadata-matched members in cluster order and then
whatever silver never described, sorted by key. The original packs contained
only the 557,065 members production had already packed; this set adds 425,978
materialized objects, of which **139,604 have no `artifacts_queue_events` row at
all** and therefore cluster on nothing. They land in the tail and compress like
unrelated documents, because that is what they are to the compressor.

This also settles the ordering question for the tail specifically: a
true-listing sort could not help members that have no listing either.

The storage case is unaffected. After retire and prune, April goes from 2.13 GB
(old packs) + 8.83 GB (loose objects) + 13.66 GB (legacy Parquet) ≈ **24.6 GB**
down to **4.58 GB**.

#### `repack-verify` — PASS

Run `repack-4ea1c730c8b96ac1`, 20:22:52–20:24:05, read-only.

```
old packs (frozen 3b)          32        replacement packs          68
baseline members          557,065        live population       983,043
replacement members       983,043
old member not replaced         0        old member bytes changed    0
live object not packed          0        member in two packs         0
packed, no live object          0
read back sampled           1,972        mismatched                  0
VERDICT                      PASS        refusals                   []
```

Every coverage class is zero. The read-back sampled 1,972 members — 29 from each
of the 68 replacement sidecars — and extracted each from **the pack its
replacement sidecar names**, not through `read_packed_html`, which the old
sidecars would still answer for all 557,065 replaced members while both sets
exist.

#### The corrected gate, confirmed cell by cell

The Stage 6 gate correction derived its figures from the assign and full-apply
censuses and marked them *derived, not yet measured*. The verifier measures
them:

| | derived | measured |
|---|---:|---:|
| no `artifacts_queue_events` row, total | 139,604 | **139,604** |
| — materialized | 133,548 | **133,548** |
| — old pack member | 6,056 | **6,056** |
| no `listing_id`, total | 325,414 | **325,414** |
| with a subject listing | 657,629 | **657,629** |

Exact in every cell. By origin:

| origin | members | no `artifact_id` | no `listing_id` | attributed |
|---|---:|---:|---:|---:|
| `old_pack_member` | 557,065 | 6,056 | 54,682 | 502,383 |
| `materialized` | 425,978 | 133,548 | 270,732 | 155,246 |
| **total** | **983,043** | **139,604** | **325,414** | **657,629** |

The two NULL columns differ by **185,810** — members that carry an
`artifact_id` but no `source='detail'` silver row, which is precisely the
carousel-only cohort the pre-run DuckDB breakdown measured at 137,209 + 48,601 =
185,810. Three independent measurements of the same population agree: the
metadata query, the arithmetic from Stage 5's censuses, and the sidecars the
packer actually wrote.

#### The Stage 5b fix reached the sidecars

```
compared              557,065
listing_id unchanged  143,873   (25.8%)
listing_id differs    358,510   (64.4%)
listing_id now NULL    54,682   ( 9.8%)
changed share          74.17%   (floor 50%)
```

74.17% of replaced members carry a different `listing_id` than the old sidecar
did, well clear of the 50% floor. The 54,682 that became NULL are carousel-only
artifacts whose old sidecar asserted a scrambled listing and whose replacement
correctly asserts none — the NULL is the signal Plan 145 depends on, now being
written rather than guessed at.

The 25.8% left unchanged is where the historical `any_value` happened to pick
the detail subject, against the 31.4% the plan measured for April by a different
method. The two are not the same comparison and are not expected to match
exactly; both say the same thing, which is that roughly a quarter to a third of
April's old sidecar identities were accidentally right.

### Evidence — retiring the superseded packs, 2026-08-29

`retire-packs --apply --verify-run-id repack-4ea1c730c8b96ac1`, 23:07 UTC.

```
verified by            repack-4ea1c730c8b96ac1
replacement packs                68
packs to retire                  32
objects to delete                64
members they held           557,065
  receipt deleted                  64
```

The manifest was written to
`recovery/plan145/retire/repack-4ea1c730c8b96ac1/manifest.parquet` **before the
first delete**, so an interrupted run would still leave a complete record of
what it intended to remove. Sixty-four receipts, every one `deleted`; no
`absent`, no `error:`.

**The run-id argument was not optional.** Two verify reports existed — the
failed `repack-e3b0c44298fc1c14` from the dry-run phase and the passing
`repack-4ea1c730c8b96ac1` — and the mode refused to choose between them, naming
both and stopping. It is worth recording that the ambiguity guard fired on a
real ambiguity rather than a contrived one.

Store afterwards: **68 packs, 68 sidecars, sequence 32–99, 4,577,402,957 bytes,
and no pack below sequence 32.** The originals are gone.

At this point every one of the 983,043 members exists in exactly two places —
its loose `.html.zst` object and a verified replacement pack. The prune removes
the first, which is what makes the verification above load-bearing rather than
ceremonial.

### Evidence — the prune, 2026-08-29/30

`delete_packed_source_html --year 2026 --month 4 --max-packs 0 --max-objects 0
--apply`, 23:43:52–02:26:44 UTC, **2h43m** — 29 min listing, 2h13m draining 68
packs at ~2.0 min each.

```
run complete (apply) — deleted=983043 verified=983043 refused=0 already_gone=0
bytes=9478040747 inodes~2202016 (measured delta 1937177)
by_status={'complete': 445796, 'no_event_row': 468254, 'ok': 19950,
           'retry': 443, 'skip': 48600}
```

**983,043 deleted, 983,043 verified, zero refused, zero already gone.** Every
deletion was preceded by the full per-member check: the key resolves to the
right pack prefix, the extracted member matches the sidecar's `raw_sha256`, and
the loose object matches the packed bytes exactly. 25 members per pack also went
through the complete production resolver. `surviving == members == handled` on
all 68 packs, so nothing was skipped and nothing was assumed.

8.83 GB and **1,937,177 inodes** reclaimed — the measured delta against an
estimate of 2,202,016, the difference being ordinary filesystem churn from
production running alongside.

#### `no_event_row: 468,254` decomposes exactly

| | members |
|---|---:|
| no `artifacts_queue_events` row at all | 139,604 |
| Stage 5 `allocated_sequence` artifacts | 328,650 |
| **total** | **468,254** |

The second term is the whole `allocated_sequence` census from Stage 5's
`assign`, and it appears here because those artifacts' `recovered` queue events
carry `event_at = now()` — August — while `fetch_terminal_status` scans only the
April window. They have identity; this particular query cannot see it.

This is harmless and was predicted: status is **report-only** in this processor
and cannot gate a deletion, which is exactly why the run sheet warns against
"fixing" the packer's `paths` glob to an April window. It is recorded because a
future reader will meet the same 468,254 and should not have to re-derive it.

`skip: 48,600` corresponds to the blocked cohort — within one of the 48,601
members the pre-run breakdown counted as having a queue event but no April
silver row.

---

### Evidence — deleting the legacy Parquet, 2026-08-30

**This is the plan's goal.** `delete-legacy --apply --maintainer-approval
"Andrew Miller" --census-from-manifests --verify-run-id repack-4ea1c730c8b96ac1`,
03:53:25–03:54 UTC, under a minute.

```
verified by            repack-4ea1c730c8b96ac1
approved by            Andrew Miller
legacy objects                1,172  (baseline 1,172)
planned for deletion          1,172
refused                           0
mode                   apply

  receipt deleted               1,172
  reconciled                    1,172

legacy detail Parquet remaining             0  (must be 0)
results_page objects                    2,380  (was 2,380)
```

Every gate the plan set for this moment:

- **1,172 planned, 1,172 deleted, 1,172 reconciled**, zero `absent` and zero
  `error:`. Deletion by exact key from a manifest written to
  `recovery/plan145/legacy_delete/repack-4ea1c730c8b96ac1/manifest.parquet`
  **before the first delete**, in capped batches, never by prefix.
- **`refused 0`** — the coverage join cleared. Every body Stage 2 derived from
  every one of the 1,172 objects is present in a replacement sidecar. The
  matching hash set held **983,041 distinct hashes across 983,043 members**,
  the two-member gap being the packed↔packed content duplicates that Stage 3a's
  own verification first surfaced as 557,063 distinct across 557,065. The same
  discrepancy arriving independently from the other end is a good sign the
  accounting is coherent rather than coincidental.
- **1,172 against baseline 1,172** — the drift gate passed against the frozen
  Stage 1 census, so the set deleted is the set frozen on 2026-08-21. Its bytes
  were still `14,670,223,837` on the morning of the deletion, exact to the byte
  against what Stage 1 recorded nine days earlier.
- **The results-page population is unchanged at 2,380 objects** (2,253 `.zst`
  and the 127 out-of-scope Parquet). Refused by key as a predicate, not filtered
  out of a listing.
- **Named approval recorded** in the manifest and every one of the 1,172
  receipts.

#### The end state

| prefix | before Stage 6 | after |
|---|---:|---:|
| old April packs | 2,133,921,814 | — |
| loose `.html.zst` | 9,478,040,747 | — |
| legacy `.parquet` | 14,670,223,837 | — |
| replacement packs + sidecars | — | 4,655,215,649 |
| **total** | **24.48 GiB** | **4.34 GiB** |

`html/year=2026/month=4/artifact_type=detail_page/` is **empty** — zero objects,
zero bytes. **20.14 GiB reclaimed**, against the ~13.66 GiB the plan set out to
delete; the rest is the loose population the flattening created and the prune
removed.

April's 983,043 distinct captures now live in 68 verified packs, each member
carrying the corrected `listing_id` where silver can describe it and an honest
NULL where it cannot.

### Gate — met

- Every retained member reads byte-identically before old packs are retired.
  **Met** — `repack-verify` PASS: 0 unreplaced, 0 bytes changed, 0 in two packs,
  1,972 sampled read-backs with 0 mismatches, each extracted from the pack its
  replacement sidecar names.
- The existing packer verifies every replacement member; prune reports zero
  unexplained failures. **Met** — 983,043 of 983,043 verified at pack time, and
  the prune verified 983,043 and refused 0.
- Sidecar identity is reported **decomposed by origin** — old pack member
  against materialized, attributed against NULL — and reconciles with the
  derivation above (~139,604 members with no `artifacts_queue_events` row), or
  the difference is explained.
- Replacement sidecars carry the **correct** `listing_id`, whichever ordering
  wins; identity and sort key are recorded independently. Agreement with the
  old sidecar across most members is a **failure**, not a pass: April's was
  correct for 31.4% of members, so near-total agreement means the scrambled
  column was written again.
- The ordering trial runs on fixed ~50,000-member subsets — **one drawn in each
  ordering**, because a single contiguous sample keeps whole the clusters of
  whichever ordering drew it — both orderings are compared on each, the winner
  carries the single full pass only if it wins on **both**, and the trial packs
  are discarded. The decision rule is fixed before the run.
- Deleted, absent and failed legacy-key counts reconcile to exactly 1,172.
  **Met** — 1,172 `deleted`, 0 `absent`, 0 `error:`.
- The legacy `detail_page` prefix contains zero Parquet objects. **Met** — the
  prefix is empty: zero objects, zero bytes.
- The legacy `results_page` population is unchanged. **Met** — 2,380 objects
  before and after, including all 127 out-of-scope Parquet.

---

## Testing

- fixture Parquet in the exact Plan 72 schema proving census, hash
  recomputation and duplicate collapse;
- disposition rules, content-derived key stability, and refusal to write on a
  read-back hash disagreement;
- the deletion join refusing a key whose content is not in a verified pack;
- the unpack preserving original keys and verifying every member;
- the parser consuming production-decoded HTML with manifest identity and
  authoritative time;
- block-page classification, against real Akamai and Cloudflare bodies;
- comparison partitioning every parsed observation exactly once;
- a real-Postgres test proving missing `artifact_id` values come from the
  sequence, committed batch receipts make a retry a zero-write no-op, no
  `artifacts_queue` row is created, and live tables and V040 views are
  unchanged;
- existing pack/read/prune integration tests as the Stage 6 storage proof;
- deletion refusing a results-page key, an unapproved run, or an unverified
  manifest.

---

## Success criteria

**All met, 2026-08-30.**

| Metric | Required | Achieved |
|---|---|---|
| Legacy detail Parquet deleted | 1,172 objects / ~13.66 GiB | **1,172 / 14,670,223,837 B** |
| Legacy results Parquet deleted | 0 | **0** — 127 untouched, prefix unchanged at 2,380 objects |
| Distinct successful captures unaccounted for | 0 | **0** — 983,043 packed and verified |
| Materialized objects failing read-back | 0 | **0** across 807,797 |
| Objects deleted whose content was not in a verified pack | 0 | **0** — prune refused 0 of 983,043; legacy coverage join refused 0 of 1,172 |
| Duplicate `(listing_id, fetched_at)` writes | 0 | **0** — the canary exclusion arithmetic closes in both directions |
| Block pages imported as observations | 0 | **0** — 59,460 quarantined by the Stage 5 filter |
| Import-bearing artifacts without a trusted preserved or sequence-allocated `artifact_id` | 0 | **0** — 13,253 preserved + 328,650 allocated = 341,903 |
| Rows inserted into `ops.artifacts_queue` | 0 | **0** |
| Legacy `artifact_id` used as a join key | 0 | **0** |
| Sidecar `listing_id` used as a join key | 0 | **0** |
| Hot-state mutations caused by recovery | 0 | **0** — six protected relations byte-identical across the V040 window |
| Deletion without named approval | 0 | **0** — recorded in the manifest and all 1,172 receipts |

Unrecoverable by construction, and accepted as a closed loss: the **11,453**
successful captures with no bytes in the Parquet, no surviving individual
object, and no silver observation.

---

## Rollback and stopping points

- **After Stage 1:** nothing to roll back.
- **During Stage 2:** stop between source files; content-derived keys mean a
  restart resumes rather than duplicates.
- **During Stage 3a:** deleted objects are recoverable from the packs, which
  are untouched. `read_html` falls back to the pack throughout.
- **During Stage 3b:** stop between packs; existing keys are skipped.
- **During Stage 4:** parsing writes only to the recovery prefix.
- **During Stage 5:** stop between capped batches; written observations remain
  valid.
- **During Stage 6, before the retirement:** the original packs remained
  authoritative until the replacements verified. That is now spent — the 32
  originals were retired 2026-08-29 after `repack-verify` PASSED, and there is
  no older pack set to fall back to.
- **During Stage 6, between the retirement and the prune:** every member exists
  twice, as a loose `.html.zst` object and as a verified replacement pack
  member. Either side alone reconstitutes the population.
- **During the prune:** stop between packs. It verifies each member against its
  pack before deleting the object and refuses rather than deletes on any
  disagreement, so a partial run leaves a consistent store — some members with
  two copies, the rest with one.
- **After the prune:** the replacement packs are the only copy of the recovered
  population, and the legacy Parquet is still the only copy of anything the
  packs do not hold. That is the state the deletion manifest's coverage join
  exists to check.
- **After legacy deletion:** restoration from MinIO versioning or backup only.
  This is why named approval and complete receipts are the final gates.

---

## Effect on other plans

| Plan | Effect |
|---|---|
| 132 | Superseded; its orphan measurements remain evidence |
| 137 | Superseded; its census and its sub-1 KiB cohort remain evidence |
| 131 | Its pack, verify, fallback and prune machinery carries Stages 3 and 6 |
| 133 | Its hardened pack read path carries the unpack |
| 111/112/113 | Receive the recovered April observations; no live refresh state changes |

---

## Out of scope

- All legacy and current `results_page` cleanup.
- Preserving 403 challenge pages or 5xx response bodies.
- **Fixing `_detect_challenge`'s blindness to non-Cloudflare block pages.**
  Measured, documented and filtered here; the parser fix is its own ticket so
  that this plan's comparison runs against an unmodified parser.
- A reusable historical-reprocessing framework.
- A new pack format, generation selector, reader or prune algorithm.
