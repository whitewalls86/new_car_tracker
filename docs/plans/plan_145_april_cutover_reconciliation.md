# Plan 145: Recover April Detail Artifacts and Delete the Legacy Parquet

## Status

**Third revision — 2026-08-27.** The goal and success criteria have survived
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

Stage 1 is complete (commit `3f6e6d4`). Stage 2 is complete (2026-08-27).
Stage 3a is complete (2026-08-27); Stage 3b is in flight.

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

### The compression question — open, and not to be guessed at

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

**Stage 6 answers it with a bounded trial instead.** April is repacked
regardless, so a fixed ~50,000-member subset is packed both ways, the winner
carries the single full pass, and the trial packs are discarded. Holding the
population fixed is what makes the difference attributable — cross-month
comparison cannot, because achieved ratios already range 43.66x to 82.00x
without tracking clustering quality at all. See *The ordering trial* under
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

## Stage 3 — Flatten the population (CAR-20)

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

### Evidence — 3b, in flight

Dry run verified 2 packs / 34,751 members — every member ranged-read and
checked against its sidecar `raw_sha256`, no failure. `unpack --apply` started
2026-08-27 20:24 UTC under tmux `plan145-s3b` (log
`/home/ubuntu/plan145-unpack.log`), writing via `write_html` under each
member's original `source_key` with production dictionary `1367127621`.
557,065 members, ~3h budget.

### Gate

- Deletion is by exact key from a written manifest with receipts, never by
  prefix; the count reconciles against the sidecar hash join. **3a: met.**
- No key is deleted whose content is not provably in a verified pack.
  **3a: verified, 0 exceptions.**
- Every unpacked member verifies against its `raw_sha256` on write.
  *(3b, in flight.)*
- The flattened population contains no two objects with identical content.
  *(end-of-3b check.)*
- `ops.artifacts_queue` is not written; no silver row is written.
  **3a: held.**

---

## Stage 4 — Parse the flattened population (CAR-23)

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

### Gate

- Every input is parsed or carries an explicit recorded failure.
- Every parsed row carries the authoritative capture time.
- The block-page cohort is measured and excluded, with counts.
- No production mutation outside the recovery prefix.

---

## Stage 5 — Compare to silver, then apply (CAR-21)

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

| | 20 units | 315 units |
|---|---:|---:|
| parsed rows | 44,446 | 657,963 |
| source objects | 7,866 | 113,438 |
| already represented | 77.2% | **80.7%** |
| to import | 22.8% | **19.3%** |
| unclassifiable | 0 | 0 |
| more than one silver candidate | 21.04% | **21.1%** |
| carousel rows per object | 4.65 | 4.80 |
| recovery duplicates collapsed | 0 | 0 |
| unrepresented captures with a neighbour ≤300 s | 39.1% of to-import | **21.3%** |

The multiple-candidate share is stable across a 15× increase in sample and
agrees with the 2026-08-27 design probe's 18%, which is the evidence that the
existence test behaves consistently at scale. Object counts extrapolate to
~978,000 against `EXPECTED_FLATTENED_INPUTS`'s 983,043, so the arithmetic
closes. Memory did not move on the Grafana panels; CPU spiked.

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
after the whole population has been scanned and the cohort reported, so the
maintainer learns its size rather than the first offending row. One object path
mapped to two queue-event artifact ids stops the run rather than choosing.
Re-assigning a run under different batch caps is refused, because the caps
decide membership and the batch names would not change. `apply --apply` across
more than one batch is refused without `--maintainer-approval <name>`, which is
the plan's own non-negotiable expressed in the tool.

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

### Gate

- Every parsed observation is classified exactly once, into one of the three
  families, whose counts sum to the parsed row total.
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

## Stage 5b — Fix the packer before it writes new packs

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

### Gate

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

## Stage 6 — Repack, prune and delete (CAR-22)

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

Take the first ~50,000 members of the flattened population (about two packs'
worth), pack that same set twice, and compare total stored bytes:

| trial | members added in |
|---|---|
| **current** | the existing clustering key, as the packer orders today |
| **true** | `(listing_id, fetched_at)` on the corrected listing |

Same members, same dictionary, level and frame target; only the order differs.
Discard both trial packs, then run **one** full pass over the whole population
in the winning order. Fix the rule before running: smaller wins.

Cost is minutes. Risk is nil — the trial packs are thrown away and the original
April packs stay authoritative until the real replacement set verifies.

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

### Gate

- Every retained member reads byte-identically before old packs are retired.
- The existing packer verifies every replacement member; prune reports zero
  unexplained failures.
- Sidecar NULL-identity members drop from 99,981 to the 42,276 that have no
  `artifacts_queue_events` row, or the difference is explained.
- Replacement sidecars carry the **correct** `listing_id`, whichever ordering
  wins; identity and sort key are recorded independently.
- The ordering trial runs on a fixed ~50,000-member subset, both orderings are
  compared, the winner carries the single full pass, and the trial packs are
  discarded — with the decision rule fixed before the run.
- Deleted, absent and failed legacy-key counts reconcile to exactly 1,172.
- The legacy `detail_page` prefix contains zero Parquet objects.
- The legacy `results_page` population is unchanged.

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

| Metric | Required result |
|---|---|
| Legacy detail Parquet deleted | 1,172 objects / approximately 13.66 GiB |
| Legacy results Parquet deleted | 0 |
| Distinct successful captures unaccounted for | 0 |
| Materialized objects failing read-back | 0 |
| Objects deleted whose content was not in a verified pack | 0 |
| Duplicate `(listing_id, fetched_at)` writes | 0 |
| Block pages imported as observations | 0 |
| Import-bearing artifacts without a trusted preserved or sequence-allocated `artifact_id` | 0 |
| Rows inserted into `ops.artifacts_queue` | 0 |
| Legacy `artifact_id` used as a join key | 0 |
| Sidecar `listing_id` used as a join key | 0 |
| Hot-state mutations caused by recovery | 0 |
| Deletion without named approval | 0 |

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
- **During Stage 6:** the original packs remain authoritative until the
  replacements verify.
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
