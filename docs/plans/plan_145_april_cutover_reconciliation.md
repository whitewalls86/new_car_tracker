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

### The compression question, settled

`listing_id` is also the packer's sort key, and `PackWriter.add` seals a frame
at a listing boundary so a vehicle's repeat captures compress together. The
obvious worry is that a scrambled key defeats that.

**It does not. The scrambled order compresses better.** Two real packs were
repacked in memory with the production dictionary, level and frame target; the
control reproduced the stored size exactly. Holding frame structure constant so
that only the ordering differs, sorting by the *true* listing is **19.4% worse**
in July (10.2% correct today) and **3.2% worse** in May (85.3% correct) — the
gap tracking how wrong the sidecar is, as a real ordering effect should.

The mechanism, measured on the same two packs: the scrambled column is not
random, it is a **coarser clustering key** than the true listing. One carousel
car appears on many different detail pages, so `any_value` collapses them
together.

| July pack-00023 | scrambled id | true listing |
|---|---:|---:|
| distinct values | 2,591 | 6,078 |
| members per value | 9.98 | 4.25 |
| largest cluster | 1,955 | — |
| neighbours sharing it, stored order | 90.0% | 26.0% |

zstd's match window at level 9 spans roughly 15–20 pages of ~180 KB. Filling it
with ~10 pages that share a carousel vehicle — same model, trim, dealer, region,
and so a great deal of shared markup and spec text — beats filling it with ~4
near-identical captures of one car followed by unrelated vehicles. May's gap is
small for the same reason: there the two keys are nearly the same coarseness
(9.41 vs 7.94 members each, 89.4% vs 79.3% adjacency), so there is little to
change.

So the naive one-line `source = 'detail'` fix is a **regression**, because the
same column feeds `ORDER BY o.listing_id`. The correct repair records the right
`listing_id` in the sidecar while leaving ordering and frame sealing on the
existing clustering key, and it needs no repack: rewriting that one column
while preserving `source_key`, `frame_ordinal`, `offset_in_frame`, `length` and
`raw_sha256` leaves the pack bytes untouched and every read and index check
passing.

Tracked as CAR-28. Within this plan it matters only in Stage 6: the April
repack must write the correct `listing_id` into the replacement sidecars
**without** reordering members on it.

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

**For the 557,065 existing pack members: preserve, do not re-derive.** Their
`artifact_id` lives in `ops_normalized/artifacts_queue_events` in the lake,
keyed by `minio_path`. It is durable there — unlike `ops.artifacts_queue`,
which step 20 empties. **Unpacking under each member's original `source_key`
keeps that join intact for free**, which is the decisive argument for original
keys over content-derived ones. 42,276 members have no event row and stay
unattributed; they are counted, not invented.

**For the newly materialized legacy bodies: allocate from the sequence, never
compute one.** `ops.artifacts_queue_artifact_id_seq` is a `bigserial`
sequence, currently around 7.52M. `nextval` is concurrency-safe by
construction: it never returns a value twice, and it does not roll back or
reuse on abort. Concurrent production inserts are therefore safe **as long as
identity comes from the sequence** — a `max(artifact_id) + 1` would race and
collide, and is forbidden.

**Allocate without enqueueing.** Do *not* insert `ops.artifacts_queue` rows:
`claim_artifacts.sql` claims anything `pending` or `retry`, so an enqueued row
is picked up by live processing within seconds and runs the full hot-state
path this plan forbids. Instead:

1. `SELECT nextval('ops.artifacts_queue_artifact_id_seq')` per recovered artifact.
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
| of which content already in the packs (45.6% measured over 392 shards) | ~371,000 |
| materialized objects surviving step 2 | ~436,702 |
| April pack members unpacked in step 3 | 557,065 |
| **flattened population** | **~993,767** |

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

Produce two immutable outputs — already represented, and to import — then
write the import set with the treatment table above: silver rows and
historical price events at the legacy capture time, sequence-allocated
`artifact_id`s, `recovered` queue events, and **no** mutation of
`ops.price_observations`, `ops.vin_to_listing`, `ops.blocked_cooldown`,
`ops.detail_scrape_claims`, or live message emission.

Run an approximately 500-observation canary against normal-parser controls,
with before/after snapshots of live tables and V040 views, before the full apply.

### Gate

- Every parsed observation is classified exactly once.
- No duplicate `(listing_id, fetched_at)` observation is written.
- Silver and price-event times equal the legacy capture time.
- Every recovered artifact carries a sequence-allocated `artifact_id`; none is
  computed from `max()`.
- No row is inserted into `ops.artifacts_queue`.
- Live pricing, VIN, cooldown, claim and refresh state is unchanged.
- The carousel fan-out is measured and reviewed before the full apply.

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

### Gate

- Every retained member reads byte-identically before old packs are retired.
- The existing packer verifies every replacement member; prune reports zero
  unexplained failures.
- Sidecar NULL-identity members drop from 99,981 to the 42,276 that have no
  `artifacts_queue_events` row, or the difference is explained.
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
- a real-Postgres test proving `artifact_id` comes from the sequence, that no
  `artifacts_queue` row is created, and that live tables and V040 views are
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
| Recovered artifacts without a sequence-allocated `artifact_id` | 0 |
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
