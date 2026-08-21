# Plan 145: Deleting the April Cutover Backlog Without Losing Data

## Status

**Draft — Stage 0 gates 0a, 0b, 0c, 0e and 0f closed 2026-08-21. Only 0d
remains, and it is a test to write rather than a measurement to take.**

**Supersedes [Plan 132](plan_132_unrecorded_artifact_recovery.md) and
[Plan 137](plan_137_legacy_bronze_parquet_disposition.md).** See
[Effect on other plans](#effect-on-other-plans).

April 2026 is the month the system moved from local storage and n8n to MinIO
and the processing service. Three separate investigations each found one face
of that cutover and none could see the other two.

---

## Goal

**Delete the 1,299 legacy Parquet objects (13.79 GiB) having lost nothing that
matters.** Everything before that is loss minimisation.

This framing is deliberate and replaces an earlier draft whose goal was "build
a reconciliation ledger." A ledger-shaped plan is complete when the ledger
exists, which is how [Plan 137](plan_137_legacy_bronze_parquet_disposition.md)
came to be written and never scheduled. **The plan is not done until the bytes
are gone.**

The precedent is in this project's own history: Plan 102's n8n decommission
removed the only lifecycle rule bronze ever had and nothing replaced it — the
origin of the inode problem Plan 131 exists to solve. A recovery plan that
defers deletion to a separate document repeats that.

**There is no deadline.** 13.79 GiB is ~23% of `/mnt/data`'s 59 GiB, so the
prize is real, but 1,299 objects is nothing in inode terms and Plan 131 Stage 5
already converted the inode constraint into a steady state. Deletion is not
racing anything, which is what makes careful loss-minimisation affordable.

---

## The three populations

Verified against production 2026-08-21, read-only, inside `cartracker-archiver`.

| population | n | bytes live in | record lives in |
|---|---:|---|---|
| orphan captures | **36,241** | packs (verified) | **nowhere** |
| challenge-page stubs | 48,600 | packs | events say `skip` — correctly no observation |
| legacy 200s with no observation | ~224,000 | legacy Parquet **only** | legacy Parquet only — but see [0f](#stage-0f--does-recovering-them-add-price-information-almost-never-closed): only **~11,600** carry information silver lacks |

### What "missing" means, precisely

**No listing was lost.** Of 847,785 successful legacy captures, 847,647
(100.0%) are for a `listing_id` that appears in silver; only **138** are for a
listing that never appears at all. What is missing is ~224,000 **observations**
— individual price points — on listings that other scrapes still cover.

That lowers the stakes and it is recorded here so nobody plans around the
wrong number. The value of recovery is **price-history density in a specific
window**, which matters most to Plan 111/112/113's volatility and
adaptive-refresh features, not rescue from oblivion.

### Stage 0a — window sensitivity: the gap is real (CLOSED)

Nearest silver observation for the same `listing_id`, by ASOF join, silver
widened to March–May:

| window | matched | share |
|---|---:|---:|
| <= 5 s | 622,617 | 73.4% |
| <= 60 s | 623,155 | 73.5% |
| <= 600 s | 624,330 | 73.6% |
| <= 3,600 s | 688,579 | 81.2% |
| <= 86,400 s | 837,114 | 98.7% |
| listing present at all | 847,647 | 100.0% |

**73.4% match within five seconds and the curve is flat to ten minutes.** That
is exact fetch-to-observation matching, not a fuzzy window catching neighbours.
The remaining 26.6% have no observation within ten minutes and a median nearest
neighbour 8,051 s away (p90 48,642 s) — a different scrape of the same listing,
hours later. **Not a join artifact. Gate passes.**

### Stage 0b — duplication: none (CLOSED)

847,785 rows hold **837,061 distinct `sha256`** and **837,061 distinct
`(listing_id, fetched_at)`**. ~10,700 exact duplicates, and the two counts
agreeing exactly is an independent consistency signal. The row count is a
population.

### Stage 0c — month boundary: not the explanation (CLOSED)

Silver was widened from April to March–May. The gap did not move.

### Stage 0f — does recovering them add price information? Almost never (CLOSED)

The decision this plan turns on is not "how many captures are missing" but
"what would recovering them tell us that silver does not already say."

For every legacy 200-row with no **priced** silver observation within 60 s,
the bracketing priced observations were classified:

| class | n | share |
|---|---:|---:|
| bracketed <=24h, price **stable** | 21,209 | 6.0% |
| bracketed <=24h, price **changed** | **270** | **0.1%** |
| one side only within 24h | 16,203 | 4.6% |
| nearest priced obs >24h away | 306,964 | 86.3% |
| no priced observation for this listing at all | 11,199 | 3.1% |

**270 captures out of 355,845 sit inside an observed price change** — 0.076%.
Their median bracket is 24.2 h wide (p90 32.5 h), so recovering even those would
not date a change tightly. **The strongest argument for bulk recovery — that
these captures are the only witness to a price movement — does not survive
measurement.**

#### Two denominators, and why they differ

Requiring `price IS NOT NULL` moves the unmatched count from 224,630 to
355,845. The cause is not missing data; it is that `detail`-source rows are
30.6% price-less, and the ASOF join takes the single nearest row:

| source | state | rows | null price |
|---|---|---:|---:|
| carousel | active | 5,971,440 | 0.4% |
| detail | active | 952,694 | **10.2%** |
| detail | unlisted | 319,923 | 91.3% |
| srp | active | 80,651 | 0.6% |

`detail/unlisted` at 91.3% is correct and benign — an unlisted car has no price,
and the legacy capture would not have supplied one either.

**`detail/active` at 10.2% is not benign.** 96,909 April observations parsed an
active detail page and produced no price. That is a parser gap, it is unrelated
to the cutover, and it is recorded in
[Out of Scope](#out-of-scope) rather than silently folded into these counts.

Listing-level coverage is unaffected either way: of 139,146 distinct April
listings, **138,050 (99.2%) have at least one priced observation.**

### The incident window is April 11–21

Misses by capture day:

```
04-09   1,705      04-15  17,454      04-21  38,168
04-10   2,497      04-16  16,463      04-22   1,056
04-11   6,356      04-17  20,390      04-23     775
04-12  15,934      04-18  26,111      04-24     988
04-13  10,054      04-19  24,011      04-25   1,059
04-14  14,885      04-20  25,156      04-27     555
```

Rising through the month, peaking 2026-04-21, collapsing the next day.
**All five sampled orphans decoded to 2026-04-21 as well** — one incident, two
faces.

**This contradicts Plan 137's stated April 20–27 dual-write boundary.** The
damage precedes it by nine days and ends where 137 thought it began. Plan 137's
whole-file safety classes were built without this profile and must be re-derived
against it before any deletion manifest is trusted.

### Other measurements

**Bronze, April `detail_page`:** 0 surviving `.html.zst` objects — Plan 131's
prune is complete. 32 packs + 32 sidecars hold 557,065 members, 56,808 under 1 KB.

**The orphan predicate reproduces exactly** from the sidecars alone, four months
on, at the cost of 32 GETs and no object reads:

| | Plan 132 (2026-08-14) | sidecars (2026-08-21) |
|---|---:|---:|
| `artifact_id IS NULL` | 42,276 | **42,276** |
| >=50 KB | 36,241 | **36,241** |
| 1-50 KB | 294 | **294** |
| <1 KB | 5,741 | **5,741** |

**Orphans read back clean through the pack path** — `object_exists=False`,
`read_html` byte-identical, `sha256` verified, `timestamp_utc` present on all
five samples.

**Legacy Parquet, April `detail_page`:** 1,172 objects, 951,821 rows, carrying
`artifact_id, run_id, source, search_key, search_scope, url, fetched_at,
http_status, content_bytes, sha256, error, page_num, html`. By status: 847,785
× 200, 104,025 × 403, 11 × 5xx. The 403s are challenge pages and correctly have
no observation — `_process_detail_page` marks them `skip` and writes nothing.

### `artifact_id` does not survive the cutover — never join on it

Recorded because it produced a plausible and entirely false result:

```
legacy artifact_id range 61-3,782,652   silver 827-3,872,055
joined rows      : 4,672,074
listing_id agrees: 48  (0.0%)
fetched_at <=60s : 0  (0.0%)
median |delta|   : 6.4 days
```

The ID spaces overlap numerically, so the join runs and returns millions of rows
describing unrelated artifacts. **The valid keys are content-based: `sha256`
against pack `raw_sha256`, and `(listing_id, fetched_at)` against silver.**

### The legacy Parquet is the orphans' missing record

A 60-file sample (5%, 48,118 rows) joined on `sha256` against all 42,276 orphan
`raw_sha256` values:

```
matches an ORPHAN pack member : 800   (200: 676, 403: 124)
matches a non-orphan member   : 16,906
in no pack at all             : 30,412
```

Where that holds, the legacy row supplies an authoritative `fetched_at`, `url`,
`http_status`, `run_id` and `search_key`. That 5% sample extrapolated to ~37%
coverage. **The full run says 100%** — see 0e.

### Stage 0e — every orphan is in the legacy Parquet (CLOSED)

All 1,172 legacy files joined on `sha256` against all 42,276 orphan
`raw_sha256` values:

```
legacy rows reaching an orphan by sha256 : 42,976
distinct orphan members reached          : 42,276  (100.0%)
```

**Every orphan is identifiable from the legacy Parquet.** The 5% sample was off
by 2.7x because orphan rows cluster in the files written during the incident
rather than spreading evenly — a reminder that an extrapolation over a
non-uniform population is a guess wearing a percentage sign.

Two consequences:

- **HTML `timestamp_utc` extraction is unnecessary.** Plan 132's central gate
  was dating orphans from the page body at >=99% agreement. The legacy row
  carries an authoritative `fetched_at` for 100% of them, so
  `scripts/probe_html_capture_timestamp.py` is dropped from this plan entirely.
- **Plan 132's 0b residual is closed.** The orphan-matching rows split
  **37,715 `200` / 5,261 `403`** against the sidecar's 36,241 large / 5,741
  sub-1KB. The sub-kilobyte orphans are challenge pages, confirmed from a third
  independent direction.

### The orphans were never processed — not merely unnamed

An approach was tried and **failed**, and it is recorded because it looked
correct: use the legacy row's `(listing_id, fetched_at)` to find the silver
observation for that capture, read its current `artifact_id`, and write that id
back into the pack sidecar — naming the orphan without touching the `.zpack`,
since `artifact_id` lives only in the index (`write_index_parquet`).

It returns **zero** at 5 s, 60 s and 300 s. The join is sound — 100% of the
candidate `listing_id`s are present in silver — but:

```
nearest silver detail observation: min=678s  median=86,460s  max=1,469,456s
nearest observation, any source  : min= 31s  median=45,001s
```

**Not one of the 42,976 captures has an observation within five minutes.** The
orphans are not processed-but-unnamed; **no observation was ever written for
these fetches**, so there is no current `artifact_id` to recover. Silver cannot
name them, and it never could.

It does not matter, because the legacy row carries `listing_id` and `fetched_at`
directly. The silver hop was never needed.

### What deletion actually costs: re-derivability, not integrity

**Nothing dereferences silver's `artifact_id`.** `read_html` has exactly one
production caller, `processing/routers/batch.py`, and it reads
`artifact["minio_path"]` from the **queue**. The 16 dbt models touching
`artifact_id` use it for grouping, dedup and fingerprinting — never to fetch
bytes. **So deleting the legacy Parquet breaks no pointer and requires no
update to any analytics row.**

What is lost is the ability to re-derive. Of April's silver artifacts, only
**41.0% resolve into a pack**:

| source | artifacts | resolved |
|---|---:|---:|
| detail | 1,110,888 | 41.1% |
| carousel | 843,005 | 37.2% |
| srp | 3,692 | see caveat |

1,110,888 detail artifacts against 557,065 April bronze objects means ~554,000
artifacts never had a bronze object at all. The likely cause is in Plan 132's
archaeology: the n8n-era policy was **`ok` -> delete after 48 hours**. Those
observations have had no retrievable source since April 2026 *by design*.

**Caveat, and it is a real one:** that measurement checked **pack membership**,
which is not existence. `results_page` was never packed for any month, so the
`srp` row reads 0% while its objects sit in MinIO untouched. The 41.0% figure
is therefore a floor, overstated as a loss by an unmeasured amount. Re-measure
against objects *and* packs before quoting it anywhere.

Deleting the Parquet permanently removes the source HTML behind Plan 137's
488,494 rows whose content is in no pack. That is the honest cost, and it is
what the [preservation stage](#stage-4--preserve-what-is-worth-keeping-for-reasons-other-than-history)
exists to bound.

### Adjacent finding: `results_page` is outside Plan 131's coverage

PLANS.md records Plan 131 as *"April-July packed and pruned"*. That is true for
`detail_page`. `results_page` objects still exist unpacked and unpruned for
**April through August** — April measured at 2,253 objects / 58.7 MB, later
months larger. Small, not urgent, and **not this plan's job** — recorded so the
next reader does not inherit "everything is packed" the way this plan inherited
"the orphans are readable as objects."

---

## Design

### One backfill write path, not three

Reusing `POST /process/batch` to replay April — Plan 132's stated design, and
the natural approach for Plan 137's recovery too — **corrupts live state**:

- [`upsert_price_observation.sql`](../processing/sql/upsert_price_observation.sql)
  has no `fetched_at` guard. `price`, `make`, `model`, `last_seen_at` and
  `last_artifact_id` are unconditionally `EXCLUDED`, so an April observation
  overwrites a still-active listing's current price.
- `last_detail_scraped_at` is `COALESCE(EXCLUDED, existing)` — "a non-NULL
  incoming value always wins", so April overwrites today.
- [`V040__detail_scrape_circuit_breaker.sql`](../db/migrations/V040__detail_scrape_circuit_breaker.sql)
  computes `is_price_stale` and the detail-refresh predicate from exactly those
  columns and orders the claim queue by `last_seen_at ASC`. Backdating
  re-enqueues recovered listings at the front of the live scrape queue.
- [`detail_writer.py`](../processing/writers/detail_writer.py) DELETEs the
  `price_observations` row at any other `listing_id` holding the same VIN, and
  `_clear_cooldown` wipes live `blocked_cooldown` state.

None of it is needed. The silver history is event-sourced —
`write_silver_observations_postgres` -> `staging.silver_observations`, flushed
to Parquet partitioned by `fetched_at` — so a backdated `fetched_at` lands in
the correct April partition on its own. **The hot-table mutation, the VIN delete
and the cooldown clear are current-state maintenance a backfill has no business
performing.** Stage 1 removes the capability rather than guarding it.

### Provenance is recorded, not inferred

Every backfilled observation carries what it was recovered from: which system
supplied `fetched_at`, and the `sha256` it was keyed on. A recovery nobody can
audit later is a second unrecorded write.

---

## Stages

### Stage 0 — Can this be deleted safely? (measurement only)

- **0a window sensitivity — CLOSED, gap is real.**
- **0b duplication — CLOSED, none.**
- **0c month boundary — CLOSED, not the explanation.**
- **0d backdated-write safety.** Confirm the Stage 1 path leaves
  `ops.price_observations`, the V040 view and Plan 111/113 refresh state
  untouched. **Blocker.**
- **0e orphan dating coverage — CLOSED, 100%.** Legacy Parquet identifies every
  orphan; `timestamp_utc` extraction dropped.
- **0f recovery value — CLOSED.** 270 of 355,845 witness a price change.

Output is one ledger keyed on `sha256`: present in packs, present in legacy
Parquet, has a queue event, has a silver observation, `http_status`, and
best-available `fetched_at`.

### Stage 1 — The backfill write path

`backfill=True` through `write_detail_active` / `write_detail_unlisted`: silver
row and price-observation event only, no hot-table upsert, no VIN-collision
delete, no cooldown clear, provenance recorded. Dry-run by default, hard
per-run cap. The only new machinery in the plan.

### Stage 2 — Recover the orphans (36,241)

Bytes from the pack, metadata from the legacy Parquet, write through Stage 1.
**No timestamp extraction, no silver round-trip, no sidecar rewrite, and the
`.zpack` files are never touched.** 0e made this the most certain stage in the
plan: every input is known for every artifact.

Target the 37,715 `200`s; the 5,261 `403`s are challenge pages and are
discarded, consistent with `_process_detail_page`.

Run ~500 first and compare against a control where both records exist. Also the
first real measurement of `PACK_INDEX_CACHE_PACKS=48` under a month-sized
sequential scan — Plan 133 proved it safe, not effective.

### Stage 3 — Recover only what carries information (gated on 0d)

**Bulk recovery of all ~224,000 is not proposed. Stage 0f killed it:** 270
captures out of 355,845 sit inside a price change, and their brackets are a day
wide. Recovering the rest would re-derive prices silver already holds.

Recover only:

| cohort | n | why |
|---|---:|---|
| no priced observation for the listing at all | 11,199 | the legacy row is the only price evidence |
| listings absent from silver entirely | 138 | nothing else knows they existed |
| bracketed, price changed | 270 | cheap, and the only cohort with movement |

~11,600 rows against ~224,000 — small enough to run in one pass through the
Stage 1 path. Everything else is discarded at Stage 5 with its redundancy
recorded in the ledger, not assumed.

### Stage 4 — Preserve what is worth keeping for reasons other than history

Before deletion, extract and store separately:

- **Parser fixtures.** Historical-layout samples, which Plan 137 identified and
  which are the one thing genuinely unrecoverable from silver.
- **The 138 listings that appear nowhere in silver.** Small enough to handle
  individually.
- **The 5xx bodies** (11 rows) and a bounded sample of the 403 challenge pages,
  as evidence for Plan 128's classifier.

### Stage 5 — Delete, by reviewed manifest and explicit approval

**This stage requires named human approval and is irreversible.** Deletion is
by explicit key manifest, in waves, never by prefix:

1. The 57 empty-placeholder files (43,019 rows of `b""`).
2. Files whose every non-empty row is recovered, already present in silver, or
   preserved by Stage 4.
3. The remainder, individually justified.

Plan 137's whole-file safety classes are the starting point but **must be
re-derived** — they were built against an April 20–27 window this plan has since
disproved.

**Nothing is deleted until the ledger accounts for every row.**

---

## Effect on other plans

| Plan | Change | Why |
|---|---|---|
| **132** | **Superseded** | Its Stage 4 was completed by Plan 131's prune on 2026-08-17; its Stage 0c is a known failure, not an open question; its Stage 5 pointed at Plan 128, which is closed; and its population is one of three. Its measurements, its `ok`-is-success finding, and its `LastModified` correction carry forward here |
| **137** | **Superseded** | Its read-only census is an input to Stage 0 and its recovery half merged into Stage 1. Splitting deletion into its own plan was the error that left it unscheduled — deletion is now this plan's stated goal, with approval as a gate rather than a separate document. Its April 20–27 window is disproved above; its whole-file classes need re-deriving |
| **131** | No change | Complete. Its prune is what makes the pack path load-bearing here |
| **133** | No change | Complete, and carrying this plan — `artifact_exists` and the pack fallback are why Stage 2 is possible |
| **111/112/113** | Inputs improve | ~224,000 recovered April observations raise density in the window these plans' volatility features are computed over |

---

## Testing

- Ledger construction is deterministic and reproducible across reruns.
- `sha256` join is exact; `artifact_id` is never used as a join key.
- Backfill write path asserts `ops.price_observations`, `blocked_cooldown` and
  the V040 view are unchanged after a backfilled write.
- Backfill is dry-run by default and idempotent — running twice writes one
  observation.
- `timestamp_utc` extraction: valid epoch-ms, missing, garbage, and a value
  outside the artifact's own hive month — the last must be rejected, not written.
- A recovered observation equals what a normal scrape would have written, on a
  control artifact where both exist.
- Deletion manifest: every key traced to a ledger row; a file with one
  unaccounted row is refused.

---

## Files Changed

| File | Change | Stage |
|---|---|---|
| `scripts/build_april_ledger.py` | Stage 0, read-only | 0 |
| ~~`scripts/probe_html_capture_timestamp.py`~~ | **Dropped** — 0e made HTML dating unnecessary | — |
| `processing/writers/detail_writer.py` | `backfill=True` path | 1 |
| `archiver/processors/backfill_unrecorded_observations.py` | Manifest + backfill driver, dry-run default | 1-3 |
| `archiver/processors/preserve_legacy_samples.py` | Fixture/edge-case extraction | 4 |
| `archiver/processors/delete_legacy_parquet.py` | Manifest-driven deletion, dry-run default | 5 |
| `tests/processing/test_detail_writer_backfill.py` | New | 1 |
| `tests/archiver/test_backfill_unrecorded_observations.py` | New | 1 |
| `tests/archiver/test_delete_legacy_parquet.py` | New | 5 |

---

## Success Criteria

| Metric | Gate |
|---|---|
| Legacy Parquet objects deleted | **1,299** |
| Bytes freed | **~13.79 GiB** |
| April captures unaccounted for in the ledger at deletion time | Zero |
| `artifact_id` used as a cross-system join key | Never |
| Hot-state rows mutated by a backfill write | Zero |
| `fetched_at` agreement, legacy vs `timestamp_utc`, on the overlap | >=99% within 1 minute |
| Observations written with an unverified `fetched_at` | Zero |
| Deletion performed without named approval | Must not happen |
| Deletion by prefix rather than reviewed key manifest | Must not happen |

---

## Risks

| Risk | Mitigation |
|---|---|
| **Deletion removes something unrecoverable** | Stages 0-4 exist entirely to prevent this; Stage 5 is manifest-driven, wave-by-wave, approval-gated |
| A backfill corrupts live pricing or refresh state | Stage 1 removes the capability rather than guarding it; 0d asserts it |
| Plan 137's whole-file classes are trusted as-is | Explicitly disproved above; re-derive against the April 11-21 window |
| Backfill starves the live pipeline | Per-run cap and rate limit; the queue is shared with production |
| Recovery is treated as the finish line | The goal is deletion; the plan is not done until the bytes are gone |
| dbt aggregates move | Expected and intended — flagged so it is not a surprise |

---

## Out of Scope

- Fixing the events pipeline. May and June are exact; there is nothing to fix.
- Reprocessing anything that already has a silver observation.
- **The `detail/active` null-price gap.** 96,909 April observations (10.2% of
  `detail/active`) parsed an active detail page and produced no price. Found
  while measuring Stage 0f, unrelated to the cutover, and almost certainly not
  April-specific. It needs its own investigation against current months before
  anyone decides whether it is a live parser defect or an artifact of the
  April-era parser. **Do not let it ride along with a deletion plan.**
- The long-term raw-HTML retention policy that Plans 129/130/131 still need.
  This plan deletes one bounded historical backlog, not a recurring rule.
