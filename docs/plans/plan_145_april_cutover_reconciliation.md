# Plan 145: Recover April Detail Artifacts and Delete the Legacy Parquet

## Status

**Second revision — 2026-08-27.** The first implementation guide and two CAR-13
implementations were reverted before the 2026-08-26 rewrite. That rewrite's
goal and success criteria survive unchanged; its *method* did not.

The 2026-08-26 design reconciled the legacy Parquet to production through
metadata joins. Production measurement on 2026-08-27 disproved the assumption
that made that possible: **the pack sidecar's `listing_id` is wrong** for
194,639 of the 371,095 members that match legacy content by hash. The join it
specified cannot be trusted, and its Stage 2 rule — "join using recomputed SHA
only" — structurally excludes the very population that most needs joining.

This revision therefore replaces metadata reconciliation with re-derivation
from the bytes: materialize every surviving successful body as a normal HTML
object, parse the whole April population with the production parser, and
compare *parsed output* to silver. It is slower and less clever. It depends on
nothing but content and the parser, both of which are trustworthy.

Stage 1 is complete (commit `3f6e6d4`). Stage 2 is in flight.

**Supersedes [Plan 132](plan_132_unrecorded_artifact_recovery.md) and
[Plan 137](plan_137_legacy_bronze_parquet_disposition.md).**

---

## Goal

Delete the **1,172 April 2026 `detail_page` Parquet objects** containing
951,821 legacy row occurrences and occupying 14,670,223,837 bytes
(approximately 13.66 GiB), after every distinct successful capture is either:

1. already represented by a silver observation; or
2. folded into production as a real HTML artifact and parsed into silver.

The 127 legacy `results_page` Parquet objects are out of scope, as are all
current `results_page` objects and the broader raw-HTML retention policy.

This is a one-time migration, not a recovery platform.

---

## What the three populations actually are

Three separate things, and most of the 2026-08-26 design's trouble came from
conflating them.

| population | what it is | trust |
|---|---|---|
| **The archive** — 144 packs + 144 sidecars under `html_packs/detail_page/`, 32 packs and 557,065 members for April | Production. The authoritative store of captured HTML. | Bytes: authoritative. **Sidecar identity columns: not trustworthy** (below). |
| **The legacy Parquet** — 1,172 objects under `html/year=2026/month=4/artifact_type=detail_page/` | An orphaned parallel store from the Plan 72 archiver. Holds page bodies *inline* in an `html` column, not pointers to objects elsewhere. | Bytes: verified. Identity: corroborated by silver. |
| **Silver** — `silver_normalized/observations` | Production's parsed record. | Authoritative. |

There is no separate population of loose April `.html.zst` objects: the April
detail prefix contained **zero** of them before this plan began, because Plan
131's prune deleted the individual objects after packing. The legacy Parquet is
itself the content store — 147.1 GiB of HTML compressed to 14.67 GiB.

The work is a three-way set difference: fold into production whatever exists
outside it.

---

## Evidence

All figures below are production measurements from 2026-08-21 and 2026-08-27,
reproduced by the Stage 1 census unless noted.

### The census reproduces exactly

Stage 1 scanned all 1,172 objects with zero drift and zero hash mismatches
across 807,797 non-empty bodies:

| measure | value |
|---|---:|
| objects | 1,172 |
| stored bytes | 14,670,223,837 |
| row occurrences | 951,821 |
| HTTP 200 | 847,785 |
| HTTP 403 | 104,025 |
| 5xx | 11 |

### Empty bodies carry a real hash and no bytes

The Plan 72 writer (`archiver/processors/archive_artifacts.py` at `1798a99`)
read the page off local disk and archived `b""` when the file was already
gone, while still copying `sha256` from `raw_artifacts`. So for those rows the
stored hash describes a page whose bytes are absent, and
`sha256(b"") != stored_sha256` is the expected state rather than corruption.

| | rows |
|---|---:|
| empty bodies, total | 43,014 |
| of which HTTP 200 | 39,988 |
| of which HTTP 403 | 3,026 |

This is why the census reports **stored** and **recomputed** distinct-SHA counts
separately, and why they disagree:

| | count |
|---|---:|
| distinct `(listing_id, fetched_at)` identities | 837,061 |
| distinct **stored** SHA | 837,061 |
| distinct **recomputed** SHA (bodies that exist) | 797,073 |
| duplicate occurrences collapsed | 10,724 |

A content-based join must use the recomputed hash. The 2026-08-26 claim that
"distinct SHA count equals distinct identity count" is true only of the stored
hash, and only because an absent body still had a recorded one.

### Legacy identity is trustworthy; sidecar identity is not

The legacy schema has no `listing_id`; it is extracted from `url` with the same
UUID rule the production parser uses. That derivation was tested against
production:

- For the 194,734 cases where legacy and sidecar disagree on which listing a
  byte-identical page belongs to, silver holds a `detail` observation for the
  **legacy** listing at that exact timestamp in **194,734 of 194,734** cases,
  and none for the sidecar's listing.
- Sidecar `listing_id` matches legacy for only 124,320 of 371,095 content
  matches, while `fetched_at` matches for 318,959. Right time, wrong listing.
- 104,669 sidecar members carry NULL `listing_id`/`fetched_at`, and 43,741
  carry NULL `artifact_id`.

**Consequence:** sidecars may be used for `raw_sha256` and content location and
for nothing else. Legacy `artifact_id` remains unusable across the cutover, as
the 2026-08-26 revision established.

### The state of the population, measured

Of the 797,073 distinct successful captures whose bytes exist, cross-tabulating
content presence in the packs against observation presence in March–May silver:

| | observation in silver | **not** in silver | total |
|---|---:|---:|---:|
| **content in packs** | 318,959 | 52,136 | 371,095 |
| **content NOT in packs** | 270,652 | 155,326 | 425,978 |
| total | 589,611 | 207,462 | 797,073 |

Both margins reconcile against independent measurements. The 52,136 are real:
52,041 of them match pack members with NULL identity — bytes production holds
but cannot name.

Separately, of the 39,988 metadata-only HTTP 200 rows, 28,535 are already
represented in silver and **11,453 are not**. Those 11,453 have no bytes in the
Parquet and no surviving individual object; they are unrecoverable and are
recorded as a closed loss.

### Time matching, and the 15-minute clock

Silver's April inter-observation intervals have their top non-zero mode at
**15 minutes** (7.92%), flanked by 14 and 16, with 30/45/60-minute harmonics.
Consecutive real captures of a listing are therefore ~900 s apart.

Nearest-neighbour distance for the strict-anti-join gap is flat through ten
minutes and then climbs steeply:

| window | share of gap rows |
|---|---:|
| ≤ 1 s | 0.01% |
| ≤ 60 s | 0.31% |
| ≤ 600 s | 0.88% |
| ≤ 3600 s | 29.66% |

So the gap is real rather than an artifact of exact matching. A tolerance is
still warranted for clock skew, but must stay far below the cycle boundary:
exclusions rise from 1,140 at 300 s to 11,804 at 899 s, because a window
approaching 900 s reaches the *neighbouring scrape* and swallows a genuine
capture.

**The comparison uses a 300 s tolerance**, excluding 1,140 probable skew
duplicates and leaving 206,322 candidate imports. This figure is superseded in
practice by the Stage 4 comparison of parsed output, and is retained as the
expected order of magnitude.

### Carousel rows come from detail artifacts

`parse_cars_detail_page_html_v1` returns `(primary, carousel, meta)` from one
detail artifact, and `detail_writer` writes primary plus all carousel rows to
silver. April silver is `carousel` 5,971,440 / `detail` 1,272,617 / `srp`
80,651, and a sampled page yields **5.7 carousel rows**.

Two consequences: a nearby `carousel` observation is real coverage of that
listing and must count as such when deciding what is missing; and parsing an
imported artifact emits carousel rows for *other* listings, so the silver
footprint of an import is several times its artifact count.

### Costs, measured

| operation | cost |
|---|---|
| parse | 90.7 ms/page → 24.8 core-hours for the 983,043-page deduped union |
| compress (zstd-9 + dictionary `1367127621`) | 3.1 ms/page, **16.0x** vs 6.5x without the dictionary |
| materialize | ~5 source files/min → ~4 h for 1,172 |
| stored size | ~12–18 KB/object → ~13 GiB for 807,797 |

Host headroom at the time of writing: 123 GB disk free, 11.2M inodes free,
4 cores, load ~1.0.

---

## Method

Re-derive from the bytes. Do not reconcile through metadata.

1. **Materialize** every surviving successful body as an ordinary
   `.html.zst` object with the production writer, dictionary and level.
2. **Parse** the whole April population — materialized objects plus existing
   pack members — with the unmodified production parser into a Parquet table.
3. **Compare** that parsed table to silver and decide, per observation, what
   is missing.
4. **Apply** the missing observations.
5. **Repack** April with the existing packer, prune, and delete the legacy
   Parquet.

Identity comes from the legacy row (corroborated) and content comes from the
bytes (verified). Nothing depends on `artifact_id` or on sidecar identity.

### Disposition rules

Every one of the 951,821 legacy occurrences is recorded with exactly one
disposition, so the population stays fully accounted for:

| disposition | rows | meaning |
|---|---:|---|
| `written` / `exists` | ~807,797 | HTTP 200 with bytes; materialized |
| `skipped_empty` | 43,014 | no bytes to write |
| `skipped_non_success` | ~101,000 | 403 and 5xx bodies parse to a blocked state and yield no observation |

403 and 5xx bodies are recorded but never materialized: carrying ~101,000
challenge pages into the parse stage would cost storage and CPU for nothing.

### Object naming

Materialized objects take a key from `make_key` with a **content-derived**
`file_id` (the first 32 hex of the SHA formatted as a UUID) rather than a
random one. This makes the job idempotent — a re-run recomputes the same key,
`object_exists` skips it, and identical bytes cannot produce two objects. It is
a visible, permanent departure from the scraper's naming; nothing reads meaning
from the stem, so the only effect is reproducibility.

### Where the work runs

`april-processor`, a profile-gated one-shot Compose service on the
snapshot-worker pattern, built from the processing image because the parse
stage needs bs4/lxml. It sets `HTML_COMPRESSION_DICT_ID`; without it every
object would be written dictionary-less and 2.5x larger than the rest of the
store. Deliberately not resource-capped, matching pack-worker.

---

## Delivery map

| Order | Issue | Stage | Outcome | Production mutation |
|---:|---|---|---|---|
| 1 | CAR-13 | Stage 1 | Freeze and prove the census | No |
| 2 | CAR-19 | Stage 2 | Materialize successful bodies as HTML objects | **Yes: HTML objects** |
| 3 | CAR-20 | Stage 3 | Parse the April population into a table | No |
| 4 | CAR-21 | Stages 4–5 | Compare to silver, then apply the missing observations | **Yes: silver** |
| 5 | CAR-22 | Stage 6 | Repack, prune and delete the legacy Parquet | **Yes: irreversible deletion** |

---

## Stage 1 — Freeze the census (CAR-13) — **complete**

`scripts/reconcile_april_detail.py census`, read-only. Enumerates the exact
prefix, streams every occurrence a row group at a time, recomputes every
non-empty hash, collapses exact duplicates, and writes deterministic manifests
with fingerprints.

### Gate — met

- Baseline counts reproduce exactly, or the run stops. **Reproduced, zero drift.**
- Every non-empty body agrees with its stored hash. **807,797 checked, zero mismatches.**
- Duplicate identities agree on content, or the run stops. **No conflicts.**
- Deterministic manifests, report and fingerprints written. **Yes.**
- No production object or database row written. **None.**

The published criterion "distinct SHA count equals distinct `(listing_id,
fetched_at)` count" holds for the **stored** hash (837,061 = 837,061) and not
for the recomputed hash (797,073), for the reason given under *Empty bodies*.
The discrepancy is the finding, not a failure.

---

## Stage 2 — Materialize successful bodies (CAR-19)

`scripts/reconcile_april_detail.py materialize`, dry-run by default.

For each legacy row: skip empty and non-success with a recorded disposition;
otherwise derive the content-based key, skip if the object already exists,
write with `write_html`, read back through `read_html` and require a hash
match, and record a manifest row. Manifests are written as one Parquet shard
per source file under `recovery/plan145/materialized/`, so an interruption
loses only the in-flight file.

### Gate

- Every legacy occurrence carries exactly one disposition and the four
  disposition counts sum to 951,821.
- Every materialized object reads back byte-identically; any mismatch stops
  the run.
- Every manifest row retains its legacy locator, `listing_id` and `fetched_at`.
- Object keys are distinct and content-derived; a re-run writes nothing.
- No silver, artifact-queue or live-state row is written.

---

## Stage 3 — Parse the April population (CAR-20)

Run `parse_cars_detail_page_html_v1` **unchanged** over the union of the
materialized objects and the existing April pack members, deduplicated by
content hash, and write primary and carousel output to a Parquet table under
`recovery/plan145/parsed/`.

HTML must be decoded exactly as production decodes it —
`read_html(...).decode("utf-8", errors="replace")` — or parsed output will
differ from silver for encoding reasons and poison the comparison.

Each parsed row carries the authoritative `fetched_at` from the legacy row or
the sidecar, never from the parser or the run.

### Gate

- Every input is parsed or has an explicit recorded failure.
- Parsed listing identity is compared to manifest identity and disagreements
  are reported rather than silently resolved.
- A sub-1 KiB body cohort exists (Plan 137 recorded 5,741 sidecar members
  under 1 KiB); it is measured and reported rather than discovered as parse
  failures.
- No production mutation.

---

## Stage 4–5 — Compare to silver, then apply (CAR-21)

Compare the parsed table to March–May silver on `(listing_id, fetched_at)` with
a **300 s** tolerance, counting observations from any source because carousel
rows are real coverage produced by detail artifacts.

Produce two immutable outputs: observations already represented, and
observations to import. Then write the import set into silver with the
authoritative legacy capture time, in capped batches, dry-run first.

The recovery path must not mutate `ops.price_observations`,
`ops.vin_to_listing`, `ops.blocked_cooldown`, `ops.detail_scrape_claims`, or
emit live messages. Run an approximately 500-observation canary against
normal-parser controls, with before/after snapshots of live tables and V040
views, before the full apply.

### Gate

- Every parsed observation is classified exactly once.
- No duplicate `(listing_id, fetched_at)` observation is written.
- Silver and price-event times equal the legacy capture time.
- Live pricing, VIN, cooldown, claim and refresh state is unchanged.
- The carousel fan-out is measured and reviewed before the full apply.

---

## Stage 6 — Repack, prune and delete (CAR-22)

Run the existing April packer over the complete individual-object population,
which by this point includes the materialized objects. Verify every member,
then retire the superseded packs and sidecars by exact reviewed manifest, then
run the existing prune to delete the individual objects.

Keep the original April packs until the replacements verify. The temporary
duplication is ~2.13 GB and it is what makes every step before deletion
reversible.

Finally, regenerate the exact 1,172-key deletion manifest from the frozen
Stage 1 object census and, with named approval, delete those keys in capped
batches with receipts. Deletion is by exact key, never by prefix, and the 127
`results_page` keys must not appear.

No new pack format, generation selector, reader contract or prune algorithm is
part of this plan.

### Gate

- Every retained member reads byte-identically before old packs are retired.
- The existing packer verifies every replacement member; prune reports zero
  unexplained failures.
- Deleted, absent and failed legacy-key counts reconcile to exactly 1,172.
- The legacy `detail_page` prefix contains zero Parquet objects.
- The legacy `results_page` population is unchanged.

---

## Testing

- fixture Parquet in the exact Plan 72 schema — partition columns absent, as
  in production — proving census, hash recomputation and duplicate collapse;
- disposition rules, content-derived key stability, and the refusal to write
  when a read-back hash disagrees;
- the parser consuming production-decoded HTML with manifest identity and
  authoritative time;
- comparison partitioning every parsed observation exactly once under the
  chosen tolerance;
- a real-Postgres test proving live tables and V040 views are unchanged;
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
| Duplicate `(listing_id, fetched_at)` writes | 0 |
| Legacy `artifact_id` used as a join key | 0 |
| Sidecar `listing_id` used as a join key | 0 |
| Hot-state mutations caused by recovery | 0 |
| Deletion without named approval | 0 |

Unrecoverable by construction, and accepted as a closed loss: the **11,453**
successful captures that have no bytes in the Parquet, no surviving individual
object, and no silver observation.

---

## Rollback and stopping points

- **After Stage 1:** nothing to roll back.
- **During Stage 2:** stop between source files. Materialized objects are
  inert — nothing reads them until Stage 3 — and content-derived keys mean a
  restart resumes rather than duplicates. Objects may be deleted by exact
  manifest.
- **During Stage 3:** parsing writes only to the recovery prefix.
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
| 131 | Its pack, verify, fallback and prune machinery carries Stage 6 |
| 133 | Its hardened pack read path carries the parse stage |
| 111/112/113 | Receive the recovered April observations; no live refresh state changes |

---

## Out of scope

- All legacy and current `results_page` cleanup.
- Preserving 403 challenge pages or 5xx response bodies.
- Fixing the unrelated April `detail/active` null-price parser gap.
- A reusable historical-reprocessing framework.
- A new pack format, generation selector, reader or prune algorithm.
- Correcting the sidecar `listing_id` defect, which this plan documents and
  routes around but does not fix.
