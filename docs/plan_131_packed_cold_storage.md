# Plan 131: Packed Cold Storage for Bronze HTML

## Status

**Stage 0 COMPLETE (2026-08-13) - gate passed, grouping decided.**
Monthly capture buckets: ~68% logical / ~79-81% physical saving, 1.00x
reprocessing, and the object count collapses from ~4.5M to a few hundred packs.

**Stages 1-2 BUILT (2026-08-13), not yet run against production.** Format,
writer, indexed reader, sidecar index, and a checkpointed archiver job that
writes packs alongside their sources and deletes nothing. See
[Stage 1-2 as built](#stage-1-2-as-built) for what the implementation settled
that the plan had left open, and for the one place the plan's own wording was
wrong.

**Stages 3-5 NOT STARTED.** Stage 4 is the only step that removes data.

Headline from the measurements: the inode ceiling is ~61 days out and
results-page deletion is not a viable substitute (0.7% of objects).

Comes out of [Plan 129](plan_129_zstd_dictionary_compression.md), whose Out of
Scope section names "packing multiple artifacts per object" as the deferred
lever, and out of [Plan 114](plan_114_sectioned_html_artifact_audit.md), which
established that MinIO's ~8 KB/object floor is the cost that decides every
storage question in this bucket.

**This plan is lossless and reversible.** Extracting a packed member returns the
exact bytes `read_html` returned for its source object, verified byte-for-byte
against a per-member `raw_sha256` before any source object is deleted, and
nothing is discarded. That is the whole reason it is worth doing before
[Plan 130](plan_130_parser_input_projection.md), which is not.

*(An earlier revision said packs contain "the exact compressed bytes of the
artifacts they replace". They do not, and could not: re-using each object's own
frame would forfeit the shared compression window, which is where the measured
`D/B = 0.250` comes from. What is preserved exactly is the decompressed HTML —
the artifact — not one particular encoding of it.)*

---

## Goal

Store cold bronze HTML as a small number of large, indexed pack objects instead
of ~3.9M individual objects.

Two wins, in priority order:

1. **Object count**, which is the constraint Plan 129 did not touch at all.
2. **Bytes**, from paying the per-object floor once per pack instead of once per
   page, and from giving the compressor a window measured in megabytes.

Access to a single artifact becomes slower. That is the accepted trade.

---

## Context: the problem Plan 129 did not solve

Plan 129 cut logical bytes by 73% and physical bytes by ~60%. It changed object
count by **zero**. The inode ceiling is untouched by compression, and it is the
nearer wall.

Measured on the VM, two readings five days apart:

| Measure | 2026-08-08 (Plan 114 Stage 3) | 2026-08-13 (Stage 0a) |
|---|---|---|
| Objects in `bronze` | 3,918,760 | ~4.07M (derived) |
| Inodes used on `/mnt/data` | 8,774,058 of 13,107,200 | **9,101,670** of 13,107,200 |
| Inodes free | 4,333,142 | **4,005,530** |
| Disk used of 196 G | 184 G | **162 G** |

Inodes per object is ~2.24 (directory + `xl.meta`). Plan 129 records bronze
growing at **~1M objects/month** with no lifecycle rule and no HTML deletion
anywhere in the codebase.

> **Measured burn: ~65,500 inodes/day, so ~61 days of headroom — about
> mid-October 2026.**
>
> The two readings bracket the Plan 129 backfill, which is what makes the point
> rather than softening it: **disk fell 22 GB and the inode clock did not slow
> at all.** Compression cannot move this number. Only packing and deletion can,
> and Stage 0b establishes that deletion has nothing large enough to delete.

### The byte win, as arithmetic rather than estimate

Post-dictionary, per object (Plan 129, "What the rollout taught us", note 2):

| | payload | file (4 KB-rounded) | + dir | physical |
|---|---|---|---|---|
| before dictionary | 28 KB | 28 KB | 4 KB | 32 KB |
| after dictionary | 7.3 KB | 8 KB | 4 KB | **12 KB** |

So 7.3 KB of content occupies 12 KB of disk. Across ~3.9M objects that is
**~18 GB of padding against ~28 GB of actual content** — roughly 39% of the
post-dictionary bucket is floor, not data.

**Packing reclaims that with zero additional compression.** Any gain from the
larger window is on top, and this plan deliberately does **not** put a number on
that gain before Stage 0 measures it. Plan 114 shipped a bytes-only projection
that was wrong by 254 points; the lesson is not to repeat the genre.

---

## What this plan is not

**It is not sectioning.** [Plan 114](plan_114_sectioned_html_artifact_audit.md)
proved `processing/html_sections.py` correct and lossless and then measured its
storage case at **−223%**. The failure was per-object overhead, and a pack
removes that overhead directly. Explicit content-addressed section objects were
only ever a way to share bytes when there was no container to share them *in*.

Concretely: a multi-megabyte zstd frame already finds cross-page redundancy —
that is what a compression window is. Section decomposition adds a manifest
layer, a reconstruction step, and a new correctness surface, in exchange for
redundancy the window finds anyway. `html_sections.py` stays exactly where Plan
114 left it: the right tool for *reprocessing*, not part of the storage path.

**It is not Plan 130.** Nothing is discarded. A packed artifact can be extracted
to the exact bytes that were written, forever.

---

## Design

### The redundancy is within a listing, so members are ordered by listing

Plan 114 Stage 3, whole-section hash reuse:

| Axis | Reuse |
|---|---|
| Within a listing, across captures | **30.42%** |
| Across listings (additional) | **0.65%** |

The two techniques are complementary and neither substitutes for the other:
**the dictionary handles cross-listing boilerplate; the frame window handles
within-listing repeat captures.** That is why members are always ordered
`listing_id, fetched_at` — a vehicle's captures must be adjacent inside one
frame — and why the dictionary stays enabled inside packs.

It matches the read pattern too: reprocessing after a parser fix wants every
capture of the affected listings, which becomes one pack read instead of N
object GETs.

### Grouping: monthly capture buckets - DECIDED by Stage 0d (2026-08-13)

Measured, 140 listings / 2,394 captures, dictionary 1367127621, level 9:

| | logical | physical |
|---|---|---|
| per-listing grouping (ceiling) | 71.3% | 85.0% |
| **monthly capture bucket** | **67.8%** | **83.1%** (see correction) |
| gap | 3.5 pts | **1.9 pts** |

**Cold-listing cohorts would buy 1.9 percentage points of physical storage in
exchange for a 4.46x full-corpus scan on every date-range reprocess.** Rejected.
Monthly capture buckets get 1.00x reprocessing AND land within two points of the
compression ceiling.

`B` = 8,578 bytes (first capture in a group), `D` = 2,142 bytes (each
subsequent), **D/B = 0.250**. The grouping win is real rather than pure floor
elimination, which is why members stay ordered `listing_id, fetched_at`.

**The dictionary is NOT subsumed by the frame window - but its entire
contribution is to the first member of each frame.** Without it `B` rises to
32,952 bytes while `D` is essentially unchanged (2,058 vs 2,142): inside a frame
the window already supplies the context the dictionary would have. At 441,448
groups that is still ~10 GiB. **Keep the dictionary in packs.** This closes the
question raised under "Pack layout".

#### Two measurement corrections - do not quote 83.1%

- **The projection's baseline is inflated ~22%.** `B` is the mean *first*
  capture (8,578 B); the mean *ordinary* capture is 7,313 B. 8,578 crosses a
  4 KB block boundary, so `physical_bytes(B)` charges 16 KB per object where the
  measured mean is ~13.4 KB, and the projection multiplies that across 4.56M
  artifacts. The sample's own assumption-free figure is 30.6 MiB physical ->
  5.8 MiB packed = **81%**. Honest range is **~79-81%**, roughly 45 GiB
  reclaimed. Fix: project the baseline from mean per-capture physical bytes, not
  from `physical_bytes(B)`.
- **The sample under-represents fragmentation.** It reports monthly retaining
  99.1% of the grouping win; the projection says 95%. The sampled listings span
  fewer calendar months than the corpus does, so **trust the projection**, which
  uses the Stage 0c census group counts (441,448 vs 224,459) rather than the
  sample's own shape.

Both corrections move the headline down and neither moves the decision.

---

#### Prior analysis, retained for the reasoning

Ordering within a pack was always settled. Which artifacts share a pack is not,
and **an earlier revision of this section wrongly marked it DECIDED in favour of
monthly.** That call rested on a cost model that assumed an extra base copy
costs the same as an extra ordinary capture — which assumes away the exact
asymmetry that makes packing by listing worth doing. Corrected below.

Per-vehicle remains disqualified on write-once grounds. Monthly and closure
cohort are both live, and **0d measures real pack bytes under each.**

| Grouping | Compression | Write-once? | Objects | Reprocessing |
|---|---|---|---|---|
| Per-vehicle, all time | Best — no split, ever | **No** | ~256K (~15×) | ~256K GETs |
| Time bucket (week/month) | Loses cross-boundary reuse | Yes | ~200-800 | Natural by date |
| **Closure cohort** | Best — no split, ever | Yes | ~200-800 | By closure week |

**Per-vehicle is disqualified as stated, for a reason that is not about
compression: it is not write-once.** An active listing keeps being captured, so
its archive would need a read-modify-write on every new capture — repeatedly,
over the listing's whole life, consuming exactly the transient free space the
section below establishes is scarce. It also lands at only ~15× object
reduction, which is marginal against this plan's gate.

**Closure cohorts get per-vehicle's compression with a time bucket's
ergonomics.** When a listing goes dormant — no observation for
`LISTING_CLOSED_AFTER_DAYS` — its capture set is final and can never grow.
Batch every listing that closed in week W into one pack, ordered by
`listing_id, fetched_at`. A vehicle's captures are therefore never split across
packs, and a pack is sealed on write.

`int_price_history.last_seen_at` is already the closure signal; Plan 111's state-
run machinery models this. Backstop for listings that never close: force-archive
any listing whose oldest capture exceeds `PACK_FORCE_AFTER_DAYS`, accepting the
split for that long-lived tail.

**The measurement that decides this needs no MinIO reads and can run today:**
the distribution of `max(fetched_at) - min(fetched_at)` per `listing_id` in
silver. If p95 capture span is under a week, plain weekly buckets lose almost
nothing and closure cohorts are over-engineering. If listings stay active for
months, buckets fragment badly and closure cohorts win outright. One query
collapses the design space — see Stage 0c.

### Pack layout

Stock `zstandard` only. No seekable-format dependency (the Python bindings do
not expose it), no new service.

```
pack object:  bronze/html_packs/{artifact_type}/{yyyy}/{mm}/pack-{seq}.zpack
  header      magic, format version, dict_id, frame count
  frame 0     independent zstd frame, ~N pages, dictionary-preloaded
  frame 1     ...
  ...
  footer      frame offsets

sidecar index: bronze/html_packs/.../pack-{seq}.idx.parquet
  artifact_id, listing_id, fetched_at, source_key,
  frame_ordinal, offset_in_frame, length, raw_sha256
```

- **Frames are independently decodable.** Random access decompresses one frame
  (~4-16 MB uncompressed, tunable), not the whole pack. That is the "slow" in
  slow access, and it is bounded.
- **The index is a sidecar object, not a Postgres table.** 3.9M rows of index
  is historical record, not hot operational state, so it belongs in object
  storage per the MinIO-first architecture. DuckDB globs every `.idx.parquet` to
  resolve an artifact without opening a single pack.
- **`raw_sha256` per member is the verification anchor** for Stage 2 and for any
  later audit.
- Keep writing dictionary `1367127621` inside frames. It costs nothing and helps
  the head of each frame; Stage 0 measures with and without.

### Pack eligibility and delete eligibility are separate knobs

Corrected 2026-08-13. An earlier draft gated packing on
`PACK_MIN_AGE_DAYS = 60`, a number chosen arbitrarily and never justified. It
conflated two independent decisions:

- **Writing a pack is additive and safe.** Nothing is lost if a pack is written
  early; the sources still exist.
- **Deleting the sources is the irreversible step**, and it is the only one that
  needs a grace period — plus a check that the artifact has been processed.

Gating both on one threshold meant deletion-safety concerns were silently
delaying the inode relief that is this plan's entire purpose. As of 2026-08-13
that was the difference between packing April-May (~45% of the corpus) and
April-July (**~85-90%**).

Nothing arrives late: `write_html` is called inline by the scraper, so an
object's `fetched_at` is effectively its write time and a closed calendar month
is genuinely closed. `PACK_SETTLE_DAYS` covers only artifacts fetched either
side of midnight on the boundary.

**Packing also improves the case a hot window would protect.** Reprocessing a
month from individual objects is ~1M round trips at Plan 129's measured
~83 obj/s — over three hours of pure latency. From packs it is a handful of
sequential reads. Packing degrades only *single-artifact random access*, which
is the rare case and still works.

### Hot/cold boundary, and why the write path does not change

Only artifacts in a **completed calendar month** are eligible, plus
`PACK_SETTLE_DAYS` (default 1) for boundary writes. The
scraper write path is **unchanged** — new artifacts land as individual
dictionary-compressed objects exactly as today. Packing is a background
lifecycle job over settled data.

**Do not "save" the write-time compression by writing raw and compressing
later.** This was proposed and rejected on 2026-08-13; recording it so it is not
re-proposed:

- **There is no recompression tax to remove.** `write_html` has exactly two
  callers, both in the scraper (`scrape_detail.py:178`, `scrape_results.py:312`).
  `read_html` has exactly one production caller
  (`processing/routers/batch.py:87`). Nothing writes HTML back after parsing.
  The cycle is compress-once, decompress-once.
- **The saving is one decompress**, which is level-independent at 500+ MB/s —
  ~0.3 ms for a 158 KB page. It is invisible next to the scraper's deliberate
  8-20 s inter-page delay.
- **The cost is an extra PUT and DELETE per artifact.** The compress is *moved*,
  not saved, so raw objects must later be rewritten and removed — 2 more S3
  operations and more inode churn on the resource this plan exists to protect.
- **It makes bronze durability depend on a second service.** Today a processing
  outage delays parsing and loses no bronze data. Under deferred compression it
  leaves objects at ~158 KB instead of ~7.3 KB — 22x larger — on a disk that hit
  100% twice during Plan 129's rollout. The "sweep for uncompressed objects" job
  that would be needed is cleanup for a failure mode the change itself creates.
- **It cannot be deferred to pack time either.** Artifacts sit individually for
  the whole `PACK_MIN_AGE_DAYS` window; 60 days at ~1M objects/month stored raw
  is ~316 GB against a 196 GB disk.

### The transient-doubling constraint, learned the hard way

Plan 129 lesson 1: MinIO enforces a minimum-free-drive threshold and refuses
**every** `PutObject` below it, including the small ones that would free space.
That killed a 768 KB dictionary upload.

Packing is strictly worse in this respect: a pack must be **fully written and
verified before its source objects can be deleted**, so it needs free space
equal to the pack size, transiently, before it frees anything.

Consequences, which are requirements not suggestions:

- Pack size is a tunable with a **small default** (start ~64 MB, not GB).
- The job **refuses to start** below a configurable free-space floor, and says
  so, rather than discovering it mid-write.
- One pack at a time: write → verify → delete sources → next. Never batch the
  deletes.
- The bucket is un-versioned (verified 2026-08-10, Plan 129 Stage 4), so a
  delete frees its space immediately with no version expunge step.

---

## Stages

### Stage 0 — Measure, and price the alternative (GATE)

Nothing is built until this passes. Three questions, in order.

**0a. Re-read the constraint.** `df -i /mnt/data` and current object count. The
headroom figure drives the whole priority of this plan. **DONE — see results.**

**0b. Price the cheaper alternative first.** Plan 129 note 6 found `results_page`
is a separate population — raw mean **706 KB** vs 158 KB for detail, stored
44.2 KB vs 28.1 KB — and Plan 114 put SRP reprocessing out of scope, making
results pages *"the better deletion candidate if space is ever needed
urgently."*

Report the object-count and inode split between `detail_page` and
`results_page`. **If a results-page retention policy reclaims comparable inodes
for a fraction of the effort, it wins and this plan waits.** An archive format
is not the answer to a problem a `DELETE` solves. **DONE — see results.**

**0c. Settle the grouping question with a query, before measuring anything
expensive.** Distribution of `max(fetched_at) - min(fetched_at)` per
`listing_id` in silver, plus captures per listing. Silver only, no MinIO reads,
no production writes. **DONE — see results.**

---

### Stage 0a-0c results (2026-08-13)

Read-only, run against production: `df -i` over SSH, and DuckDB over silver
Parquet inside `cartracker-archiver` (2 GB memory limit, 2 threads).

**0a — the inode clock is real, and compression did not slow it.**

| | 2026-08-08 (Plan 114) | 2026-08-13 | delta |
|---|---|---|---|
| Inodes used on `/mnt/data` | 8,774,058 | **9,101,670** | +327,612 |
| Inodes free | 4,333,142 | **4,005,530** | −327,612 |
| Disk used | 184 G | **162 G** | −22 G |

~65,500 inodes/day → **~61 days of headroom, about mid-October 2026.** The disk
improved by 22 GB over the same window as the Plan 129 backfill ran, which is
the point sharpened rather than softened: **bytes got better and the inode clock
did not move at all.**

**0b — the cheaper alternative is dead.** Distinct artifacts by source since
2026-04-01 (the HTML-backed window):

| source | artifacts | note |
|---|---|---|
| detail | 4,557,751 | |
| carousel | 4,017,371 | the *same* detail-page artifacts — carousel is a block on the detail page |
| **srp** | **32,443** | the entire `results_page` population |

Results pages are **0.7%** of objects. They are large by bytes (~1.4 GB total)
and negligible by object count: deleting every results page ever captured buys
roughly **half a day** of inode headroom. Plan 129 note 6 was right that they are
the best *byte* deletion candidate and that is simply not the constraint.
**Packing has no cheaper substitute on this axis.**

**0c — weekly buckets are wrong; monthly is much better than expected.**
Detail listings, April+ only:

| | value |
|---|---|
| listings / artifacts | 224,459 / 4,558,956 |
| capture span days, p50 / p90 / p95 | 21 / 81 / 105 (mean 31.6) |
| captures per listing, p50 / p90 / p95 | 12 / 50 / 67 (mean 20.3) |
| single-capture listings | 5.7% of listings, **0.3% of artifacts** |

| bucket | artifacts whose listing fits entirely inside one bucket | avg buckets/listing |
|---|---|---|
| weekly | 1.7% | 5.02 |
| monthly | 15.7% | **1.97** |

Weekly bucketing is rejected outright at 5.02 buckets per listing.

**Monthly vs closure cohort is NOT settled by these numbers.** A first pass
concluded monthly cost only ~5% and flipped the recommendation. That was wrong:
it modelled an extra base copy as costing the same as an extra ordinary capture,
which assumes away the base/delta asymmetry that is the whole premise of packing
by listing. Corrected model and follow-up measurement below.

#### 0c follow-up (2026-08-13): calendar months, artifact-weighted

The listing-weighted mean of 1.97 flattered monthly. Long-lived listings carry
disproportionately many captures, so weighted by **artifacts** the mean is
**2.84 calendar months**:

| calendar months | % of listings | % of artifacts |
|---|---|---|
| 1 | 41.2% | 17.9% |
| 2 | 35.3% | 27.4% |
| 3 | 13.0% | 22.3% |
| 4 | 6.5% | 17.7% |
| 5+ | 3.9% | 14.7% |

**54.7% of artifacts are in listings spanning 3+ calendar months.**

Effect on group size, which is what actually drives compression:

| grouping | groups | mean captures | p50 / p90 | artifacts alone | artifacts in groups ≤3 |
|---|---|---|---|---|---|
| per-listing (ceiling) | 224,459 | 20.9 | 12 / 51 | 0.3% | 2.2% |
| monthly | 441,448 | 10.6 | 7 / 23 | 1.2% | 5.4% |

The two views disagree, and they resolve to one unmeasured quantity. Total cost
is `(#groups)·B + (artifacts − #groups)·D`, so the extra cost of monthly is
exactly **216,989 × (B − D)**:

| delta size D (B ≈ 7.3 KB) | monthly penalty vs perfect grouping |
|---|---|
| D ≈ B (the wrong assumption) | ~5% |
| 3.0 KB | ~6% |
| 1.5 KB | **~15%** |
| 1.0 KB | **~22%** |

Monthly roughly **doubles the number of base copies** (224K → 441K) while
keeping groups healthy — median 7 captures, only 1.2% of artifacts stranded
alone. Whether that is cheap or expensive cannot be decided analytically.
**0d measures it.**

*Data caveat: this query counted 4,681,233 `(artifact_id, listing_id,
fetched_at)` triples against 4,557,751 distinct `artifact_id`s — ~2.7% of
artifacts appear under more than one listing or timestamp, plausibly VIN-moved
listings. It does not move any conclusion here but it is noise in the
denominators.*

**Two findings to carry forward:**

- **0.3% of artifacts are in single-capture listings**, so the within-listing
  redundancy applies to essentially the whole corpus.
- **20.3 captures over a 31.6-day mean span is a capture every ~1.5 days per
  vehicle.** [Plan 113](plan_113_production_adaptive_refresh.md)'s adaptive
  refresh attacks object *creation* where this plan attacks object *count*. They
  are the same problem from opposite ends and should be sequenced together.

**Open discrepancy, do not paper over it:** silver holds ~4.56M detail artifacts
since April but bronze held 3.92M objects on 2026-08-08 (~4.07M now). Some
artifacts have no surviving HTML object — Plan 128's challenge-page eviction is
one plausible source. **The packable universe is bronze objects, not silver
rows.** Stage 1 counts from MinIO directly rather than inferring it from silver.

**0d. Measure the pack win — the only Stage 0 step still outstanding.** On a
real sample, offline, writing nothing to production. Extend the Plan 129
measurement harness rather than starting fresh;
`scripts/estimate_dictionary_savings.py` already samples the corpus correctly via
`fetch_corpus_sample` (evenly across months, deterministic, **not** the
duplicate-biased `fetch_sample` that cost Plan 114 a re-run).

Report, per configuration:

| Variable | Values to sweep |
|---|---|
| Grouping | **monthly** vs **closure cohort** (both live) / per-vehicle (ceiling control) |
| Member ordering | `listing_id, fetched_at` vs arrival order |
| Pages per frame | 100 / 1,000 / 10,000 |
| Dictionary | with `1367127621` / without |

Weekly is dropped — 0c settled it at 5.02 buckets/listing. Per-vehicle is
measured as the compression **ceiling** only, since it is disqualified on
write-once grounds; it says how much any write-once grouping gives up.

**Monthly and closure cohort are both live candidates and 0d decides between
them.** Report `B` (bytes for the first capture in a group) and `D` (marginal
bytes per subsequent capture) explicitly — they are what the 0c follow-up could
not resolve, and the whole margin between the two groupings is
`216,989 × (B − D)`.

and for each, the three numbers that matter:

| Metric | Why |
|---|---|
| Bytes per page in-pack | the window win, isolated |
| **Physical bytes per page, floor applied** | the only one you can spend |
| **Objects and inodes per 1M artifacts** | the actual constraint |

Also report single-artifact extraction latency at each frame size. "Slow" needs
a number attached before it can be accepted.

**Gate:** ≥50% *physical* reduction against the current post-dictionary state,
**and** ≥20× object-count reduction. 0b is already satisfied — no cheaper
alternative exists. Below either remaining bar, stop and write down why.

### Stage 1 — Format, writer, reader — BUILT 2026-08-13

- `shared/packfile.py`: write a pack + sidecar index; read a member by
  `artifact_id`; in-process LRU of decompressed frames.
- Round-trip is asserted per member on write; a pack whose members do not
  extract byte-identically is never finalized.
- Format version in the header from day one. Packs are immutable and will
  outlive this plan's assumptions.
- Offline only. Nothing in production reads or writes packs at this stage.

### Stage 2 — Pack one cold cohort, delete nothing — BUILT 2026-08-13, not yet run in production

**This is archiver work, and it mirrors Plan 109's `compact_silver` exactly.**
The archiver is the service that owns lifecycle and compaction jobs, and it
already carries every dependency needed (`zstandard`, `boto3`, `duckdb`,
`pyarrow`, `s3fs`) plus `shared/` via `COPY . .`.

| Plan 109 precedent | Plan 131 equivalent |
|---|---|
| `archiver/processors/compact_silver.py` | `archiver/processors/pack_bronze_html.py` |
| `POST /compact/silver/run` | `POST /pack/bronze/run` |
| `airflow/dags/compact_silver.py` | `airflow/dags/pack_bronze_html.py` |
| `COMPACT_SILVER_MAX_PARTITIONS` (default 10) | `PACK_BRONZE_MAX_COHORTS` |
| write `.tmp` → assert row count → delete originals → rename | write pack → verify every member → delete sources |

**Prerequisite: the archiver image must be rebuilt before it can pack anything.**
Discovered 2026-08-13 while trying to run Stage 0d there. Plan 129 rebuilt only
`processing` and `scraper`, reasoning that a stale `archiver` was harmless
because it "only ever calls `object_size`/`read_json`/`get_s3fs`, never
`read_html`". That was correct at the time. **Plan 131 invalidates it:** the
packer's whole job is reading HTML.

The running archiver image today has no `shared/compression.py` at all, and its
`shared/minio.py` contains zero references to dictionaries — so its `read_html`
cannot decode the dictionary frames the Plan 129 backfill is actively creating.
A `docker cp` of the packer alone would not fix this; the stale library is the
problem, not the missing script. Rebuild `archiver` before Stage 2, and treat
"which images read HTML" as a question to re-ask whenever the read path changes.

*(Plan 129's "run it in the `processing` container" note was advice for a
**one-off backfill**, chosen because processing bundles `scripts/`. A recurring
lifecycle job belongs in the archiver by every precedent in this repo.)*

- Write packs alongside the source objects. **Delete nothing.**
- Verify **100%** of members extract byte-identically against `raw_sha256`.
- Checkpointed and resumable — Plan 129's backfill checkpoint was O(n²) once
  (commit `f98e69b`); do not reintroduce that shape.
- Never run over an SSH tunnel; Plan 129 measured in-container at ~8x the
  throughput.

---

## Stage 1-2 as built

Built 2026-08-13. Offline and dry-run-by-default; **nothing has been packed in
production yet.**

| File | What it is |
|---|---|
| `shared/packfile.py` | Format, writer, indexed reader, sidecar index |
| `tests/shared/test_packfile.py` | 35 tests, no MinIO/DuckDB/dictionary needed |
| `archiver/processors/pack_bronze_html.py` | Stage 2 packer + CLI |
| `tests/archiver/test_pack_bronze_html.py` | 26 tests over an in-memory object store |
| `tests/integration/archiver/test_pack_bronze_html_integration.py` | 5 tests, real MinIO |
| `archiver/app.py` | `POST /pack/bronze/run` |
| `docker-compose.yml` | `PACK_BRONZE_DICT_ID` on the archiver |

### The format, concretely

```
header   32 B      magic, format version, dict_id, frame count, member count
frame k            an independent zstd frame: members concatenated, compressed
                   as one unit (~8 MiB uncompressed, tunable)
footer   28 B/frame  offset, compressed length, uncompressed length, members
trailer  20 B      footer offset, frame count, magic
```

The trailer is last so a reader that knows only the object's size finds
everything in two ranged GETs, then fetches exactly one frame to read one
member. `PackReader` takes a `fetch_range(offset, length)` callable and nothing
else, which is why the same reader serves a local `bytes` and a ranged S3 GET
with no S3 code in `shared/packfile.py` at all.

### What the implementation settled

- **The bronze hive partition already *is* the monthly capture bucket.**
  `make_key` partitions by the capture's own `fetched_at`, so
  `html/year=Y/month=M/artifact_type=T/` is exactly the Stage 0d grouping. No
  regrouping, no scan to assign buckets — the packer's unit of work is one
  existing prefix.
- **The sidecar index is the checkpoint.** Resume reads the `.idx.parquet`
  files already written for the bucket and subtracts their source keys. Cost is
  one GET per existing pack, and nothing is re-serialised per object — the
  O(n²) shape of Plan 129's first checkpoint (`f98e69b`) is not reachable from
  this design rather than merely avoided in it.
- **Verification runs twice.** `PackWriter.finish()` extracts and hashes every
  member of the in-memory pack before returning it, so an unverified pack
  cannot be uploaded; then the *stored* object is re-read over ranged GETs and
  every member re-hashed before the sidecar is written. A pack with no sidecar
  is therefore always an interrupted run, never a verified one.
- **Objects with no silver row are packed, not skipped.** They sort after
  everything silver can describe. An artifact nobody can name is still an
  inode, and skipping it would leave it unpackable forever — the Stage 0c
  discrepancy (~4.56M silver rows against ~4.07M objects) cuts both ways.
- **The dictionary must be named explicitly.** `HTML_COMPRESSION_DICT_ID` is a
  *writer*-side variable, so `configured_dictionary_id()` is empty in the
  archiver. The packer reads `PACK_BRONZE_DICT_ID`, resolves it through the
  registry before reading a single object, and **refuses to run** without one
  unless `allow_no_dictionary` is passed — an undictionaried pack is a silent
  ~10 GiB regression, which is exactly the kind of thing that should not be
  possible to do by forgetting.
- **The free-space floor refuses an apply run and warns a dry run.** "How much
  would packing this month free?" is precisely the question worth asking when
  the disk is full, and a dry run writes nothing.
- **`PACK_BRONZE_MAX_PACK_BYTES` is measured in stored bytes**, because what it
  bounds is the transient free space a pack needs. Cutting on raw bytes instead
  would be ~20x conservative — detail pages are ~158 KB raw against ~7.3 KB
  stored — and would have produced ~3 MB packs from a 64 MB setting. A pack is
  always at least one frame, so a target below one frame's compressed size just
  yields one-frame packs.
- **An orphan pack is reported, never deleted.** Its sequence number is
  respected so nothing overwrites it. Stage 2 deletes nothing at all, including
  its own mistakes.
- **Eligibility is month completion plus `PACK_SETTLE_DAYS`, not an age
  threshold** — `_month_has_settled()` asks whether `(today - month_end).days
  >= settle_days`, against a `_today_utc()` that tests patch, exactly as
  `compact_silver` does with its 2-day watermark. Naming a bucket explicitly
  with `--year/--month` bypasses it and warns rather than refusing: Stage 2
  deletes nothing, so packing an open month only means a later run packs the
  rest of it.
- `PACK_BRONZE_MAX_COHORTS` is `PACK_BRONZE_MAX_BUCKETS` — the grouping is
  calendar buckets, and cohorts were the rejected alternative.

### WARC, checked as the plan required

WARC + CDX is the right *shape* and this plan borrowed it: one container, many
captures, an external index. The WARC serialization itself was rejected, for a
mechanical reason rather than a taste one.

WARC compresses **per record** — that is what keeps records independently
seekable, and it is the property this plan exists to give up. Stage 0d measured
the win as coming from the shared window (`D/B = 0.250`); a per-record-gzipped
WARC would deliver the container win and none of the window win. Compressing a
whole WARC as one unit instead recovers the window but destroys random access
unless an external offset index is added — which is the sidecar, already built.
What remains of WARC is then a text header per record (~200-400 bytes against
7.3 KB members) and a new pip dependency in the archiver. Nothing left to buy.

---

### Stage 3 — Read path prefers objects, falls back to packs

- `shared/minio.py::read_html` tries `html/` first, then resolves through the
  pack index. Transparent to every caller.
- **Survey consumers first.** Plan 129 Stage 2 flagged that anything
  decompressing bronze outside `read_html` breaks at read time, long after the
  write looked fine — `scripts/audit_semantic_duplicate_html_hashes.py` reads via
  DuckDB `read_blob` and is the known case. Enumerate them again; the failure
  mode here is identical.
- Deploy and confirm reads still work **while every source object still exists**,
  so a bug is a latency regression rather than an outage.

### Stage 4 — Delete packed source objects

Only for artifacts that have a finalized pack, a verified `raw_sha256` match, are
past a **delete** grace period, and have been processed. That grace period is
this stage's own knob and is deliberately not `PACK_SETTLE_DAYS` — see
[Pack eligibility and delete eligibility are separate
knobs](#pack-eligibility-and-delete-eligibility-are-separate-knobs). Dry-run by
default, hard per-run cap, one pack's worth of sources at a time.

**This is the step that frees inodes**, and it is the first irreversible-ish one
— though the bytes still exist inside the pack, which is the entire difference
between this plan and Plan 130.

### Stage 5 — Lifecycle DAG and observability

- Airflow DAG packing eligible cohorts on a schedule, respecting the free-space
  floor, thin like `compact_silver` (sensors + one HTTP call, no logic).
- Metrics: objects packed, packs written, bytes/inodes reclaimed, extraction
  latency p50/p95, verification failures (should be zero; alert on any).

---

## Prior art

This is a solved problem and the plan should borrow rather than invent:

- **WARC** (ISO 28500) + a **CDX** index — the web-archiving standard for
  exactly this: many captures, one container, external index. **Checked in
  Stage 1 and rejected on a mechanism, not a preference — see
  [WARC, checked as the plan required](#warc-checked-as-the-plan-required).**
  Short version: WARC compresses per record, and per-record compression is the
  exact property this plan gives up to win.
- **Hadoop SequenceFile / HAR** — the same small-files problem, same answer.
- **tar + zstd** — the degenerate case, rejected only because it has no index.

Plan 114 independently reconstructed content-defined chunking and found out
afterwards. Look first this time.

---

## Testing

### `tests/shared/test_packfile.py`
- Round-trip: every member extracts byte-identically, including the last member
  in a frame and a single-member pack.
- A member whose extraction does not match `raw_sha256` fails the pack, loudly.
- Frame boundaries: reading member *k* decompresses exactly one frame.
- Header/format-version mismatch raises rather than misreading.
- Index sidecar and pack footer agree; disagreement is an error, not a silent
  preference.

### `tests/archiver/test_pack_bronze_html.py`
- Checkpoint resume packs each artifact exactly once, and is not O(n²).
- Refuses to run below the free-space floor.
- `--apply` absent deletes and writes nothing.

### Integration (`tests/integration/shared/`)
- Pack to a test prefix, read back through the production `read_html` path,
  assert equality for both packed and unpacked artifacts.
- Both frame types still readable: no-dictionary, dictionary, and packed.

---

## Files Changed

| File | Change | Stage | Status |
|------|--------|-------|--------|
| `scripts/estimate_pack_savings.py` | Stage 0 measurement, offline | 0 | **done** |
| `shared/packfile.py` | Pack format, writer, indexed reader, frame cache | 1 | **done** |
| `tests/shared/test_packfile.py` | New | 1 | **done** |
| `archiver/processors/pack_bronze_html.py` | Stage 2 packer, checkpointed | 2 | **done** |
| `archiver/app.py` | `POST /pack/bronze/run` | 2 | **done** |
| `docker-compose.yml` | `PACK_BRONZE_DICT_ID` on the archiver | 2 | **done** |
| `tests/archiver/test_pack_bronze_html.py` | New | 2 | **done** |
| `tests/integration/archiver/test_pack_bronze_html_integration.py` | New | 2 | **done** |
| `shared/minio.py` | `read_html` falls back to pack lookup | 3 | not started |
| `archiver/processors/delete_packed_source_html.py` | Stage 4, dry-run default | 4 | not started |
| `archiver/app.py` | `POST /pack/bronze/prune` | 4 | not started |
| `airflow/dags/pack_bronze_html.py` | Lifecycle DAG | 5 | not started |
| `tests/integration/airflow/test_dag_integrity.py` | Register the new DAG | 5 | not started |

---

## Success Criteria

| Metric | Gate |
|--------|------|
| Stage 0d physical reduction vs post-dictionary state | ≥50% |
| Stage 0d object-count reduction | ≥20× |
| Cheaper alternative (results-page retention) priced first | **Met** — 0.7% of objects, rejected |
| Byte-identical extraction before any source delete | **100%, blocking** |
| Single-artifact extraction latency | Measured and accepted, not assumed |
| Consumers reading bronze outside `read_html` | Zero remaining before Stage 4 |
| Source objects deleted without a verified pack member | Impossible |
| Grouping chosen from measured pack bytes, not a cost model | Stage 0d |
| `B` and `D` reported explicitly | Stage 0d |
| Scraper write path changed | None — deferred compression is rejected, see Design |

---

## Risks

| Risk | Mitigation |
|------|------------|
| **Pack corruption loses thousands of artifacts at once** | Per-member `raw_sha256` verified before any delete; packs immutable; sources retained until verification passes; blast radius is why pack size starts small |
| Full disk blocks pack writes, as it blocked Plan 129's dictionary upload | Free-space floor check refuses to start; small packs; write→verify→delete one pack at a time |
| A consumer reads bronze outside `read_html` and 404s after Stage 4 | Stage 3 survey, repeating Plan 129's; deploy read path while all sources still exist |
| Extraction latency makes reprocessing impractical | Measured in Stage 0 as a gate input, not discovered in Stage 4; frame size is the tunable |
| Window win is smaller than hoped | Stage 0 gate; the ~18 GB floor-elimination win is arithmetic and does not depend on it |
| Format outlives its assumptions | Version in the header from day one; packs are append-only and never rewritten in place |
| Effort spent here when a retention policy was the answer | Stage 0b prices it first and this plan defers to it |

---

## Out of Scope

- Changing the scraper write path. New artifacts stay individual objects.
- Sectioning, manifests, or content-addressed section objects — see Plan 114.
- Discarding any content — see Plan 130.
- Retention/expiry policy. Still unwritten, still needed, and Stage 0b may show
  it should come first. Packing changes the slope of the growth curve; it does
  not change its direction.
