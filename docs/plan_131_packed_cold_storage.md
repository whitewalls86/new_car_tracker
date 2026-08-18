# Plan 131: Packed Cold Storage for Bronze HTML

## Status

**Stage 0 COMPLETE (2026-08-13) - gate passed, grouping decided.**
Monthly capture buckets: ~68% logical / ~79-81% physical saving, 1.00x
reprocessing, and the object count collapses from ~4.5M to a few hundred packs.

**Stages 1-2 BUILT and RUN IN PRODUCTION (2026-08-14).** One April pack:
17,291 members, **100% byte-identical verification, zero source objects
deleted**, 8,646x object reduction, 75.2% physical saving. See
[Stage 1-2 as built](#stage-1-2-as-built) for what the implementation settled,
and [the first production run](#stage-2-first-production-run--measured-2026-08-14)
for the measurements — including the one that came in **under** projection and
the frame-boundary bug it exposed.

**April `detail_page` PACKED IN FULL (2026-08-14).** 557,065 objects -> 64,
**8,704x**, 100% verified, zero deleted. Measured **53.4% logical / ~72.5%
physical** — both gates pass, both projections missed, and the cause is now
understood: see [April packed in full](#april-packed-in-full--measured-2026-08-14).

**APRIL, MAY AND JUNE PACKED AND PRUNED (verified 2026-08-17).** 2,702,453
members, 100% verified, 0 refused, **2,702,453 objects -> 222** (12,173x), and
**447.5 GiB of raw HTML now occupies 7.01 GiB**. June measured **2,217
B/member** — below the projection this plan retracted on April — and **82.8%
physical**. July is a complete month and is **not packed yet**. See
[May and June packed in full](#may-and-june-packed-in-full--measured-2026-08-17)
and [the full compression cascade](#the-whole-compression-cascade-on-one-set-of-bytes--measured-2026-08-14).

**Stage 3 DEPLOYED AND VERIFIED 2026-08-14.** `read_html` falls back to the
pack index when an object is gone, transparently to every caller, and verifies
every packed read against the sidecar's `raw_sha256`. **365 members across April and
May read byte-identically through both paths, 0 failed, with every source
object still in place** — the Stage 4 precondition is met. The object path is
unchanged at ~5.5-5.8 ms; a packed read is 6-20 ms with its index resident and
207-296 ms cold, where the cost is the sidecar scan rather than decompression.
See [Latency](#latency-measured-in-production-2026-08-14--the-gate-passed). The consumer survey found
**four** bypasses where it had previously named one; two are fixed and two are
harmless by construction. See [Stage 3 as built](#stage-3-as-built) — including
the measurement that chose pyarrow over DuckDB and the latency number that is
still outstanding.

**Stage 4 BUILT AND FIRST RUN 2026-08-14 — the first inodes this plan has
freed.** 100 April objects deleted, 100/100 verified, **0 refused**, 1,423,143
bytes and ~224 inodes freed, and **every deleted artifact read back
byte-identically from its pack afterwards**. See
[First production run](#first-production-run--measured-2026-08-14).

**Stage 4 as originally specified:** `delete_packed_source_html` +
`POST /pack/bronze/prune`: dry-run by default, hard per-run cap, one pack at a
time, and three mandatory checks per member before anything is removed — see
[Stage 4 as built](#stage-4-as-built). It must not run until Stage 3 is
deployed and the read path verified against real April packs; the
[run sheet](runbook_plan_131_stage_3_4.md) is the order of operations.

**Stage 5 BUILT (2026-08-17).** Cap semantics, endpoint failure contracts,
single-flight, deploy-intent pause/resume, the isolated pack worker, the
recurring read-path verifier endpoint, and the lifecycle DAG are all built and
merged. The DAG runs `0 6 3 * *` against `pack-worker` — pack, then prune, then
a bounded read-path canary — and holds no packing logic of its own. See
[Stage 5 as built](#stage-5-as-built). **It is in production testing and has not
yet completed a scheduled run**; the first falls on 2026-09-03, and the measured
numbers this plan asks for are recorded there once it has.

**Inode alerting moved to [Plan 135](plan_135_storage_observability.md), and has
since shipped there.** It was Stage 5's Step 6, it is Plan 135's Stage 3, and it
could not work here on its own: node-exporter had never reported `/mnt/data` at
all, so an inode rule written in this plan would have evaluated `/` — 9% used —
while the volume both plans exist to protect sat at 61% and invisible. Plan 135
Stage 1 was the prerequisite, and one plan owning those rules beat two plans
editing the same file. **Both landed 2026-08-17** (PR #204): `/mnt/data` now
reports, and the byte and inode rules cover both volumes. What stays here is the
verification-failure alert, which is pack integrity rather than disk capacity.

That 61% is now **21%** — packing removed roughly 4.05M inodes, about two per
packed object, and July is still draining. Read the inode figures elsewhere in
this document as measurements of the date attached to them, not of today; the
motivating pressure is what this plan set out to relieve, and it did.

Stage 4 is the only step that removes data. Two of
its three would-be gates have now been settled and **neither is a gate**: the
[delete grace period defaults to
0](#the-delete-grace-period--0-days-revised-2026-08-14) (decided 2026-08-14 —
nothing it was supposed to protect against survived examination), and
[processing status is reported rather than
enforced](#the-has-been-processed-check--proposed-2026-08-14-needs-agreement-before-stage-4-is-built)
(still proposed, not yet agreed). What remains load-bearing is per-artifact
verification through the production read path at delete time, a dry-run
default, and a hard per-run cap.

> **Sequencing constraint with [Plan 132](plan_132_unrecorded_artifact_recovery.md):**
> 42,276 April captures were never recorded downstream and are recoverable
> only by reparsing their HTML. They are readable as individual objects today.
> **Either run Plan 132's reparse before Stage 4, or land Stage 3 first.**
> Stage 4 must not run over April while that is outstanding.

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

*(Both halves of that second bullet were revised on 2026-08-14, in the same
spirit as the correction above: neither a waiting period nor a processing status
survived being asked what it protects against. See [the delete grace
period](#the-delete-grace-period--0-days-revised-2026-08-14) and [the processed
check](#the-has-been-processed-check--proposed-2026-08-14-needs-agreement-before-stage-4-is-built).
What is irreversible about deletion is real; what makes it safe turned out to be
verification, not delay.)*

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

### Stage 2 first production run — measured 2026-08-14

One pack, April 2026 `detail_page`, on the production VM. **Zero source objects
deleted.**

| | |
|---|---|
| bucket | 557,065 objects, 4,579,267,375 B stored |
| listing the bucket | 1,032 s at ~530 keys/s — flat, no deceleration |
| pack | 17,291 members, 387 frames, 67,168,784 B + 1,365,502 B sidecar |
| packing rate | 17,291 members in 114 s (**152 objects/s**) |
| verification | **17,291 / 17,291 byte-identical**, 0 read failures |
| independent re-check | 25/25 extracted byte-identically from the *stored* pack over ranged GETs; index and footer agree; `dict_id` 1367127621 present; listing order preserved |

Saving on the packed members, every source object HEADed for an exact baseline:

| | measured | Stage 0d projected |
|---|---|---|
| logical | **57.8%** | 67.8% |
| physical | **75.2%** | 79-81% |
| objects | 17,291 -> 2 (**8,646x**) | >=20x |
| inodes | ~38,732 -> ~4.5 | |

Both gates pass. Both projections were missed, and the cause is not the
projection.

#### Why it fell short: frames were sealed by byte count, not by listing

| | measured in the pack |
|---|---|
| distinct listings | 544 (31.8 captures each — *better* than the corpus's 10.6) |
| listings split across >1 frame | **270 of 544** |
| implied D | **3,732 B** against Stage 0d's measured 2,142 B |

Group sizes were never the problem. **A zstd frame is an independent
compression window, so a boundary landing inside a listing makes the
continuation frame re-pay that listing's full base cost instead of a ~2 KB
delta.** Stage 0d measured one frame per (listing, month) group — never split.
The first implementation cut frames at a fixed 8 MiB, which ignored listing
boundaries and split half of them.

Fixed by sealing frames at listing boundaries: `frame_target_bytes` became a
*soft* target (seal at the first listing boundary at or after it) with
`frame_max_bytes` as a hard ceiling so one enormous listing cannot produce one
enormous frame. The default target rose 8 -> 16 MiB, inside the plan's stated
4-16 MB range, because p90 here was 59 captures ~ 11 MB.

On synthetic members with production-like proportions the change removes every
split and cuts the pack 67.4%; on this pack, Stage 0d's B and D applied to its
544 real groups project 40.5 MB instead of 67.2 MB — **~74% logical / ~84%
physical**, past the original projection. **That projection was wrong. See the
next section, which supersedes it.**

### April packed in full — measured 2026-08-14

30 further packs, `--max-packs 0`, one run. **522,314 / 522,314 members
verified byte-identical, 0 read failures.** With the two earlier packs, all
557,065 April `detail_page` objects are packed. Still zero source objects
deleted.

| | measured | previously projected |
|---|---|---|
| objects -> pack objects | 557,065 -> 64 (32 packs + 32 sidecars) | 40 |
| **object reduction** | **8,704x** | >=20x gate |
| pack bytes | 2,133,921,814 (+~44 MB sidecars) | 1.10 G |
| logical, vs 4,579,267,375 stored | **53.4%** | 74% |
| physical, 1.99 GiB vs 7.24 GiB | **~72.5%** | 84.4% |

**Both gates still pass with room. Both projections were missed again, and
this time the cause is understood.**

#### The alignment fix worked. It was not the bottleneck.

From the sidecar indexes, pre-fix packs against post-fix:

| | pack-00000 / 00001 (pre-fix) | pack-00002-00025 (fixed) |
|---|---|---|
| listings split across frames | 270/544 and 313/668 — **~48%** | **0-5 per pack** of 786-2,560 (~0.1%) |
| frames per pack | 387, 392 | 138-174 |
| frame p50 uncompressed | 8.1 MB | 17-19 MB |

Sealing at listing boundaries did exactly what it claimed. Two other things
account for the shortfall.

**1. 18% of April has no usable metadata.** 99,981 members carry a null
`listing_id`; they sort last and land entirely in packs 00026-00031. The
packer sees one listing 44K members long, so those frames pin at
`frame_max_bytes` (32.1 MB) with no boundary ever found:

| group | members | raw/member | packed/member |
|---|---|---|---|
| named listings | 457,084 | 180.7 KB | **3,900 B** |
| small nulls | 55,996 | 36.9 KB | **1,060 B** |
| full-size nulls | 43,985 | 164 KB | **7,157 B** |

The full-size nulls cost ~143 MB, 6.7% of April's pack bytes. **This is an
April-only data problem, not a packer defect — see
[Plan 132](plan_132_unrecorded_artifact_recovery.md).** May and June have
complete metadata coverage.

**2. Stage 0d's `B`/`D` constants do not generalize.** `captures_per_listing`
falls monotonically across the packs — 31.8, 26.1, 23.4 ... 6.4 — because
members sort by `listing_id` ascending and low IDs are listings that predate
April. April's named mean is **12.3** against Stage 0d's sample at 17.1.
Applying `B=8,578 / D=2,142` to each pack's *measured* captures-per-listing
predicts 2,417-3,148 B/member; measured, with zero splits, is 3,657-4,097 —
**35-50% above the model.** The constants came from 140 listings and do not
hold at 37,038.

Fixing the nulls entirely would move April from 53.4% to about **56.5%**.
**74% is not reachable on this corpus and the earlier projection should not be
quoted.**

#### Pack boundaries are nearly free

Only **138 of 37,038** listings span more than one pack, which retroactively
justifies the small `--max-pack-bytes` default: blast radius and transient
free space were bought for almost nothing.

#### The whole compression cascade, on one set of bytes — measured 2026-08-14

Every prior plan measured its own step against its own baseline, so the steps
were never comparable. This is 300 members pulled from the real pack and run
through all five encodings — same bytes, same order, one baseline.

| stage | B/member | physical B/member | vs raw | step |
|---|---|---|---|---|
| uncompressed | 189,894 | 196,116 | — | — |
| zstd level 3 | 34,488 | 40,400 | 81.8% | **81.8%** |
| zstd level 9 ([Plan 116](plan_116_estimate_recompression_savings.md)) | 31,586 | 37,833 | 83.4% | **8.4%** |
| zstd L9 + dictionary ([Plan 129](plan_129_zstd_dictionary_compression.md)) | 9,492 | 15,933 | 95.0% | **69.9%** |
| *as actually stored today* | *9,492* | *15,933* | *95.0%* | *0.0%* |
| packed (Plan 131, measured on `pack-00000`) | 3,964 | 3,964 | 97.9% | **58.2%** |
| ~~packed (aligned, projected)~~ | ~~2,423~~ | ~~2,423~~ | ~~98.7%~~ | ~~74.5%~~ |

**The bottom row is the retracted projection** — see
[the alignment fix](#the-alignment-fix-worked-it-was-not-the-bottleneck). It is
kept struck through because the rest of the table is a sound measurement and
the row is what the fix was expected to buy. Full-April measured 3,900 B/member
on named listings.

Four things the table says that the individual plans could not:

- **The "as stored today" row exactly equals the dictionary row — 9,492 both.**
  Not arranged: it is independent confirmation that April is fully
  dictionary-backfilled *and* that recompressing out of the pack reproduces
  production's stored bytes byte-for-byte. Had the pack lost anything, the two
  rows would diverge.
- **Level 3 → level 9 measured 8.4%**, against Plan 116's claimed ~8-10%,
  confirmed on real April data three plans later.
- **Only the last step changes the object count.** Levels 3, 9 and the
  dictionary all leave one object per artifact — April's 557,065 objects before
  and after. (The original write-up of this table put 1,124,122 here, which is
  not April's object count; it is June's, measured three days later. Treat the
  count as unchanged-by-construction rather than as a measurement.) That is this
  plan's whole thesis in one column, and it is why physical
  is the honest view: the dictionary took physical from 37,833 to 15,933 per
  artifact while the 4 KB directory entry and the 8 KB floor rode along
  untouched. Packing is the only step that removes them, so physical
  (15,933 → 3,964, **75.1%**) beats logical (58.2%).
- **189,894 → 3,964 B is 47.9:1**, measured and in production.

#### The average member is not a compressed page — measured 2026-08-14

The per-member average hides two very different costs, and quoting it as a
compression ratio overstates what packing does to any single page:

| | cost | ratio vs ~190 KB |
|---|---|---|
| first capture of a vehicle | ~8,578 B | 22:1 |
| each **repeat** capture | ~2,142 B | **89:1** |

The 89:1 is not compressing a page; it is storing the difference between two
photographs of the same listing a day apart, where nothing changed but the
price. An isolated one-off page never does better than that first-capture 22:1.
Three kinds of redundancy stack here, none lossy: HTML is ~82% air (plain zstd
takes that), the dictionary knows what every Cars.com page looks like (another
70%), and packing knows this specific car looked almost identical yesterday
(another 75%).

These are Stage 0d's `B`/`D` constants, and
[they do not generalize](#the-alignment-fix-worked-it-was-not-the-bottleneck) —
they came from 140 listings and ran 35-50% below measurement at 37,038.

#### Corpus scale — projected 2026-08-14, superseded

Recorded as it stood, because the shape of the argument is right even where the
arithmetic moved:

| | |
|---|---|
| raw HTML, 4,557,751 detail artifacts since April | **~806 GiB** |
| stored today (physical) | ~67.6 GiB |
| ~~packed + aligned (projected)~~ | ~~**~10.3 GiB**~~ |
| objects | 4.56M → ~330 |
| inodes | ~10.2M → ~740 |

**The packed row assumed the retracted 2,423 B/member.** The object and inode
rows stand — those follow from pack count, not from compression ratio. For the
same table built on measured months rather than a projection, see
[Corpus scale on complete months](#corpus-scale-on-complete-months--measured-2026-08-17).

#### May, first two packs — measured 2026-08-14

May is running overnight. Early signal, 2 packs of ~37:

| | April main body | May pack-00000/00001 |
|---|---|---|
| members per 64 MB pack | ~17,500 | **27,980** |
| packed bytes/member | 3,900 | **2,410** |
| stored bytes/object | 8,220 | 6,978 |
| implied logical | 53.4% | **~65.5%** |

No null tail, no pinned frames, more captures per listing. April showed a
degradation gradient through the month, so expect May to drift up too — but at
the equivalent position April was at 3,657 B/member. **Do not restate the
three-month projection table until May completes.** *(It has. See
[May and June packed in full](#may-and-june-packed-in-full--measured-2026-08-17),
which restates it on measurement.)*

#### Operational notes from the run

- **Listing is the fixed cost.** 557,065 objects took 18m42s at ~500 keys/s,
  paid on every run regardless of how much is packed. At `--max-packs 0` a
  whole month is one run and it amortizes to ~10%; caching the enumeration is
  therefore a nice-to-have, not a prerequisite. (An earlier revision of this
  section recommended the cache on the strength of a `--max-packs 1` run. That
  was the wrong denominator.)
- **The free-space floor must not measure the container's `/`.** On the VM `/`
  is a 49 GB overlay on /dev/sda1; the bucket lives on /dev/sdb. Default is now
  `/usr/app/logs`, the archiver's own named volume, which reads identical to
  /mnt/data down to the inode count.
- **A detached `docker exec -d` is the only sane way to run this.** Two
  foreground attempts died with their SSH connection mid-listing.
- **Packing is far faster than reading was assumed to be**: 152 objects/s
  including read, decompress, recompress, verify and upload, against Plan 129's
  ~35-83 obj/s. A whole month is ~1 hour of packing after the listing.

### May and June packed in full — measured 2026-08-17

April, May and June are packed and their source objects pruned — **2,702,453
members, 100% verified, 0 refused, 0 read failures across the three months**.
That is enough to test the retracted `2,423 B/member` projection against
measurement instead of against April alone.

Raw bytes are summed from every sidecar's `length` column (111 sidecars); stored
bytes are the pack runs' own `source_bytes`, taken from the object listing at
pack time; pack bytes are re-listed from the bucket today.

| | April | May | June |
|---|---|---|---|
| members | 557,065 | 1,021,266 | 1,124,122 |
| packs (each + one sidecar) | 32 | 41 | 38 |
| distinct listings | 37,038 | 88,273 | 71,157 |
| members with null `listing_id` | **99,981** | 1,320 | **0** |
| captures per listing *(named members only)* | 12.3 | 11.6 | **15.8** |
| raw B/member | 167,255 | 179,211 | 181,768 |
| stored B/object (dictionary, as it was) | 8,220 | 6,978 | 7,088 |
| **packed B/member** | 3,831 | 2,637 | **2,217** |
| packed B/member incl. sidecar | 3,908 | 2,717 | 2,295 |
| logical saving vs stored | 53.4% | 62.2% | **68.7%** |
| ratio vs raw | 43.7:1 | 68.0:1 | **82.0:1** |

**The projection was right to be retracted on April, and June beats it.** 2,217
against 2,423, on a month 2x April's size. Retracting it was still correct: the
claim was made about April, where it was unreachable for the two reasons already
recorded — 18% null metadata and 12.3 captures per listing. Both move the right
way in June, which has **no** null-metadata members at all and 15.8 captures per
listing, and the per-member cost lands below the projection rather than 35-50%
above the model.

May sits between them for a reason the table shows: 88,273 distinct listings for
1.02M members is **11.6 captures each**, the lowest of the three, so it amortizes
fewer deltas per base. Captures per listing, not month size, is what sets the
per-member cost.

#### Physical, and the inodes — measured at prune time

Physical is the honest view here for the reason the cascade table gives: the
dictionary never touched the 4 KiB directory entry or the 8 KiB floor, and
packing is the only step that removes them. These are the pruner's own
before/after readings of the volume, not a model.

| | April | May | June |
|---|---|---|---|
| objects deleted | 556,965 | 1,021,266 (2 runs) | 1,124,122 |
| logical bytes freed | 4.26 GiB | 6.64 GiB | 7.42 GiB |
| physical freed *(free-space delta)* | 6.79 GiB | ~13.4 GiB † | **14.01 GiB** |
| physical / logical | 1.59 ‡ | 2.02 † | 1.89 |
| inodes freed | 1,102,455 | 857,010 † | 2,227,795 |
| inodes per object | 1.98 | 2.03 † | 1.98 |
| **physical saving** | 70.1% | ~80.7% † | **82.8%** |

† May was pruned in two runs and only the second's JSON survives, so its
measured figures cover 421,266 of the 1,021,266 objects; the whole-month
physical is that run's measured 2.018 ratio applied to the full logical total.
‡ April's delta is the one number here that looks wrong, and it reads *low* —
1.59 against the 1.73-1.76 the 4 KiB-block model predicts for its mean object
size. June's packing was running during April's prune window, writing pack bytes
into the same volume the delta was measured on. Treat 6.79 GiB as a floor.

**1.98 inodes per object, twice, on 1.7M objects.** The plan's estimate was
~2.24; the measurement is stable and slightly better.

#### Corpus scale on complete months — measured 2026-08-17

Replaces [the superseded projection](#corpus-scale--projected-2026-08-14-superseded).
Everything here is measured, on the three months that are packed and pruned.
July is a complete calendar month but is **not packed**, so it is not in this
table.

| | April-June, measured |
|---|---|
| raw HTML, 2,702,453 detail captures | **447.5 GiB** |
| stored as dictionary objects (logical) | 18.32 GiB |
| stored as dictionary objects (physical) | ~34.2 GiB |
| **packed, incl. sidecars (logical = physical)** | **7.01 GiB** |
| objects | 2,702,453 → **222** (**12,173x**) |
| inodes | ~5.35M → ~500 |
| ratio, raw → packed | **63.8:1** |

447.5 GiB of web pages in 7.01 GiB, on a 196 GB disk — and the object count that
was the actual constraint fell by four orders of magnitude. The sidecars are 2.8%
of the packed total (212 MB against 7.32 GB of packs) and are the price of random
access; the WARC alternative would have paid it in per-record headers instead and
lost the shared window.

#### What this does to the Stage 5 runway

[The runway estimate](#the-constraint-moves-to-bytes-and-moves-out-about-three-years)
assumed **~2.4 GiB per packed month**. Measured: 2.03, 2.58, 2.40 — mean **2.34
GiB**. The assumption holds.

It also predicted ~102 GiB free after the whole April-July cycle. The volume
reads **114 GiB free today with July neither packed nor pruned**, so the estimate
was conservative by at least 12 GiB, and July's prune should return roughly
another 14 GiB physical against ~2.4 GiB of new packs. The ~36-month
full-retention runway is a floor, not a midpoint.

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

### Stage 3 — Read path prefers objects, falls back to packs — BUILT 2026-08-14, not yet deployed

- `shared/minio.py::read_html` tries `html/` first, then resolves through the
  pack index. Transparent to every caller.
- **Survey consumers first.** Plan 129 Stage 2 flagged that anything
  decompressing bronze outside `read_html` breaks at read time, long after the
  write looked fine — `scripts/audit_semantic_duplicate_html_hashes.py` reads via
  DuckDB `read_blob` and is the known case. Enumerate them again; the failure
  mode here is identical.
- Deploy and confirm reads still work **while every source object still exists**,
  so a bug is a latency regression rather than an outage.

---

## Stage 3 as built

### The consumer survey, re-run 2026-08-14 — it named one bypass and there were four

`read_html` still has **exactly one production caller**,
`processing/routers/batch.py:87`, so a fallback inside `shared/minio.py` covers
production and every offline script for free. All six offline scripts that read
HTML — `train_html_dictionary`, `estimate_dictionary_savings`,
`estimate_pack_savings`, `audit_sectioned_html_storage`,
`diff_semantic_duplicate_html`, `diff_log_analysis` — go through it, as does
the packer itself.

**The bypass count was wrong.** Grepping for direct decompression rather than
for the known script found four sites, not one:

| site | what it did | decision |
|---|---|---|
| `audit_semantic_duplicate_html_hashes.py` | DuckDB `read_blob`, sha256 of the **stored** bytes | **routed through `read_html`**, now hashes the raw HTML |
| `diff_log_analysis.py::fetch_blob_size` | DuckDB `read_blob`, `octet_length` of the stored bytes | **now `object_size()`** — a HEAD, and `None` means packed |
| `recompress_bronze_html.py` | `get_object` over an enumerated prefix | **left as is** — it acts on the objects it enumerated |
| `estimate_recompression_savings.py` | `get_object` over an enumerated prefix | **left as is**, same reason |

The last two never 404 after Stage 4 because they only ever touch objects a
listing just returned. The first two would have, and both were resolved rather
than documented as a limitation — the plan's own success criteria say *zero*
consumers reading bronze outside `read_html` before Stage 4, and two small
edits are cheaper than an exception to that.

**Routing the hash audit through `read_html` also fixes what it measures.** It
hashed *compressed* bytes, and Plan 129's dictionary backfill re-compressed
every object in the corpus on 2026-08-13 — so two byte-identical pages captured
either side of that hash differently while nothing about the HTML changed. It
now reports `raw_bytes`/`raw_sha256`, which is the same quantity a pack sidecar
stores, so an audit's output is directly comparable to a pack index.

**A quantity that stops existing.** `fetch_blob_size` is the general case worth
naming: *stored bytes per artifact* is not defined for a packed member. Its
bytes are a slice of a shared compression window — that is the entire win — so
the script reports `packed` and excludes it from the byte comparison rather
than inventing a number. Any future tooling that divides by per-object stored
size has the same problem.

### How a reader finds the pack: bounded by the key's own hive partition

The plan proposed globbing every `.idx.parquet` through DuckDB. That is fine
for an audit and wrong here, for a reason that is mechanical rather than
performance-based:

> **The processing image has `pyarrow` and `boto3`. It does not have DuckDB.**
> The one production caller of `read_html` lives there, so a DuckDB resolver
> means a new dependency on the parsing hot path.

It would not buy anything either. Measured 2026-08-14 on April-shaped sidecars
(32 packs, 17,291 rows each, 1.17 MB stored, local files, so network is
excluded from both sides):

| operation | cost |
|---|---|
| parse one sidecar to an Arrow table | **1.7 ms** |
| parse one sidecar to `PackIndexEntry` objects | 58.8 ms |
| lookup in a cached Arrow table (`pc.index`) | **0.04 ms** |
| build a Python `dict` from a cached sidecar | 7.7 ms |
| cold scan of a month, hit in sidecar 1 / 16 / 32 | 3.1 / 23.1 / **41.2 ms** |
| DuckDB glob over the same 32 files, 2 threads | 19.0 ms |

So the resolver reads one month's sidecars in sequence and looks up with
`pyarrow.compute.index`. A source key under
`html/year=2026/month=4/artifact_type=detail_page/` can only be in a pack under
`html_packs/detail_page/2026/04/`, so the search is bounded to 32 sidecars
rather than every month's — and a *warm* lookup is 0.04 ms, which is three
orders of magnitude below the frame decompress it precedes.

**Cache the index as Arrow, not as Python.** This is the measurement that
changed the design: converting a sidecar to `PackIndexEntry` objects costs
58.8 ms and several MB, against 1.7 ms and 3.8 MB to hold the same data as an
Arrow table that answers lookups faster. Only the one row that matches is ever
converted.

Both caches are bounded and both are needed, at different steps:

| knob | default | what it bounds |
|---|---|---|
| `PACK_INDEX_CACHE_PACKS` | 4 | sidecars held (~3.8 MB each) — resolution |
| `PACK_READER_CACHE_PACKS` | 1 | open packs, each holding ≤2 decompressed frames (~32 MB) — extraction |
| `PACK_INDEX_LIST_TTL_SECONDS` | 300 | how often an unchanged month pays for a LIST |
| `PACK_READ_FALLBACK` | on | set to 0 to prove a failure is not the fallback's doing |

A miss against a *cached* listing re-lists once before giving up, so a pack
written since the listing was cached is never invisible.

### Every packed read verifies itself

`read_html` hashes what it extracted and compares it to the sidecar's own
`raw_sha256` before returning. That costs ~0.15 ms against a ~16 MiB frame
decompress, and it is the claim Stage 4 deletes source objects on the strength
of, so it is checked on the path that will be doing the reading rather than
only at pack time.

Three failures are distinguished, and none of them silently prefers one source
of truth over another:

- sidecar row count vs the pack header's member count → `PackIndexMismatchError`
- an index entry spanning past its frame → `PackIndexMismatchError`
- extracted bytes not matching `raw_sha256` → `PackVerificationError`

A key in **neither** place raises the same `NoSuchKey` from the original GET
that it raises today. A stored object that fails to decompress raises too:
falling back to the pack there would hide exactly the corruption this
verification exists to catch.

**The packer reads through `read_html` too, and that is safe.** A source object
Stage 4 has deleted is by definition already in a sidecar, and the packer's
checkpoint subtracts sidecar source keys from the objects it listed — so a
key the fallback could now serve is a key the packer already skipped. The
fallback cannot cause an artifact to be packed twice.

### Latency, measured in production 2026-08-14 — the gate passed

`scripts/verify_pack_read_path.py`, April 2026 `detail_page`, in the archiver
container. **160 members sampled across all 32 packs, 160 verified, 0 failed,
`sources_already_deleted: 0`** — every artifact read byte-identically through
both paths while every source object still existed. That is the Stage 4
precondition, met.

May was run immediately after and is the cleaner month — no null tail,
complete metadata coverage: **41 sidecars, 205 sampled, 205 verified, 0 failed,
every source still in place.** Across both months **365 artifacts verified,
0 failures.**

| path | April p50 | April p95 | May p50 | May p95 |
|---|---|---|---|---|
| object (today's read) | **5.79 ms** | 8.35 ms | **5.51 ms** | 7.49 ms |
| pack, cold | **206.65 ms** | 361.93 ms | **296.34 ms** | 673.92 ms |
| pack, warm | **5.90 ms** | 34.31 ms | **19.63 ms** | 62.97 ms |

**The object path is unchanged and identical across months** — 5.79 and
5.51 ms. That is the point: live parsing reads hot-month objects and never
touches a pack.

**`pack_warm` is index-warm but frame-cold, and that is the honest
single-artifact number.** An earlier revision of this section called it "the
reprocessing shape". It is not. The five warm samples come from one pack but
land in five *different* frames (April: 238, 275, 373, 68, 286) and the reader
caches two, so each read still decompresses a fresh ~16 MiB frame. 6-20 ms is
therefore the cost of **random access within a pack whose index is already
resident**. Sequential reprocessing walks members in frame order and amortizes
one decompress across every member of that frame, so it is faster than this
number, not slower. *(n=5 in both months — too small to read the April/May
gap as signal.)*

**Cold is 36-54x the object path, and the cost is not decompression.** It is
the sidecar scan. `verify_pack_read_path` drops every cache before each cold
read, so resolving a key in pack *k* re-fetches and re-parses *k+1* sidecars.
April's throughput shows it directly: **9.4/s → 6.4/s → 4.8/s**, degrading
monotonically as the sample walked toward pack-00031. Frame decompression is a
small constant on top.

**May confirms the mechanism by being worse, despite being the better-packed
month.** It has no null tail and complete metadata, and it is still 43% slower
cold (296 vs 207 ms p50) with a p95 nearly double (674 vs 362 ms) — because it
has **41 packs to April's 32, and ~27,980 members per pack against ~17,500**,
so both the number of sidecars scanned and the bytes parsed per sidecar are
larger. The cold cost tracks packs-per-month times sidecar size, not data
quality. **June and every later month will be slower again on the same
trajectory**, which turns the cache defect below from a nuisance into the thing
to fix before any large reparse.

#### The cold figure is a deliberate worst case, but it exposes a real cache bug

Production never drops its caches between reads, so the steady state sits much
closer to warm. **But the LRU is pathological for exactly this access
pattern:** `PACK_INDEX_CACHE_PACKS` defaults to 4 while a month holds 32-41
sidecars, and the resolver scans from `pack-00000` upward. A single scan of 32
sidecars evicts the low-numbered ones it will need first next time, leaving the
cache holding 28-31 — so the *next* lookup re-fetches from the start. Classic
LRU-versus-sequential-scan thrashing, and a random-access reader across a
packed month would pay ~200 ms per artifact indefinitely rather than warming
up.

Two fixes, neither blocking Stage 4 (whose deleter walks one pack with its own
`PackReader` and never uses the resolver), both relevant to
[Plan 132](plan_132_unrecorded_artifact_recovery.md)'s 36K-artifact reparse:

- **Cache the `source_key` column, not the whole sidecar.** Measured 1.30 ms to
  parse and 1.69 MB in Arrow, against 1.69 ms and 3.78 MB for all columns; only
  the one matching row ever needs the rest. A whole month resident becomes
  ~68 MB instead of ~150 MB.
- **Raise `PACK_INDEX_CACHE_PACKS` to hold a month.** With the column-pruned
  cache, ~40 is affordable.

Recorded rather than fixed in the same breath: the gate passed, and changing
the read path after it was verified would invalidate the verification. Both
this and the `object_exists` gap found alongside it are tracked in
[Plan 133](plan_133_pack_read_path_hardening.md), which is sequenced before
Plan 132's reparse rather than before Stage 4 — neither defect blocks
deletion.

### Stage 4 — Delete packed source objects

Only for artifacts that have a finalized pack and a verified `raw_sha256` match
**through the production read path, at delete time**. Dry-run by default, hard
per-run cap, one pack's worth of sources at a time.

The two additional conditions this section used to require — a delete grace
period and a processing-status check — were both revised on 2026-08-14 and
neither is a gate. [The grace period defaults to
0](#the-delete-grace-period--0-days-revised-2026-08-14); [status is reported,
never a
veto](#the-has-been-processed-check--proposed-2026-08-14-needs-agreement-before-stage-4-is-built).

**Two things measured on 2026-08-14 that this stage must handle:**

- **42,276 April objects can never satisfy a "has been processed" check** —
  they have no `artifacts_queue_events` row to check against. As specified,
  Stage 4 would refuse them forever, stranding ~95K inodes as individual
  objects in the one fully packed month. They are inside verified packs, so
  deleting them loses nothing, but it needs an explicit *"packed and verified,
  no provenance"* branch rather than falling through the processed check.
  **No longer blocked on [Plan 132](plan_132_unrecorded_artifact_recovery.md)'s
  reparse**, once Stage 3 is deployed and verified: the reparse reads these
  artifacts out of the packs, which is precisely what Stage 3 makes possible.
  Landing Stage 3 first was always the option that removed the conflict rather
  than sequencing around it.
- **The retention policy this plan defers to Out of Scope already existed and
  was lost.** `n8n/workflows/Cleanup Artifacts.json`, pre-2026-04-16, deleted
  by processing status (`ok` after 48 h, `skip` immediately, `retry` after
  7 days). The Plan 102 n8n decommission removed it and nothing replaced it,
  which is why Plan 129 found "no HTML deletion anywhere in the codebase."
  **That removal is the origin of the inode problem this plan exists to
  solve.** Plan 132 Stage 4 argues the recurring predicate belongs in this
  plan's Stage 5 DAG rather than in a separate job.

**This is the step that frees inodes**, and it is the first irreversible-ish one
— though the bytes still exist inside the pack, which is the entire difference
between this plan and Plan 130.

#### The delete grace period — 0 days, REVISED 2026-08-14

**`PACK_DELETE_GRACE_DAYS` defaults to 0.** The knob stays — it costs nothing
and it is the lever if something ever does surface — but it defaults to no wait,
because a 14-day default was proposed and could not survive being asked what it
protects against.

The proposal was 14 days measured from the sidecar's write time, on two
arguments. Neither holds:

- **"A window in which a lost pack is still recoverable from its sources."**
  This assumes pack loss is concentrated in the fortnight after writing. It is
  not. Write-time defects are already caught three times — `PackWriter.finish()`
  on the in-memory pack, a re-read of the *stored* object over ranged GETs, and
  the sidecar written only after both pass. What remains is flat over time, so a
  14-day window covers 14 days of a risk that runs for years.
- **"Time for a human to notice."** Notice by what mechanism? Nothing schedules
  the looking. The per-artifact read-path verification runs at delete time on
  every object, and a defect that passes it does not become visible by waiting.

The precedent points the same way. The frame-boundary bug (2026-08-14) was found
by **measuring the results**, not by waiting, and verification had passed
17,291/17,291 because it was a compression-efficiency defect rather than a
correctness one. No source object was ever at risk from it.

Nor is there a failure mode that (a) passes per-member verification, (b) loses
data when the source goes, and (c) surfaces in days rather than instantly. An
object in no sidecar is never deleted — safe by construction. A key-derivation
bug that verified A and deleted B would destroy the sources the moment it ran,
and a timer would not help; the dry-run, the per-run cap and inspecting the
first run by hand are what catch that.

**What replaces the timer is a gate that gets satisfied rather than expires:**
Stage 3 deployed and `scripts/verify_pack_read_path.py` clean against real
April packs while every source still exists. That is already on this plan's
definition of done. A clock is not.

The bucket is un-versioned (verified 2026-08-10), so a delete is immediate and
there is no undo. What makes that acceptable is not a waiting period — it is
that the bytes are already inside a pack that was verified, stored, and re-read
from storage before anything was removed.

##### Recent months, and why they are not a grace-period problem

Age does not make an artifact riskier to delete. Being **live** does — still
queued for a first parse, still being retried, still likely to be read by
something running now. That is a property of the *capture month*, not of how
old the pack is, which is what made pack age the wrong anchor in the first
place.

The plan already expresses it: **month closure plus `PACK_SETTLE_DAYS`** is
exactly "do not touch a month that might still be moving", and a month only
becomes packable once it has passed that. July inherits it automatically when
it is packed. Even then it is soft, because Stage 3 means a pending artifact is
still readable and parseable after its object is gone — the concern is not
surprising the live pipeline, not losing anything.

So April, May and June are deleted by naming them explicitly: dry-run first,
capped, verified per object, no timer.

#### The "has been processed" check — PROPOSED 2026-08-14, needs agreement before Stage 4 is built

The plan requires a processed check. **Stage 3 changes what it is for**, so it
is worth stating what it protects against before deciding how strict it is.

The check exists so an artifact is not deleted before it can be parsed. Once
Stage 3 is live, **a packed artifact is still readable and still parseable
after its source object is gone** — that is the whole point of Stage 3, and
Plan 132's reparse is the concrete case. So the check stops being load-bearing
and becomes belt-and-braces.

The proposal, in one sentence:

> **Delete a source object when, and only when, the Stage 3 read path returns
> its exact bytes. Processing status is reported, never a veto.**

Concretely, per candidate object:

| gate | rule |
|---|---|
| sidecar entry exists | **required.** No entry, no delete — Stage 4 never deletes what it cannot name. |
| read-path verification | **required.** `read_html` returns bytes whose sha256 matches the sidecar's `raw_sha256`, through the production path, for that artifact. |
| delete grace period | **`PACK_DELETE_GRACE_DAYS`, default 0.** The knob exists; the wait does not — see [above](#the-delete-grace-period--0-days-revised-2026-08-14). |
| pack has a sidecar (not an orphan) | **required.** Interrupted runs are reported and never deleted from, exactly as Stage 2 does. |
| processing status | **reported, not required.** Counted and logged per class; no class blocks a delete. |

Why status cannot be a gate, from the measurements:

- **42,276 April objects have no `artifacts_queue_events` row at all**, so they
  can never satisfy any status predicate. A gating check strands ~95K inodes
  in the one fully packed month, forever, and it strands them for the artifacts
  Plan 132 exists to recover — which will read them *out of the packs*.
- **`ok` is success** (n8n era, 19,950 April artifacts), and treating it as
  anything else is the single most expensive mistake available here.
- **April's terminal statuses do not reconcile**: 445,796 `complete` + 19,950
  `ok` = 465,746 against 457,084 artifacts holding a silver observation. A
  delete predicate built on that arithmetic is built on a discrepancy nobody
  has explained.
- The historical record is `ops_normalized/artifacts_queue_events` in the lake
  (timestamp column `event_at`), not `ops.artifacts_queue`, which is a hot
  table pruned by `cleanup_queue` and held ~400 rows from the preceding 40
  seconds when it was measured.

**What the reporting is for.** Every run breaks its deletions down by terminal
status — `complete`, `ok`, `skip`, `retry`, `pending`, `processing`, `failed`,
and `no_event_row` — so an unexpected shape is visible immediately. A run that
suddenly deletes tens of thousands of `pending` objects is a signal worth
having, and the dry-run default plus the per-run cap keep it a small,
inspectable signal rather than a large irreversible one — which is the job a
waiting period was proposed for and could not actually do. The `no_event_row`
count is the *"packed and verified, no provenance"* branch the sequencing
constraint asks for: it is a distinct, reported outcome rather than a silent
fall-through.

**The alternative, and why it is not proposed.** A hard gate on
`status IN ('complete','ok')` would refuse the 42,276 orphans forever, and
would additionally need the event lake joined for every candidate — a DuckDB
scan per batch, on a job whose whole purpose is to be small and boring. If the
gate is wanted anyway, the honest version is *"delete only artifacts with a
terminal status, plus the explicitly enumerated no-provenance population"*,
which is the same delete set reached by a more expensive route.

**This section is a proposal. Stage 4 is not implemented until it is agreed
or amended.**

---

## Stage 4 as built

Built 2026-08-14. **Never run.** Dry-run by default, and its first apply run is
gated on Stage 3 being deployed and verified — see the
[run sheet](runbook_plan_131_stage_3_4.md).

### Three checks per member, none optional

| check | what it proves |
|---|---|
| **Resolvable** — `pack_lookup_prefix(source_key)` names the prefix the pack actually lives under | a reader could still *find* it once the object is gone |
| **Extractable** — pulled from the *stored* pack over ranged GETs, sha256 equals the sidecar's `raw_sha256` | the pack holds what its index claims |
| **Identical** — the source object is read and compared byte-for-byte | the pack holds what *this object* holds |

The third is the one that makes deletion safe rather than merely consistent: a
pack that agrees with its own index but not with the object would pass the first
two. Per *pack*, a bounded sample (25 by default) additionally goes through
`read_packed_html` end to end, exercising the real resolver — prefix derivation,
sidecar listing, index lookup — rather than inferring it from the per-member
checks. Doing that for every member would rescan every earlier sidecar in the
month for every artifact.

**Verification deliberately does not call `read_html`.** With Stage 3 live,
`read_html` answers from the pack once an object is missing, so using it would
compare the pack against itself and always agree. There is a test asserting it
is never called.

### What the implementation settled

- **The surviving-object listing is the checkpoint.** An object that is gone
  has already been deleted, so a resumed run skips it with no request and each
  object is deleted at most once. No state file, and the O(n²) shape of Plan
  129's first checkpoint (`f98e69b`) is not reachable from this design. A
  fully drained month costs one listing and nothing else — without that
  short-circuit a re-run read all 32 April sidecars to discover there was
  nothing to do.
- **`year`/`month` are required; there is no discovery mode.** The packer can
  discover what is eligible because packing is additive. This cannot, because
  it is not.
- **Both caps mean "no cap" at `<= 0`** — aligned 2026-08-14, and they did not
  start that way. `max_packs = 0` was always unlimited; `max_objects = 0` set
  `budget = 0`, tripped `capped = True`, deleted nothing, and **reported
  success**. Two adjacent caps on the same call with opposite meanings, and the
  wrong one failed silently. It went unnoticed because every run so far named a
  positive cap by hand; Stage 5's DAG is the first caller that has to pass an
  uncapped delete, and the listing being the fixed cost (below) means uncapped
  is how it has to run.
- **The free-space floor does not apply.** The packer refuses to start below
  one because MinIO rejects every `PutObject` below its minimum-free-drive
  threshold. A DELETE is not a PutObject and still succeeds on a full drive,
  which is exactly what makes this job the recovery lever rather than another
  casualty. Free space and inodes are reported, never gated.
- **Inodes are reported two ways.** The estimate is what this run freed
  (deleted objects × ~2.24, Stage 0a); the measured delta is what the
  filesystem shows and moves with everything else on it. A reading, not a
  proof — but the plan exists for this number, so a summary that omitted it
  would be measuring the wrong thing.
- **A sidecar that disagrees with its pack blocks that whole pack**, not just
  the member that exposed it. Neither is trustworthy as evidence about the
  other.
- **An orphan pack is never deleted from.** Stage 2 reports and never deletes
  them; an unverified pack is not evidence that anything is safe to remove.

### First production run — measured 2026-08-14

April 2026, `--max-objects 100 --max-packs 1`, dry run then apply. **The first
inodes this plan has ever freed.**

| | dry run | apply |
|---|---|---|
| objects considered / verified | 100 / 100 | 100 / 100 |
| **refused** | **0** | **0** |
| deleted | 0 | **100** |
| bytes freed | 0 | **1,423,143** |
| inodes freed, estimated | 0 | **224** |
| inodes freed, measured | −590 | +63 |
| `by_status` | `complete: 97, ok: 3` | same |
| `orphan_packs` | `[]` | `[]` |
| `objects_surviving_before` | 557,065 | 557,065 |

`objects_surviving_before` matching the Stage 2 census exactly (557,065) is the
first independent confirmation that the deleter and the packer are looking at
the same universe.

**`ok` appears in real data**, three of the first hundred — the n8n-era success
status that a naive processing gate would have had an opinion about, deleted
like any other member under the agreed policy.

**No `no_event_row` in this sample, and that is correct**: null-`artifact_id`
members sort last and land in packs 00026-00031, so pack-00000 is entirely
named artifacts. The no-provenance branch is exercised at the end of the month,
not the start.

#### Readback after deletion — the property the plan exists for

Verified immediately afterwards, deriving keys from the sidecar rather than
trusting the run's own account of itself:

```
100 of the first 150 members have no source object left
  c116a381-...  824188 bytes  sha256_match=True
  0d1edf51-...  211152 bytes  sha256_match=True
  ff3a7ae8-...  212153 bytes  sha256_match=True
  fadae602-...  599792 bytes  sha256_match=True
  1bd534b5-...  600387 bytes  sha256_match=True
```

**Exactly 100 of the first 150 are gone**, which is an independent confirmation
that the per-run cap is honoured to the object and that the deleter walks
members in frame order. Every sampled read returned bytes matching the
sidecar's `raw_sha256` — from artifacts that no longer exist anywhere except
inside a pack.

#### The measured inode delta is below its own noise floor at this size

−590 on a run that deleted nothing, +63 on a run that freed an estimated 224.
Both readings span ~12 minutes of wall clock — dominated by the 700-800 s
listing — during which the scraper kept creating objects at ~65,500/day. The
figure is a reading of the whole volume, not of this job, exactly as the
summary claims; at 100 objects the signal is smaller than the concurrent
churn. **It only becomes meaningful at month scale**, where April's ~1.248M
dwarfs anything else moving.

#### The listing is the fixed cost, and it decides how to run this

701-809 s to enumerate 557,065 objects, **paid on every run regardless of the
cap**. Draining April one pack at a time would be 32 listings — roughly seven
hours of pure enumeration to do about an hour of work. One run at
`--max-packs 0` pays it once.

#### `sample_full_reads` collapses under a small cap

The run logged **one** full-resolver read, not 25. `_sample_positions` spreads
its samples across the whole pack — for 17,291 members that is positions 0,
720, 1441, ... — while the per-run budget stops at 100, so only position 0 is
ever reached. A capped run therefore carries a fraction of the end-to-end
resolver evidence it reports.

It does not weaken this run: the three load-bearing checks (resolvable,
extractable, identical-to-object) ran on all 100, and Stage 3 already put 365
members through the full resolver. It also self-corrects once the budget
exceeds the pack size. **It matters only for small capped runs, which is
precisely the first-run posture**, so the sample should be computed against
`min(len(pending), remaining_budget)`.

### Testing

26 tests over an in-memory object store — real packs, real ranged GETs, real
verification. The ones that matter are the refusals: no sidecar entry, orphan
pack, sidecar/pack disagreement, sha256 mismatch, an object whose bytes differ
from the pack, and objects living where the production resolver would not look.
Plus: `--apply` absent deletes nothing, the per-run cap is honoured exactly,
resume deletes each object at most once, `ok` and `pending` are deleted like
any other status, and every deleted object is asserted still readable through
`read_html` afterwards.

---

### Stage 5 — Lifecycle DAG and observability — BUILT 2026-08-17, in production testing

- Airflow DAG packing eligible cohorts on a schedule, respecting the free-space
  floor, thin like `compact_silver` (sensors + one HTTP call, no logic).
- Metrics: objects packed, packs written, bytes/inodes reclaimed, extraction
  latency p50/p95, verification failures (should be zero; alert on any).

#### Why Stage 5 is the point of the plan, not its tidy-up

Stages 1-4 buy a **one-time** reclamation. April through July is ~3.65M objects
— about 90% of everything in bronze — worth ~8.2M inodes, or ~125 days at the
measured burn. That is a bigger deadline, not the absence of one, and framing
this plan as "how many days does it buy" undersells what it actually does.

**Run continuously, packing changes the shape of the problem.** A closed month
collapses from ~1.1M objects to about **82 objects** — 41 packs plus 41
sidecars, which is May's measured shape. At ~2.24 inodes each that is ~185
inodes per month of permanent cold storage, or roughly **2,200 inodes a year**
against a 13.1M total.

The only meaningful consumer left is the hot window: the current month plus
`PACK_SETTLE_DAYS`, peaking around **2.5M inodes** before it is packed and
pruned. Against the ~12.2M free that the April-July cycle leaves behind, that
is not years of headroom — **inodes stop being the binding constraint at all.**

#### The constraint moves to bytes, and moves out about three years

Estimated 2026-08-14, measured inputs marked:

| | |
|---|---|
| free bytes now *(measured, archiver)* | 82.3 GiB |
| + source bytes returned by pruning Apr-Jul | ~24 GiB |
| − packs still to write for Jun + Jul | ~4.6 GiB |
| **free after the full cycle** | **~102 GiB** |
| reserve: hot month + transient pack space | ~15 GiB |
| **usable for cold growth** | **~87 GiB** |
| packed cold storage per month | **~2.4 GiB** |
| **full-retention runway** | **~36 months** |

So roughly **2.5-3.5 years of retaining every page ever scraped**, on the
current 196 GB disk at the current scrape rate. Measured inputs are April's
4.27 GiB of sources against 1.99 GiB packed, and May's 6,978 stored bytes per
object against ~2,410 packed bytes per member. June and July are inferred from
May's shape and should be *better* than April, which carries Plan 132's
null-metadata tail.

#### Three consequences worth stating

- **[Plan 130](plan_130_parser_input_projection.md) stays unnecessary.** It is
  the only irreversible lever in this arc — discarding HTML content — and it is
  blocked until the reversible options are exhausted. A three-year runway means
  they are not exhausted for a long time.
- **[Plan 113](plan_113_production_adaptive_refresh.md) becomes an efficiency
  play, not a rescue.** Adaptive refresh cuts object *creation*, so it extends
  both the hot window and the per-month packed cost. Valuable, no longer
  urgent.
- **The cheapest remaining lever is not software.** At ~2.4 GiB/month, another
  200 GB of disk buys roughly seven more years. Worth remembering before anyone
  proposes a cleverer storage format.

Two things would move these numbers: a change in scrape volume — all of it
assumes ~1.1M objects/month — and whether June and July pack like May or like
April.

---

## Stage 5 as built

Built 2026-08-17. **In production testing; no scheduled run has completed yet.**
The first is 2026-09-03.

### The DAG holds no logic

`airflow/dags/pack_bronze_html.py` is two sensors, three HTTP calls and a
notifier. Everything it knows how to do lives behind an endpoint on
`pack-worker`, which is the same shape `compact_silver` and
`export_ci_lake_snapshot` already have.

```
deploy_intent_sensor >> pack_worker_health >> pack >> prune >> verify
[every task] >> notify            # trigger_rule="one_failed"
```

Each task pushes its summary to XCom under `result` **including on failure** —
`JsonPostError` carries the endpoint's summary through, so `notify` can quote
the actual `failure_reason` rather than an HTTP status. That is why the pack
endpoints had to signal failure properly (D5) before the DAG could be thin: a
DAG that has to infer failure from a 200 is a DAG that grows logic.

### What the implementation settled

- **The prune target comes from the pack task, not from the schedule.** `_run_prune`
  pulls `check_pack_result`'s selected bucket out of XCom; a pack that selected
  nothing yields `{"skipped": true}` and the prune never runs. This is D1's
  "never automate the pack without the prune" in the only form that is safe —
  the prune cannot address a month the pack did not just finish.
- **The canary is bounded and sits last.** `verify` gets `retries=1` where pack
  and prune get `retries=6`; it is a read-only sample, not a job worth grinding
  on. It is also deliberately **outside** the single-flight lock — the moment it
  most needs to run is the moment a pack job is in flight.
- **Retries are sized for the deploy pause, not for flakiness.** `retries=6` at
  15-minute intervals is 90 minutes of runway, which is what a deploy needs to
  land and clear. A `stopped_for_deploy` return is an ordinary intermediate
  failure and does not page; only exhausting the retries does.
- **`timeout=43200`** on pack and prune. A backlog month is a ten-hour call, and
  both jobs are resumable — sidecars checkpoint the packer, the surviving-object
  listing checkpoints the deleter — so a retry re-enters cheaply.
- **The schedule is steady-state only.** One month per run, `catchup=False`,
  `max_active_runs=1`. July and any other backlog stays a deliberate manual
  trigger with explicit params; the DAG exposes them precisely so that works.
- **Single-flight is in-process and per-job-name.** A `docker exec` CLI run is
  invisible to it and it to the CLI, so the run sheet's rule — do not hand-run a
  month the schedule might also take — is load-bearing rather than advisory.
- **`long_jobs_paused()` fails open with no staleness clause.** A Postgres blip
  must not stop a ten-hour job; a *forgotten* deploy intent keeps jobs paused
  until the retries exhaust and someone is paged. Both are the designed outcome.

### D3b validated in production, 2026-08-18

The deploy-intent pause had never been exercised against a live long job. It was
on 2026-08-18, deliberately, using a real deploy (Plan 135 Stage 4) against a
real multi-hour July prune rather than a synthetic test.

| | |
|---|---|
| Job | `delete_packed_source_html`, July `detail_page`, `apply`, started 23:50 UTC |
| Stopped at | **`pack-00014` boundary**, 01:10:19 UTC |
| Durable at stop | **15 packs, 482,000 deletions** |
| Log | `stopping for a deploy after 482000 deletion(s); resume is a re-run` |
| Task state | `up_for_retry` — not failed |
| Auto-resume | **confirmed** — retry re-entered and continued once intent cleared |
| Collateral DAG failures | **none** |

Every part of the design held: it stopped at a boundary it already had, nothing
in flight was lost, the completed work stayed durable, and the retry resumed it
without anyone re-issuing the job.

**The operational constraint the test exposed.** Every DAG opens with
`deploy_intent_sensor()`, whose `poke()` returns True only when
`intent = 'none'`, at **`timeout=600`**. So a deploy-intent window longer than
**10 minutes** fails the first task of every scheduled DAG — and with
`orphan_checker` and `results_processing` on `*/5`, that is a burst of failures
and pages. The window here stayed short because the sequence put the slow work
outside it:

1. `docker compose build` **before** declaring intent — it touches nothing running
2. declare intent
3. wait for the pack boundary (bounded by one pack, ~5 min at July's sizes)
4. `docker compose up -d`
5. release intent immediately, and confirm `intent: none`

Verified afterwards: the most recent failed runs for `orphan_checker`,
`results_processing`, `scrape_detail_pages` and `scrape_listings` were all from
**July**, and the scheduler logged **zero** `check_deploy_intent` timeouts.
**Build before intent, not during it** is the rule worth carrying.

**One diagnostic caution.** `airflow dags list-runs pack_bronze_html` returned an
empty table while a manual run was live and visible in the UI. Do not conclude
from that CLI output alone that a DAG has no active run — check the UI or the
metadata DB before deciding a job is unmanaged.

### The July run, across two attempts — 2026-08-18

Counters are **per attempt**, not per month. A resumed run starts its own
counter at zero, so reading the live log alone understates the month. July, in
full:

| | attempt 1 | attempt 2 |
|---|---|---|
| Started | 23:50 | 01:25:20 |
| Ended | 01:10:19, stopped for deploy at `pack-00014` | still running |
| Packs | 15 | walked `pack-00000` → `pack-00018` |
| Deleted | **482,000** | **129,000** and climbing |

Surviving-object listing at attempt 2's start: **427,000 objects in 655s**. So
July held roughly **909,000** objects when the second attempt began listing
(482,000 already gone + 427,000 still present), and total deletions stand at
about **611,000** with ~298,000 to go.

**Nothing is deleted twice, and the re-walk is not wasted work.** Attempt 2
covers `pack-00000` onward because the surviving-object listing *is* the
checkpoint — already-drained packs simply contribute nothing to it. What the
resume genuinely re-pays is the listing: **11 minutes**, matching the ~12
minutes this plan recorded for April. That is the fixed cost the design
accepted in exchange for holding no state file, and July is the first
measurement of it on a month this size.

### The apiserver outage, and what it proved about the shape — 2026-08-18

Unplanned, and more informative than the deliberate D3b test. At **01:31** the
Airflow apiserver wedged: its SQLAlchemy pool exhausted
(`QueuePool limit of size 5 overflow 10 reached`), leaving it accepting TCP and
answering nothing. It was restarted at **01:45:55**.

Seven scheduled DAG runs died in that window — `orphan_checker` ×3,
`results_processing` ×3, `scrape_detail_pages` — all `upstream_failed` at their
sensors, plus a `scrape_listings` run. **The prune did not notice.** It had
started at 01:25:20, ran straight through the outage *and* the restart, and was
still deleting afterwards.

That is Stage 5's architecture paying off in a way nothing planned for:

- **The DAG holds no logic**, so there was nothing in Airflow to lose. The task
  is one blocking HTTP call.
- **The work lives in `pack-worker`**, a container the outage never touched.
- **The endpoint is a sync `def`**, so FastAPI runs it in a threadpool, and a
  threadpool handler is not cancelled when its client goes away. Even had the
  task process died, the prune would have run to completion.

The decision that made this true was made for testability — *"the DAG holds
sensors, one HTTP call per task, and the result predicates. It holds no logic
and shells out to nothing."* Surviving a control-plane outage was not the
argument for it, and is the better one.

**The sharp edge this exposes, which did not fire.** Had the task instance
died, its retry would have POSTed again while the original run was still going
and hit the D3a single-flight guard — **409**, surfaced as `JsonPostError`, a
failed retry. At `retries=6` × 15 min that is 90 minutes of runway; a month-scale
prune outliving it would exhaust the retries, fail the DAG and page, **while the
work completed normally in the worker.** A spurious failure, not a real one.
Confirm from the worker's summary and `df -i` before treating such a page as
data loss.

**Not this plan's defect, but this plan's traffic.** The pool is Airflow's stock
`5 + 10` and was never tuned, while `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`
routes every task's state through that apiserver. It held for four weeks and
wedged the day after two DAGs were added — `pack_bronze_html` among them. Sizing
belongs to whoever owns the Airflow deployment, but Stage 5 added load to the
component that broke.

### Where the numbers come from

Stage 5 asks for objects packed, packs written, bytes/inodes reclaimed,
extraction latency p50/p95, and verification failures. None of it is a new
metrics pipeline:

| number | source |
|---|---|
| objects packed, packs written, bytes/inodes freed | the run summary each endpoint returns, in the task log and XCom |
| extraction latency p50/p95 | the `verify` canary's summary, every run |
| verification failures | `REFUSED` at ERROR from `delete_packed_source_html`, shipped to Loki, alerted on |
| bronze object count over time | `minio_bucket_usage_object_total`, already scraped |

**Per-run Prometheus gauges are deliberately not built.** A gauge resets to 0 on
restart, and for a monthly job a zero is indistinguishable from a good run, so
honest gauges need a `last_run_timestamp` and a staleness alert alongside them.
[Plan 136](plan_136_solver_recycle_and_liveness.md) Stage 1 is building exactly
that convention — a freshness timestamp plus NaN-on-failure — for the DuckDB
gauges that have the same defect. When it lands, these numbers can adopt it.
Inventing a second staleness pattern here first would be the wrong order.

### The alert scope, which the original spec got wrong

The Stage 5 prompt specified the verification alert as `{service="archiver"}`.
That would never fire. `ARCHIVER_ALLOW_PACK_JOBS` is set only on `pack-worker`
([docker-compose.yml](../docker-compose.yml)), and the archiver returns **409**
for pack endpoints, so every scheduled run logs under `service="pack-worker"`
([promtail.yml](../promtail/promtail.yml)). The spec predates Step 4 creating the
worker.

`ct-pack-verification-refused` matches `{service=~"archiver|pack-worker"}` —
both, because the 409's own message points at `ARCHIVER_ALLOW_PACK_JOBS=true`
as the manual-run override, so the archiver can still legitimately run one.
It is `gt 0` with `for: 0s` rather than the spike thresholds the file's other
two Loki rules use, because "should be zero; alert on any" is the requirement.

A related assumption also proved wrong: the prompt expected
`tests/test_observability_config.py` to cover new rules automatically. It
checks an explicit UID allowlist, so a new rule is invisible to it until the
UID is added. It is added, along with assertions on the service selector and
the threshold — the two things whose silent regression would make the alert
useless rather than noisy.

### The alert, validated in production — 2026-08-17

An alert that has never matched anything is not an alert that works. This one
was proven in both directions against live data before it was trusted, using
Loki's query API on the published port 3100 (Grafana itself is not
port-published — it is reachable only through Caddy).

| step | query | result |
|---|---|---|
| selector matches live streams | `sum by (service) (count_over_time({service=~"archiver\|pack-worker"}[10m]))` | `archiver` **14**, `pack-worker` **4** |
| filter rejects normal traffic | the same, plus `\|= "REFUSED"` | `[]` — **81 lines in, `totalPostFilterLines: 0`** |
| a real REFUSED fires | the same, after pushing one synthetic line | `pack-worker` **1** — 70 lines in, **1** post-filter |

The middle row is the one that matters and is the row usually skipped. An empty
result proves nothing on its own — an empty result against a *stream that is
demonstrably carrying 81 lines* proves the filter is discriminating rather than
the query being dead. Same corpus, same selector, one term added, everything
rejected.

The first row also settled the both-services question empirically: the archiver
was actively logging at the time, so an archiver-scoped `REFUSED` had a live
stream to appear in. A worker-only matcher would have been a silent gap.

The synthetic line then carried the whole chain through Grafana's evaluation and
the notification policy to Telegram.

> **Two synthetic `REFUSED` lines are permanently in Loki**, pushed 2026-08-17
> during this validation. Loki has no retention configured — that is
> [Plan 135](plan_135_storage_observability.md) Stage 5b, still outstanding —
> and the delete API needs a compactor with `delete_request_store`, which is not
> set up either. Both carry the label `synthetic="alert-test"` and the text
> `SYNTHETIC ALERT TEST` with a timestamp. **A future grep for `REFUSED` that
> finds them has not found a verification failure.** A real one comes from
> `delete_packed_source_html` and names an artifact.

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
| `shared/minio.py` | `read_html` falls back to pack lookup, self-verifying | 3 | **done** |
| `tests/shared/test_minio_packfallback.py` | New — 27 tests over an in-memory store | 3 | **done** |
| `tests/integration/shared/test_read_html_pack_fallback.py` | New — packed and unpacked both readable, real MinIO | 3 | **done** |
| `archiver/processors/verify_pack_read_path.py` | Read-path proof + latency p50/p95, read-only | 3/5 | **done**, prior CLI path verified in production |
| `tests/archiver/test_verify_pack_read_path.py` | New, then moved with the processor | 3/5 | **done** |
| `scripts/audit_semantic_duplicate_html_hashes.py` | Bypass removed: hashes raw HTML via `read_html` | 3 | **done** |
| `scripts/diff_log_analysis.py` | Bypass removed: `object_size()` HEAD, `None` = packed | 3 | **done** |
| `archiver/processors/delete_packed_source_html.py` | Stage 4, dry-run default, hard cap, per-artifact verification; then `max_objects <= 0` = no cap and a deploy-intent boundary check | 4, 5 | **done**, April-June pruned |
| `archiver/app.py` | `POST /pack/bronze/prune` | 4 | **done** |
| `tests/archiver/test_delete_packed_source_html.py` | New — 26 tests over an in-memory store; uncapped drain and deploy-intent stop | 4, 5 | **done** |
| `docs/runbook_plan_131_stage_3_4.md` | Deploy + verification + prune run sheet; then the Stage 5 close | 3-5 | **done** |
| `shared/job_counter.py` | Named, per-job single-flight lock (D3a) | 5 | **done** |
| `db/migrations/V042__deploy_intent_pause_long_jobs.sql` | New — `pause_long_jobs` column + `scraper_user` grant | 5 | **done** |
| `shared/deploy_intent.py` | New — `long_jobs_paused()`, fails open (D3b) | 5 | **done** |
| `ops/routers/deploy.py` | `pause_long_jobs` on `/deploy/start`, surfaced in status | 5 | **done** |
| `archiver/processors/pack_bronze_html.py` | Deploy-intent boundary check, skips the tail flush | 5 | **done** |
| `archiver/app.py` | `_failure_reason` → 500 on the pack endpoints (D5); 409 unless `ARCHIVER_ALLOW_PACK_JOBS`; `POST /pack/bronze/verify` | 5 | **done** |
| `docker-compose.yml` | `pack-worker` service, `pack_worker_logs` volume, promtail mount | 5 | **done** |
| `promtail/promtail.yml` | `pack-worker` log job — where every scheduled run logs | 5 | **done** |
| `scripts/verify_pack_read_path.py` | Deleted — moved to `archiver/processors/` | 5 | **done** |
| `airflow/dags/pack_bronze_html.py` | Lifecycle DAG — sensors, three HTTP calls, notify | 5 | **done**, no scheduled run yet |
| `tests/airflow/test_pack_bronze_html_dag.py` | New — result predicates (D5) without Airflow | 5 | **done** |
| `tests/test_pack_worker_compose_config.py` | New — worker isolation, archiver unaffected | 5 | **done** |
| `tests/integration/airflow/test_dag_integrity.py` | Register the new DAG | 5 | **done** |
| `grafana/provisioning/alerting/rules.yml` | `ct-pack-verification-refused` — `REFUSED` on `{service=~"archiver\|pack-worker"}`, `gt 0`, `for: 0s` | 5 | **done** |
| `tests/test_observability_config.py` | Register the new UID; assert the worker selector and the alert-on-any threshold | 5 | **done** |
| Inode alerts + filesystem/inode panels | **Moved to [Plan 135](plan_135_storage_observability.md)** — blocked on its Stage 1 node-exporter fix | 5 → 135 | see Plan 135 |

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
| Lifecycle is single-flight | **Met** — per-job lock, 409 on the second caller |
| Lifecycle is deploy-aware | **Met** — validated in production 2026-08-18: stopped at a pack boundary with 482,000 deletions durable, retried, resumed |
| Lifecycle survives a control-plane outage | **Met, unplanned** — ran through a 15-min Airflow apiserver wedge and its restart, 2026-08-18 |
| Lifecycle is measured | Run summaries + the canary's p50/p95, every run; **first scheduled run 2026-09-03** |
| Lifecycle is alertable | **Met** — `ct-pack-verification-refused` fires on any occurrence; inode alerting is [Plan 135](plan_135_storage_observability.md) Stage 3 |

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
