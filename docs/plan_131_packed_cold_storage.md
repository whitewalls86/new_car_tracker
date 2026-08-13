# Plan 131: Packed Cold Storage for Bronze HTML

## Status

**Draft. Nothing measured yet.** Stage 0 is a measurement gate and it can fail.

Comes out of [Plan 129](plan_129_zstd_dictionary_compression.md), whose Out of
Scope section names "packing multiple artifacts per object" as the deferred
lever, and out of [Plan 114](plan_114_sectioned_html_artifact_audit.md), which
established that MinIO's ~8 KB/object floor is the cost that decides every
storage question in this bucket.

**This plan is lossless and reversible.** Packs contain the exact compressed
bytes of the artifacts they replace, extraction is verified byte-for-byte before
any source object is deleted, and nothing is discarded. That is the whole reason
it is worth doing before [Plan 130](plan_130_parser_input_projection.md), which
is not.

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

Measured on the VM 2026-08-08 (Plan 114 Stage 3, "Storage Accounting"):

| Measure | Value |
|---|---|
| Objects in `bronze` | 3,918,760 |
| Inodes used on `/mnt/data` | 8,774,058 of 13,107,200 |
| Inodes per object | ~2.24 (directory + `xl.meta`) |
| Headroom | **4,333,142 inodes** |

Plan 129 records bronze growing at **~1M objects/month** with no lifecycle rule
and no HTML deletion anywhere in the codebase. At 2.24 inodes per object that is
~2.24M inodes/month against 4.33M of headroom.

> **Derived, not measured: that is under two months of headroom from 2026-08-08.**
> The arithmetic is straightforward but the inputs are a single snapshot taken
> while diagnosing an unrelated full disk. **Stage 0 re-reads `df -i /mnt/data`
> before anything else in this plan is taken seriously.** If the real figure is
> comfortable, this plan drops to normal priority. If it is not, packing and
> retention are the only two levers that move it, and compression is not on the
> list.

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

### Pack by listing, not by section type or by time

This is the one design decision the existing measurements already settle. Plan
114 Stage 3, whole-section hash reuse:

| Axis | Reuse |
|---|---|
| Within a listing, across captures | **30.42%** |
| Across listings (additional) | **0.65%** |

Grouping like content across *different* listings targets the 0.65%, and the
trained dictionary already captures most of that class of redundancy. The 30%
lives in repeat captures of the **same vehicle**, so pack members are ordered
`listing_id, fetched_at` and a listing's captures are always adjacent inside one
frame.

It matches the read pattern too: reprocessing after a parser fix wants every
capture of the affected listings, which becomes one pack read instead of N
object GETs.

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

### Hot/cold boundary

Only months older than `PACK_MIN_AGE_DAYS` (start at 60) are eligible. The
scraper write path is **unchanged** — new artifacts land as individual objects
exactly as today. Packing is a background lifecycle job over settled data.

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
"under two months" figure above is extrapolated from one snapshot and drives the
whole priority of this plan.

**0b. Price the cheaper alternative first.** Plan 129 note 6 found `results_page`
is a separate population — raw mean **706 KB** vs 158 KB for detail, stored
44.2 KB vs 28.1 KB — and Plan 114 put SRP reprocessing out of scope, making
results pages *"the better deletion candidate if space is ever needed
urgently."*

Report the object-count and inode split between `detail_page` and
`results_page`. **If a results-page retention policy reclaims comparable inodes
for a fraction of the effort, it wins and this plan waits.** An archive format
is not the answer to a problem a `DELETE` solves.

**0c. Measure the pack win** on a real sample, offline, writing nothing to
production. Extend the Plan 129 measurement harness rather than starting fresh;
`scripts/estimate_dictionary_savings.py` already samples the corpus correctly via
`fetch_corpus_sample` (evenly across months, deterministic, **not** the
duplicate-biased `fetch_sample` that cost Plan 114 a re-run).

Report, per configuration:

| Variable | Values to sweep |
|---|---|
| Member ordering | `listing_id, fetched_at` vs arrival order |
| Pages per frame | 100 / 1,000 / 10,000 |
| Dictionary | with `1367127621` / without |

and for each, the three numbers that matter:

| Metric | Why |
|---|---|
| Bytes per page in-pack | the window win, isolated |
| **Physical bytes per page, floor applied** | the only one you can spend |
| **Objects and inodes per 1M artifacts** | the actual constraint |

Also report single-artifact extraction latency at each frame size. "Slow" needs
a number attached before it can be accepted.

**Gate:** ≥50% *physical* reduction against the current post-dictionary state,
**and** ≥20× object-count reduction, **and** 0b did not produce a cheaper answer.
Below any of those, stop and write down why.

### Stage 1 — Format, writer, reader

- `shared/packfile.py`: write a pack + sidecar index; read a member by
  `artifact_id`; in-process LRU of decompressed frames.
- Round-trip is asserted per member on write; a pack whose members do not
  extract byte-identically is never finalized.
- Format version in the header from day one. Packs are immutable and will
  outlive this plan's assumptions.
- Offline only. Nothing in production reads or writes packs at this stage.

### Stage 2 — Pack one cold month, delete nothing

- `scripts/pack_bronze_html.py`, `--apply` required, checkpointed, resumable
  (Plan 129's backfill checkpoint was O(n²) once — see commit `f98e69b` — do not
  reintroduce that shape).
- Write packs alongside the source objects. **Delete nothing.**
- Verify **100%** of members extract byte-identically against `raw_sha256`.
- Run in the `processing` container, not over an SSH tunnel — Plan 129 measured
  that at ~8× throughput. Note `ps` is not installed in that image; liveness-check
  the checkpoint file.

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

Only for artifacts that have a finalized pack, a verified `raw_sha256` match, and
are past `PACK_MIN_AGE_DAYS`. Dry-run by default, hard per-run cap, one pack's
worth of sources at a time.

**This is the step that frees inodes**, and it is the first irreversible-ish one
— though the bytes still exist inside the pack, which is the entire difference
between this plan and Plan 130.

### Stage 5 — Lifecycle DAG and observability

- Airflow DAG packing eligible months on a schedule, respecting the free-space
  floor.
- Metrics: objects packed, packs written, bytes/inodes reclaimed, extraction
  latency p50/p95, verification failures (should be zero; alert on any).

---

## Prior art

This is a solved problem and the plan should borrow rather than invent:

- **WARC** (ISO 28500) + a **CDX** index — the web-archiving standard for
  exactly this: many captures, one container, external index. Stage 1 should
  check whether WARC + `warcio` gets there off the shelf before a bespoke format
  is written. The argument against is that our members are already
  dictionary-compressed zstd frames and WARC would add a second envelope; that
  argument needs testing, not assuming.
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

### `tests/scripts/test_pack_bronze_html.py`
- Checkpoint resume packs each artifact exactly once, and is not O(n²).
- Refuses to run below the free-space floor.
- `--apply` absent deletes and writes nothing.

### Integration (`tests/integration/shared/`)
- Pack to a test prefix, read back through the production `read_html` path,
  assert equality for both packed and unpacked artifacts.
- Both frame types still readable: no-dictionary, dictionary, and packed.

---

## Files Changed

| File | Change |
|------|--------|
| `shared/packfile.py` | New: pack format, writer, indexed reader, frame cache |
| `shared/minio.py` | `read_html` falls back to pack lookup |
| `scripts/estimate_pack_savings.py` | New: Stage 0 measurement |
| `scripts/pack_bronze_html.py` | New: Stage 2 packer, `--apply`, checkpointed |
| `scripts/delete_packed_source_html.py` | New: Stage 4, dry-run by default |
| `airflow/dags/pack_bronze_html.py` | New: Stage 5 lifecycle DAG |
| `tests/shared/test_packfile.py` | New |
| `tests/scripts/test_pack_bronze_html.py` | New |

---

## Success Criteria

| Metric | Gate |
|--------|------|
| Stage 0 physical reduction vs post-dictionary state | ≥50% |
| Stage 0 object-count reduction | ≥20× |
| Cheaper alternative (results-page retention) priced first | Reported, explicitly |
| Byte-identical extraction before any source delete | **100%, blocking** |
| Single-artifact extraction latency | Measured and accepted, not assumed |
| Consumers reading bronze outside `read_html` | Zero remaining before Stage 4 |
| Source objects deleted without a verified pack member | Impossible |

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
