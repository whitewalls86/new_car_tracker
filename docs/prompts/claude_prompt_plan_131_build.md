# Claude Prompt: Plan 131 Stages 1-2 — Pack Format And First Cold Pack

You are working in the `cartracker-scraper` repo. **Branch off
`plan-131-packed-cold-storage`**, not `master`.

Most of Plan 131 Stage 0 is on `master` (PR #187), but three docs-only commits
are not: the Stage 0d grouping decision and this prompt. They live on
`plan-131-packed-cold-storage` and are meant to ride along with your Stage 1-2
PR rather than land on their own. `master`'s copy of the plan doc still says
grouping is undecided; the branch's copy is correct.

Read `docs/plan_131_packed_cold_storage.md` first. It is the source of truth and
it records every decision below along with the measurements behind them.

## Why this plan exists

`bronze` is running out of **inodes**, not bytes. Plan 129's trained zstd
dictionary cut bronze bytes ~73% logical / ~60% physical and changed object count
by **zero**. Measured on the VM:

| | 2026-08-08 | 2026-08-13 |
|---|---|---|
| inodes used on `/mnt/data` | 8,774,058 | **9,101,670** of 13,107,200 |
| disk used of 196 G | 184 G | **162 G** |

~65,500 inodes/day against ~4.0M free — **~61 days, about mid-October 2026.**
Disk fell 22 GB over that window while the clock did not slow at all.
Compression cannot move this number. Packing many artifacts into few objects can.

## Scope of this prompt

**Stages 1 and 2 only.**

- **Stage 1** — the pack format, writer, and indexed reader. Offline. Nothing in
  production reads or writes packs.
- **Stage 2** — pack one cold month against real data, verify every member
  round-trips byte-for-byte, and **delete nothing**.

**Out of scope, do not start:** Stage 3 (`read_html` fallback), Stage 4
(deleting packed sources), Stage 5 (lifecycle DAG + metrics). Stage 4 is the
only step that removes data and it must not be approached casually.

## The design is DECIDED. Do not re-litigate it.

Stage 0 measured all of this. Re-opening any of it wastes a session.

**Grouping: monthly capture buckets**, members ordered `listing_id, fetched_at`
inside. Measured 67.8% logical / ~79-81% physical saving, against a per-listing
ceiling of 71.3% / 85.0% — a gap of 1.9 physical points. Reprocessing a month is
**1.00x** read amplification.

**Keep the trained dictionary (`dict_id` 1367127621) inside pack frames.** The
frame window does not subsume it: without it `B` rises from 8,578 to 32,952
bytes while `D` is unchanged (2,142 vs 2,058). Its entire contribution is the
*first* member of each frame, which is still ~10 GiB across 441,448 groups.

**Measured constants** (detail artifacts, 2026-04-01 onward):

| | |
|---|---|
| artifacts | 4,557,751 |
| per-listing groups | 224,459 |
| monthly groups | 441,448 |
| `B` first capture in a group | 8,578 bytes |
| `D` each subsequent capture | 2,142 bytes (D/B = 0.250) |
| mean ordinary capture | 7,313 bytes |

### Rejected designs, with the reason each died

Do not propose these again.

| Rejected | Why |
|---|---|
| Section decomposition / content-addressed sections | Plan 114 measured it at **-223%**. The win is the container, not content-addressing; a multi-MB frame finds the same redundancy with no manifest layer. |
| Per-vehicle archives | Not write-once (an active listing needs a read-modify-write per capture), 100,237 objects, **and** still 3.03x amplification. |
| One monolithic archive, appended | Rewriting needs its full size free while the old copy exists; the VM has ~24 GB free, so a growing tier becomes permanently un-rewritable. Cumulative cost O(n²). Blast radius is the whole history. |
| Cold-listing cohorts (pack on dormancy) | 4.46x amplification — 5 packs but 99.8% of the corpus — to buy 1.9 physical points. |
| Weekly buckets | 5.02 buckets per listing. |
| Quarterly buckets | 3.19x amplification, worse than per-listing, for partial compression gain. |
| Deferring write-time compression | Nothing recompresses today (`write_html` has two callers, both scraper; `read_html` one, processing). Saves one ~0.3 ms decompress, costs an extra PUT+DELETE per artifact and makes bronze durability depend on `processing`. |

## Prerequisite: archiver rebuild — CLEARED 2026-08-13

Recorded because the reasoning still matters, not because it blocks you.

Plan 129 rebuilt only `processing` and `scraper`, reasoning a stale `archiver`
was harmless because it never calls `read_html`. **Plan 131 invalidated that** —
reading HTML is the packer's entire job. The old archiver image had no
`shared/compression.py` and a `shared/minio.py` with zero dictionary support, so
it could not decode the frames the Plan 129 backfill creates.

**The archiver has since been rebuilt and verified**: `shared/compression.py`
present, dictionary support in `shared/minio.py`, `duckdb` and `zstandard`
importable, and `get_dictionary(1367127621)` resolves. It can now both sample
and measure, so no cross-container split is needed.

The durable lesson: **re-ask "which images read HTML" whenever the read path
changes.** A stale image fails at read time, long after the write looked fine.

Any further image change is a production deploy — `git pull` on the VM then
`bash scripts/redeploy.sh archiver` (note the `bash` prefix; the script is
tracked `100644` and is not executable on a fresh checkout). **Ask before
deploying. Never scp or docker cp code into production.**

## Where the code goes

This is **archiver** work and it mirrors Plan 109's `compact_silver` exactly.

| Plan 109 precedent | Plan 131 equivalent |
|---|---|
| `archiver/processors/compact_silver.py` | `archiver/processors/pack_bronze_html.py` |
| `POST /compact/silver/run` | `POST /pack/bronze/run` |
| `airflow/dags/compact_silver.py` | `airflow/dags/pack_bronze_html.py` (Stage 5) |
| `COMPACT_SILVER_MAX_PARTITIONS` (default 10) | `PACK_BRONZE_MAX_BUCKETS` |
| 2-day watermark via `_today_utc()` (patchable in tests) | `PACK_SETTLE_DAYS` (default 1), same patchable-clock pattern |
| write `.tmp` → assert row count → delete originals → rename | write pack → verify every member → (Stage 4) delete sources |

Read `archiver/processors/compact_silver.py` and `archiver/app.py` before
writing anything. Match their structure, logging, and error handling.

New files:

| File | Stage |
|---|---|
| `shared/packfile.py` | 1 |
| `tests/shared/test_packfile.py` | 1 |
| `archiver/processors/pack_bronze_html.py` | 2 |
| `tests/archiver/test_pack_bronze_html.py` | 2 |
| `tests/integration/archiver/test_pack_bronze_html_integration.py` | 2 |
| `archiver/app.py` (`POST /pack/bronze/run`) | 2 |

## Stage 1: the pack format

Stock `zstandard` only. No seekable-format dependency (the Python bindings do
not expose it). No new service, no new pip package.

```
pack object:  bronze/html_packs/{artifact_type}/{yyyy}/{mm}/pack-{seq}.zpack
  header      magic, format version, dict_id, frame count
  frame 0     independent zstd frame, ~N members, dictionary-preloaded
  frame 1     ...
  footer      frame offsets

sidecar:      bronze/html_packs/.../pack-{seq}.idx.parquet
  artifact_id, listing_id, fetched_at, source_key,
  frame_ordinal, offset_in_frame, length, raw_sha256
```

Requirements:

- **Frames are independently decodable.** Reading one member decompresses one
  frame (~4-16 MB uncompressed, tunable), never the whole pack.
- **Members concatenate with no separator.** The index carries offsets; a
  separator would spend bytes the format does not need.
- **Format version in the header from day one.** Packs are immutable and will
  outlive this plan's assumptions.
- **`raw_sha256` per member** is the verification anchor for Stage 2 and any
  later audit.
- **The index is a sidecar object, not a Postgres table.** 4.5M index rows are
  historical record, not hot operational state — see the MinIO-first
  architecture. DuckDB globs every `.idx.parquet` to resolve an artifact.
- A pack whose members do not all extract byte-identically is **never
  finalized**.

## Stage 2: pack one cold month, delete nothing

- **Eligibility: the calendar month of `fetched_at` is complete**, plus
  `PACK_SETTLE_DAYS` (default 1) for boundary artifacts. Grouped by that month,
  ordered `listing_id, fetched_at`.

  **Do not gate packing on an age threshold.** Writing a pack is additive and
  safe; deleting the sources is the irreversible step, and only the latter needs
  a grace period. Bronze objects are written at fetch time (`write_html` is
  called inline by the scraper), so nothing arrives late with an old
  `fetched_at` and a closed month is genuinely closed. Conflating the two knobs
  delays the inode relief that is the entire point of this plan — as of
  2026-08-13 it is the difference between packing ~45% and ~85-90% of the
  corpus.

  Deletion eligibility (Stage 4, out of scope here) is a **separate** knob:
  grace period **and** a check that the artifact has been processed.
- Write packs **alongside** the source objects. Delete nothing.
- Verify **100%** of members extract byte-identically against `raw_sha256`.
  Anything less is a bug in the extractor, not an acceptable rate.
- Checkpointed and resumable.
- Dry-run by default; require an explicit `--apply`-equivalent.

### Transient-space constraint — this is a hard requirement

A pack must be fully written and verified **before** its sources could ever be
deleted, so it needs free space equal to the pack size while both exist. Plan
129 learned this the expensive way: MinIO enforces a minimum-free-drive
threshold and refuses **every** `PutObject` below it — including the small
writes that would have freed space. It killed a 768 KB dictionary upload and
only deleting ~40 MB unblocked it.

Therefore:

- Pack size is tunable with a **small default** (~64 MB, not GB).
- The job **refuses to start** below a configurable free-space floor and says
  so, rather than discovering it mid-write.
- One pack at a time: write → verify → next.
- The bucket is un-versioned (verified 2026-08-10), so deletes free space
  immediately with no version expunge.

## Gotchas that will cost you a debugging pass

- **`configured_dictionary_id()` returns 0 in reader containers.**
  `HTML_COMPRESSION_DICT_ID` is set on the **writer** (scraper); readers resolve
  the id from each frame's own header. Pass the dictionary explicitly rather
  than assuming the env var is present.
- **`read_html` handles both frame types** (pre-dictionary and dictionary) by
  reading `zstd.get_frame_parameters(data).dict_id` from the frame. Never trust
  object metadata for this — metadata can be rewritten by a copy, the frame
  cannot.
- **Do not build a checkpoint that is O(n²).** Plan 129 shipped one and had to
  fix it (commit `f98e69b`).
- **`ps` is not installed in the `processing` image.** Liveness-check a
  checkpoint file, not a process.
- **Never run bulk object work over an SSH tunnel** — Plan 129 measured
  in-container at ~8x the throughput (~35 obj/s with writes vs ~10 tunnelled).
- **`docker compose build` is required after adding new files.** Cached images
  will not include them.
- Silver holds ~4.56M detail artifacts but bronze had ~4.07M objects. Some
  artifacts have no surviving HTML (Plan 128 challenge-page eviction is one
  plausible source). **Count the packable universe from MinIO, not from
  silver.**

## Testing

Follow the repo convention: real unit coverage of pure functions, no MinIO or
DuckDB required; integration tests separately marked.

`tests/shared/test_packfile.py`:
- Every member extracts byte-identically, including the last member in a frame
  and a single-member pack.
- A member failing its `raw_sha256` fails the pack, loudly.
- Reading member *k* decompresses exactly one frame.
- Header/format-version mismatch raises rather than misreading.
- Index sidecar and pack footer agree; disagreement is an error, not a silent
  preference for one.

`tests/archiver/test_pack_bronze_html.py`:
- Checkpoint resume packs each artifact exactly once, and is not O(n²).
- Refuses to run below the free-space floor.
- Dry-run writes and deletes nothing.

Run `python -m pytest tests/ -m "not integration"` and `python -m ruff check`
before declaring done. Re-read every function you edit.

## Definition of done

- [ ] `shared/packfile.py` writes and reads packs, with round-trip asserted per
      member on write.
- [ ] Unit tests pass, ruff clean.
- [ ] `archiver/processors/pack_bronze_html.py` packs one eligible month, dry-run
      by default.
- [ ] `POST /pack/bronze/run` follows the `compact_silver` endpoint pattern.
- [ ] One real cold month packed on the VM with **100%** byte-identical
      verification and **zero source objects deleted**.
- [ ] Free-space floor check demonstrably refuses to start when below it.
- [ ] Plan doc updated with what was built and any measurement that contradicted
      the plan.

## Working agreements

- **Never deploy, restart containers, or copy files to production without
  explicit confirmation.** Files reach production via git commit + push + pull
  only.
- Surface confusion rather than guessing. If a measurement contradicts the plan,
  say so and stop — the plan has already been wrong twice and was corrected by
  measurement both times.
- Prefer the smallest change that works. This plan explicitly rejected a more
  elegant design (content-addressed sections) because a simpler one measured
  better.
- Do not quote **83.1%** as the physical saving. The projection's baseline is
  inflated ~22%; the honest range is **79-81%**. The plan doc explains why.
