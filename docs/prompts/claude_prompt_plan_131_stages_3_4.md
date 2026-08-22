# Claude Prompt: Plan 131 Stages 3-4 — Pack Read Path And Source Deletion

You are working in the `cartracker-scraper` repo. Branch off `master`.

Read `docs/plans/plan_131_packed_cold_storage.md` first, then
`docs/plans/plan_132_unrecorded_artifact_recovery.md`. They are the source of truth
and record every decision below with the measurements behind it.

## Where this stands

Stages 1-2 are built and have run in production. **April 2026 `detail_page` is
packed in full**: 557,065 objects into 32 packs + 32 sidecar indexes, 8,704x
object reduction, **557,065 / 557,065 members verified byte-identical, zero
source objects deleted.** May packed overnight on 2026-08-14; check the plan
doc for its measured result before assuming anything about it.

**Nothing has freed a single inode yet.** That is what Stage 4 is for, and it
is the only step in this plan that removes data.

The constraint, measured on the VM: ~65,500 inodes/day against ~4.0M free —
**~61 days from 2026-08-13, about mid-October 2026.** April alone is ~1.248M
inodes (~19 days). April+May+June is ~6.05M inodes (**+92 days**).

## Scope of this prompt

**Stages 3 and 4 only.**

- **Stage 3** — `read_html` falls back to the pack index when an object is
  gone. Deployed and proven **while every source object still exists**, so a
  bug is a latency regression rather than an outage.
- **Stage 4** — delete source objects that are provably inside a verified
  pack. Dry-run by default, hard per-run cap, one pack's worth at a time.

**Out of scope, do not start:** Stage 5 (lifecycle DAG + metrics), re-packing
anything, Plan 132's reparse, `results_page` packing.

## Stage 3 and Stage 4 are separate deploys. Do not merge them.

Stage 3 must be in production and exercised against real reads **before** any
delete runs. The whole safety argument of this plan is that deletion happens
only after the replacement read path is proven on data that still exists in
both places. Landing them together forfeits that and there is no reason to.

---

## HARD SEQUENCING CONSTRAINT — read this before writing any Stage 4 code

**42,276 April captures were never recorded downstream** — no
`artifacts_queue_events` row, no silver observation. They are recoverable only
by reparsing their HTML, which is [Plan
132](../plans/plan_132_unrecorded_artifact_recovery.md).

Two consequences that are requirements, not suggestions:

1. **Those 42,276 objects can never satisfy a "has been processed" check** —
   there is no event row to check against. A naive Stage 4 refuses them
   forever, stranding ~95K inodes as individual objects in the one fully
   packed month. They are inside verified packs, so deleting them loses
   nothing, but it needs an explicit *"packed and verified, no provenance"*
   branch rather than silently falling through the processed check.
2. **Plan 132's reparse must run before Stage 4 deletes April, or Stage 3 must
   be live first.** Stage 3 being live satisfies this — once packs are
   readable, the reparse works after deletion. **Stage 3 first is the correct
   order and removes the conflict entirely.**

---

## Stage 3: the read path

### The consumer survey is done. These are the results.

Plan 129 Stage 2 flagged that anything decompressing bronze outside
`read_html` breaks at read time, long after the write looked fine. Surveyed
again 2026-08-14:

- **Exactly one production caller of `read_html`:**
  `processing/routers/batch.py:87`. A fallback inside `shared/minio.py` covers
  it and every script for free.
- **All offline scripts already route through `read_html`**:
  `train_html_dictionary`, `estimate_dictionary_savings`, `estimate_pack_savings`,
  `audit_sectioned_html_storage`, `diff_semantic_duplicate_html`,
  `diff_log_analysis`.
- **One real bypass remains**:
  `scripts/audit_semantic_duplicate_html_hashes.py:209` hashes blobs via DuckDB
  `read_blob` on `minio_path`. It is an offline audit script, so post-Stage-4
  it degrades to 404s rather than breaking production. **Decide explicitly**:
  route it through `read_html`, or leave it and document the limitation in the
  plan. Do not leave it undecided.

**Re-run the survey yourself before finishing.** It has been correct twice and
the failure mode is silent.

### What to build

`shared/minio.py::read_html` tries `html/` first, then resolves through the
pack index. Transparent to every caller — no signature change.

`shared/packfile.py` already has what you need. **Read it before designing
anything.** `PackReader` takes a `fetch_range(offset, length)` callable and
nothing else, which is exactly why the same reader serves a local `bytes` and
a ranged S3 GET with no S3 code in the module. It already carries an
in-process LRU of decompressed frames.

The sidecar schema, per member:

```
artifact_id, listing_id, fetched_at, source_key,
frame_ordinal, offset_in_frame, length, raw_sha256
```

Resolution is by `source_key` — that is the key `read_html` was handed.

### Design questions to settle with measurement, not preference

- **How does a reader find which pack holds a key?** Globbing every
  `.idx.parquet` through DuckDB is the plan's stated approach and is fine for
  an audit, but `read_html` is on the hot path for parsing. Measure it. A pack
  key is derivable from the source key's hive partition
  (`html/year=Y/month=M/artifact_type=T/` → `html_packs/T/Y/MM/`), which bounds
  the search to one month's sidecars rather than all of them. Prefer the
  bounded lookup if it measures better; say so either way.
- **Cache the index, not just frames.** A month's sidecar is ~1.4 MB per pack.
  Reparsing walks many members of the same pack, so an index cache probably
  matters more than the frame LRU. Measure before assuming.
- **Report single-artifact extraction latency p50/p95.** The plan's success
  criteria require it measured and accepted, not assumed. "Slow" needs a number
  attached.

### Proving it

Deploy, then verify against artifacts that exist in **both** places. For a
sample of packed April keys, assert `read_html` returns identical bytes
whether it reads the object or the pack. That is a real end-to-end test of the
exact path Stage 4 will depend on, run while the safety net is still in place.

---

## Stage 4: deleting sources

### The safety property, stated precisely

**Delete an object only after proving that the Stage 3 read path returns its
exact bytes.** Not "a pack exists". Not "the sidecar lists it". The actual
read path, for that actual artifact, returning bytes whose sha256 matches the
sidecar's `raw_sha256`.

Verification already ran twice at pack time (`PackWriter.finish()` on the
in-memory pack, then a re-read of the stored object over ranged GETs). Stage 4
verifying a third time through the production read path is not redundant — it
is testing a different code path, and it is the one that has to work
afterwards.

### Requirements

- **Dry-run by default.** Require an explicit `--apply`.
- **Hard per-run cap**, defaulting small. One pack's worth of sources at a
  time; never batch deletes across packs.
- **Delete grace period is this stage's own knob**, deliberately not
  `PACK_SETTLE_DAYS`. Writing a pack is additive and safe; deleting is the
  irreversible step and only it needs a grace period.
- **The bucket is un-versioned** (verified 2026-08-10), so a delete frees its
  space and inode immediately with no version expunge step.
- **Report inodes freed, not just objects and bytes.** Inodes are the
  constraint this plan exists to relieve; a summary that omits them is
  measuring the wrong thing.
- Checkpointed and resumable. Do not reintroduce the O(n²) checkpoint shape
  Plan 129 shipped once and fixed in `f98e69b`.

### The "has been processed" check — resolve this deliberately

The plan requires it. Think about what it is actually protecting against,
because Stage 3 changes the answer.

The check exists so an unparsed artifact is not deleted before it can be
parsed. **But once Stage 3 is live, a packed artifact is still readable and
still parseable after its source object is gone.** The check becomes
belt-and-braces rather than load-bearing.

That argues for keeping it but not letting it block: an artifact that is
packed, verified through the read path, and past the grace period is safe to
delete regardless of processing status. **Propose an explicit policy in the
plan doc and get it agreed before implementing.** Do not silently pick one.

Facts you need to get this right, all measured 2026-08-14:

| status | meaning |
|---|---|
| `complete` | success, processing-service era (from `b217a71`, 2026-04-16) |
| **`ok`** | **success, n8n era.** 19,950 April artifacts. **Never treat as junk.** |
| `skip` | Cloudflare challenge page — `_process_detail_page` writes no observation, correctly. 48,600 in April, 0 in May/June, 1,903 in July |
| `retry`, `pending`, `processing`, `failed` | as named |

- **`ops.artifacts_queue` is a hot table** — measured at 400 rows, all from the
  preceding 40 seconds, pruned by `cleanup_queue`. It cannot tell you what
  happened in April. The historical record is
  `ops_normalized/artifacts_queue_events` in the lake. Its timestamp column is
  `event_at`, not `event_ts`.
- April's terminal statuses do not reconcile exactly: 445,796 `complete` +
  19,950 `ok` = 465,746 against 457,084 artifacts holding a silver
  observation. **Do not build a delete predicate on this arithmetic.**

### What Stage 4 must not do

- Delete anything with no sidecar entry.
- Delete an orphan pack (a pack with no sidecar — an interrupted run). Stage 2
  reports these and never deletes them; keep that property.
- Delete `ok` artifacts as junk. `ok` is success.
- Run over April before Stage 3 is live — see the sequencing constraint.

---

## Where the code goes

Archiver work, mirroring `compact_silver` and the existing
`pack_bronze_html` exactly. Read
`archiver/processors/pack_bronze_html.py` and `archiver/app.py` before writing
anything; match their structure, logging, dry-run posture and error handling.

| File | Stage |
|---|---|
| `shared/minio.py` (`read_html` pack fallback) | 3 |
| `tests/shared/test_minio_packfallback.py` | 3 |
| `tests/integration/shared/` (packed + unpacked both readable) | 3 |
| `archiver/processors/delete_packed_source_html.py` | 4 |
| `archiver/app.py` (`POST /pack/bronze/prune`) | 4 |
| `tests/archiver/test_delete_packed_source_html.py` | 4 |

## Gotchas that will cost you a debugging pass

- **The archiver's app root is `/app`, not `/usr/app`.** `/usr/app` holds only
  the logs volume. `docker exec -w /app` or `import shared` fails.
- **`configured_dictionary_id()` returns empty in reader containers.**
  `HTML_COMPRESSION_DICT_ID` is a writer-side variable. Readers resolve the
  dict id from each frame's own header; `PACK_BRONZE_DICT_ID` is the packer's
  explicit knob.
- **`read_html` handles both frame types** by reading
  `zstd.get_frame_parameters(data).dict_id` from the frame. Never trust object
  metadata — metadata can be rewritten by a copy, the frame cannot.
- **`write_html` stores no custom metadata**, and Plan 129's backfill re-PUT
  every object on 2026-08-13. **`LastModified` is the recompression time, not
  the capture time.** Do not date anything from it.
- **The free-space floor must not measure the container's `/`** — that is a
  49 GB overlay on /dev/sda1; the bucket lives on /dev/sdb. Default is
  `/usr/app/logs`, the archiver's own named volume.
- **Never run bulk object work over an SSH tunnel** — ~8x slower than
  in-container. Use `docker exec -d`; two foreground attempts died with their
  SSH connection mid-listing.
- **`docker compose build` is required after adding new files.**

## Testing

Repo convention: real unit coverage of pure functions with no MinIO or DuckDB
required; integration tests separately marked.

Stage 3:
- `read_html` returns the object when it exists, without touching any pack.
- `read_html` falls back to the pack when the object is absent, and the bytes
  are identical.
- A key in neither place raises the same error shape it raises today.
- Both frame types still readable: no-dictionary, dictionary, and packed.
- Index sidecar and pack footer disagreement is an error, not a silent
  preference.

Stage 4:
- Refuses to delete a key with no sidecar entry.
- Refuses to delete when read-path verification fails.
- `--apply` absent deletes nothing.
- Per-run cap is honoured exactly.
- Resume deletes each object at most once and is not O(n²).
- An orphan pack's sources are never deleted.

Run `python -m pytest tests/ -m "not integration"` and `python -m ruff check`
before declaring done. Re-read every function you edit.

## Definition of done

- [ ] `read_html` falls back to packs, transparent to callers, unit tested.
- [ ] Consumer survey re-run and its result recorded in the plan doc.
- [ ] `scripts/audit_semantic_duplicate_html_hashes.py` explicitly decided.
- [ ] Extraction latency p50/p95 measured and recorded, not assumed.
- [ ] Stage 3 deployed and verified against real April packs **while every
      source object still exists**.
- [ ] Processed-check policy proposed in the plan doc and agreed before Stage 4
      is implemented.
- [ ] `delete_packed_source_html.py` dry-run by default, hard cap, per-artifact
      read-path verification before every delete.
- [ ] `POST /pack/bronze/prune` follows the `compact_silver` endpoint pattern.
- [ ] First real delete run is small, capped, and reports objects, bytes **and
      inodes** freed.
- [ ] Plan doc updated with measurements, including any that contradict it.

## Working agreements

- **Never deploy, restart containers, or copy files to production without
  explicit confirmation.** Files reach production via git commit + push + pull,
  then `bash scripts/redeploy.sh <service>` (note the `bash` prefix; the script
  is tracked `100644` and is not executable on a fresh checkout). Never scp or
  `docker cp` code into production.
- **Stage 4 deletes data. Confirm before the first apply run, every time, and
  keep the first run small enough to inspect by hand.**
- Surface confusion rather than guessing. If a measurement contradicts the
  plan, say so and stop — this plan has been wrong three times and was
  corrected by measurement every time.
- Do not quote the **74% logical / 84% physical** projection. April measured
  **53.4% / ~72.5%** and the plan doc explains why the projection was
  unreachable. Both gates still pass comfortably.
