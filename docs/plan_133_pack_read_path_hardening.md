# Plan 133: Pack Read Path Hardening

## Status

**Draft — not started, and deliberately not urgent.** Two defects found on
2026-08-14 while verifying [Plan 131](plan_131_packed_cold_storage.md) Stage 3
in production. Both are real, neither blocks Plan 131 Stage 4, and both become
blocking for [Plan 132](plan_132_unrecorded_artifact_recovery.md)'s reparse.

They are recorded here rather than fixed inline because Stage 3 was verified
against real April and May packs — **365 members, 0 failures** — and changing
the read path immediately after verifying it would invalidate the verification
for no gain that anything currently scheduled needs.

| # | Defect | Blocks Stage 4? | Blocks what |
|---|---|---|---|
| 1 | Existence checks bypass the pack read path | **No** | Plan 132 Stage 2+ reparse |
| 2 | Sidecar index cache thrashes on sequential scans | **No** | Plan 132 Stage 3 full reparse |

---

## Defect 1 — `object_exists` has no pack fallback

`ops/routers/maintenance.py::_reap_stuck_processing` decides whether a stranded
artifact is re-queued or abandoned:

```python
new_status = "retry" if object_exists(r["minio_path"]) else "skip"
```

`shared.minio.object_exists` is a bare `head_object`. Plan 131 Stage 3 gave
`read_html` a pack fallback; it gave `object_exists` nothing. So once Stage 4
prunes a month, an artifact from that month stranded in `processing` is
**abandoned as `skip` even though its HTML is intact and readable from the
pack**.

**This is a lost job, not lost data.** The bytes remain in a verified pack and
the artifact can be re-enqueued. What is lost is the automatic retry.

### Why it is not reachable today

For the reaper to see an artifact from a pruned month, that artifact needs a
row in `ops.artifacts_queue` with `status='processing'`. Two things prevent it:

- `orphan_checker` runs the reaper **every 5 minutes**, so anything stranded
  from April or May was reaped months before Stage 4 existed.
- Nothing enqueues historic artifacts. The queue is fed by live scraping.

It becomes reachable the moment something *does* enqueue a pruned month — which
is precisely what **Plan 132 Stage 2 does** when it re-enqueues April orphans
for reparse. An artifact that then strands for two hours gets abandoned rather
than retried.

### The survey gap that let it through

Plan 131 Stage 3's consumer survey looked for code that **decompresses** bronze
outside `read_html` — `read_blob`, `ZstdDecompressor`, direct `get_object`. It
found four such sites and resolved them. It never looked for code that asks
whether an object **exists**, which is a different shape with the same
dependency on objects that Stage 4 removes.

**The next survey must cover both.** The general form of the question is *"what
reads or interrogates `html/` without going through a pack-aware path?"*, not
*"what decompresses bronze?"*

### Fix

`object_exists` has exactly one non-test production caller, so the blast radius
of either option is one line.

- **Preferred:** add `artifact_exists(minio_path)` to `shared/minio.py` — object
  first, then the pack index — and call it from the reaper. Leaves
  `object_exists` meaning what its name says, which other callers (and the
  packer's orphan detection) may legitimately want.
- **Rejected:** giving `object_exists` itself a fallback. The name would then
  lie, and "is this object present" is a genuinely different question from "can
  this artifact be read".

---

## Defect 2 — the sidecar index cache thrashes on a sequential scan

`PACK_INDEX_CACHE_PACKS` defaults to **4**. A packed month holds **32 sidecars
(April) to 41 (May)**, and the resolver scans upward from `pack-00000`. One
scan therefore evicts exactly the low-numbered entries the next scan will need
first, ending with the highest four resident — so the next lookup re-fetches
from the start. Textbook LRU-versus-sequential-scan behaviour.

Measured in production 2026-08-14 (`archiver/processors/verify_pack_read_path.py`, cold
reads drop every cache by design):

| | April (32 packs) | May (41 packs) |
|---|---|---|
| pack cold p50 | 206.65 ms | **296.34 ms** |
| pack cold p95 | 361.93 ms | **673.92 ms** |
| object p50 | 5.79 ms | 5.51 ms |

**May is the better-packed month and it is 43% slower**, because the cold cost
tracks *packs-per-month × sidecar size*, not data quality: 41 sidecars instead
of 32, and ~27,980 members per pack instead of ~17,500. June and later months
continue the trajectory.

At May's numbers a 36K-artifact reparse spends roughly **3 hours in index
scanning alone**.

### Why it does not block Stage 4

`delete_packed_source_html` opens one `PackReader` per pack and walks that
pack's members in frame order. It never uses the resolver, so it never pays the
scan.

### Fix

- **Cache the `source_key` column, not the whole sidecar.** Measured
  2026-08-14: 1.30 ms to parse and 1.69 MB in Arrow, against 1.69 ms and
  3.78 MB for every column. Only the one matching row needs the rest, and it is
  already fetched from the sidecar that matched.
- **Then raise `PACK_INDEX_CACHE_PACKS` to hold a whole month** (~40). With
  column pruning that is ~68 MB resident instead of ~150 MB.

A month-level "which pack holds this key" manifest would remove the scan
entirely, but it is a new artifact written by the packer and is out of scope
here — the two changes above are configuration and one column list.

---

## Sequencing

**Do this before [Plan 132](plan_132_unrecorded_artifact_recovery.md) Stage 2**,
which is the first thing that both re-enqueues a pruned month (defect 1) and
reads packed artifacts in bulk (defect 2). Plan 132's Stage 0 gate has not run,
so there is time.

**Re-run `archiver/processors/verify_pack_read_path.py` for April and May afterwards.**
Stage 3's gate was established against the current read path; any change to it
re-opens that gate and the same script closes it again.

---

## Files Changed

| File | Change | Defect |
|------|--------|--------|
| `shared/minio.py` | `artifact_exists()`; column-pruned sidecar cache; cache default | 1, 2 |
| `ops/routers/maintenance.py` | reaper calls `artifact_exists` | 1 |
| `tests/shared/test_minio_packfallback.py` | existence-after-prune; cache holds a month-sized scan | 1, 2 |
| `tests/ops/routers/test_maintenance.py` | retry rather than skip for a packed-and-pruned artifact | 1 |

## Success Criteria

| Metric | Gate |
|--------|------|
| A stranded artifact whose source was pruned | `retry`, never `skip` |
| Cold read p50 after a month-sized scan | no worse than a single sidecar fetch |
| `verify_pack_read_path` after the change | 0 failures, April and May |
| Consumers interrogating `html/` outside a pack-aware path | Zero — existence checks included this time |

## Out of Scope

- A per-month pack manifest. It would remove the scan outright, but it is a new
  packer-written artifact and the two fixes here are enough for the reparse.
- Re-packing anything. No re-pack path exists and none is proposed.
- Plan 131 Stage 5's lifecycle DAG and metrics.
