# Plan 133: Pack Read Path Hardening

## Status

**COMPLETE — deployed and verified in production 2026-08-20.** Merged as PR #219
(`5066bc1`); both defects are fixed, and the post-deploy gate re-ran across all
four packed months with **720 artifacts verified and 0 failures**. This unblocks
[Plan 132](plan_132_unrecorded_artifact_recovery.md) Stage 2. Two defects found
on 2026-08-14 while verifying
[Plan 131](plan_131_packed_cold_storage.md) Stage 3 in production. Both are
real, neither blocked Plan 131 Stage 4, and both become blocking for
[Plan 132](plan_132_unrecorded_artifact_recovery.md)'s reparse.

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

## Defect 2 — the sidecar index cache thrashed on a sequential scan

Before this plan, `PACK_INDEX_CACHE_PACKS` defaulted to **4**. A packed month
holds **32 sidecars (April) to 41 (May)**, and the resolver scans upward from
`pack-00000`. One scan therefore evicted exactly the low-numbered entries the
next scan needed first, ending with the highest four resident — so the next
lookup re-fetched from the start. Textbook LRU-versus-sequential-scan
behaviour.

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
- **Then raise `PACK_INDEX_CACHE_PACKS` to hold a whole month.** The default is
  now 48, covering April-May-June-July's 32, 41, 38, and 33 sidecars with
  headroom. With column pruning that is ~81 MB at the configured limit instead
  of ~181 MB for every column.

A month-level "which pack holds this key" manifest would remove the scan
entirely, but it is a new artifact written by the packer and is out of scope
here — the two changes above are configuration and one column list.

---

## Sequencing

**Do this before [Plan 132](plan_132_unrecorded_artifact_recovery.md) Stage 2**,
which is the first thing that both re-enqueues a pruned month (defect 1) and
reads packed artifacts in bulk (defect 2). Plan 132's Stage 0 gate has not run,
so there is time.

**Re-run `archiver/processors/verify_pack_read_path.py` for April, May, June,
and July afterwards.** All four months are packed and pruned now. Stage 3's gate
was established against the prior read path; changing it re-opens that gate and
the same script closes it again.

---

## Pre-deployment verification — 2026-08-18

The changed `shared/minio.py` was bind-mounted read-only into disposable
`archiver` containers on the production host. The running checkout, images,
and services were not changed. One member from every current pack was checked
through both the packed read path and `artifact_exists`:

| Month | Sidecars / packs | Packed reads verified | `artifact_exists` true | Failures |
|---|---:|---:|---:|---:|
| April | 32 | 32 | 32 | 0 |
| May | 41 | 41 | 41 | 0 |
| June | 38 | 38 | 38 | 0 |
| July | 33 | 33 | 33 | 0 |
| **Total** | **144** | **144** | **144** | **0** |

All 144 sampled loose source objects were already deleted, so every positive
result exercised the pack-aware branch. This is a deploy-safety canary, not the
post-deploy closeout gate; re-run the verifier against all four months after
the production services use the merged code.

---

## Production deployment and post-deploy verification — 2026-08-20

Deployed through the existing deploy-intent/drain procedure. Intent was declared
with `pause_long_jobs: true` — Plan 131's pack and prune jobs use the read path
this plan changes, so they had to stop at a safe boundary rather than mid-pack.
The system was already quiet (0 in-flight, no running DAGs) and drained on the
first poll.

**Scope: four services**, chosen by tracing callers rather than rebuilding
everything that bakes `shared/`:

| Service | Reason |
|---|---|
| `ops` | `maintenance.py::_reap_stuck_processing` calls `artifact_exists` — defect 1 |
| `archiver` | `pack_bronze_html.py` reads through `read_html`; also hosts the verifier |
| `pack-worker` | Shares the `cartracker-archiver` image; needs recreation to pick up the rebuild |
| `processing` | `batch.py::_read_html` is defect 2's hot path |

`scraper` only writes (`write_html`), and `dashboard`/`dbt_runner` never read
bronze, so they were left to pick up `shared/` on their next deploy.

| Gate | Evidence |
|---|---|
| Revision | Production already at `5066bc1`; `git pull` was a no-op, confirming the code was **pulled but never built** |
| Loaded code | All four containers were asked directly: `artifact_exists` imports, `PACK_INDEX_CACHE_PACKS=48`, pack fallback present. The reaper calls `artifact_exists` and no longer calls bare `object_exists` |
| Dependency side effect | `redeploy.sh` omits `--no-deps`, so `flyway` re-ran. It validated 43 migrations, found schema at `042` with none pending, and exited 0. `postgres` and `minio` were left running |
| Fleet health | All four services healthy with `failing_streak=0` and `RestartCount=0`; every running healthchecked container healthy; 0 ERROR lines in 20 minutes |
| Reaper on new code | `POST /maintenance/reap-stuck-processing` returned 200 on the 5-minute `orphan_checker` cycle |
| Work resumed | `scrape_detail_pages` succeeded at 15:45-15:46 at its normal 400 fetches per 15-minute cycle; queue 1600 complete, 0 pending |

### The closeout gate — `verify_pack_read_path`, all four months

| Month | Sidecars | Sampled | Verified | Failed | Sources already deleted |
|---|---:|---:|---:|---:|---:|
| April | 32 | 160 | 160 | **0** | 160 |
| May | 41 | 205 | 205 | **0** | 205 |
| June | 38 | 190 | 190 | **0** | 190 |
| July | 33 | 165 | 165 | **0** | 165 |
| **Total** | **144** | **720** | **720** | **0** | **720** |

Every sampled loose source object was already deleted, so all 720 reads
exercised the pack-aware branch rather than falling through to an object.

### Latency, and what it does and does not prove

| Month | cold p50 before | cold p50 after | cold p95 before | cold p95 after |
|---|---:|---:|---:|---:|
| April | 206.65 ms | **174.42 ms** | 361.93 ms | **302.43 ms** |
| May | 296.34 ms | **256.31 ms** | 673.92 ms | **447.90 ms** |
| June | — | 279.61 ms | — | 442.60 ms |
| July | — | 230.80 ms | — | 393.58 ms |

June and July have no pre-change baseline; they were packed after the 2026-08-14
measurement.

**This improvement is the column-pruned sidecar parse, not the cache-size
change.** The verifier drops every cache before each cold read by design, so
`pack_cold` measures the no-cache path — where the only change is parsing one
`source_key` column instead of all of them. Raising `PACK_INDEX_CACHE_PACKS`
from 4 to 48 is invisible to this measurement, because the verifier never lets
the cache survive.

The cache change is therefore **verified as safe here, not verified as
effective**. Its payoff is a sequential scan over a whole month, which is
[Plan 132](plan_132_unrecorded_artifact_recovery.md) Stage 2's reparse workload.
That reparse is where the ~3-hour index-scanning estimate should be re-measured,
and it is the correct place to confirm defect 2 is actually fixed rather than
merely un-regressed.

---

## Files Changed

| File | Change | Defect |
|------|--------|--------|
| `shared/minio.py` | `artifact_exists()`; column-pruned sidecar cache; cache default | 1, 2 |
| `ops/routers/maintenance.py` | reaper calls `artifact_exists` | 1 |
| `tests/shared/test_minio_packfallback.py` | existence-after-prune; cache holds a month-sized scan | 1, 2 |
| `tests/ops/routers/test_maintenance.py` | retry rather than skip for a packed-and-pruned artifact | 1 |

## Success Criteria

| Metric | Gate | Result |
|--------|------|--------|
| A stranded artifact whose source was pruned | `retry`, never `skip` | **Met** — reaper calls `artifact_exists` in production; bare `object_exists` no longer reachable from it |
| Cold read p50 after a month-sized scan | no worse than a single sidecar fetch | **Not measured by this gate** — the verifier drops caches by design. Re-measure during Plan 132 Stage 2's reparse |
| `verify_pack_read_path` after the change | 0 failures, April-May-June-July | **Met** — 720 sampled, 720 verified, 0 failed |
| Consumers interrogating `html/` outside a pack-aware path | Zero — existence checks included this time | **Met** — caller trace before deploy found `read_html`/`artifact_exists` only in `ops`, `archiver`, `processing`, plus read-only audit scripts |

## Out of Scope

- A per-month pack manifest. It would remove the scan outright, but it is a new
  packer-written artifact and the two fixes here are enough for the reparse.
- Re-packing anything. No re-pack path exists and none is proposed.
- Plan 131 Stage 5's lifecycle DAG and metrics.
