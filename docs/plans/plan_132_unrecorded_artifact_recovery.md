# Plan 132: Recovering Unrecorded Bronze Artifacts

## Status

**Draft — Stage 0 gate not run.** Discovered 2026-08-14 while measuring
[Plan 131](plan_131_packed_cold_storage.md)'s first full month of packing.

April 2026 holds **42,276 detail-page captures that bronze stored and no
other system ever recorded** — no `artifacts_queue_events` row, no silver
observation. **36,241 of them are full-size pages.** The HTML is intact and
byte-verified inside Plan 131's packs.

This plan recovers them. It is possible at all because bronze did its job:
the raw bytes were retained even though every downstream record of them was
lost.

**Bounded and non-recurring.** May and June have complete event coverage
(1,021,266/1,021,266 and 1,124,122/1,124,122). Nothing needs fixing upstream.

---

## Goal

Reparse the 36,241 recoverable April captures into silver, and decide what
to do with the ~54K sub-kilobyte stubs the same investigation surfaced.

Secondary, and explicitly **not** the reason to do this: these artifacts are
why April packed at 53.4% instead of the projected 74%. There is no re-pack
path, so the bytes are already spent. **Do this for the missing data, not for
the storage.**

---

## What was measured (2026-08-14)

April 2026 `detail_page`, read-only, against production. Object counts from
MinIO, event counts from `ops_normalized/artifacts_queue_events` via DuckDB,
per-artifact classes from Plan 131's own pack sidecar indexes.

| | objects |
|---|---|
| in bronze | 557,065 |
| with an `artifacts_queue_events` row | 514,789 |
| **with no event row at all** | **42,276** |
| with a silver observation | 457,084 |

Three populations, and they behave completely differently:

| class | n | p50 size | meaning |
|---|---|---|---|
| **normal** | 457,084 | 169.2 KB | event + observation |
| **no_silver** | 57,705 | 0.4 KB | event exists, no observation |
| **orphan** | 42,276 | 165.2 KB | no event, no observation |

Split by size, which is what decides whether a page can be reparsed:

| class | <1 KB | 1–50 KB | ≥50 KB |
|---|---|---|---|
| orphan | 5,741 | 294 | **36,241** |
| no_silver | 48,600 | 0 | 9,105 |

So the investigation found three separate things, not one:

1. **36,241 orphaned real pages** — the recoverable data loss. This plan's target.
2. **9,105 full-size pages that were queued and produced no observation** — a
   different failure, with its own stage below.
3. **54,341 sub-kilobyte stubs** across both classes — 441 bytes each, no
   `initial-activity-data`. Not reparseable, and ~122K inodes of pure waste on
   the resource Plan 131 exists to protect.

### What is ruled out

- **Not duplication.** `paths_per_artifact` is `{1: N}` for April, May and June
  alike, and `any_value(minio_path) GROUP BY artifact_id` in
  `fetch_member_metadata` drops **zero** paths. The initial hypothesis — that
  the packer's CTE was silently discarding duplicate paths — was tested and is
  wrong.
- **Not a packer bug.** Zero duplicate source keys were packed, and every
  `artifact_id` maps to exactly one packed object. Plan 131 packed the orphans
  correctly and deliberately, per its own rule that *"an artifact nobody can
  name is still an inode."*
- **Not ongoing.** May and June event coverage is exact.

### Why April, specifically

**April 2026 is the month the system moved to VM-local hosting and to MinIO
storage.** The record is in the repo and worth following rather than
restating: `1798a99` *"Add parquet archival layer (archiver service +
MinIO)"*, Plan 102 wiring `artifacts_queue_events` on 2026-04-29, and 395
commits across the month.

That same history explains the second anomaly too. April is when 403 handling
was being built — `d80071e` *"Adding 403 handling more robustly"*, `323aa67`
*"Adding 403 cooldown table"*, `d369cb5` *"adding flaresolverr support to
resolve 403 errors"* — which is precisely the machinery that produces the
`skip` terminal status this plan's Stage 4 keys on.

The point for this plan is narrow: **April is the only month that straddles
the cutover, and it is anomalous in three independent ways** — 42,276
unrecorded captures, 48,600 challenge-page skips, and 19,950 artifacts
carrying a terminal status (`ok`) that appears in no other month. May and June
are clean on all three, which is what makes this a bounded one-off rather than
a defect that could recur.

**Stage 0 still confirms the date profile rather than assuming it**, because
"we know why" and "we know the extent" are different claims.

---

## Recovering `fetched_at`, and a correction

An earlier note in this investigation said capture time could be read from the
object's `LastModified`. **That is wrong and must not be built on.** Plan 129's
dictionary backfill re-PUT every object on 2026-08-13, so `LastModified` is the
recompression time — orphans and a named control both return the same date.
`write_html` sets no custom metadata either, only `ContentEncoding` and
`ContentType`. **Object storage cannot date these artifacts.**

The HTML can. A sampled orphan carries `"timestamp_utc": 1776773832715` inside
the page — epoch milliseconds decoding to **2026-04-21T12:17Z**, consistent
with its `year=2026/month=4` bucket. The hive partition independently bounds
the answer to the right month.

This is the plan's central unknown and Stage 0's main gate: `timestamp_utc`
must be present and correct across the population, not in one sample.

The other input a reparse needs is free. `parse_cars_detail_page_html_v1(html,
url: Optional[str] = None)` extracts `listing_id` from
`script#initial-activity-data`, and `batch.py` prefers it —
`resolved_listing_id = primary.get("listing_id") or listing_id`. The queue
row's `listing_id` is only a fallback, so not knowing it costs nothing.

---

## Design

### The queue is the mechanism; it is also not a record

`ops.artifacts_queue` is a **hot table** — measured at 400 rows, all from the
preceding 40 seconds, pruned by the archiver's `cleanup_queue`. It cannot tell
us what happened in April, and it is not where history lives.

But it is exactly how a reparse is triggered: insert rows with
`status='pending'` and the existing `POST /process/batch` claims and processes
them with **no new parsing code**. `CLAIM_ARTIFACTS` returns
`artifact_id, minio_path, artifact_type, listing_id, run_id, fetched_at,
search_key`; of those only `minio_path`, `artifact_type` and `fetched_at`
must be supplied, and `run_id`/`search_key` are nullable.

### Sequencing against Plan 131 — this is a real constraint

The orphans are readable **today** as individual objects. Plan 131 Stage 4
deletes source objects; after that, reading them requires Stage 3's pack
fallback in `read_html`.

> **Either run this plan's reparse before Plan 131 Stage 4, or land Plan 131
> Stage 3 first.** Do not let Stage 4 run over April while this is
> outstanding.

There is no danger to the data either way — the bytes live in verified packs
regardless — but the reparse job would start 404ing, and the failure would look
like a bug rather than a sequencing mistake.

> **Prerequisite: [Plan 133](plan_133_pack_read_path_hardening.md).** Verifying
> Plan 131 Stage 3 in production surfaced two read-path defects that land
> squarely on this plan and on nothing else currently scheduled: the stuck-
> artifact reaper decides `retry` vs `skip` with `object_exists`, which has no
> pack fallback and would abandon reparse jobs for a pruned month; and the
> sidecar index cache thrashes on a month-sized scan, costing ~300 ms per cold
> artifact — roughly 3 hours across this plan's 36K. **Do Plan 133 before
> Stage 2.**

### New artifact_ids, and what that costs

The orphans have no `artifact_id` to reuse; re-enqueueing mints new ones. The
recovered observations will therefore carry IDs unrelated to the original
April scrape, and the events written for them will be dated at reparse time.
That is cosmetic for the price history and confusing for anyone auditing the
queue later, so it is recorded here rather than discovered there.

---

## Stages

### Stage 0 — Gate: can these be dated, and is it safe to write them?

Nothing is built until all three pass.

**0a. `timestamp_utc` fidelity, measured against artifacts where the answer is
already known.** Take a sample of **normal** April artifacts — which have both
HTML and a silver `fetched_at` — extract `timestamp_utc` from the HTML, and
compare. Report the distribution of the difference, not just a mean.

**Gate: agreement within a minute on ≥99% of the control sample.** Below that,
stop: undated observations in a price history are worse than absent ones.

**0b. Identify the 441-byte stubs — largely answered already.** Terminal status
per artifact gives a principled predicate, and it cross-validates exactly:
April has **48,600** artifacts with terminal status `skip`, which is precisely
the count of sub-1KB `no_silver` objects found independently in Plan 131's
pack sidecars. Two unrelated measurements — lake events and pack indexes —
landing on the same number identifies the stubs as the Cloudflare
challenge-page population that `_process_detail_page` correctly skips.

What remains for 0b: confirm the ~5,741 sub-1KB **orphans** (no event row, so
no status) are the same thing, since they cannot be identified by status.

**0e. `ok` means success — ANSWERED from the git history, 2026-08-14.**

19,950 April artifacts carry terminal status `ok` and no other month has any.
It is the **n8n-era success status**. Before `b217a71` (2026-04-16, *"adding
processing service, moving initial logic out of scraper"*) processing state
lived in a table called `artifact_processing`, and every insert into
`detail_observations` in `n8n/workflows/Results Processing.json` is gated on
`WHERE p.status = 'ok'`. The processing service replaced it with `complete`,
and April straddles the 2026-04-16 cutover.

**So `ok` artifacts are successfully processed and must never be treated as
junk.** Stage 4 keys on `skip` alone. This closes what was previously a
blocker.

*Residual, not blocking: 445,796 `complete` + 19,950 `ok` = 465,746 against
457,084 artifacts holding a silver observation. The direction is now explained
— `complete` alone was never going to cover the month — but the counts still
do not reconcile exactly. Do not build a delete predicate on this arithmetic.*

**0c. Confirm backdated writes are safe.** `write_detail_active` writes price
observations at a supplied `fetched_at`. Establish whether it — or anything
downstream in Plan 111/113's adaptive-refresh state or the listing-closure
logic — treats write order as observation order. **If backdated observations
corrupt refresh state, that is a blocker and this plan stops here.**

**0d. Confirm the date profile.** Plot orphan captures by day from their
recovered `timestamp_utc`. If they stop around 2026-04-29 the Plan 102 wiring
explanation holds and the population is closed. If they run the whole month,
the cause is not understood and Stage 1 waits.

### Stage 1 — Build the manifest

- Extract orphan `source_key`s from Plan 131's sidecar indexes: every member
  with a null `artifact_id`. Already demonstrated; costs 32 small GETs and no
  object reads.
- Split by size at 50 KB into `reparse` and `stub` manifests.
- Write the manifest to MinIO as Parquet. It is a historical record of a
  one-off recovery and belongs in object storage, not a Postgres table.

### Stage 2 — Reparse a small cohort

- `archiver/processors/reenqueue_unrecorded_html.py`, **dry-run by default**,
  hard per-run cap, mirroring `pack_bronze_html`'s shape and safety posture.
- Enqueue ~500 artifacts. Verify the observations that land carry the right
  `listing_id`, the right `fetched_at`, and plausible prices.
- **Verify against the packs, not just the objects**: extract the same members
  from Plan 131's packs and confirm the reparse would see identical bytes.
  This is free Stage 3 evidence for Plan 131.

### Stage 3 — Full reparse

- The remaining ~35,700, checkpointed and resumable, rate-limited so the
  processing service keeps serving live scrapes.
- Re-run counts afterwards: orphans with no silver row should approach zero.

### Stage 4 — Stub deletion, and why this is not its own DAG

Gated on 0b and **0e**. Delete April's challenge-page stubs by the `skip`
predicate, not by size: 48,600 objects, ~109K inodes, ~1.7 days of headroom.
This is the one place in the Plan 131/132 arc where deletion frees inodes with
**no** archive step, because there is nothing worth archiving.

#### This retention policy already existed, and was lost

`n8n/workflows/Cleanup Artifacts.json`, before the 2026-04-16 cutover:

```
ok            -> delete after 48 hours
skip (no ok)  -> delete immediately
retry (no ok) -> delete after 7 days
```

It deleted `raw_artifacts.filepath` — local files, pre-MinIO — so the
mechanism does not port. **The policy does, and its loss is the reason Plan
129 could observe that there is "no HTML deletion anywhere in the codebase."**
The n8n decommission (Plan 102) removed the only lifecycle rule bronze ever
had, and nothing replaced it. That is the actual origin of the inode problem
Plan 131 exists to solve.

Note the old policy deleted **`ok` artifacts after 48 hours** — successfully
processed pages were not retained at all. This plan does not propose
reinstating that; bronze is now deliberately a permanent archive, which is
what makes Plan 132's recovery possible in the first place. Only the junk
predicates are worth carrying forward.

#### Why the recurring job is not proposed here

**A standalone "delete 403s / challenge pages / failed fetches after X days"
DAG was considered and deliberately not proposed.** The measurements do not
support it as its own job:

| month | `skip` artifacts | share of captures |
|---|---|---|
| April | 48,600 | 9.4% |
| May | 0 | — |
| June | 0 | — |
| July | 1,903 | **0.2%** |
| August (partial) | 866 | — |

July is the representative current rate. At ~2,000 objects/month that is
~4,500 inodes against a burn of **65,500 inodes/day** — roughly 100 minutes of
headroom per month, against the ~92 days Plan 131 Stage 4 buys. There is also
no terminal `failed` status anywhere in 2026, so there is no large stored
failed-fetch population to reclaim.

The structural argument matters more than the volume one: **for any month that
gets packed, Plan 131 Stage 4 deletes these objects anyway**, skip or not. A
skip-specific DAG is additive only for months too recent to pack — the current
month plus `PACK_SETTLE_DAYS`.

**So the recurring policy belongs as a predicate inside [Plan 131](plan_131_packed_cold_storage.md)
Stage 5's lifecycle DAG**, which already exists in that plan, already respects
the free-space floor, and already walks buckets. A few lines there beat a
parallel deletion path with its own failure modes. Plan 131's Out of Scope
notes retention is "still unwritten, still needed"; this is where it lands.

### Stage 5 — The 9,105 queued-but-unwritten pages

Full-size pages that had an event row and still produced no observation. That
is a parse failure or a deliberate `skip` — `_process_detail_page` marks
Cloudflare challenges `skip` and writes nothing, which is correct behaviour and
may account for all of them. Separate investigation, and it may belong to
[Plan 128](plan_128_false_block_detection.md) rather than here.

---

## Testing

- Manifest extraction returns exactly the null-`artifact_id` members, and is
  stable across reruns.
- `timestamp_utc` extraction: valid epoch-ms, missing field, garbage field, and
  a value outside the artifact's own hive month — the last must be rejected,
  not written.
- Re-enqueue is dry-run by default and writes no queue rows without `--apply`.
- Re-enqueue is idempotent: running twice does not enqueue an artifact twice.
- Reparsed observation equals the observation a normal scrape would have
  written, on a control artifact where both exist.

---

## Files Changed

| File | Change | Stage | Status |
|------|--------|-------|--------|
| `scripts/probe_html_capture_timestamp.py` | Stage 0a/0d measurement, offline | 0 | not started |
| `archiver/processors/reenqueue_unrecorded_html.py` | Manifest + re-enqueue, dry-run default | 1–3 | not started |
| `archiver/app.py` | `POST /reenqueue/bronze/run` | 2 | not started |
| `tests/archiver/test_reenqueue_unrecorded_html.py` | New | 2 | not started |
| `archiver/processors/delete_stub_html.py` | Stage 4, gated on 0b | 4 | not started |

---

## Success Criteria

| Metric | Gate |
|--------|------|
| `timestamp_utc` agreement with silver on control artifacts | ≥99% within 1 minute |
| Backdated writes corrupt adaptive-refresh state | Must be proven safe before Stage 2 |
| Orphan captures recovered into silver | ~36,241 |
| Artifacts enqueued twice | Zero — idempotent by construction |
| Observations written with an unverified `fetched_at` | Zero |
| Reparse run after Plan 131 Stage 4 without Stage 3 | Must not happen — see sequencing |
| `ok` artifacts treated as junk | Must not happen — `ok` is the n8n-era **success** status; Stage 4 keys on `skip` alone |

---

## Risks

| Risk | Mitigation |
|------|------------|
| **`timestamp_utc` is absent or wrong for much of the population** | Stage 0a gate on a control sample where the true answer is known; the hive partition bounds the month independently |
| Backdated observations corrupt Plan 111/113 refresh state | Stage 0c, before anything is written; blocker if it fails |
| Backfill moves `int_price_history` and dbt aggregates | Expected and intended — ~36K new April observations; flagged so it is not a surprise |
| Plan 131 Stage 4 deletes sources first and the reparse 404s | Explicit sequencing rule; bytes remain safe in packs either way |
| Reparse starves the live scrape pipeline | Per-run cap and rate limit; the queue is shared with production |
| Effort spent recovering junk | Size split at 50 KB before any reparse; 0b identifies the stubs first |

---

## Out of Scope

- Fixing the events pipeline. May and June are clean; there is nothing to fix.
- Plan 131's packing efficiency. No re-pack path exists and none is proposed
  here — see Goal.
- `results_page` artifacts. Not measured; Plan 131 Stage 0b already found them
  to be 0.7% of objects.
- Reprocessing anything that already has a silver observation.
