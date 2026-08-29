# Plan 145 — Post-Mortem (Draft)

**Status: draft, gathered 2026-08-29. Plan 145 is not finished.** Stage 5 slice 3
Phase B is built but has never run; Stage 6 is unstarted; not one of the 1,172
legacy Parquet objects has been deleted. This is a mid-flight account assembled
so the narrative can be written while the evidence is still recoverable — not a
closeout.

**Sources.** Current plan, handoff, runbook and reference documents; the full git
history including reverted and superseded revisions; the VM at `147.224.199.86`
(tmux sessions, run logs, timings); and 20 Claude Code transcripts on this
machine spanning 2026-08-25 to 2026-08-29.

**One evidence gap.** No local transcript covers 2026-08-26 12:00–22:00, the
window in which the first implementation was built, merged and reverted. That
episode is reconstructed from git alone.

---

## 1. The story in one paragraph

A plan whose **goal never changed once** — delete 1,172 legacy Parquet objects
without losing anything that matters — took **four distinct designs, two
reverted merge commits, one thrown-away uncommitted implementation, 18 merged
pull requests, 79 commits, 7,562 lines of script and 5,513 lines of test** to
reach a state where it still has not deleted a single byte. Every design died
the same death: it trusted an identity key that measurement then disproved. The
final method works because it stopped joining on metadata altogether and
flattened the population into one directory of bytes first — an idea the
maintainer proposed, in plain language, after saying he felt like he was
"talking to a wall."

---

## 2. Timeline

| date | event |
|---|---|
| 2026-08-13 | [Plan 132](../plans/plan_132_unrecorded_artifact_recovery.md) created — 36,241 unrecorded bronze artifacts |
| 2026-08-17 | [Plan 137](../plans/plan_137_legacy_bronze_parquet_disposition.md) created — legacy Parquet disposition, never scheduled |
| 2026-08-21 | **Plan 145 created** (`136cb9f`, PR #227). Supersedes 132 and 137: "three investigations turn out to be one April 2026 cutover seen from three sides." Goal: delete **1,299** objects / 13.79 GiB. Stage 0 gates 0a–0f closed the same day |
| 2026-08-26 15:28 | First implementation merged — PR #255, `b21fd48`: a backfill processor, an April ledger script, and **changes to the production `detail_writer` and `silver_writer`** |
| 2026-08-26 19:36 | PR #256, `45f13a5` — "build complete April reconciliation ledger" |
| 2026-08-26 21:49 | **Both reverted**, one minute apart (`f946f88`, `1601379`). 1,443 + 662 lines removed, including a 435-line implementation plan |
| 2026-08-26 | **Design 2** (`7504f88`): the selective-recovery design is discarded for a staged recovery/write/repack/delete workflow |
| 2026-08-27 12:09 | Implementation restarts. Within 90 minutes the sidecar `listing_id` defect surfaces |
| 2026-08-27 | **Design 3** (`5c1162c`, PR #258): refuse all metadata, re-derive from bytes. Two-store union, 24.8 core-hours |
| 2026-08-27 16:30 | The maintainer proposes flattening. **Design 4** (`57baacd`) lands the same day |
| 2026-08-27 15:00–19:08 | Stage 2 `materialize --apply` — 4h10m, 807,797 objects |
| 2026-08-27 19:57–20:10 | Stage 3a `dedupe --apply` — 13 min, 371,095 distinct objects deleted |
| 2026-08-27 20:24–22:27 | Stage 3b `unpack --apply` — 2h03m, 557,065 members |
| 2026-08-27 | Stage 5b packer fix (`dd6aa26`); the compression question opens, closes, reopens and is deferred to a bounded trial — four commits in one evening |
| 2026-08-28 02:15–19:00 | **Stage 4 `parse --apply` — 16h45m**, 983,043 inputs, 5,738,532 rows |
| 2026-08-28 | Slices 1 and 2 built and merged (#265, #266); `--probe` added (#268, #269) so slice 2 could meet real data while the parse ran |
| 2026-08-28 | Slice 3 Phase A built (#271). **The block-page leak is found** (#272) — Stage 4's classifier was structurally dead for 59,458 objects |
| 2026-08-29 04:30–04:54 | **Authoritative `compare`** — `cmp-6c7c90d807bbdf13` |
| 2026-08-29 05:13 | Parser control runs for the first time and reports **FINDINGS** — diagnosed within the hour as the Plan 100 migration boundary |
| 2026-08-29 ~05:50–06:16 | **The VM wedges.** SSH dead for ~25 minutes, self-inflicted |
| 2026-08-29 06:25–06:33 | `assign --apply` (sequence +329,856, permanent) and `canary-sample` |
| 2026-08-29 06:41– | Phase B built (PR #274, open), two review rounds |

---

## 3. The goal never moved. The method moved four times.

This is the single most important fact for the narrative, and it is the plan's
own framing: *"The goal and success criteria have survived every revision
unchanged. The method has now changed three times, each time because a
measurement contradicted an identity key the previous method relied on."*

Counting the discarded pre-implementation design, there were four:

| # | design | dated | what it assumed | how it died |
|---|---|---|---|---|
| 1 | **Selective recovery** | 2026-08-21 | Stage 0f showed only 270 of 355,845 unmatched captures witness a price change, so recover ~11,600 information-bearing rows and drop the rest | Discarded 2026-08-26 as under-ambitious once deletion became the stated goal |
| 2 | **Metadata reconciliation** | 2026-08-26 | The pack sidecar's `listing_id` could carry a join between legacy Parquet and production | **Disproved:** sidecar `listing_id` is wrong for 194,639 of 371,095 content matches |
| 3 | **Two-store union by content hash** | 2026-08-27 | Refuse all metadata; parse the union of both stores deduplicated by hash | Correct but overbuilt — 24.8 core-hours, and it carried a two-store reconciliation through every stage |
| 4 | **Flatten first** | 2026-08-27 | Materialize, delete the twins, unpack — then everything downstream reads *one* store | In flight |

Two of these produced code that was thrown away: design 2's merged
implementation (reverted) and design 3's `parse` mode, which the Stage 3 handoff
records as *"reverted before commit; the patch is in the session scratchpad
only. Do not go looking for it."*

### The trust boundary is the scar tissue

Design 4 opens with a table that exists purely so no later stage re-derives a
dead end. It is the clearest artifact of the difficulty:

| key | verdict |
|---|---|
| Content bytes | Trustworthy — 807,797 legacy bodies agree with their stored hash, zero mismatches |
| Legacy `listing_id` / `fetched_at` | Trustworthy — corroborated in 194,734 of 194,734 disagreements |
| Sidecar `raw_sha256` / `artifact_id` / `fetched_at` | Trustworthy where present |
| **Sidecar `listing_id`** | **Unusable as identity** — but its NULL-ness is a reliable signal |
| **Legacy `artifact_id`** | **Unusable** — two separate `bigserial` sequences; the same integer names two different artifacts across the cutover |
| **Stored `sha256` of an empty body** | **Unusable** — the Plan 72 writer archived `b""` while copying the database hash (43,014 rows) |

---

## 4. Root cause: one bad `any_value`, four months earlier

`archiver/processors/pack_bronze_html.py` reduced silver with
`any_value(listing_id) GROUP BY artifact_id` and **no `source` filter**. One
detail artifact writes one `source='detail'` row plus ~5.7 `source='carousel'`
rows sharing that `artifact_id`, so `any_value` returned one of ~6.7 listings —
and usually the wrong one.

Correctness across all 144 packs: **April 31.4%, May 59.5%, June 9.8%, July
8.4%.**

Three things make this the plan's defining defect:

1. **It is invisible in production.** Artifact serving looks up by `source_key`
   and verifies `raw_sha256`; `check_index` validates member counts, frame
   ordinals and offset tiling. Nothing on the read path reads `listing_id`. It
   has never served a wrong byte.
2. **The packer's own tests could not catch it.** Every silver fixture gave one
   row per `artifact_id`, so `any_value` had nothing to pick wrong. The
   regression test added in `dd6aa26` deliberately writes the six carousel hints
   *before* the subject, so a reducer ignoring `source` cannot pass by luck of
   scan order.
3. **Its only real cost was to this plan.** As the plan says: *"Its one real
   cost is that sidecar identity is a trap for metadata joins — three revisions
   of this plan were lost to it."*

The fix could not be the obvious one. That same column is the packer's **sort
key**, and `PackWriter.add` seals a frame at a listing boundary — so a bare
`source = 'detail'` filter would silently reorder every pack. Stage 5b therefore
split identity from placement (`PackMember.cluster_key`), correcting the sidecar
**without relaying out a single pack**.

---

## 5. The compression question: settled, mechanised, reopened, deferred — in one evening

Four commits on 2026-08-27, each withdrawing the previous claim. Worth the
narrative space because it is the plan's best example of measuring rather than
asserting, *and* of how easy it was to measure the wrong thing.

| commit | claim |
|---|---|
| `71db6c6` | **"Settle the compression question — the scrambled order wins."** True-listing sort is 19.4% worse on a July pack, 3.2% on May |
| `27749ce` | **"Replace the compression mechanism with the measurement."** The previous commit asserted a mechanism without checking it. Measured: 2,591 scrambled ids over 25,860 members vs 6,078 true listings — a *coarser* clustering key, not a random one |
| `f0f8f3d` | **"Reopen the compression question — both tests were biased."** Production sorts a whole month then slices it into packs, so the member set was chosen *by* the sort under test. Restricting to listings wholly inside one pack halved the deficit to 8.4% — but that selection is biased the other way |
| `74ac0a1` | **Bound the trial.** Pack a fixed ~50,000-member subset both ways, discard both, run one full pass in the winner's order. Decision rule fixed before the run |

The rejected shortcut is documented too: comparing April's bytes-per-page against
other months cannot work, because achieved ratios (43.66x / 67.95x / 82.00x /
77.44x) do not track clustering quality at all — June is the worst-clustered
month and compresses best.

---

## 6. What the process caught — and what it caught late

### Caught by design

- **Every gate is fail-closed and scoped.** `compare` refuses to start until
  `parse_report.json` reports 1,204/1,204 units. `assign` and `apply` refuse any
  compare run whose report has no `blocked_excluded` section — which by
  construction is any run predating the block-page filter.
- **`--probe` was invented mid-flight** so slice 2 could meet real data during
  the 16-hour parse instead of finding its bugs on the run that mattered. The
  first probe `apply` failed immediately on `UndefinedTable:
  plan145_recovery_batch_receipts` — the production DB was one migration behind.
  A precondition surfaced by a rollback rather than by a half-written batch.
- **`--probe` and a real commit are mutually exclusive with no override.** The
  handoff's reasoning: *"A refusal a human can wave through will eventually be
  waved through."*
- **The receipt table exists because the staging tables are flushed and
  deleted**, so querying Postgres after an ambiguous client response cannot
  distinguish "never committed" from "committed and flushed away."
- **Neither production helper survived contact.** `shared.db.db_cursor` opens
  its own connection and commits on exit; `write_silver_observations_postgres`
  catches every exception and returns 0. Both were reimplemented rather than
  reused.

### Caught late

| finding | when | why it was missed |
|---|---|---|
| **The block-page leak** — Stage 4's classifier only fired when `url is None`, so 59,458 sub-1 KiB block pages were recorded as `parsed` against an expected ~54,341 | 2026-08-28, *after* Stage 4's 16h45m run completed | The classifier and the URL-construction line were 15 lines apart and could not both be true. Fixing it by re-parsing would have cost another ~17 hours; the filter went into `compare` instead |
| **Every never-fired check was structurally blind** | 2026-08-29 | Both probes ran on a materialized-only population, where the cohorts they guard are empty *by construction*. `unclassifiable 0`, `recovery duplicates 0` and `0 of 42,276 unattributed pack members import-bearing` were all artifacts of sampling, not results. The authoritative run put that last number at **36,220 of 42,276 — 85.7%** |
| **A gate that fired on dry runs** | slice 1 review | *"the run whose only job was to measure a cohort died with one sentence instead of reporting it."* Now a standing rule in every handoff: scope a refusal to `apply and not probe` |
| **The canary manifest froze counts, not rows** | PR #274 review | Flipping one selected row from carousel to detail keeps every count intact while minting a historical price event the sample never approved. Replaced by `write_set_digest` over the *built* write set — silver rows, price events and queue event, the exact tuples the three INSERTs send |
| **The migration prescribed for that fix destroyed its own subject** | PR #274 follow-up review | §6.1 told the maintainer to delete the frozen manifest and re-sample. Re-sampling *reselects* — determinism reproduces the selection only while every input is unchanged, which is the exact assumption the digest exists to distrust. New `canary-remanifest` mode migrates in place and never deletes |

---

## 7. The parser control: the check the whole plan rests on, and it failed

On 2026-08-29 the control ran for the first time against real data and exited
non-zero: **2,867 field disagreements over 498 compared rows — 46.8%.**

The diagnosis took under an hour and it is not a parse defect. [Plan
100](../plans/plan_100_historical_data_migration.md) migrated legacy observation
tables into silver for `fetched_at < 2026-04-21`, the date the Airflow
processing service went live. **April silver is a mix, and Plan 145's population
straddles the boundary.** The legacy schema carried `dealer_name`, `dealer_zip`
and `customer_id` and nothing else dealer-side — which is precisely the field
set that disagreed.

Split on the cutoff, over 19,872 exact-distance rows:

| | rows | disagree | mean fields |
|---|---:|---:|---:|
| `fetched_at >= 2026-04-21` | 11,665 | **4 (0.03%)** | 0.00 |
| `fetched_at < 2026-04-21` | 8,404 | 8,404 (100.0%) | 12.19 |

A competing hypothesis — that the control was matching a carousel row written by
a *different* page — was tested and refuted: `different_object` is **zero in
every bucket**.

**So the assertion Stage 5 rests on holds at 0.03%.** And the failure turned out
to be upside: the reparse recovers a mean of **12.19 silver fields per
pre-cutoff row** that the legacy pipeline never captured. The maintainer's
reaction, in the moment — *"It'd be a shame to throw it away after we spent 16
hours creating it"* — became [Plan
157](../plans/plan_157_april_reprocessing_enrichment.md), blocked on Plan 125's
Iceberg migration.

A side finding, recorded and not chased: migrated carousel rows carry
`make`/`model`, which Plan 100's own schema table says they should not. *"Plan
100's schema tables should not be trusted as a description of what is actually
in the lake."*

---

## 8. The near-duplicate cohort: the plan's own explanation was wrong

For two days the plan carried the reading that 21.3% of `to_import` rows with a
neighbour within 300 s were "most likely genuine burst re-scrapes." Measured
directly on the authoritative run:

| pair type | pairs | identical values | what it is |
|---|---:|---:|---|
| carousel ↔ carousel | 82,280 | 82,249 (100.0%) | one listing in two pages' carousels, same pass |
| carousel ↔ detail | 14,415 | **0 (0.0%)** | a card and a full page — different observations |
| detail ↔ detail | **105** | 105 | the burst re-scrape case the plan assumed |

**The assumed mechanism is 0.11% of the cohort.** The conclusion — do not
collapse — survived; the reason did not. The recommendation to import all of
them rests on a verified fact rather than an intuition: production writes this
shape today with no deduplication anywhere in the live path (no `ON CONFLICT` in
`_INSERT_SQL`, no uniqueness on `(listing_id, fetched_at)`, no dedup in the
flusher or `stg_observations.sql`).

---

## 9. Cost and scale

### Population arithmetic

| | count |
|---|---:|
| legacy detail Parquet objects to delete | 1,172 (13.66 GiB) |
| legacy row occurrences | 951,821 |
| distinct successful captures with bytes | 797,073 |
| materialized objects | 807,797 |
| deleted as content twins (Stage 3a) | **371,095** |
| materialized survivors | 425,978 |
| April pack members unpacked (Stage 3b) | 557,065 |
| **flattened population** | **983,043** |
| parsed observation rows | 5,738,532 |
| `already_represented` / `to_import` / `blocked_excluded` / `unclassifiable` | 4,977,697 / **701,375** / 59,460 / **0** |
| artifacts assigned (13,253 preserved + 328,650 allocated) | 341,903 |
| unrecoverable by construction, accepted as closed loss | 11,453 |

### Wall clock, from the VM logs

| stage | elapsed |
|---|---|
| Stage 2 `materialize` | 4h10m |
| Stage 3a `dedupe` | **13 min** |
| Stage 3b `unpack` | 2h03m |
| **Stage 4 `parse`** | **16h45m** |
| Stage 5 `compare` (dry + apply) | 8m23s + 13m19s |
| `control`, `assign`, `canary-sample` | minutes each |

**The parse's tail is worth a sentence in the narrative.** Units 1–1,170 (the
materialized shards) finished in 7h20m. Units 1,171–1,204 — the 32 unpacked pack
shards, ~17,400 members each — took **9h20m**: 2.7% of the units, 56% of the
wall clock. That skew is also why every probe was structurally blind (§6): the
unpacked shards sort last.

### Code

| | at first commit | now |
|---|---:|---:|
| `scripts/reconcile_april_detail.py` | 876 | **7,562** |
| `tests/scripts/test_reconcile_april_detail.py` | 496 | **5,513** |

Plus `scripts/verify_recovery_live_state.py` (274), three real-Postgres
integration suites (1,183), one Flyway migration, and **13 CLI modes**: `census`,
`materialize`, `dedupe`, `unpack`, `parse`, `compare`, `assign`, `apply`,
`control`, `canary-sample`, `canary-remanifest`, `canary-commit`,
`canary-flush-verify`.

**Documentation is comparable in size to the code**: 1,679-line plan, six
handoffs (1,332 lines), a 796-line run sheet, a 250-line flag reference.

### Process

79 commits mentioning plan-145 · 18 merged PRs (#227, #255–#272) + 1 open (#274)
· **2 reverted merges** · 5 review rounds that produced follow-up fix commits.

---

## 10. Operational incidents

**The CI failure the maintainer walked in on (2026-08-27 23:46).** Splitting
`listing_id` from `cluster_key` widened `fetch_member_metadata`'s yield from four
values to five; two *integration* stubs still built four. The unit suite had been
run with `-m "not integration"` and reported clean — *"Running the unit suite is
not evidence about a shared contract, and I presented it as if it were."* Most of
the hour that followed was not work: a CI monitor with a broken exit condition
sat through its full 15-minute timeout after the checks had already completed.

**The VM wedge (2026-08-29 ~05:50–06:16).** Two heavy DuckDB jobs ran
concurrently on a 24 GB box already carrying the full production stack — a
subagent's fan-out comparison downloading ~3,500 Parquet objects, plus the main
session's cross-artifact scan, each with its own multi-GB memory limit. Memory
pinned near 100% and CPU saturated from ~00:47 to ~01:15 local, with a gap in the
memory series where the box was too starved to scrape its own metrics. TCP
port 22 stayed open while sshd never sent a banner. **The box recovered on its
own — containers showed `Up 3–11 days`, and OOM count was 0**, matching the known
thrash-without-OOM-kill pattern on this host.

The root cause of the load was an instruction that was simply wrong: the subagent
was told "DuckDB cannot read `s3://` here — download the objects first," when
`shared/duckdb_s3.py::get_duckdb_s3_connection()` configures httpfs for MinIO.
Run correctly, the same query took **29 seconds**.

The maintainer's reaction is part of the record — *"The instance was stopped. You
fucking nuked the server"* — and so is the correction four minutes later: *"I
might have been confused. I'll own that one."* The instance had not been stopped.

**Friction worth noting.** tmux was unusable from the Mac at first ("too many
lines to see anything… ctrl bd, option bd, and command bd do nothing"), an
`~/p145status.sh` helper had to be written to make progress legible, and
`docker exec` vs `compose run` earned its own runbook section after it was found
to run stale code.

---

## 11. Where it stands

> **Superseded within hours of being written (2026-08-29 14:40–15:30 UTC).**
> Everything this section calls unproven was proven and committed the same day:
> the V040 window ran twice, the canary committed and its flush round trip
> verified by key, `apply` gained the canary exclusion, and the full apply
> committed all 69 batches. The authoritative record is the plan document,
> *Evidence — slice 3 Phase B and the full apply, 2026-08-29*. The text below is
> left as written, because when it was written it was true, and the gap between
> these two paragraphs is itself part of the account.


**Done and irreversible:** 371,095 objects deleted (recoverable from the packs);
557,065 members unpacked; V047 applied; `ops.artifacts_queue_artifact_id_seq`
advanced twice, by 292,432 (probe) and 329,856 (authoritative) — sanctioned
`bigserial` gaps, not reuses.

**Committed to production tables: nothing.** `plan145_recovery_batch_receipts` is
empty; `staging.artifacts_queue_events WHERE status='recovered'` is 0.

**Unproven, per the run sheet's own §8:**

- the write canary as a real commit, and the flush round trip into
  `silver_normalized/observations/` and `ops_normalized/` — built and tested,
  never run;
- the V040 before/after equality — needs Phase B plus a maintainer-opened
  maintenance window with writers quiesced;
- the duplicate-write interaction between a committed canary and a later full
  `apply` of the same batches (§7.6) — the canary's 234 artifacts also belong to
  slice-2 batches, and `apply` does not yet read the canary's commit record;
- the full apply, Stage 5b's trial, and all of Stage 6 — repack, prune, and the
  deletion that is the entire point.

**Two maintainer rulings are recorded but not made:** the near-duplicate cohort
and the carousel fan-out. The run sheet states plainly that `assign --apply` —
which advanced the sequence permanently — *was run without one*.

**Work this plan spawned:** [Plan 156](../plans/plan_156_block_page_detection.md)
(fix `_detect_challenge`'s blindness to non-Cloudflare blocks), [Plan
157](../plans/plan_157_april_reprocessing_enrichment.md) (the 12.19-field
enrichment, blocked on Plan 125), CAR-28 (the packer defect), and an unhomed
follow-up: the parser control needs a scope predicate before its exit code means
anything again.

---

## 12. Draft lessons — for the maintainer to cut or keep

1. **Every design that failed, failed on identity.** Bytes and hashes never lied;
   every derived key did. The plan only stabilised once it stopped joining and
   started flattening.
2. **A defect invisible in production can still be the most expensive thing in
   the repo.** The `any_value` reduction has never served a wrong byte and cost
   three design revisions.
3. **A sample that cannot contain the cohort you are guarding proves nothing.**
   Three separate "0" results were structural, not empirical, and one of them was
   really 85.7%. The probes should have been forced to include unpacked shards —
   the parse's tail ordering made that inconvenient, which is exactly why it
   should have been deliberate.
4. **A refusal that fires on a run which writes nothing protects nothing.** This
   became a standing rule after it cost a measurement run.
5. **Freeze the product, not its summary.** Counts, aggregates and re-derivable
   selections are not contracts; `write_set_digest` over the built write set is.
6. **Fixing the thing that produced the frozen artifact can destroy the artifact.**
   The re-sample instruction would have deleted the only record of the V040
   window's subject before set equality was established.
7. **The narrative asset is the handoff.** Six handoff documents, each written to
   be executable by a session with no context, are why four days of work across
   many sessions and two models stayed coherent. They also carry the honest parts
   — "expect ~760, and if it is materially larger, stop and say so."
8. **The maintainer's plain-language restatement beat the model's design twice.**
   Flattening (2026-08-27 16:30) and doing the deletion before the unpack to save
   space (16:38) both came from the human, in one sitting, after he asked: *"I
   want to understand why this has been so complicated. I feel like we've taken 8
   stabs at this, each of them better, but each of them convoluted, when what
   we're really trying to do is pretty simple."*

---

## 13. Open questions for the narrative

- Was the 2026-08-26 revert the right call, and what did it cost? Reconstructing
  it needs Linear (CAR-13) or the PR discussions — no transcript survives locally.
- Would forcing an unpacked shard into the early probes have been cheap? If yes,
  the block-page leak and the 42,276-member finding were both catchable a day
  earlier.
- Was 16h45m of parsing the price of correctness, or of the flatten-everything
  design? Design 3 costed 24.8 core-hours for the same answer, so flattening was
  cheaper — but neither was ever compared against a targeted reparse of only the
  captures silver lacks.
- What is the honest estimate-versus-actual? `docs/PLANS.md` still carries Plan
  145 at effort **M**.
