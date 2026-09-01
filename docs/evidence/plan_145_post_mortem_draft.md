# Plan 145 — Post-Mortem (Draft)

**Status: closed. Plan 145 finished 2026-08-30 and was archived that morning.**
All 1,172 legacy Parquet objects are deleted,
`html/year=2026/month=4/artifact_type=detail_page/` is empty, and April went from
24.48 GiB to 4.34 GiB. The first two revisions of this document were written
mid-flight, so the narrative could be assembled while the evidence was still
recoverable; this one closes it against the finished plan. Sections dated before
2026-08-30 are left as they were written, and marked where the run has since
answered them.

**Sources.** Current plan, handoff, runbook and reference documents; the full git
history including reverted and superseded revisions; the VM at `147.224.199.86`
(tmux sessions, run logs, timings); and 20 Claude Code transcripts on this
machine spanning 2026-08-25 to 2026-08-29.

**Added 2026-09-01, from sources that did not exist when the draft was written:**
Stage 6's evidence sections in the plan document, the [2026-08-30
recap](../recaps/2026-08-30.md), the archive entry in
[`completed_plans.md`](../planning/completed_plans.md), and [Plan
149](../plans/plan_149_linear_execution_layer.md)'s closed cycle-1 measures,
which answer two of §13's open questions.

**One evidence gap, now partly closed.** The window of 2026-08-26 12:00–22:00 —
in which the first implementation was built, merged and reverted — was recorded
here as reconstructed from git alone. A local transcript does cover
**17:23–17:50** of it: the deploy of that implementation to production and the
point at which it could not run. §3.1 sets out what it shows. The decision to
revert, four hours later, remains unrecorded.

---

## 1. The story in one paragraph

A plan whose **goal never changed once** — delete 1,172 legacy Parquet objects
without losing anything that matters — took **four distinct designs, two
reverted merge commits, one thrown-away uncommitted implementation, 23 merged
pull requests, 76 non-merge commits, 9,410 lines of script and 7,107 lines of
test** to delete 14,670,223,837 bytes in a command that ran for under a minute.
Every design died the same death: it trusted an identity key that measurement
then disproved. The final method worked because it stopped joining on metadata
altogether and
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
| 2026-08-26 17:23–17:50 | Design 2 is **pulled onto the VM, rebuilt and health-gated into production** — then the ledger scan cannot start, because the legacy Parquet prefix is written down nowhere. See §3.1 |
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
| 2026-08-29 06:41–08:00 | Phase B built (PR #274). **Four review rounds**, each finding a real hole — see §6 |
| 2026-08-29 14:40 | Deploy intent declared; in-flight work drained from 400 to 0 |
| 2026-08-29 14:4x | **V040 window run 1** — PASS, then rolled back deliberately |
| 2026-08-29 14:51:23 | **V040 window run 2** — PASS, kept. The canary's 505 rows are committed |
| 2026-08-29 ~14:55 | Flush; `canary-flush-verify` finds all 505 / 140 / 234 in the lake by key |
| 2026-08-29 15:17 | PR #276 merged — `apply` skips what the canary already wrote |
| 2026-08-29 15:20–15:30 | **The full apply.** 69 batches in 7 chunked rounds. 341,903 artifacts / 701,375 rows committed |
| 2026-08-29 | Stage 6 dry runs — all four modes read-only on `master` at `64631de`. `fetch_member_metadata` returns 843,439 lake-described members: exactly the 551,009 + 292,430 the corrected gate had derived before anything ran |
| 2026-08-29 | **`pack-trial --apply`** — 18m02s, and the ordering trial **splits** (`d7d4e7a`). The incumbent clustering carries |
| 2026-08-29 17:56–20:17 | **The repack** — 2h20m. 68 packs, 983,043 members packed and 983,043 verified, 0 read failures. `repack-verify` returns PASS |
| 2026-08-29 | `retire-packs --apply` — the 32 superseded packs and their 32 sidecars deleted, 64 receipts, none absent |
| 2026-08-29 23:43 – 08-30 02:26 | **The prune** — 2h43m. 983,043 loose objects deleted, 983,043 verified, **0 refused** |
| 2026-08-30 03:53 | **`delete-legacy --apply`** — 1,172 planned, 1,172 deleted, 1,172 reconciled, 0 refused, in under a minute. **This is the thing the plan existed to do** |
| 2026-08-30 | Plan 145 archived (`066d3e2`). April: 24.48 GiB → 4.34 GiB |

---

## 3. The goal never moved. The method moved four times.

This is the single most important fact for the narrative, and it is the plan's
own framing, and the sentence survived unchanged into the archived version:
*"The goal and success criteria have survived every revision unchanged. The
method has now changed three times, each time because a measurement contradicted
an identity key the previous method relied on."*

Counting the discarded pre-implementation design, there were four:

| # | design | dated | what it assumed | how it died |
|---|---|---|---|---|
| 1 | **Selective recovery** | 2026-08-21 | Stage 0f showed only 270 of 355,845 unmatched captures witness a price change, so recover ~11,600 information-bearing rows and drop the rest | Discarded 2026-08-26 as under-ambitious once deletion became the stated goal |
| 2 | **Metadata reconciliation** | 2026-08-26 | The pack sidecar's `listing_id` could carry a join between legacy Parquet and production | **Disproved:** sidecar `listing_id` is wrong for 194,639 of 371,095 content matches |
| 3 | **Two-store union by content hash** | 2026-08-27 | Refuse all metadata; parse the union of both stores deduplicated by hash | Correct but overbuilt — 24.8 core-hours, and it carried a two-store reconciliation through every stage |
| 4 | **Flatten first** | 2026-08-27 | Materialize, delete the twins, unpack — then everything downstream reads *one* store | **It did not die.** It carried the plan to completion on 2026-08-30 |

Two of these produced code that was thrown away: design 2's merged
implementation (reverted) and design 3's `parse` mode, which the Stage 3 handoff
records as *"reverted before commit; the patch is in the session scratchpad
only. Do not go looking for it."*

### 3.1 What the 2026-08-26 revert looked like from inside — recovered

One local transcript covers **2026-08-26 17:23–17:50**, four hours before the
reverts. It is short and it is damning, and it changes the previous revision's
claim that this episode is reconstructible from git alone.

Design 2's implementation was not merely merged. It was **merged, pulled onto
the VM, rebuilt into the `archiver` and `processing` images, and health-gated
into production** — the maintainer's opening line is *"The code is merged, I'll
pull on the VM."* The deploy succeeded. `docker exec cartracker-archiver python
-c "import scripts.build_april_ledger as m; print(m.__file__)"` returned
`/app/scripts/build_april_ledger.py`; the verify-the-loaded-code check passed.

Then the run could not start, because **nobody knew where the input was.**
The assistant said so before guessing, and the caveat is worth quoting in full
because the guess followed anyway:

> I do **not** have the literal legacy-Parquet prefix confirmed anywhere in the
> docs or code — Plan 137 only describes it in prose ("the bronze HTML prefix,"
> hour-partitioned `part-<uuid>-0.parquet`). Don't let me guess that string;
> list it first.

The listing came back with twelve top-level prefixes — `ci_snapshots/`,
`dictionaries/`, `html/`, `html_packs/`, `lakehouse_spike/`, `mlflow/`,
`ops_normalized/`, `scratch/`, `silver_normalized/`, `snapshot_archives/`,
`snapshot_exports/`, `snapshot_planning_cache/` — and nothing in the list said
which one it was. The assistant's own reading: *"None of these jump out as
'legacy Parquet' by name."* It then proposed drilling into `html/` as *"the
most likely candidate"* — a guess, one message after warning against guessing.
The transcript ends there, at 17:50, with the ledger scan never run.

**What this establishes, and what it does not.** It establishes that the
design-2 implementation reached production and was then found to be
**un-runnable against the population it existed to reconcile**, because the
plan it was built from — Plan 137 — described the legacy lake only in prose and
never pinned an object prefix. It does not establish that this is *why* the
maintainer reverted four hours later; both revert commits carry bare
`git revert` messages, PR #255 and #256 have **zero review comments**, and no
transcript covers 17:50–21:49. But it is the last recorded state of that work
before it was removed, and it is a much better lead for §13's first open
question than "git alone."

**The guess was right, and design 4 still refused to make it.** The legacy root
is `html/`, exactly the prefix the assistant picked. That is not the
vindication it looks like — design 4's Stage 1 opens with a `census` mode whose
entire job is to *discover* the prefix by listing, and the constant carries a
comment explaining why:

> The Plan 72 archiver wrote `s3://<bucket>/html` partitioned by year, month and
> artifact_type. pyarrow renders int32 partition values unpadded (`month=4`),
> but the exact spelling is discovered by listing rather than assumed — **a
> hardcoded guess that matches nothing would look like an empty population
> instead of a failure.**
> — `scripts/oneoff/reconcile_april_detail.py:152`

That is the entire lesson of 2026-08-26, compiled into a comment above a
constant, three days later: the failure mode of guessing an input location is
not an error, it is a **silent zero**. Design 4 also pins `BASELINE_OBJECTS =
1172`, `BASELINE_ROWS = 951821` and a status census as *assertions about
production*, "restated so drift is a stop rather than a shrug." Design 2 had
none of that and could not have; nobody had yet been burned.

**The revert diffstat sharpens what was being backed out.** The plan calls it
"changes to the production `detail_writer` and `silver_writer`"; the numbers
say more. Reverting #255 restored `processing/writers/detail_writer.py` across
**471 changed lines** and `silver_writer.py` across 69 — design 2 had
restructured two live production write paths so they could carry a historical
backfill mode, added `processing/queries.py` and a new
`insert_backfill_price_observation_event.sql`, and shipped
`tests/processing/test_detail_writer_backfill.py` at **two different paths at
once**. Design 4, by contrast, reimplemented both helpers inside the
reconciliation script and left the production writers untouched (§6). The
distance between those two answers is most of what the plan spent four days
learning.

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

## 5. The compression question: settled, mechanised, reopened, deferred — then refused

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

### The answer, 2026-08-29 — and it was a refusal

`pack-trial --apply` ran the bounded trial as `trial-5fbadb36972161fb`, 18m02s,
on the production dictionary, level and frame target. Both draws, same
population, same dictionary, same frame target — only the 50,000 members differ:

```
drawn in current  true ordering is larger  by 27.50%
drawn in true     true ordering is smaller by  4.76%
```

**A 32-point swing from sample selection alone, larger than the effect under
test.** This is the bias the plan's own Caveats section predicted and could not
size; the trial sized it, and it is bigger than the signal. The decision rule,
fixed before the run, returns `current`, so the incumbent clustering carried and
`fetch_member_metadata` was left alone. The trial wrote no pack — both sets were
built in memory and discarded, verified afterwards against a prefix still holding
exactly 32 `.zpack` and 32 `.idx.parquet`.

**The plan originally specified one sample** — *"the first ~50,000 members of the
flattened population"* — which taken in stored order is the `current` draw. It
would have reported true ordering 27.50% worse and closed the question on an
artefact. The two-sample rule is what turned an answer into a refusal.

The mechanism is the coarse-key asymmetry the plan had already measured: the
scrambled `cluster_key` averages 9.98 members per value against the true
listing's 4.25. A contiguous draw in `current` order captures whole coarse
clusters, and re-sorting by true listing shatters them; a draw in `true` order
captures fine clusters spread across partial coarse ones, so re-sorting costs
less. **A bounded trial therefore measures containment at least as much as
ordering** — which is a caveat for any future trial of this shape, not just this
one.

For the question the plan left open about May, June and July: in its most
favourable condition true ordering wins 4.76%, about 0.33 GiB against 6.86 GiB
for a full repack of ~3M members. Not a decision — 50,000 members is not a
month-global sort — but the first evidence that the ceiling is low.

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
| **The migrated manifest was trusted because it existed** | PR #274, round 3 | Resolution picked the digest-bearing sibling on existence alone, and the proof that it preserved the frozen object set lived in a separate report no consumer read. A substituted sibling could be pinned and committed. The promotion is now re-derived from the two manifests at consumption time |
| **…and proving the object set was not enough** | PR #274, round 4 | A sibling can keep every object key and still change `detail_rows`, `strata` or `artifact_id`, carrying digests that agree with equally-mutated inputs — everything downstream compares against those *current* inputs. Every field the frozen manifest carried is now compared, per object key |
| **The canary's own commit set up a duplicate write** | PR #276 | Receipts are keyed by *batch name*. The canary commits under `<run>-canary` while its 234 artifacts also sit in `b00001`–`b00069`, so the full apply would have written those 505 observations twice and nothing downstream would have noticed. Flagged when Phase B was built; fixed only when the apply was imminent |
| **The documented invocation could not run** | the window itself | The run sheet said `python scripts/verify_recovery_live_state.py` on the VM host. There is no `python` there, only `python3` — and no `psycopg2` and no venv, so it could never have imported `shared.db`. Three failures deep in one line, found by the maintainer typing it under time pressure |

### 6.4 Three more findings, from the 08-28 transcripts

The first two come from PR #272's review rounds and neither is in the table
above; the third is about this repository rather than this plan. All three
generalise past Plan 145.

**A test double that cannot express absence makes every fail-closed gate
untestable.** `_patch_slice2_io`'s `read_json` mock raised `KeyError` for a key
it did not hold. The real `shared.minio` helper returns `None`. So no test
could construct "the object is not there," and every refusal keyed on
absence — the whole fail-closed family — was unprovable while looking
thoroughly tested. The gate that mattered read `if report and "blocked_excluded"
not in report: raise`, which **fails open** on a run with no report at all. It
was re-keyed to `if apply and "blocked_excluded" not in report: raise` and the
mock taught to return `None`. The defect was in the fixture, not the feature,
and it had made the feature's own tests lie.

**A row-level guard behind an object-level filter is not a backstop.**
`compare`'s block-page filter quarantines whole objects; the fallback guard in
`_scan_to_import` was per-row and detail-only. When a block page emits carousel
rows, its detail row — the only row carrying the block signature — can land in
`already_represented` while the junk carousel rows alone reach `to_import`, and
the per-row check never sees them. The coverage of that gap depended on
`objects_that_emitted_carousel_rows`, the one quantity the session had already
decided not to measure (§6.5, the abandoned SSH check). The fix was not to
measure it: `assign`, and later `apply`, simply refuse any compare run whose
report has no `blocked_excluded` section. That refusal is what §6 calls
"fail-closed and scoped" — this is where it came from, and it came from a reviewer,
not from the builder.

**And the escape hatch has an escape hatch.** This repo blocks bare `git commit`
with a hook that says: *"BLOCKED: this repo requires the commit-plan-attribution
workflow for every commit. Read this, then re-run the same command with
`COMMIT_SKILL_OK=1` in front of it."* The guard's own error message hands over
the bypass. That is the plan's own §12.4 rule — *a refusal a human can wave
through will eventually be waved through* — turned back on the repository's
tooling, and it is worth a line in the narrative because the plan spent four
review rounds building refusals with no override while working inside a
tool-chain whose refusals all have one.

### 6.5 How the work was actually organised

This is not in the plan documents and it is the thing most likely to be
interesting to a reader outside the project. Nine review rounds finding nine
real defects is not luck; it is a topology.

**Roles were split across sessions, and the split was deliberate.**

| role | who | evidence |
|---|---|---|
| build | a Sonnet session, given one handoff document and nothing else | *"Hello, please read and implement …canary_handoff.md"* — the entire opening prompt |
| review | a separate Opus session, started cold, given the spec and told to read the diff against it | *"A Sonnet session built it from a handoff; I want a cold, independent read against that handoff and against reality."* |
| operate | a third session with SSH to the VM, holding the run state | *"ssh into the VM, inspect the active tmux sessions, and report back with next steps"* |

The reviewer never saw the builder's reasoning — only the handoff and the diff.
That is why the reviews found contract holes rather than style: the reviewer had
the same specification the builder had, and no access to the story the builder
told itself about satisfying it.

**The review prompt was itself a written artifact, and it did most of the
work.** The prompts run to ~90 lines. They name the spec files *in reading
order*, declare scope in an IN/OUT list ("OUT: executing the write canary;
anything that commits to Postgres; merging the branch"), enumerate the
non-negotiables to check **"verified in code not prose"**, and — the part worth
stealing — close with a mutation test on the tests themselves:

> Would a plausible mutation (drop a field from the ignore list; take the two
> snapshots in two transactions; split an artifact; skip the window check) fail
> exactly one test and nothing else? Call out any test that passes without
> proving its claim.

For a second review of already-reviewed code, the prompt adds: *"Two rounds
already happened and several findings were fixed. Do NOT re-report what they
covered — I want what they missed plus your own read of the whole."* Each round
was therefore forced onto new ground rather than re-litigating the last one.
That is the mechanism behind "nine review rounds, nine real findings."

**The handoff is the interface — and it went stale in hours, not days.** §12.7
credits the handoffs, and it should; the qualifier is that they decayed at the
speed of the run. The Stage 5 canary handoff was written on 2026-08-28 and
audited **the same morning**. Every situational number in it was already false:
it said Stage 4 was at 426/1,204 (it was at 1,188), that the 32 unpacked shards
had not started (they had finished), that slice 2 "cannot run for days" (it had
already run end to end), and it did not mention `--probe`, which had not existed
when it was written. Its stated *rationale* was gone too — "three slices merged
and none run against production data, Phase A is the mitigation" had been
discharged by the probe runs.

The refresh is the instructive part: the phase ordering was **kept** and the
reason for it was **replaced** — *"Phase A is worth doing first because the
parser-control assumption is the deepest one the probe did not test, not because
untested code needs de-risking."* A handoff whose numbers are refreshed but
whose rationale is not is the more dangerous artifact, because it will look
current.

**Friction, recorded because it is representative.** Three exchanges on 08-28,
all inside twenty minutes, all the same shape — the model doing an adjacent
thing well instead of the requested thing:

> *"I didn't ask for a code review. The review is done"*
> *"Why a stacked PR? That isn't what I asked for."*
> *"no. I've added a review on the new branch, please review it and commit any
> fixes required TO THAT PR."*

And one of the model's own, after the maintainer asked *"What are you trying to
chase down?"* mid-investigation: *"Sorry — I should have said. Here's the whole
picture; the last check was a nice-to-have footnote, not load-bearing."* The
finding it had been sitting on for six minutes was the block-page leak. The cost
of not leading with it was small here; the pattern is that the session held a
Stage-4-invalidating result while visibly working on a footnote.

The same exchange contains a good decision, worth keeping alongside the bad
ones: the session **abandoned** the carousel-emission check rather than let it
run — *"it's reading 1,204 objects one at a time over SSH, and the answer isn't
worth the wait… object-level quarantine is correct whether the count is 0 or
not."* It scoped the fix so the unmeasured quantity stopped mattering. A reviewer
then found that the fallback guard still depended on exactly that quantity
(§6.4 above), which is the honest end of the story: the decision was right and it was
not free.

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
| members repacked and verified (Stage 6) | 983,043 |
| replacement packs | 68 |
| superseded packs retired | 32, plus 32 sidecars |
| loose objects pruned | 983,043, **0 refused** |
| **legacy Parquet objects deleted** | **1,172 / 14,670,223,837 bytes** |

### Wall clock, from the VM logs

| stage | elapsed |
|---|---|
| Stage 2 `materialize` | 4h10m |
| Stage 3a `dedupe` | **13 min** |
| Stage 3b `unpack` | 2h03m |
| **Stage 4 `parse`** | **16h45m** |
| Stage 5 `compare` (dry + apply) | 8m23s + 13m19s |
| `control`, `assign`, `canary-sample` | minutes each |
| Phase B — migrate, dry run, two windows, flush verify | ~50 min, most of it drain |
| **Stage 5 `apply` — all 69 batches** | **~10 min** (4.2 s/batch, 7 chunked rounds) |
| Stage 6 `pack-trial` | 18m02s |
| Stage 6 repack — 68 packs | 2h20m |
| Stage 6 `retire-packs` | minutes |
| Stage 6 prune | **2h43m** (29 min listing, 2h13m draining 68 packs) |
| **Stage 6 `delete-legacy`** | **under a minute** |

**The two moments the plan existed for were its two fastest.** The apply — ten
minutes for 701,375 rows — and the deletion itself, under a minute for 13.66 GiB.
Roughly thirty hours of measured compute, four days of design and 7,000 lines of
test stood behind eleven minutes of writing and deleting. The cost was never in
the destructive step; it was in earning the right to take it.

**The parse's tail is worth a sentence in the narrative.** Units 1–1,170 (the
materialized shards) finished in 7h20m. Units 1,171–1,204 — the 32 unpacked pack
shards, ~17,400 members each — took **9h20m**: 2.7% of the units, 56% of the
wall clock. That skew is also why every probe was structurally blind (§6): the
unpacked shards sort last.

### The probe run, and how far it was off

The probe (`cmp-e37723ede49fad4f`, 2026-08-28) is the only Stage 5 measurement
this machine holds a transcript for, and setting it beside the authoritative run
shows what a structurally-blind sample buys and what it costs.

| | probe, 08-28 | authoritative, 08-29 |
|---|---:|---:|
| parsed rows compared | 4,455,248 | 5,738,532 |
| `already_represented` | 3,980,701 | 4,977,697 |
| `to_import` | 474,547 | 701,375 |
| `blocked_excluded` | — (family did not exist) | 59,460 |
| `unclassifiable` | 0 | 0 |
| artifacts assigned | 294,325 | 341,903 |
| batches | 59 | 69 |
| preserved / allocated | 1,893 / 292,432 | 13,253 / 328,650 |
| near-duplicate pairs ≤300 s | 67,994 over 7,483 listings | 96,800 |
| pack members newly attributed | **0 of 42,276** | **36,220 of 42,276 (85.7%)** |

The last row is §6's structural blindness in one line, and the second-to-last is
its quieter twin: the probe's near-duplicate cohort was 70% of the real one and
was already being carried into the maintainer's ruling.

**The carousel fan-out is the counter-example — measurement that worked.** The
canary handoff had been written against *"biased probe 5.25 / 9 on one shard"*;
the probe replaced it with **5.6332 carousel rows per object over 671,657
objects, max 8**. That number was correct and survived to the authoritative run,
because fan-out is a property of the page, not of which shards the sample
reached.

**The probe's real yield was three preconditions, not three numbers.** It caught
V047 unapplied on production (`UndefinedTable`, fixed at 15:56:39 UTC via
`docker compose run --rm flyway`); it type-checked the entire write set — the
`::uuid` cast, both `NOT NULL`s, both CHECKs — against real production rows and
rolled back; and it proved authoritative/probe prefix isolation directly
(`compared/` 0 objects, `assigned/` 0 objects, 0 `recovered` rows, 0 receipts)
rather than by argument. It also left a residue that later mattered: **59
assignment shards written under `assigned_probe/` from a pre-block-page-filter
compare**, still sitting on the VM and reachable by `apply --run-id`. That
residue is precisely what the stale-compare-run refusal in PR #272 was built to
stop.

One cosmetic defect is recorded here because it is the kind that survives:
`compare --probe --apply`'s success line printed `wrote compared/…,
inventory/….json` **without** the `_probe` suffix
(`_print_compare_report`, `scripts/oneoff/reconcile_april_detail.py:3935`). The
data went to the right place — the isolation check proves it — but the operator's
only on-screen confirmation of isolation said the opposite of the truth. It was
never fixed: the line is still there in the archived script, which is the honest
fate of a cosmetic defect on a one-off tool.

### Code

| | at first commit | now |
|---|---:|---:|
| `scripts/oneoff/reconcile_april_detail.py` | 876 | **9,410** |
| `tests/scripts/oneoff/test_reconcile_april_detail.py` | 496 | **7,107** |

Plus `scripts/oneoff/verify_recovery_live_state.py` (283), three real-Postgres
integration suites (1,273), one Flyway migration, and **17 CLI modes**: `census`,
`materialize`, `dedupe`, `unpack`, `parse`, `compare`, `assign`, `apply`,
`control`, `canary-sample`, `canary-remanifest`, `canary-commit`,
`canary-flush-verify`, `pack-trial`, `repack-verify`, `retire-packs`,
`delete-legacy`.

Five of those seventeen modes exist only so ~500 rows could be committed safely.
Whether that was proportionate is §13's question, not this section's.

**Both files moved after the plan closed.** [Plan
162](../plans/plan_162_testing_census_and_restructure.md) split `scripts/` into
production and spent, and this one — a one-off by construction, run to completion
and never to run again — went to `scripts/oneoff/` with its suite (`e954306`).
The eight handoffs moved to `docs/prompts/` under [Plan
146](../plans/plan_146_planning_system.md) (`538c9fa`). Every path in this
document is written as the tree stands today.

**Documentation ran to a third the size of the code**, which is the honest
ratio and larger than it sounds: a 2,485-line plan, eight handoffs (2,084
lines), two run sheets (1,356), a 310-line flag reference — **6,235 lines
against 18,073** of script, test and integration suite, before counting this
post-mortem.

### Process

**76 non-merge commits** mentioning plan-145 on `master`, 100 including merges ·
**23 merged PRs** (#227, #255–#283) · **2 reverted merges** · **9 review rounds**
that produced follow-up fix commits — four of them on PR #274 alone, each finding
a real defect in the canary's contract.

**The review rounds were run by a separate cold session against the same
handoff the builder had; §6.5 describes the topology and reproduces the review
prompt.** That is the part of the process most worth carrying to the next plan,
and it is currently written down nowhere but here.

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

**The window, and the rollback that was not a mistake (2026-08-29 14:4x).** The
first V040 run passed, and the maintainer then ran the rollback block —
deliberately, because the commit had only just landed and rolling back while the
flush is still held is one transaction rather than two systems. That is exactly
what holding `hourly_analytics_refresh` through the window is *for*. It also
exercised two paths that would otherwise have stayed theory: the scoped delete
(`fetched_at < '2026-05-01'`, because 13,253 artifacts carry preserved historical
ids that could otherwise match a live staging row), and the commit report
repairing its own timestamp from the receipt on the second run instead of
keeping the first run's — which is the value `canary-flush-verify` uses as its
scan lower bound.

**Operating instructions that were wrong three ways in one line
(2026-08-29 ~14:45).** The run sheet's window command specified `python` on a
host that has only `python3`; on a host with no `psycopg2` and no venv, so it
could never have imported `shared.db`; with an
`--expect-manifest-sha256 <from step 2>` placeholder that reads as prose and was
pasted literally. All three surfaced with production drained and the maintainer
typing under time pressure. The assistant had written and revised that line
across four review rounds without once running it.

**Friction worth noting.** tmux was unusable from the Mac at first ("too many
lines to see anything… ctrl bd, option bd, and command bd do nothing"), an
`~/p145status.sh` helper had to be written to make progress legible, and
`docker exec` vs `compose run` earned its own runbook section after it was found
to run stale code.

**The same stale-code class, one stage later (2026-08-29).** The Stage 6 dry runs
opened with `invalid choice: 'pack-trial'`. The archiver bakes its source into
its image rather than mounting it, so `compose run` against an unrebuilt image
runs the code from the last build. Identical in shape to the `docker exec`
finding above, met again a stage later, and now a line in the Stage 6 run sheet.

**A running average made someone else's load look like decay (2026-08-29).** The
27.4-minute MinIO listing is paid twice across Stage 6 — once by the dry run,
again by the repack — because nothing caches the enumeration. Two concurrent
DuckDB queries during one of those listings cost about 90 seconds, and read
against a running-average rate that looked like the run slowing down rather than
like somebody else on the disk. The run sheet now says to keep other work off
MinIO while it lists. It is the small, cheap cousin of the VM wedge above: the
same mistake — treating a shared box as a private one — costing 90 seconds
instead of 25 minutes.

**The ambiguity guard fired on a real ambiguity (2026-08-29).** `retire-packs`
requires `--verify-run-id`, which had looked like insurance against a contrived
case. It was not optional: two verify reports existed on the store — the failed
one from the dry-run phase, and the passing one — and the mode named both and
refused to choose. Between the guard that refuses a *missing* report, which was
what the plan anticipated, and the one that refuses an *ambiguous* one, the
second is what met the world.

---

## 11. Where it ended

**Plan 145 is complete and archived.** It closed 2026-08-30 and was archived that
morning (`066d3e2`), in the same transition that moved [Plan
156](../plans/plan_156_block_page_detection.md) into the build order — its only
trigger had been Plan 145 Stage 5 finishing.

### The deletion, 2026-08-30 03:53 UTC

`delete-legacy --apply --maintainer-approval "Andrew Miller"
--census-from-manifests --verify-run-id repack-4ea1c730c8b96ac1`. Under a minute.

| | |
|---|---:|
| legacy objects, against a frozen baseline of 1,172 | 1,172 |
| planned for deletion | 1,172 |
| receipts `deleted` / `reconciled` | 1,172 / 1,172 |
| `refused` / `absent` / `error:` | **0 / 0 / 0** |
| legacy detail Parquet remaining | **0** |
| `results_page` objects, before and after | 2,380 / 2,380 |

By exact key from a manifest written **before the first delete**, in capped
batches, never by prefix, with the approving name recorded in the manifest and in
all 1,172 receipts. The drift gate passed against the census frozen on
2026-08-21: nine days later the bytes were still `14,670,223,837`, exact to the
byte. The out-of-scope results-page population was refused *by key as a
predicate*, not filtered out of a listing — its 127 Parquet objects untouched.

### The prune that made it safe, 2026-08-29 23:43 – 08-30 02:26

983,043 loose objects deleted, 983,043 verified, **0 refused**, 0 already gone,
8.83 GB and 1,937,177 inodes reclaimed. Every deletion was preceded by the full
per-member check — the key resolves to the right pack, the extracted member
matches the sidecar's `raw_sha256`, and the loose object matches the packed bytes
exactly — and `surviving == members == handled` on all 68 packs, so nothing was
skipped and nothing was assumed.

Its `no_event_row: 468,254` is worth one line because it looks alarming and is
not: 139,604 members with no queue event at all, plus **328,650 — the entire
`allocated_sequence` census from Stage 5**, whose `recovered` events carry
`event_at = now()` and so fall outside the April window that query scans. Those
artifacts have identity; this particular query cannot see it. Status is
report-only in this processor and cannot gate a deletion, which is exactly why
the run sheet warns against "fixing" the packer's `paths` glob to April.

### The end state

| prefix | before Stage 6 | after |
|---|---:|---:|
| old April packs | 2,133,921,814 | — |
| loose `.html.zst` | 9,478,040,747 | — |
| legacy `.parquet` | 14,670,223,837 | — |
| replacement packs + sidecars | — | 4,655,215,649 |
| **total** | **24.48 GiB** | **4.34 GiB** |

**20.14 GiB reclaimed against the 13.66 GiB the plan set out to delete** — the
excess being the loose population the flattening itself created and the prune
then removed. April's 983,043 distinct captures live in 68 verified packs, each
member carrying the corrected `listing_id` where silver can describe it and an
honest NULL where it cannot.

Every success criterion is met, and the plan's table now carries achieved values
against required ones rather than a column of aspirational zeroes.

### Five numbers that were derived before they were measured, and came back exact

This is the part of the closeout most worth carrying forward. The corrected
Stage 6 gate's figures were computed from the assign and full-apply censuses and
committed **marked "derived, not yet measured"**:

| | derived | measured |
|---|---:|---:|
| no queue event | 139,604 | 139,604 |
| — materialized | 133,548 | 133,548 |
| — old pack member | 6,056 | 6,056 |
| no `listing_id` | 325,414 | 325,414 |
| with a listing | 657,629 | 657,629 |

Exact in every cell. The two NULL columns differ by 185,810, which is precisely
the carousel-only cohort a separate DuckDB breakdown had measured at 137,209 +
48,601. Three independent measurements of the same population agree: the metadata
query, the arithmetic from Stage 5's censuses, and the sidecars the packer
actually wrote. Labelling them as predictions is what made the agreement a
confirmation rather than a coincidence.

The same coherence arrives from the other end. The legacy coverage join cleared
against **983,041 distinct hashes over 983,043 members** — a two-member gap, and
it is the same two members Stage 3a's own verification surfaced as 557,063
distinct across 557,065. A discrepancy showing up twice from opposite directions
is evidence the accounting is coherent rather than lucky.

### One number that came back wrong, and why

Achieved compression on the replacement packs is **39.28x**, *below* the original
packs' 43.66x — and a mid-run extrapolation from four samples that predicted ~53x
was simply wrong. The head packs do reach 53.2x; the tail collapses to 23.1x,
because the 139,604 members with no queue event cluster on nothing and land
there. It is a property of the population — the original packs never held the
425,978 materialized objects — not of the packing, and it did not change the
storage case. Recorded because the extrapolation was made confidently, in public,
off four samples that were never going to carry it.

### Done and irreversible

371,095 objects deleted as content twins; 557,065 members unpacked; 983,043 loose
objects pruned; 32 superseded packs retired; **1,172 legacy Parquet objects
deleted**; V047 applied; `ops.artifacts_queue_artifact_id_seq` advanced twice, by
292,432 (probe) and 329,856 (authoritative) — sanctioned `bigserial` gaps, not
reuses; and 701,375 silver rows, 200,599 historical price events and 341,903
recovered queue events committed and flushed.

**The V040 assertion held.** Two runs in a named window on a deploy-intent drain,
`single transaction True` in both, all six protected relations byte-identical
across the canary. Recovery changed no live state — measured, not asserted.

**One verification gap, and it is now permanent.** `canary-flush-verify` proved
the round trip by key for the canary's 234 artifacts. The evidence for the other
341,669 is the receipts plus the flushers' delete-on-success contract — strong,
but not the same thing. A by-key check at full population would have needed a
mode that does not exist, and none was written. Stage 6 does not close it: its
per-member verification proves bronze bytes are in a pack, not that silver rows
are in the lake.

**Two maintainer rulings** — the near-duplicate cohort and the carousel fan-out.
The fan-out was approved verbally during the run; the run sheet still records
that `assign --apply`, which advanced the sequence permanently, ran before either
was recorded.

### Work this plan spawned, and where it went

- [**Plan 156**](../plans/plan_156_block_page_detection.md) — `_detect_challenge`
  is blind to non-Cloudflare blocks. It sat in the backlog for exactly as long as
  Plan 145 needed the parser unmodified, because the plan's whole deliverable was
  a comparison against what production wrote and a parser change would have
  invalidated it. It moved into the build order in the same commit that archived
  Plan 145, and now sits at build-order 15, priority 42, effort S.
- [**Plan 157**](../plans/plan_157_april_reprocessing_enrichment.md) — the
  12.19-field enrichment. Backlog, priority 57, triggered by Plan 125's Iceberg
  migration, which is at build-order 7 and has not landed.
  `recovery/plan145/parsed/` (329 MB) is retained as its frozen input and is
  marked **do not prune** in the index.
- **CAR-28**, the packer defect, was fixed under Stage 5b — and then became a
  finding in someone else's plan. The issue accumulated roughly 4,000 words of
  measured compression evidence, was **canceled on 2026-08-27, and was still
  being edited on 2026-08-30**. [Plan
  149](../plans/plan_149_linear_execution_layer.md) records it as a second owner
  of a fact its own issue contract forbids, and is clear about the cause rather
  than treating it as carelessness: **CAR-28 was a question that outlived its
  issue, and the tracker was where the question was being asked.** The rule
  survives — the durable copy belongs in the plan document — but "an open
  question with no plan section yet" is a real gap in that contract.
- The unhomed follow-up is **still unhomed**: the parser control needs a scope
  predicate before its exit code means anything again. Two days after the plan
  closed, no plan owns it.

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
7. **The narrative asset is the handoff.** Eight handoff documents, each written to
   be executable by a session with no context, are why four days of work across
   many sessions and two models stayed coherent. They also carry the honest parts
   — "expect ~760, and if it is materially larger, stop and say so."
8. **A safeguard can be built and still not be finished.** The canary's separate
   receipt name was the right call and it created a duplicate write that sat
   unaddressed until the apply was imminent — flagged in the same breath as it
   was created, then left. Naming a consequence is not handling it.
9. **Nothing you have not executed is a procedure.** The window command was
   wrong three ways in one line and survived four review rounds, because every
   round reviewed its *reasoning*. It failed the first time a human typed it,
   with production drained.
10. **The maintainer's plain-language restatement beat the model's design twice.**
   Flattening (2026-08-27 16:30) and doing the deletion before the unpack to save
   space (16:38) both came from the human, in one sitting, after he asked: *"I
   want to understand why this has been so complicated. I feel like we've taken 8
   stabs at this, each of them better, but each of them convoluted, when what
   we're really trying to do is pretty simple."*
11. **A plan that describes its input in prose has not specified its input.**
   Design 2 was built, merged, deployed to production and health-gated before
   anyone discovered that the legacy Parquet prefix — the location of the
   1,299 objects the whole plan exists to delete — was written down nowhere
   except Plan 137's prose. Stage 0 had closed six gates; none of them was
   "name the prefix." Design 4's opening trust-boundary table is the direct
   descendant of that, and it works because it pins *objects*, not
   descriptions of objects.
12. **Cheap fixtures make expensive gates unprovable.** The packer's tests gave
   one silver row per `artifact_id`, so `any_value` could not pick wrong (§4).
   The slice-2 mock raised `KeyError` where production returns `None`, so no
   fail-closed gate could be tested (§6.4). Same failure twice, four months
   apart, in unrelated code: the fixture was built to make the happy path
   convenient rather than to represent the world, and in both cases the test
   suite reported green over the exact defect it existed to catch.
13. **Separate the builder from the reviewer, and give them the same spec.**
   Nine review rounds produced nine real defects because the reviewer was a
   cold session holding the handoff and the diff and nothing else — no access
   to the builder's account of why the diff satisfies the handoff. The review
   prompt's closing mutation test ("would a plausible mutation fail exactly one
   test and nothing else?") is the single most reusable artifact this plan
   produced, and it is not in any plan document.
14. **A handoff decays at the speed of the run, and its rationale decays first.**
   The canary handoff was stale within a day: every situational number false,
   and — worse — its stated reason for the phase ordering already discharged by
   the probe. Refreshing the numbers and leaving the rationale would have
   produced a document that looked current and argued for the right thing for
   the wrong reason.
15. **A bounded trial's sample is a variable, not a detail.** Same population,
   same dictionary, same frame target, two draws — and the answers landed 32
   points apart, further than the effect under test. The plan originally
   specified *one* sample, and that one sample was the draw that would have
   reported the wrong sign with confidence. Fixing the decision rule before the
   run is what let a split be read as a refusal rather than argued about
   afterwards.
16. **Label a number as derived and it becomes a testable claim.** Five figures
   went into the Stage 6 gate marked "derived, not yet measured", and all five
   came back exact. Getting them right silently would have been worth nothing;
   the label is what turned the agreement into a confirmation, and it is the
   cheapest form of prediction registration available in a repository.
17. **Guards written for contrived cases meet real ones.** `retire-packs`'s
   `--verify-run-id` looked like insurance against a situation nobody expected.
   Within a day the store held two verify reports — one failed, one passed — and
   the mode named both and refused to choose. The guard the plan reasoned about
   was the *missing*-report case; the one that met the world was the ambiguous
   one.
18. **The thing a plan exists to do can be the cheapest thing in it.** The
   deletion ran for under a minute. Behind it: four designs, thirty hours of
   compute, 9,410 lines of script, 7,107 lines of test, 23 pull requests and nine
   review rounds. None of that cost is in the destructive step. All of it is in
   earning the right to take it — and that ratio, not the 20.14 GiB, is the
   honest headline.
19. **A confident extrapolation off four samples was wrong, in public, in the
   same plan that got five predictions exactly right.** The mid-repack estimate
   of ~53x compression came in at 39.28x. The difference between the two is not
   care or skill; it is that the five were arithmetic over a fully enumerated
   population and the one was an average over a head sample of a skewed
   distribution. Same author, same day, same document.

---

## 13. Open questions for the narrative

Three of these are now answered and a fourth is at least countable. The answers
are recorded here rather than the questions deleted, because in two cases what
the answer turned out to be matters less than the fact that the question was
asked before the run rather than after it.

- **Was the 2026-08-26 revert the right call, and what did it cost?** Still open.
  §3.1 recovers what the work looked like four hours before it was reverted; the
  decision itself is still missing. PR #255 and #256 carry **zero review
  comments**, both reverts are bare `git revert` messages, and 17:50–21:49 is
  unrecorded. The tracker is the remaining source, and it is thin: CAR-13 is a
  single **3-point** issue reading "Plan 145 — close Stage 0d/0e, build backfill
  write path," which is the design that was reverted, described in one line.

- **Would forcing an unpacked shard into the early probes have been cheap?**
  Still open, and Stage 6 added no evidence either way. If yes, the block-page
  leak and the 42,276-member finding were both catchable a day earlier.

- **Was 16h45m of parsing the price of correctness, or of the flatten-everything
  design?** Now *countable*, if still not settled. Flattening cost 6h26m to build
  the loose population (materialize, dedupe, unpack) and a further 5h03m to
  remove it again (repack, prune) — about 11.5 hours of the plan's ~30, on top of
  a parse that had to read every byte once regardless. Design 3 costed 24.8
  core-hours for the same answer, so flattening was still the cheaper of the two
  designs that were actually specified. Neither was ever compared against the
  third option nobody wrote down: a targeted reparse of only the captures silver
  lacks.

- **What is the honest estimate-versus-actual?** ~~`docs/PLANS.md` still carries
  Plan 145 at effort **M**.~~ **Answered, and it is worse than "M".** The index
  no longer carries Plan 145 at all — it is archived. The estimate that can be
  audited is the tracker's: Plan 145 entered cycle 1 as **one 3-point issue**
  (CAR-13) and left it as **six issues and 15 points**, a five-fold expansion
  inside a six-day cycle. [Plan
  149](../plans/plan_149_linear_execution_layer.md)'s close-out read finds the
  point scale itself off by roughly 3x against its own day-mapping, and names
  Plan 145 as the reason the cycle's "issues added after start" measure is
  unreadable at 62%. Its verdict is the fair one and it is not an estimation
  failure: *"That is not a seeding failure — it is what a plan does when
  measurement contradicts it — but it means a seed list describes an opening
  position, never a commitment."*

- **Was the canary apparatus proportionate?** **The document should now pick
  the second reading.** Five of seventeen CLI modes, `write_set_digest`, the
  promotion proof and four review rounds existed so ~500 rows could be committed
  safely; the apply that followed, of 1,400× as many rows, took ten minutes.
  Stage 6 is the tiebreak, and it breaks toward the canary. The single most
  destructive command in the plan — `delete-legacy`, 13.66 GiB, irreversible —
  ran for under a minute, returned zeroes in every failure column, and needed no
  improvisation at all, because by then every guard it relied on had been built,
  argued over and exercised on something smaller. **The canary is what made both
  the apply and the deletion boring.** That is the case for the apparatus, and it
  is not available from Stage 5 alone.

- **Would a full-population by-key lake verification have been worth writing?**
  **Answered: it was not written, and the gap is now permanent.** Stage 6 does
  not close it — verifying that bronze bytes sit in a pack says nothing about
  whether silver rows reached the lake. The receipts and the delete-on-success
  contract are what stand behind 341,669 of the 341,903 artifacts, and that is
  where the record ends.

- **New, from the closeout: what does 39.28x mean for May, June and July?** The
  ordering trial's most favourable reading gives true-listing ordering a 4.76%
  edge, about 0.33 GiB against 6.86 GiB — so the ceiling on re-sorting other
  months is low. But April's replacement packs came in *below* the originals
  because 139,604 unattributable members cluster on nothing, and every future
  month packed from a flattened population will carry the same tail. Nobody has
  asked what that does to the ~3.5 years of runway the retention case rests on.
