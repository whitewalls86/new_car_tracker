# Plan 145 — Post-Mortem (Draft)

**Status: draft, gathered 2026-08-29, updated the same evening. Plan 145 is not
finished.** Stage 5 is now complete — the whole `to_import` population is
committed — but Stage 6 is unstarted and **not one of the 1,172 legacy Parquet
objects has been deleted**, which is the entire point of the plan. This is a
mid-flight account assembled so the narrative can be written while the evidence
is still recoverable — not a closeout.

**Sources.** Current plan, handoff, runbook and reference documents; the full git
history including reverted and superseded revisions; the VM at `147.224.199.86`
(tmux sessions, run logs, timings); and 20 Claude Code transcripts on this
machine spanning 2026-08-25 to 2026-08-29.

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
> — `scripts/reconcile_april_detail.py:152`

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

The apply being the fastest stage in the plan is worth a line. Four days of
design and 6,000 lines of test stood behind ten minutes of writing.

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
(`_print_compare_report`, `scripts/reconcile_april_detail.py:3671`). The data
went to the right place — the isolation check proves it — but the operator's
only on-screen confirmation of isolation said the opposite of the truth.

### Code

| | at first commit | now |
|---|---:|---:|
| `scripts/reconcile_april_detail.py` | 876 | **7,919** |
| `tests/scripts/test_reconcile_april_detail.py` | 496 | **6,033** |

Plus `scripts/verify_recovery_live_state.py` (283), three real-Postgres
integration suites (1,273), one Flyway migration, and **13 CLI modes**: `census`,
`materialize`, `dedupe`, `unpack`, `parse`, `compare`, `assign`, `apply`,
`control`, `canary-sample`, `canary-remanifest`, `canary-commit`,
`canary-flush-verify`.

Five of those thirteen modes exist only so ~500 rows could be committed safely.
Whether that was proportionate is §13's question, not this section's.

**Documentation is comparable in size to the code**: 1,801-line plan, six
handoffs (1,332 lines), a 975-line run sheet, a 250-line flag reference.

### Process

85 commits mentioning plan-145 · 21 merged PRs (#227, #255–#276) · **2 reverted
merges** · **9 review rounds** that produced follow-up fix commits — four of them
on PR #274 alone, each finding a real defect in the canary's contract.

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

---

## 11. Where it stands

**Stage 5 is complete.** As of 2026-08-29 15:30 UTC the whole `to_import`
population is committed and flushed to the lake.

| | artifacts | silver | price | queue |
|---|---:|---:|---:|---:|
| 69 batches | 341,669 | 700,870 | | |
| canary | 234 | 505 | | |
| **70 receipts** | **341,903** | **701,375** | **200,599** | **341,903** |

The arithmetic is the proof in both directions: the batches wrote exactly 234
artifacts and 505 rows short of the assign census, so the canary exclusion
caught every canary artifact — no duplicate; and the two rows sum to precisely
that census, so it caught nothing else — no gap. Staging drained to zero, and
the flushers delete only after a successful Parquet write.

**Done and irreversible:** 371,095 objects deleted (recoverable from the packs);
557,065 members unpacked; V047 applied; `ops.artifacts_queue_artifact_id_seq`
advanced twice, by 292,432 (probe) and 329,856 (authoritative) — sanctioned
`bigserial` gaps, not reuses; and now 701,375 silver rows, 200,599 historical
price events and 341,903 recovered queue events committed and flushed.

**The V040 assertion held.** Two runs in a named window on a deploy-intent
drain, `single transaction True` in both, all six protected relations
byte-identical across the canary. Recovery changes no live state — measured,
not asserted.

**Proven since this document was first written:**

- the write canary as a real commit, and the flush round trip into
  `silver_normalized/observations/` and `ops_normalized/`, verified by key;
- the V040 before/after equality, in a maintainer-opened window;
- the duplicate-write interaction — `apply` now reads the canary's commit
  record, with the receipt as the authority and both mismatch directions
  stopping;
- the full apply.

**Still not done, and it is the whole point:** Stage 6 — repack, prune, and the
deletion of the **1,172 legacy Parquet objects (13.66 GiB)**. Not one byte has
been deleted. Stage 5b's compression trial is also outstanding.

**One verification gap, stated plainly.** `canary-flush-verify` proves the round
trip by key for the canary's 234 artifacts. The evidence for the other 341,669
is the receipts plus the flushers' delete-on-success contract — strong, but not
the same thing. A by-key check at full population would need a mode that does
not exist, and none was written.

**Two maintainer rulings:** the near-duplicate cohort and the carousel fan-out.
The fan-out was approved verbally during the run; the run sheet still records
that `assign --apply` — which advanced the sequence permanently — was run before
either was recorded.

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

---

## 13. Open questions for the narrative

- Was the 2026-08-26 revert the right call, and what did it cost? **Partly
  answered — see §3.1**, which recovers what the work looked like four hours
  before it was reverted. The decision itself is still missing: PR #255 and #256
  carry **zero review comments**, both reverts are bare `git revert` messages,
  and 17:50–21:49 is unrecorded. Linear (CAR-13) is the remaining source.
- Would forcing an unpacked shard into the early probes have been cheap? If yes,
  the block-page leak and the 42,276-member finding were both catchable a day
  earlier.
- Was 16h45m of parsing the price of correctness, or of the flatten-everything
  design? Design 3 costed 24.8 core-hours for the same answer, so flattening was
  cheaper — but neither was ever compared against a targeted reparse of only the
  captures silver lacks.
- What is the honest estimate-versus-actual? `docs/PLANS.md` still carries Plan
  145 at effort **M**.
- **Was the canary apparatus proportionate?** Five of thirteen CLI modes,
  `write_set_digest`, the promotion proof and four review rounds exist so that
  ~500 rows could be committed safely — and the apply that followed, of 1,400×
  as many rows, took ten minutes and needed none of it. The counter-argument is
  that the canary is what made the apply boring. Both readings are available and
  the document should pick one.
- Would a full-population by-key lake verification have been worth writing? It
  was not, and the gap is now permanent for this run.
