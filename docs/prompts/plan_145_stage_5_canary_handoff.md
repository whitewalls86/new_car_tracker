# Plan 145 Stage 5, slice 3 — the canary and the live-state proof (CAR-21)

Slices 1 and 2 are merged. This slice contains the checks that stand between
Plan 145 and the full apply, and one of them requires a **named maintenance
window with production writers quiesced** — a manual action the maintainer
approves and performs.

Read `docs/plans/plan_145_april_cutover_reconciliation.md` — its **Stage 5**
section, *Canary and live-state proof*, is the specification — then
`docs/plans/plan_145_stage_5_compare_handoff.md` and
`docs/plans/plan_145_stage_5_writer_handoff.md` for what you are validating.

This slice builds tooling. **It does not decide that the gate has closed.**

---

## Preconditions (2026-08-28)

- **V047 (`plan145_recovery_batch_receipts`) is applied on prod** (2026-08-28
  15:56:39). The probe's first `apply --probe --apply` failed on it — the prod
  DB was one migration behind. If you rebuild and the receipt table is missing,
  `docker compose run --rm flyway`.
- A partial real compare is already on disk: `compared_probe/cmp-e37723ede49fad4f/`,
  with 59 assignment shards at `assigned_probe/`. Prototype phase A against it —
  it is disposable and never promoted to the authoritative prefixes.
- The recovery-writer integration tests run locally against a throwaway
  `postgres:16` + Flyway V001–V047 (`tests/integration/scripts/`), not only in
  CI.

---

## Build it in two phases

The three checks below were specified together, but only one of them needs the
**authoritative** `compare` run. Split the build so the parser control does not
wait on it.

| phase | what | needs |
|---|---|---|
| **A — now** | the parser control; the V040 live-state verifier | slice 1's output shape; nothing from slice 2 |
| **B — after the authoritative compare** | the write canary; the flush round trip | real `assigned/` shards from an authoritative run |

**Where things stand (2026-08-28).** Stage 4 is at 1,188 of 1,204 parsed units —
the authoritative `compare --apply` is hours away, not days. `--probe` on
`compare` / `assign` / `apply` (PRs #268, #269) has already produced a complete
*partial* compare at `compared_probe/cmp-e37723ede49fad4f/`, 59 real assignment
shards at `assigned_probe/`, and one `apply --probe --apply` batch that ran the
full write set against production Postgres and rolled it back with every `::uuid`
cast, `NOT NULL` and CHECK holding on real rows (plan doc, *Evidence — slice 1 &
2 probe*, 2026-08-28). So "the code has never met production data" is **not** the
risk any more; you are building the checks that let the maintainer *rule*, not
de-risking untested inserts.

**Phase A first, and the parser control before the verifier.** The probe did not
test the deepest assumption in the plan — that reprocessing reproduces what
production wrote. That is the parser control's job, and its failure would
invalidate slice 1's classification and everything built on it. It draws
entirely from `already_represented/` and touches nothing slice 2 produces.
Prototype it now against
`compared_probe/cmp-e37723ede49fad4f/already_represented/`; it then runs
unchanged against the authoritative output when that lands.

**Phase A also audits slice 2 — now against real shards, not a schema.** The
canary in phase B must stratify across identity source
(`preserved_queue_event` / `allocated_sequence`), detail/carousel,
active/unlisted and input kind. Write the sampler in phase A and point it at
`assigned_probe/cmp-e37723ede49fad4f-b*.parquet`. Two things it will show:
`id_source` and `input_kind` are on the assignment shard, but `listing_state` is
**not** — it is on the `to_import` rows, so the sampler joins `assigned_probe/`
back to `compared_probe/<run>/to_import/` to stratify on it. That join is
expected, not a slice 2 gap.

**Do not let phase B tempt you into declaring phase A done by inspection.** The
parser control is the check the whole plan rests on.

---

## Two checks that are not the same check

The plan names them together and they are routinely conflated. They prove
different things and either can pass while the other fails.

| | proves |
|---|---|
| **parser control** | the recovery reproduces what production wrote, on pages production already parsed |
| **write canary** | the writer inserts correctly and mutates nothing live, on pages production never parsed |

---

## Check 1 — the parser control

Draw approximately **500 exact, same-source represented observations** from
slice 1's `already_represented/` — rows where `nearest_distance_s == 0` and
`match_sources` is the parsed row's own source. The partial probe put
**3,980,701** rows in `already_represented`, so the exact-microsecond
same-source subset is ample however small its fraction; measure it from
`compared_probe/cmp-e37723ede49fad4f/already_represented/` rather than trusting
the one-shard smoke figure the plan doc still cites (2,879 of 3,374). For each,
compare **every silver business field** to the deployed production row.

Ignore exactly four things, and say so per field in the report:

- recovery provenance columns;
- `artifact_id` — recovered artifacts carry different, legitimate IDs;
- `written_at` — stamped by the flusher, not by the capture;
- **carousel `vin`** — Stage 4 deliberately leaves it NULL because production
  fills it from a `vin_to_listing` lookup that Stage 4 was forbidden to make.
  Slice 2 fills it from the frozen snapshot, so on a post-slice-2 row this gap
  should be closed; on the raw parsed row it is expected. State which you are
  comparing.

Any other field disagreement is a **finding**, not a tolerance. Report the
per-field disagreement census. This is the check that says the whole comparison
means something, so a quiet "close enough" here invalidates every other gate in
the plan.

---

## Check 2 — the write canary

Draw approximately **500 `to_import` observations**, grouped by artifact so no
artifact is split, and **stratified** across:

- `source` — detail / carousel;
- `listing_state` — active / unlisted;
- input kind — materialized legacy body / unpacked pack member;
- identity source — `preserved_queue_event` / `allocated_sequence`.

Run them through the **real** `assign` and `apply` code paths from slice 2. A
canary that takes a shortcut around the writer proves nothing about the writer.

**The canary is a real commit, not `apply --probe`.** `apply --probe --apply`
already ran every statement for one batch against production Postgres and rolled
it back — that retired "does the write set type-check against real data" as a
question. The canary is the next thing: ~500 rows **committed** through
authoritative `assign --apply` / `apply --apply` (inside the default
`--max-unapproved-rows` budget), because the flush round trip below cannot be
proven on rows that were rolled back. Do not use `--probe` for the canary.

Then **require the flushers to carry the canary through to their lake
prefixes** before approving the full apply. `staging.silver_observations` and
the two event tables are asynchronously flushed and then deleted; a canary that
stops at Postgres has not proven the rows survive the round trip into
`silver_normalized/observations/` and `ops_normalized/`. Verify the flushed
Parquet, by key, and record the keys.

---

## The V040 assertion, and why it needs a window

The claim is that recovery changes no live state: `ops.price_observations`,
`ops.vin_to_listing`, `ops.blocked_cooldown`, `ops.detail_scrape_claims`, and
the two V040 views (`ops.ops_vehicle_staleness` and `ops.ops_detail_scrape_queue`,
`db/migrations/V040__detail_scrape_circuit_breaker.sql:19,75`).

**Run this against live production and the claim is meaningless**: production
legitimately writes those same tables while the canary runs, so a before/after
difference proves nothing and a before/after equality is luck.

The sequence, in order:

1. The maintainer opens a **named maintenance window** and **quiesces every
   service with write access to the four protected tables**. Pausing and
   resuming those services is a manual, separately approved production action.
   **You do not perform it and you do not decide when it happens.**
2. Begin **one verifier transaction**, so PostgreSQL's `now()` is fixed for both
   V040 snapshots — the views are time-dependent, and two snapshots taken at two
   different `now()` values will differ for reasons that have nothing to do with
   recovery.
3. Capture the four hot tables and both views.
4. Run the canary **on a separate connection**.
5. Capture them again.
6. Require **byte-equivalent** results.
7. The maintainer restarts the writers.

Build this as a script that refuses to run outside a window it has been told
about — take the window name as a required argument and record it in the
report. A verifier that can be run casually will be.

---

## The carousel fan-out review

The plan's gate requires the carousel fan-out to be **measured and reviewed
before the full apply**. It is a review, not an assertion: production writes
~5.7 carousel rows per detail page, and the partial probe measured **5.6332 per
object over 671,657 objects, max 8** — within a rounding error of production, but
the ruling is the maintainer's, not yours. Re-read the number off the
authoritative `compare_report.json` when it lands, put it in front of the
maintainer, and let them rule. Do not encode a threshold and do not treat your
own summary as the approval.

The **near-duplicate cohort** rides alongside it: the probe found 67,994
adjacent same-listing pairs within 300 s (95,168 captures with a neighbour, over
7,483 listings). The slice-1 gate requires this in front of the maintainer
before an authoritative apply too — surface it, do not collapse it.

---

## Tests

```
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/scripts/test_reconcile_april_detail.py -q -m "not integration"
LOG_PATH=/tmp/p145test.log .venv/bin/python -m pytest \
  tests/integration -q -m integration
.venv/bin/python -m ruff check .
```

Cover at least:

- the control sample is drawn only from exact same-source matches, and the four
  ignored fields are ignored **by name** — a test that adds a fifth field to the
  ignore list must fail;
- a single differing business field is reported, not tolerated;
- the canary sample covers every stratum, and no artifact is split across the
  sample boundary;
- the verifier refuses to run without a named window;
- both V040 snapshots are taken inside one transaction — assert the snapshots
  share a transaction, since this is the failure that silently invalidates the
  whole proof;
- a simulated live mutation between the snapshots **fails** the assertion;
- the flush verification fails when the lake objects are absent.

---

## Non-negotiables

1. **You do not pause or resume a production service.** Propose it; the
   maintainer performs it.
2. **You do not declare the gate closed.** You produce the evidence; the
   maintainer rules.
3. **No full apply** until the control, the canary, the flush round trip, the
   V040 equality and the fan-out review are all in front of the maintainer and
   approved by name.
4. **Announce the blast radius** of every production command before running it.
5. Everything in slice 2's non-negotiables still holds.

---

## When you are done

Report, in one place: the per-field control census; the canary's stratum
coverage and its flushed lake keys; the V040 before/after result and the window
it ran in; the carousel fan-out; and an explicit statement of what remains
unproven.

Then **stop**. The full apply, Stage 5b's packer fix and Stage 6 are separate
work with their own gates. Do not merge the branch and do not open a Linear
issue — both are the maintainer's call.
