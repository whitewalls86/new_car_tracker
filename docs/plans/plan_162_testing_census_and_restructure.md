# Plan 162: The Testing Census and CI Restructure

## Status

**Stages 0, 1, 2 and 3 are complete (CAR-40, CAR-45, CAR-46 and CAR-47, all
2026-08-31).** The census enumerated the work; Stage 1 ran the 73 tests nothing
had ever invoked and found no production defects behind them, which
[confirms the L estimate](#evidence--stage-1-the-orphaned-suites-car-45-2026-08-31);
Stage 2 [unblinded the coverage instrument](#evidence--stage-2-unblinding-coverage-car-46-2026-08-31)
the later stages are graded by, taking the reported number from 88% to 75.95%
without a line of production code changing; Stage 3
[gave the two health-sensor censuses one declared source](#evidence--stage-3-one-declared-source-for-the-health-sensor-censuses-car-47-2026-08-31)
and found a third census, `DAG_SPECS`, already one DAG short. The waiver list
stands at 116 — Stage 3 closes no waivers, only Plan 139's Stage H.

This document was written as a deliberate stub on 2026-08-30, when
[Plan 161](plan_161_testing_contract.md) had not yet decided the standard this
plan measures against. That blocker is gone: 161's contract landed, was
asserted, and is archived.

Stages 2 through 10, including 5b, are scoped below and unblocked. Effort is
**L**, down from the XL placeholder, on the reasoning in
[The estimate](#the-estimate) and now confirmed against a measurement rather
than proposed. [`docs/PLANS.md`](../PLANS.md) owns priority and effort; this
document does not choose them.

## What the census found

[`tests/test_testing_contract.py`](../../tests/test_testing_contract.py)
implemented seven mechanical rules when the census ran, and eight since Stage 2
added the coverage rule. It passes, and **a pass means only that those rules
hold** — every violation standing on 2026-08-31 is grandfathered in a waiver
list. That list is this plan's backlog:

This is the census as taken, kept as the baseline the stages are measured
against; the live count is whatever `tests/test_testing_contract.py` holds
today. **Stage 1 has since cleared the CI-invocation row, leaving 116.**

| Rule | Waivers | Gap |
|---|---|---|
| CI invocation | 4 → **0** | [G1](../TESTING.md#the-gap-list) (3), G2 (1) — both closed by Stage 1 |
| Patching is `mocker` | 34 | G4 |
| Route reached through `app.routes` | 12 | G6 |
| `.sql` file touched by a Layer 2 test | 54 | G14 |
| Layer numbering | 16 | G11 |
| **Total** | **120** | |

The waiver list can only shrink, and three assertions enforce that: a waiver
that no longer describes a violation fails, one whose owner plan is archived
fails, and one naming a gap entry that does not exist fails. **So this plan's
progress is mechanically visible and its completion is provable** — which is
what makes it schedulable rather than open-ended.

### Measurement beat inspection, and the record should say so

The readings this document carried as its starting point were taken by eye on
2026-08-30. Every one of them was wrong, and wrong in the same direction:

| Read by eye, 2026-08-30 | Measured, 2026-08-31 |
|---|---|
| 21 files mixing two mock styles | **34** files patch with something other than `mocker` — the by-eye count never looked at `monkeypatch.setattr` |
| 3 of 35 routes unreferenced | **12 of 87** routes reached through no routing table |
| 16 modules with inline SQL | **10** modules (G5) — and a gap nobody had counted: **54 of 76** `.sql` files that no Layer 2 test executes (G14) |

The direction of the error is the point. Inspection undercounted three times out
of three, and the one gap that inspection missed entirely, G14, is now the
largest single item in the plan.

### Six gaps have no mechanism at all

Six of the thirteen gaps this plan owns are checked by nothing: **G5, G7, G8,
G9, G10 and G12.** (Twelve at the census; **G13 was re-owned here on
2026-08-31** when its Plan 146 half shipped, and it is half-checked — the
`PYTHONPATH` clause is asserted and nothing else is. **G10 has since been
mechanised and closed** by Stage 2 the same day, leaving five.) They are
recorded in prose, they are not among the 120, and they can worsen without
anything noticing. That is the condition
`ARCHITECTURE.md:179` was in before Plan 161, and it is why this plan's success
criteria are written the way they are below.

Three sub-cases, because they are not alike and the differences drive the stage
order:

- **G5, G9 and G10 have a natural, cheap assertion.** G10's was estimated at
  roughly five lines — every service directory appears in
  `[tool.coverage.run] source` — and Stage 2 built it, at that size plus a
  second assertion for the half the estimate had not counted.
- **G7 can never be reached by the existing rules.** `dashboard/` is Streamlit,
  not FastAPI. The route rule imports `<service>.app` and reads its OpenAPI
  schema; there is no schema to read. The "enough" floor's first clause is
  structurally inapplicable to the one service with zero test files, so G7 needs
  a kind of test invented rather than a waiver list drained.
- **G12 may correctly never get a rule.** "No module under `airflow/dags`
  imports `shared`" is *true today* — it is the constraint, not the violation.
  Closing it changes the DAG tree's import structure, which is an architecture
  decision and not an assertion.

## The stages

| Stage | Work | Closes | Waivers |
|---|---|---|---|
| **0** | **The census. Complete — CAR-40, 2026-08-31** | — | — |
| **1** | **The orphaned suites. Complete — CAR-45, 2026-08-31** | G1, G2 | 4 |
| **2** | Unblind coverage. `[tool.coverage.run] source` names every service directory, and something consumes the number | G10 | -- |
| **3** | **The two health-sensor censuses read one declared source. Complete — CAR-47, 2026-08-31** | Plan 139 Stage H | -- |
| **4** | Split the 267s `dbt build + test` job — the cheap half of the restructure | Plan 139 Stages B, C | -- |
| **5** | The mechanical sweeps: 34 mock conversions and 16 layer renames, plus the one live harness-decides-the-outcome test | G4, G11, G13 | 50 |
| **5b** | Separate production scripts from spent ones. `scripts/ops/` and `scripts/oneoff/`, the coverage denominator reads the split, and `ci_change_scope.py` gains its second prefix | — | -- |
| **6** | Route coverage. Build `container_health`'s test home, then fill it | G6, G9 | 12 |
| **7** | SQL execution, from both directions. The largest stage | G14, G5 | 54 |
| **8** | The services below the floor | G7, G8 | -- |
| **9** | `airflow/dags` and the `.sql` convention it cannot currently reach | G12 | -- |
| **10** | Suites on real Compose services, dbt against the Plan 120 snapshot, advisory CI impact selection | Plan 139 Stage E | -- |

**4 + 50 + 12 + 54 = 120.** The stages account for the whole waiver list; no
entry is left without a stage that deletes it.

### Why this order

Four of the placements are load-bearing. The rest is grouping.

**Stage 1 is first of the remaining stages because it is the only unknown that
changes the estimate.** 73 integration-marked tests sit in 11 files that no CI
step has ever invoked; `tests/integration/processing/` — 58 of them — has never
appeared in `ci.yml` in its history. **Whether they still pass is unknown**, and
every other stage can be sized from measurements Stage 0 already took. If those
suites have rotted, the areas they cover are unexercised and Stages 7 and 8 both
get worse. Running them is cheap, is a repair in its own right, and is what
converts this plan's estimate from a proposal into a measured number.

**Stage 2 is second because coverage is the instrument the rest of the work
reads.** `[tool.coverage.run] source` names six packages and omits
`container_health`, `dashboard`, `scripts` and `airflow/dags` — so **the two
services furthest below the floor are the two the instrument cannot see.** Every
stage behind this one measures better for it being fixed first.

**Stage 4 sits after Stage 1, not before it.** Stage 1 changes which suites
exist in CI; splitting the job afterwards means organising once with full
knowledge rather than twice. The rest of the restructure stays at Stage 10,
where its risk belongs — but the job split itself is largely mechanical, and
leaving it until last would mean running the most CI-intensive work this
repository has attempted across weeks of a 267-second critical path we had
already decided to remove.

**Stages 6 and 7 each pair two gaps because splitting them means touching the
same files twice.** G9 builds `container_health` a test directory and a Layer 4
that do not exist; G6's four `container_health` routes are uncoverable until it
does. G5 moves inline SQL into `.sql` files and G14 gets Layer 2 executing
`.sql` files — the same modules, from opposite ends.

**G13 joined Stage 5 on 2026-08-31, for the same reason.** It is the thirteenth
gap and the only one this plan did not originally own: the contract assigned it
to Plan 146 Stage 1 for the `PYTHONPATH` half, that half shipped as CAR-42, and
the remaining instance was left owned by a plan that owes no code. Stage 5 is
the right home because it is already the pass that reads every patch in the
suite — and "an unexplained mock of a filesystem, clock, platform or path
primitive is a finding" is the same question asked one step further out. The
pattern the rule holds up as correct, `21333ab`, is itself a mocking fix. Doing
the two together is one reading of the suite instead of two.

**Stage 5b sits immediately after Stage 5 because Stage 5 is what makes it
free.** The first instinct was to run it near the front, so the stages that
follow would visibly move the coverage number. The waiver list forbids it. Ten
of Stage 5's 34 mocker waivers name files under `tests/scripts/` and
`tests/integration/scripts/`, character for character, and `_assert_exactly`
asserts both directions — so moving a test file breaks its waiver twice, once
because the old subject has stopped existing and once because the new path is
an unwaived violation. Running 5b first means rewriting ten waiver subjects
that Stage 5 then deletes outright. Running it second costs nothing, because
Stage 5 has already emptied the colliding set. No other waiver tuple names a
`scripts/` path — `LAYER_2_WAIVERS` and `LAYER_NUMBER_WAIVERS` have none, and
`CI_INVOCATION_WAIVERS` is empty since Stage 1 — so Stage 5 is the only
collision in the plan.

Nothing is lost by the delay. **Stages 6, 7, 8 and 9 are all downstream of
Stage 5**, so every remaining stage that moves the number is still graded
against the cleaned denominator, which was the whole point of going early. The
CI payoff is not delayed either: Stage 4 is the dbt job split, and the impact
selector that reads the new prefix is Stage 10.

**It is numbered 5b rather than 6.** Inserting an integer renumbers five stages
and invalidates two issues already filed against the old numbers — CAR-50 names
Stage 6 and CAR-52 names Stage 8 in their titles. A letter costs nothing and
breaks nothing.

### Stage 5b: what the split is, and why a directory rather than a list

Three buckets, but only two moves, which is what keeps the cost near zero:

| Bucket | Where | Moves? | In the denominator? |
|---|---|---|---|
| **Production** — invoked by CI, an image, a Compose file or an ops route | `scripts/` (unchanged) | no | yes |
| **Maintenance** — human-invoked, durable, named in a live runbook | `scripts/ops/` | yes | yes |
| **One-off** — ran for a plan that has since archived | `scripts/oneoff/` | yes | **no** |

`tests/scripts/` mirrors the split and needs one new row in the contract's
*Where the newer suites sit* table, which
`test_every_test_directory_is_assigned_a_layer` will demand the moment the
directory appears.

**Production does not move, and that is the entire cost argument.** There are
20 binding references to `scripts/*` across 11 deploy surfaces — `ci.yml` (4),
`docker-compose.lakehouse.local.yml` (3), `redeploy.sh` (3),
`dbt_runner/Dockerfile` (2), `.env.example` (2), the two other lakehouse
Compose files, `.gitattributes`, `deploy-followers.txt`,
`ops/routers/snapshots.py` and `deploy.sh` — and **every one of them names a
script that stays put.** Moving only the other two buckets rewrites no deploy
surface at all.

It also gives the list the safe failure direction, the one
`maintenance-running-set.txt` already argues for in this repository: a new
script lands in production-land and is measured **by default**, and has to be
deliberately moved down to leave the instrument. Nobody drops something out of
coverage by forgetting.

**A directory rather than a manifest, because the directory is the
declaration.** A manifest would be a second mechanism to keep in step with the
first; the path is self-describing, `git log --follow` records the
reclassification, and `[tool.coverage.run]` and `ci_change_scope.py` each read
it for free. `scripts/ci_change_scope.py` is today a single-prefix classifier —
`DOCS_PREFIX = b"docs/"` and nothing else — so a changeset confined to
`scripts/oneoff/` plus its tests needs lint and its own unit tests and no
Docker build, no dbt job, no 267-second critical path. That is the first real
instance of the impact selection Stage 10 generalises, against code that
already exists.

**The classification is mechanical, and that is why this stage is small.** The
first scoping of it assumed per-file archaeology across fifteen archived plans.
It is not: **33 of the 35 Python scripts declare their owning plan in the first
three lines of the docstring**, so the bucket falls out of a join — docstring
plan number against the archived numbers in
[`completed_plans.md`](../planning/completed_plans.md), overridden by the
binding-reference grep, which wins in both directions.

**The name is never the signal, and the override is what proves it.**
`audit_adaptive_refresh_features.py` reads as forensics and is baked into
`dbt_runner/Dockerfile`; `report_dbt_run_results.py` belongs to archived Plan
123 and is in the same image. Both are production. The `audit_`, `estimate_`
and `spike_` prefixes classify nothing.

**Five scripts declare no plan and are the whole of the judgement.**
`ci_change_scope.py` is settled by its `ci.yml` reference; the remaining four —
`audit_parquet_layout.py`, `audit_normalized_parquet_layout_once.py`,
`backfill_unlisted_silver.py` and `diff_semantic_duplicate_html.py` — need
reading. That is the residual, and it is four files.

**A coupling finding that ran the other way, recorded because the first reading
of it was wrong.** Two production scripts import from scripts that look spent —
`export_volatility_features_to_iceberg.py` takes `cleanup_keys` from
`spike_iceberg_lakehouse.py`, and `train_html_dictionary.py` imports from
`estimate_dictionary_savings.py`. Scoped as "production depends on a spike" and
as this stage's hardest part. **The archive join dissolved both:** Plans 112 and
129 are not archived, so all four files stay in production-land and neither
import crosses a bucket boundary. The lesson is the one Stage 0 already
recorded — run the measurement before sizing the work it implies.

**Two constraints on `oneoff/`, stated so they are decisions rather than
drift.** Spent means *out of the ratchet's denominator* — never deleted, and
never untested: `reconcile_april_detail.py` is 84% covered **because** it
deleted 14.6 GB of production data, and its tests are why that was safe. And an
entry there should have to cite the archived plan it belongs to, so the bucket
cannot outlive its reasons the way an unchecked waiver list would.

**What it is worth, measured rather than asserted.** The `oneoff/` bucket —
archived owning plan, no binding reference — is **14 scripts, 6,338 statements
at 72%**, still over half of it `reconcile_april_detail.py` alone. Removing it
takes the denominator from 19,733 to 13,395 and the reported number from 75.91%
to **77.8%**. Stage 8's dashboard repair — 309 statements, 280 of them
currently missed — moves the total by **+1.27 points today and +1.87 after**,
so the ratchet becomes about **1.5× more responsive**. Real, and worth having
before Stages 6 through 9 are graded; nowhere near large enough to justify
paying the Stage 5 waiver collision to get it sooner. That arithmetic is why
this stage is placed on the waiver argument rather than the coverage one.

An earlier draft of this section put the bucket at 19 scripts and 7,019
statements. It was close by accident and wrong in composition: it counted
`spike_iceberg_lakehouse.py`, `run_dbt_spark.py`, `verify_dialect_datediff.py`
and both `compare_gate_*_parity.py` against Plan 125, and
`estimate_dictionary_savings.py` against Plan 129 — **six scripts belonging to
plans that are still open.** Spent is a property of the owning plan's state, not
of how finished a script looks.

### Stage 3 carries a constraint worth knowing before it starts

The two censuses live in **different virtual environments**.
`tests/airflow/test_health_sensor_demotion.py` asserts 13 DAG files wire a
sensor and runs in the main venv, where it must never import `airflow`;
`tests/integration/airflow/test_dag_integrity.py` asserts 14 sensor tasks and
runs in the isolated Airflow venv. One DAG wires two sensors, so both numbers
are right and nothing connects them. **The single declared source they both read
therefore cannot import Airflow** — a data file, or a module with no Airflow
import.

Plan 134's deletion updated the first count and missed the second, and shipped
(`056cde7`); PR #293 then failed on a count nobody had touched. The comment
added reactively in `33b275e` is documentation, not a mechanism, and will drift
again. Plan 139 scoped this as XS and warned that an XL plan should not hold a
two-file fix hostage; giving it a numbered stage near the front settles that
permanently.

## Success criteria

**1. The waiver list is empty.** All 120 entries deleted, each by the repair it
was waiting for rather than by being removed. The contract's own assertions make
this self-verifying: a waiver that no longer describes a violation fails as
loudly as an unwaived violation does.

**2. Every gap this plan closes leaves behind something that fails if it comes
back.** A repair with no assertion behind it is Plan 84 repeated exactly — real
tests, an accurate description, false within months, invisible because nothing
could tell. This is the criterion the six unmechanised gaps exist to be measured
against, and it is why Stage 2 comes before the stages that would otherwise be
graded by the instrument it repairs.

Three exceptions, stated here so they are decisions rather than omissions:

- **G7** cannot be asserted by the existing rules and needs its own approach;
  the criterion is met by whatever that approach is, not by the route rule.
- **G12** may close without a rule at all, because the condition a rule would
  assert is the constraint being removed. If it ships without one, the plan says
  so explicitly rather than leaving a silent gap.
- **G13 closes an instance without closing its class, and cannot do better.**
  Fixing the canary test's quoting is a one-file repair; asserting that no test
  lets its environment decide the outcome is not mechanisable, and the one place
  that could observe the remaining failures — CI — runs Linux and is blind to
  every Windows-only instance by construction. The `PYTHONPATH` clause is the
  only part with a mechanism and it already has one. This is the weakest of the
  three exceptions and it should be recorded as such rather than dressed up: the
  next instance of G13's class will be found the way the last two were, by
  someone running the suite somewhere CI does not.

**3. The `dbt build + test` job is no longer the critical path**, and what
replaced it is named for what it does. Measured in wall-clock seconds against
the 267s baseline, not asserted.

**4. Every suite in `tests/integration/` is either invoked by a named CI step or
declared dormant with a reason.** This is Stage 1's exit and G1's repair, and it
is the one criterion already mechanically enforced today — the four current
waivers are the whole of the outstanding work against it.

## The estimate

**L, replacing the XL placeholder**, on three grounds:

1. **The census — the largest single unknown — is done.** The plan was sized XL
   when Stage 0 was an unbounded measurement against a standard that did not
   exist yet. It is now a completed stage with an enumerated result.
2. **Roughly half the waiver count is two mechanical sweeps.** 50 of 120 are
   Stage 5: converting 34 files to `mocker` and renaming 16 layer references.
   Near-zero judgement, verified by deleting a waiver.
3. **The remainder is bounded and enumerated**, file by file, in the gap list
   and the waiver tuples.

**Stage 1 is what confirms or destroys this.** The pass state of the 73 orphaned
tests is the one input the estimate rests on that the census could not settle,
which is why it is first of the remaining stages.

## Non-goals

- **Deciding the standard.** That was Plan 161, and it is archived. If this plan
  finds itself arguing about which mock library is correct, it is relitigating a
  closed decision.
- **Rewriting the contract to match the repository.** The waiver list shrinks by
  repairing code, not by revising rules. A rule that turns out to be wrong is a
  decision to make explicitly, in `docs/TESTING.md`, with the reasoning recorded
  — not a convenience taken mid-stage.
- **Editing `docs/PLANS.md`.** Its row for this plan still reads "stub until
  Plan 161 lands" and names an archived blocker. Correcting it is a state
  transition and belongs to the `plans` skill.

## What it absorbed from Plan 139

Plan 139 was written as test-suite *maintenance* and was archived on 2026-08-31
with Stages A, B and F delivered. Its disposition, recorded when the split was
made:

| Stage | Disposition |
|---|---|
| A — make coverage visible | Shipped; `ci.yml` runs `--cov`. What it did *not* do is make the number mean anything, which is G10 and Stage 2 here |
| B — recover the CI critical path | **This plan**, Stage 4 |
| C — understand the 92s step | **This plan**, Stage 4 |
| D — intent markers and the coverage decision | Split: the gate decision was Plan 161's questions 6 and 8; the markers and the coverage-source repair are **this plan** |
| E — advisory CI impact selection | **This plan**, Stage 10. Its own premise was "before any new fast path", and the restructure is the fast path |
| F — CI's database does not model production's schemas | Shipped 2026-08-31, PR #305 (CAR-36). CI now runs `airflow db migrate` |
| G — Promtail contract checker | Moved to [Plan 160](plan_160_promtail_contract_checker_reliability.md) |
| H — one invariant, two censuses | **This plan**, Stage 3 |

Stage E carries one piece of thinking worth preserving verbatim rather than
rediscovering: Plan 142's service graph is *evidence* for a CI selector, not the
selector itself, because "production asks which live work depends on a service,
while CI asks which tests, images and integration environments can detect a
changed path."

**Two notes from Stage F (CAR-36), for this plan to pick up rather than
rediscover:**

- **CI's Postgres is greenfield; production's is populated.** Stage F made the
  `airflow` schema exist in CI, but built from empty, while production's carries
  hundreds of thousands of rows. Same root cause as bare images versus Compose
  definitions: CI's database is not shaped like production's. Worth measuring in
  Stage 10 — *which* suites depend on an empty database, and which would find
  something in a full one. The rehearsal that would close it needs a deployed
  stack, not a CI job, and is recorded in
  [Plan 121](plan_121_staging_environment.md).
- **`tests/integration/airflow/` still points at `sqlite:////tmp/airflow.db`.**
  Stage F left it deliberately: pointing the DAG tests at the same Postgres
  metadata DB the drain tests read would mix test data into it. Now that a real
  Airflow metadata schema exists in the same job, whether those suites should
  share it is Stage 10's call.

**Consequence, resolved 2026-08-30:** Plans 103 and 107 were triggered by "Plan
139 Stage D settles the coverage gate." Stage D was taken apart, so that trigger
named something that would not happen. Both were **superseded by Plans 161 and
162** — their premises were a coverage percentage and a self-scored rubric, both
last edited 2026-04-29, and both are what Plan 161's contract now decides. Parts
of each had already shipped under other plans without them.

## Intersections

### Plan 161 — the testing contract

Archived. It decided the rules and built the mechanism that measures them; this
plan closes the distance. `docs/TESTING.md`'s gap list names Plan 162 as the
owner of thirteen entries — twelve at the census, plus G13, re-owned here on
2026-08-31 — and an assertion fails if that owner is ever an archived plan — so this plan cannot be quietly abandoned without the suite
saying so.

### Plans 103 and 107 — coverage

Superseded, and **their targets are this plan's inheritance rather than their
own**: 103's per-file coverage gaps and 107's testing-rubric third are what the
gap list now measures. Read them for the gap list they assembled, not for their
numbers.

### Plan 120 — CI lake snapshot

Complete, and supplies the production-shaped fixture Stage 10 needs. It is
already seeded in CI by `scripts/seed_lake_snapshot_fixture.py` and unused for
the dbt build it was paid for.

### Plan 121 — staging environment

Owns the deployed-stack rehearsal that Stage 10's greenfield-versus-populated
question cannot close from inside a CI job.

## Evidence

### Evidence — Stage 0, the census (CAR-40), 2026-08-31

Both exit conditions met. Commit `dfa55ae`. Estimate 2, actual 1.

The census ran against the instrument Plan 161 built rather than by hand, which
is the whole reason it cost 1 rather than the XL this plan was sized at. What
it produced is [above](#what-the-census-found): 120 waived violations across
five mechanically checked rules, twelve gaps, and a stage per repair.

**Every by-eye reading this document had carried since 2026-08-30 was an
undercount, three times out of three**, and the gap inspection missed entirely
— G14, 54 of 76 `.sql` files executed by no Layer 2 test — is now the largest
single item in the plan. The direction of that error is the reusable finding,
not the individual numbers.

**`dashboard/` is Streamlit, not FastAPI.** G7 therefore cannot be reached by
the route rule or by the "enough" floor's first clause: the rule imports
`<service>.app` and reads its OpenAPI schema, and there is no schema to read.
This was not known when the gap list was written, and it means Stage 8 must
invent an approach rather than drain a waiver list.

**Half the gap list is unenforced.** Six of the twelve gaps are checked by
nothing at all, which is what success criterion 2 exists to answer — a repair
that leaves no assertion behind is Plan 84 repeated, and Plan 84's description
was accurate on the day it was written too.

**One defect was found and spun out rather than absorbed.** `docs/TESTING.md`'s
rules table claimed a check the suite did not implement: the Layer 2 rule was
written as "every `.sql` file **and module-level statement**", and only the
first half existed. Fixed as CAR-43 (PR #311) under Plan 161, which owned the
defect, and merged before this scoping was committed — the contract is the
standard this plan measures against, and scoping against a document that
overstated itself would have been building on sand. The repair added
`test_every_asserted_rule_names_a_real_test`, so the contract can no longer
claim a mechanism it does not have.

**What the census could not settle:** whether the 73 orphaned tests still pass.
That is Stage 1, and it is the one input the L estimate rests on that remains
unmeasured.

### Evidence — Stage 1, the orphaned suites (CAR-45), 2026-08-31

All five exit conditions met. Estimate 2.

**The 73 tests were run.** Against a cold `postgres:16` with all 49 Flyway
migrations applied and a MinIO container, matching the `dbt` job's services
step for step — not against a warm local stack, which would have had the state
the suites are missing:

| Suite | Files | Tests | Result |
|---|---|---|---|
| `tests/integration/processing/` | 6 | 58 | **51 passed, 7 failed** |
| `tests/integration/scraper/` | 1 | 4 | 4 passed |
| `tests/integration/shared/` | 1 | 4 | 3 passed, 1 declared skip |
| `tests/integration/lakehouse/` | 3 | 7 | dormant — not run |

**66 of 73 passed. The 7 failures were all defects in the tests, none in
production code** — which is the answer the estimate needed, and the better of
the two available answers. The areas these suites cover are not unexercised
because the code rotted; they were unexercised because nothing ran the tests.

Two distinct defects, both of the kind only running can find:

- **Six cleanups named `staging.artifact_events`, a table no migration has ever
  created.** V017 created `staging.artifacts_queue_events`; the test file has
  said `artifact_events` since `e95e426`, the commit whose message claims to
  "close processing service test gap". Every one of those six tests had already
  passed its assertions and then failed on teardown — the suite was born broken
  and merged anyway, because merging did not involve running it.
- **`test_vin_relisting_replaces_old_row` asserted a remap that
  `upsert_vin_to_listing.sql` correctly refuses.** The SQL has a recency guard —
  it only remaps on a strictly newer `mapped_at`, and production passes the new
  artifact's `fetched_at`. The fixture defaulted the prior mapping's timestamp
  to `now()`, so the value the test then supplied was never newer and the
  remap was always a no-op. The test was wrong; the guard is the feature.

**A third failure was found that the first two do not explain, and it is worth
recording as unexplained.** In the first two runs
`test_respects_batch_size_limit` reported five of its own rows claimed against
`batch_size=2`. It has not reproduced since the queue was drained, and a direct
harness confirms `_claim_batch(2)` returns exactly two rows. What *is*
reproducible is the class it belongs to: `_claim_batch` reads the whole of
`ops.artifacts_queue` lowest-`artifact_id`-first, so every "my row was claimed"
assertion in that file is really asserting the row landed inside the first
`LIMIT`. Seed twenty pending rows and two of those tests fail deterministically.

That mattered enough to fix rather than note. The suite passes today in CI's
step order — measured, not assumed: after `sql`, `ops` and `scripts` run,
`ops.artifacts_queue` is empty — but "passes because the four suites ahead of
it happened to leave no rows" is not wired in, it is booby-trapped. A
function-scoped `_quiet_queue` fixture now parks any other claimable row for
the duration of each test and restores it afterwards. Verified both ways: 58
pass against a queue holding 20 foreign pending rows, and those 20 are still
`pending` afterwards.

**Dormancy could not be a waiver, and finding out why was the stage's one
design change.** Stage 1 was scoped to declare `tests/integration/lakehouse/`
dormant through the waiver list, on the reasoning that a waiver already carries
a reason, an owner and a date. It cannot: `test_no_waiver_outlives_the_plan_that_owns_it`
fails any waiver whose owner plan has archived, so the lakehouse entry would
have failed the day *this plan* archived, and the only way to quiet it would
have been to delete the record of why the suite is not running — losing exactly
what G2 asked to be written down. Dormancy is a decision with no repair pending
and no owner to outlive, so it now lives in `DORMANT_SUITES`: same file, same
shape, no owner, no expiry. `test_no_dormant_suite_is_quietly_running` closes
the other direction, failing a declared suite that acquires a CI step.

**`CI_INVOCATION_WAIVERS` is `()`.** Both new assertions were verified by
breaking them: removing the processing step fails the invocation rule against
an empty waiver tuple, and pointing a step at the dormant suite fails the
dormancy guard. 120 waivers → **116**.

**The L estimate is confirmed.** Stage 1 was the one input it rested on that
the census could not settle, and it resolved the favourable way: no production
defects, no rot in the covered areas, and Stages 7 and 8 do not get worse. The
two test defects cost minutes, not the days a genuine failure would have. What
Stage 1 adds to the estimate is not effort but a warning about its shape — both
defects, and the queue fragility, were invisible to review and obvious to
execution, so the remaining stages should be sized on the assumption that
anything this plan has only *read* is still unmeasured.

**Found in passing, and it turned out to belong to nobody — so this plan took
it.** `tests/scripts/test_verify_recovery_live_state.py::test_a_failing_canary_command_fails_the_check`
fails on Windows ("The filename, directory name, or volume label syntax is
incorrect") and passes in CI; it quotes `sys.executable` with `shlex.quote`,
which `cmd.exe` does not honour. Reproduced on an untouched checkout, so it
predates this stage.

Chasing its owner is what made it Plan 162's. It is G13's class, and G13 was
the one gap in `docs/TESTING.md` this plan did not own — assigned to **Plan 146
Stage 1 (CAR-42)**, scoped to the `PYTHONPATH` half. That half shipped; the
`Documentation tests` step sets `PYTHONPATH` today, and `21333ab` had already
repaired the other named instance. So the G13 row described finished work while
naming an owner in closeout that owes no code, and the live instance — found and
deliberately left by Plan 161 — had no owner at all.

**G13 is therefore re-owned to Plan 162, Stage 5**, and the row rewritten to
describe what is actually left. Stage 5 rather than Stage 3: Stage 3 is a
specific two-venv census fix and "also small" is not a category, while Stage 5
is already the pass that reads every patch in the suite. What that costs is
honest and recorded above — [a third exception](#success-criteria) to success
criterion 2, and the weakest of the three.

### Evidence — Stage 2, unblinding coverage (CAR-46), 2026-08-31

All three exit conditions met. Commits `8c10d95` and `2b12294`. Estimate 1,
actual 1.

`[tool.coverage.run] source` now names all ten production directories, the unit
job gates on `--cov-fail-under=74` and uploads `coverage.xml`, and two
assertions carry the repair — `test_every_service_directory_is_measured_by_coverage`
and `test_the_coverage_number_the_unit_job_produces_is_consumed`. Both were
verified by breaking them; the mutation set is 19 and all 19 are caught.

**Unblinding moved the reported number from 88% to 75.95% with no code
changing** — the figure CI reads, and the one the gate acts on. The four
directories added hold 11,709 of 19,733 measurable statements — more than the
six that were being measured:

| Added to `source` | Statements | Covered |
|---|---|---|
| `scripts/` | 10,488 | 70% |
| `airflow/dags/` | 754 | 45% |
| `dashboard/` | 309 | **9%** |
| `container_health/` | 158 | **93%** |
| *(the six already measured)* | 8,024 | 88% |

**The two services the "enough" table calls below the floor are not alike, and
only the instrument could show it.** `container_health` is among the
best-covered directories in the repository at 93% and is still below the floor,
because the floor is routes reached through the app and a Layer 4 that exists —
neither of which a percentage measures. That is the contract's *"not a coverage
percentage"* clause holding under its first real test, and it re-scopes Stage 6:
G9 is a test-home and routing problem, not a coverage one. `dashboard/` at 9%
is the genuine gap, and it is Stage 8's.

**The threshold is a ratchet, not a target, and `scripts/` is the caveat on
it.** At 10,488 statements it is over half the denominator and largely spent
one-off code, so it damps the movement the service stages produce. That is
[Stage 5b](#the-stages), scoped from this measurement.

**Two of this stage's own claims were wrong and were corrected by measuring.**
The gap list said unblinding would expose "the two services below the floor"; it
exposed one badly covered service, one well-covered one, and a `scripts/`
denominator nobody had counted. The first sizing of Stage 5b then repeated the
error in miniature — see its section for what the archive join corrected.

**Landed alongside Stage 1 and reconciled to it.** Stage 1 established the
convention for repaired gap entries — row deleted, preamble names what closed
it, history here, letters never reused — while this branch was open; G10's
closure was rewritten to follow it. Stage 1's own deletions had left three
mutations in `scripts/verify_testing_contract_mutations.py` anchored on the
removed G1 and G2 rows, so the script aborted rather than ran; its staleness
guard is what said so. Re-anchored, and Stage 1's new dormancy rule was given
the mutation it shipped without.

**Confirmed in CI, which is the only place the gate can actually fire.**
[Run 33442835886](https://github.com/whitewalls86/new_car_tracker/actions/runs/33442835886)
on PR #318, all jobs green:

- `Required test coverage of 74% reached. Total coverage: 75.95%` — the
  threshold ran and passed, with **1.95 points of headroom**.
- `Artifact coverage-xml has been successfully uploaded` — 49,164 bytes,
  artifact 9776948765. The report outlives the log, which was the point.

**Linux reads 75.95% where Windows reads 75.92%** — same 19,733 statements,
4,745 missed against 4,752, so **seven statements are platform-dependent**.
That is the number the ratchet's headroom has to absorb, and it is why 74 was
set two points below the local measurement rather than one. A future stage
raising the floor should keep at least that much slack.

**The canary test that fails locally passes here**, which is G13 restated as
evidence rather than assertion: `test_a_failing_canary_command_fails_the_check`
is among the 3,195 that pass on Linux and is the one failure on Windows. CI
cannot see the instance Stage 5 owns, exactly as its row says. The single
Linux skip is unrelated — `test_every_sha_a_recap_names_is_a_real_commit`,
which skips on a shallow clone.

### Evidence — Stage 3, one declared source for the health-sensor censuses (CAR-47), 2026-08-31

All three exit conditions met. Commits `17d4fab`, `a5fda6c` and `92ef62b`,
[PR #319](https://github.com/whitewalls86/new_car_tracker/pull/319). Estimate 1,
actual 1.

`tests/health_sensor_census.py` declares the mapping both counts derive from:
DAG file → the service names it passes to `http_health_sensor`. Thirteen keys,
fourteen services — the two numbers that were hardcoded separately, now one
declaration. **Declaring the structure rather than the two integers is what
makes the stage's own constraint disappear**: "one DAG wires two sensors" is a
fact in the data instead of the comment that was documenting it, and neither
count can be updated without the other following.

Both assertions got stronger as a side effect of having something to compare
against:

| Test | Before | After |
|---|---|---|
| `test_the_gate_survives_the_demotion` (main venv) | `len(wired) == 13` | the whole file→services mapping, ast-extracted from the real call sites, so drift names the DAG |
| `test_health_sensors_skip_rather_than_fail_on_the_real_operators` (Airflow venv) | `health_sensors == 14` | sorted DagBag task ids against the ids the census implies, so a missing one names itself |

One assertion is new. `test_the_task_id_the_census_predicts_is_the_one_the_factory_builds`
pins `sensors.py`'s `f"check_{service_name}_health"` — the only link joining the
census's service names to real task ids, and previously unchecked from either
side.

**The exit asked that the declared source not import Airflow; it imports nothing
at all**, and that turned out to be load-bearing for a reason the exit did not
anticipate.

**The first attempt failed in exactly the way this stage exists to catch.**
`from tests.health_sensor_census import ...` resolves in the main venv and not
in the isolated `apache-airflow==3.2.0` one, where pytest leaves the repo root
off `sys.path`.
[Run 33444675959](https://github.com/whitewalls86/new_car_tracker/actions/runs/33444675959)
failed collection with `ModuleNotFoundError: No module named 'tests'` on an
import that had passed locally and could not be made to fail locally. The fix
was to remove the environment from the question — both readers now load the
census by path with `importlib.util`, which depends on nothing — rather than to
add the repo root to the CI step's `PYTHONPATH`, which would have made the
suite's outcome depend on its environment and manufactured a fresh instance of
G13's class while closing this one. **The pre-push verification had proved only
that the main venv could reach the file, and inferred the rest.** That is worth
recording as the stage's real cost: the two-venv constraint was understood,
written down in the issue, and still evaded on the one axis nobody checked.

**Mutation-checked in three directions**, each failing with the intended
message: dropping a census entry, adding a sensor to a DAG, and renaming the
`task_id` format. The path loader was then re-verified against the condition CI
actually had — with the repo root stripped from `sys.path`, the bare import
raises the same `ModuleNotFoundError` while both test modules import cleanly and
read 13 files / 14 tasks.

**Confirmed in CI, which is the only place the DagBag half can run.**
[Run 33445223553](https://github.com/whitewalls86/new_car_tracker/actions/runs/33445223553),
all jobs green:
`test_health_sensors_skip_rather_than_fail_on_the_real_operators PASSED` in the
isolated venv, with `dbt build + test` at 4m43s.

**What is not verified:** no mutation was run against the DagBag census in CI,
which would mean pushing a deliberately broken commit. That half follows by
construction — it reads the same declaration through the same loader — and it
has passed, but it has not been broken on purpose the way the main-venv half
has.

**A third census was found one layer up, and was already one short.**
`DAG_SPECS` in `tests/integration/airflow/test_dag_integrity.py` omitted
`disk_usage`, so neither `test_dag_imports_without_error` nor
`test_dag_id_and_tasks` ever reached that DAG — while
`airflow/dags/disk_usage.py`'s own `except ImportError` comment claimed *"the
Airflow integration suite imports the real DAG and asserts it exists"*. A false
claim, checked into source, in the same shape as the defect this stage removed:
a list kept honest by whoever remembers it. Fixed in `92ef62b` — the entry
added, and a new assertion comparing the DagBag's dag_ids against the ones
`DAG_SPECS` names, because parametrising over a list can only ever check the
things someone thought to list. It is not owned by any stage: G12 is about
`shared` imports, not this.
