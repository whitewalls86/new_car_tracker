# Plan 162: The Testing Census and CI Restructure

## Status

**Stage 0, the census, is complete (CAR-40, 2026-08-31).** Written as a
deliberate stub on 2026-08-30, when
[Plan 161](plan_161_testing_contract.md) had not yet decided the standard this
plan measures against. That blocker is gone: 161's contract landed, was
asserted, and is archived.

Stages 1 through 10 are scoped below and unblocked. Effort is proposed as **L**,
down from the XL placeholder, on the reasoning in
[The estimate](#the-estimate). [`docs/PLANS.md`](../PLANS.md) owns priority and
effort; this document does not choose them.

## What the census found

[`tests/test_testing_contract.py`](../../tests/test_testing_contract.py)
implements seven mechanical rules. It passes, and **a pass means only that the
seven rules hold** — every violation standing on 2026-08-31 is grandfathered in
a waiver list. That list is this plan's backlog:

| Rule | Waivers | Gap |
|---|---|---|
| CI invocation | 4 | [G1](../TESTING.md#the-gap-list) (3), G2 (1) |
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

Six of the twelve gaps this plan owns are checked by nothing: **G5, G7, G8, G9,
G10 and G12.** They are recorded in prose, they are not among the 120, and they
can worsen without anything noticing. That is the condition
`ARCHITECTURE.md:179` was in before Plan 161, and it is why this plan's success
criteria are written the way they are below.

Three sub-cases, because they are not alike and the differences drive the stage
order:

- **G5, G9 and G10 have a natural, cheap assertion.** G10's is roughly five
  lines: every service directory appears in `[tool.coverage.run] source`.
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
| **1** | Run the orphaned suites. Execute the 11 files no CI step invokes, wire the survivors into CI, declare `tests/integration/lakehouse/` dormant | G1, G2 | 4 |
| **2** | Unblind coverage. `[tool.coverage.run] source` names every service directory, and something consumes the number | G10 | -- |
| **3** | The two health-sensor censuses read one declared source instead of two hardcoded counts | Plan 139 Stage H | -- |
| **4** | Split the 267s `dbt build + test` job — the cheap half of the restructure | Plan 139 Stages B, C | -- |
| **5** | The mechanical sweeps: 34 mock conversions and 16 layer renames | G4, G11 | 50 |
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

Two exceptions, stated here so they are decisions rather than omissions:

- **G7** cannot be asserted by the existing rules and needs its own approach;
  the criterion is met by whatever that approach is, not by the route rule.
- **G12** may close without a rule at all, because the condition a rule would
  assert is the constraint being removed. If it ships without one, the plan says
  so explicitly rather than leaving a silent gap.

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
owner of twelve entries, and an assertion fails if that owner is ever an
archived plan — so this plan cannot be quietly abandoned without the suite
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
