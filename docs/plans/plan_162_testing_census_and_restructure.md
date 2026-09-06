# Plan 162: The Testing Census and CI Restructure

## What this plan is for

Runs a census of the whole automated test suite for coverage, dead assertions,
and drift between what CI checks and what the code does, then closes the gaps
it finds and restructures CI around what the census showed actually mattered.

## The case

[`tests/test_testing_contract.py`](../../tests/test_testing_contract.py)
implemented seven mechanical rules when the census ran, and eight since Stage C
added the coverage rule. It passes, and **a pass means only that those rules
hold** — every violation standing on 2026-08-31 is grandfathered in a waiver
list. That list is this plan's backlog:

This is the census as taken, kept as the baseline the stages are measured
against; the live count is whatever `tests/test_testing_contract.py` holds
today. Stage B has since cleared the CI-invocation row, Stage F the mocker and
layer-numbering rows, Stage H the route row, and Stage J's encoding rule
started empty and has stayed empty. Stage F also corrected the Layer 2 row
upward, from 54 to 56 — see
[the instrument note](../evidence/plan_162_stage_F_evidence.md#the-instrument-was-weaker-than-its-own-docstring).

**Measured 2026-09-03, by importing the tuples: the live total is 37**, down
from 120 at the census.

| Tuple | Live | Gap |
|---|---|---|
| `DUPLICATE_SQL_WAIVERS` | 1 | G17 — the one waived pair, two policies that agree |
| `INLINE_SQL_WAIVERS` | 15 | G5 |
| `SQL_LITERAL_WAIVERS` | 21 | G15 |
| `CI_INVOCATION`, `MOCKER`, `ROUTE`, `LAYER_NUMBER`, `ENCODING` | 0 | G1, G2, G4, G6, G11, G13's class — all drained |
| **Total** | **37** | |

**Every count here is the number an instrument reports, not a number this
document remembers**, and the rule exists because this document keeps breaking
it. The count read 68 for a day after Stage H had made it 56, and was caught
during Stage J's closeout by importing the waiver tuples rather than reading
the paragraph that claimed it. It then read 56 from 2026-09-02 until
2026-09-03, when Stages L, M and N had between them taken it to 37, and was
caught the same way — while scoping Stage P. Twice is a pattern, and both are
left on the record rather than quietly corrected: this plan's own subject
matter, happening to this plan, in the one section that asserts it will not.

| Rule | Waivers | Gap |
|---|---|---|
| CI invocation | 4 → **0** | [G1](../TESTING.md#the-gap-list) (3), G2 (1) — both closed by Stage B |
| Patching is `mocker` | 34 → **0** | G4 — closed by Stage F |
| Route reached through `app.routes` | 12 | G6 |
| `.sql` file touched by a Layer 2 test | 54 → **56** | G14 — the census undercounted; see Stage F |
| Layer numbering | 16 → **0** | G11 — closed by Stage F |
| **Total** | **120** | as measured; 122 on the corrected reading |

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
| 16 modules with inline SQL | **10** modules (G5) — and a gap nobody had counted: **54 of 76** `.sql` files that no Layer 2 test executes (G14). Stage F later corrected this to 56 |

The direction of the error is the point. Inspection undercounted three times out
of three, and the one gap that inspection missed entirely, G14, is now the
largest single item in the plan.

### Six gaps have no mechanism at all

Six of the thirteen gaps this plan owns are checked by nothing: **G5, G7, G8,
G9, G10 and G12.** (Twelve at the census; **G13 was re-owned here on
2026-08-31** when its Plan 146 half shipped, and it is half-checked — the
`PYTHONPATH` clause is asserted and nothing else is. **G10 has since been
mechanised and closed** by Stage C the same day, leaving five.) They are
recorded in prose, they are not among the 120, and they can worsen without
anything noticing. That is the condition
`ARCHITECTURE.md:179` was in before Plan 161, and it is why this plan's success
criteria are written the way they are below.

Three sub-cases, because they are not alike and the differences drive the stage
order:

- **G5, G9 and G10 have a natural, cheap assertion.** G10's was estimated at
  roughly five lines — every service directory appears in
  `[tool.coverage.run] source` — and Stage C built it, at that size plus a
  second assertion for the half the estimate had not counted.
- **G7 could never be reached by the existing rules, and that is why it was the
  wrong gap.** `dashboard/` is Streamlit, not FastAPI. The route rule imports
  `<service>.app` and reads its OpenAPI schema; there is no schema to read. The
  "enough" floor's first clause is structurally inapplicable to the one service
  with zero test files. **Rescoped 2026-09-02** — G7 is now the dashboard's
  Layer 2 suite asserting nothing, which is reachable, cheap and this plan's;
  the Python that needed a test invented is G18 and belongs to Plan 150. See
  [Stage M narrowed](#stage-m-narrowed-and-g7-now-names-a-different-gap).
- **G12 may correctly never get a rule.** "No module under `airflow/dags`
  imports `shared`" is *true today* — it is the constraint, not the violation.
  Closing it changes the DAG tree's import structure, which is an architecture
  decision and not an assertion.

### Why the estimate is L

**L, replacing the XL placeholder**, on three grounds:

1. **The census — the largest single unknown — is done.** The plan was sized XL
   when Stage A was an unbounded measurement against a standard that did not
   exist yet. It is now a completed stage with an enumerated result.
2. **Roughly half the waiver count is two mechanical sweeps.** 50 of 120 are
   Stage F: converting 34 files to `mocker` and renaming 16 layer references.
   Near-zero judgement, verified by deleting a waiver.
3. **The remainder is bounded and enumerated**, file by file, in the gap list
   and the waiver tuples.

**Stage B is what confirms or destroys this.** The pass state of the 73 orphaned
tests is the one input the estimate rests on that the census could not settle,
which is why it is first of the remaining stages.

### What it absorbed from Plan 139

Plan 139 was written as test-suite *maintenance* and was archived on 2026-08-31
with Stages A, B and F delivered. Its disposition, recorded when the split was
made:

| Stage | Disposition |
|---|---|
| A — make coverage visible | Shipped; `ci.yml` runs `--cov`. What it did *not* do is make the number mean anything, which is G10 and Stage C here |
| B — recover the CI critical path | **This plan**, Stage E |
| C — understand the 92s step | **This plan**, Stage E |
| D — intent markers and the coverage decision | Split: the gate decision was Plan 161's questions 6 and 8; the markers and the coverage-source repair are **this plan** |
| E — advisory CI impact selection | **This plan**, Stage P. Its own premise was "before any new fast path", and the restructure is the fast path |
| F — CI's database does not model production's schemas | Shipped 2026-08-31, PR #305 (CAR-36). CI now runs `airflow db migrate` |
| G — Promtail contract checker | Moved to [Plan 160](plan_160_promtail_contract_checker_reliability.md) |
| H — one invariant, two censuses | **This plan**, Stage D |

Plan 139 Stage E carries one piece of thinking worth preserving verbatim rather than
rediscovering: Plan 142's service graph is *evidence* for a CI selector, not the
selector itself, because "production asks which live work depends on a service,
while CI asks which tests, images and integration environments can detect a
changed path."

**Two notes from Plan 139 Stage F (CAR-36), for this plan to pick up rather than
rediscover:**

- **CI's Postgres is greenfield; production's is populated.** Plan 139 Stage F made the
  `airflow` schema exist in CI, but built from empty, while production's carries
  hundreds of thousands of rows. Same root cause as bare images versus Compose
  definitions: CI's database is not shaped like production's. Worth measuring in
  Stage P — *which* suites depend on an empty database, and which would find
  something in a full one. The rehearsal that would close it needs a deployed
  stack, not a CI job, and is recorded in
  [Plan 121](plan_121_staging_environment.md).
- **`tests/integration/airflow/` still points at `sqlite:////tmp/airflow.db`.**
  Plan 139 Stage F left it deliberately: pointing the DAG tests at the same Postgres
  metadata DB the drain tests read would mix test data into it. Now that a real
  Airflow metadata schema exists in the same job, whether those suites should
  share it is Stage P's call.

**Consequence, resolved 2026-08-30:** Plans 103 and 107 were triggered by "Plan
139 Stage D settles the coverage gate." Plan 139 Stage D was taken apart, so that trigger
named something that would not happen. Both were **superseded by Plans 161 and
162** — their premises were a coverage percentage and a self-scored rubric, both
last edited 2026-04-29, and both are what Plan 161's contract now decides. Parts
of each had already shipped under other plans without them.

## Design

### The stage letters, and the numbers they replace

**Adopted 2026-09-04.** This plan was sequenced before
[the plan-document contract](../PLAN_DOCUMENT.md) landed, so its stages were
numbered, and the contract's own adoption clause exempts it: *"Plans that were
already sequenced when this contract landed keep their existing identifiers."*
It also names the one route out — a plan rewritten wholesale **may** adopt
letters if it records an old-to-new mapping — and this is that rewrite. The
mapping is the table below, and it is permanent rather than transitional: every
stage section and every record entry carries a **Legacy** line naming its old
number, so a commit message, a branch, a Linear title or a code comment written
against the old namespace still resolves.

| Legacy | Stage | | Legacy | Stage | | Legacy | Stage |
|:---:|:---:|---|:---:|:---:|---|:---:|:---:|
| 0 | **A** | | 6b | **J** | | 11 | **S** |
| 1 | **B** | | 6c | **K** | | 12 | **T** |
| 2 | **C** | | 7 | **L** | | 13 | **U** |
| 3 | **D** | | 8 | **M** | | 14 | **V** |
| 4 | **E** | | 9 | **N** | | 15 | **W** |
| 5 | **F** | | 10 | **P** | | 16 | **X** |
| 5b | **G** | | 10b | **Q** | |  |  |
| 6 | **H** | | 10c | **R** | |  |  |

**Two deviations, both recorded rather than left to be noticed.** The letters
are allocated down the work order rather than in discovery order, because for
the fourteen stages that had closed before this rewrite the discovery order is
not recoverable except from record dates, several of which collide; the property
the contract actually needs — that a letter never moves once allocated — starts
here. And **`I` and `O` are skipped.** This plan has a live Stage 0 and 128
references to its numbered stages standing in code, CI and other plans, so a
`Stage O` sitting beside a `Stage 0` would be a collision built on purpose. `Y` and `Z`
remain for the next two stages discovered.

**This reverses a decision recorded in this document**, and the reversal is the
point rather than an embarrassment. The order table used to argue that lettering
would cost 187 stage references and buy nothing. What changed is that the
contract landed with a mechanised waiver list behind it, and that Stage R turned
out to belong last in the order while keeping its place in the numbering — which
is precisely the ambiguity a numbered `Order` beside a lettered `Stage` exists
to dissolve.

### Why this order

Four of the placements are load-bearing. The rest is grouping.

**Stage B is first of the remaining stages because it is the only unknown that
changes the estimate.** 73 integration-marked tests sit in 11 files that no CI
step has ever invoked; `tests/integration/processing/` — 58 of them — has never
appeared in `ci.yml` in its history. **Whether they still pass is unknown**, and
every other stage can be sized from measurements Stage A already took. If those
suites have rotted, the areas they cover are unexercised and Stages L and M both
get worse. Running them is cheap, is a repair in its own right, and is what
converts this plan's estimate from a proposal into a measured number.

**Stage C is second because coverage is the instrument the rest of the work
reads.** `[tool.coverage.run] source` names six packages and omits
`container_health`, `dashboard`, `scripts` and `airflow/dags` — so **the two
services furthest below the floor are the two the instrument cannot see.** Every
stage behind this one measures better for it being fixed first.

**Stage E sits after Stage B, not before it.** Stage B changes which suites
exist in CI; splitting the job afterwards means organising once with full
knowledge rather than twice. The rest of the restructure stays at Stage P,
where its risk belongs — but the job split itself is largely mechanical, and
leaving it until last would mean running the most CI-intensive work this
repository has attempted across weeks of a 267-second critical path we had
already decided to remove.

**Stages H and L each pair two gaps because splitting them means touching the
same files twice.** G9 builds `container_health` a test directory and a Layer 4
that do not exist; G6's four `container_health` routes are uncoverable until it
does. G5 moves inline SQL into `.sql` files and G14 gets Layer 2 executing
`.sql` files — the same modules, from opposite ends.

**G13 joined Stage F on 2026-08-31, for the same reason.** It is the thirteenth
gap and the only one this plan did not originally own: the contract assigned it
to Plan 146 Stage 1 for the `PYTHONPATH` half, that half shipped as CAR-42, and
the remaining instance was left owned by a plan that owes no code. Stage F is
the right home because it is already the pass that reads every patch in the
suite — and "an unexplained mock of a filesystem, clock, platform or path
primitive is a finding" is the same question asked one step further out. The
pattern the rule holds up as correct, `21333ab`, is itself a mocking fix. Doing
the two together is one reading of the suite instead of two.

**Stage G sits immediately after Stage F because Stage F is what makes it
free.** The first instinct was to run it near the front, so the stages that
follow would visibly move the coverage number. The waiver list forbids it. Ten
of Stage F's 34 mocker waivers name files under `tests/scripts/` and
`tests/integration/scripts/`, character for character, and `_assert_exactly`
asserts both directions — so moving a test file breaks its waiver twice, once
because the old subject has stopped existing and once because the new path is
an unwaived violation. Running Stage G first means rewriting ten waiver subjects
that Stage F then deletes outright. Running it second costs nothing, because
Stage F has already emptied the colliding set. No other waiver tuple names a
`scripts/` path — `LAYER_2_WAIVERS` and `LAYER_NUMBER_WAIVERS` have none, and
`CI_INVOCATION_WAIVERS` is empty since Stage B — so Stage F is the only
collision in the plan.

Nothing is lost by the delay. **Stages H, L, M and N are all downstream of
Stage F**, so every remaining stage that moves the number is still graded
against the cleaned denominator, which was the whole point of going early. The
CI payoff is not delayed either: Stage E is the dbt job split, and the impact
selector that reads the new prefix is Stage P.

**It was numbered 5b rather than 6.** Inserting an integer would have renumbered
five stages and invalidated two issues already filed against the old numbers —
CAR-50 and CAR-52 still read `Stage 6` and `Stage 8` in their titles today,
because a closed issue keeps the name it closed under. A suffix cost nothing and
broke nothing, which is the argument the lettering
[now generalises](#the-stage-letters-and-the-numbers-they-replace).

### The remaining eight, ordered 2026-09-04

The stages that closed were placed one at a time, by what unblocked what. The
eight that remain were ordered together, once, on three constraints and one
deadline: **U, X, S, T, W, Q, V, R.**

**Stage U goes first because two later stages hand work to it, and it costs a
point.** [Stage Q's fourth scoping
decision](#four-decisions-taken-while-scoping-this-stage-2026-09-04) says its
`docker compose config` guard *"must skip cleanly when `docker` is absent and be
required in CI — which is exactly [Stage U]'s mechanism, and a dependency this
stage should hand forward rather than solve locally"*, and Stage R's first piece
runs after U precisely so it can read U's output instead of simulating it. Under
the numbering, U sat after both of them. Putting it first is the third
application of one argument this plan has already made twice — Stage C ahead of
the stages it measures, Stage J ahead of L, M and N: **a guard that lands first
is one the later stages get for free rather than one that has to sweep what they
wrote.**

**Stage X is second** because nothing in its original scope is invented and it
retires the scoping compromise Stage T was carrying — and **since 2026-09-04 it
also holds the only measurement that expires.** The execution recorder moved
here from Stage S on that date: recording what text ran against which engine is
repo-wide rather than dbt's, and X is already the stage that makes every
statement live in a file and validates it against an engine. Its capture
baseline must be taken while DuckDB is still authoritative — one taken after
[Plan 125 Gate
D](plan_125_duckdb_to_iceberg_migration.md#gate-d-reader-migration) is not a
baseline — and X sitting a position ahead of S serves that deadline better than
S did. Plan 125 is build-order row 9 with Gate D two gates out, so one stage
ahead of X costs nothing; that is room for U, not for V and R. **X's estimate
predates the recorder and has not been revisited.**

**Stage S is third, and since the same date carries no deadline of its own.**
The aggregation the recorder also needs — an artifact and a gate job — is why
CAR-79 was filed blocked on CAR-78, and it travelled to X with the rest: Stages
Q and R are what settle how those jobs are defined, so the aggregation half can
land with or after Q wherever it lives. Nothing in what remains of S is lost by
waiting.

**Then T, which wants X's recorder**, and **W, which wants U's registry** — W's
whole output is a declaration that something is deliberate, and U is what builds
the shape such declarations take. **Then Q**, now holding U's skip mechanism.
**Then V**, whose own issue warns it may turn production-gated: if a variable
genuinely needs wiring into `docker-compose.yml`, only a deploy proves it
arrived, which is a different risk class and does not belong on a critical path.
**Then R**, for the three reasons its own section gives.

**Two consequences outside this document, both acted on 2026-09-04.** CAR-78
bundled Stages Q and R, because both were CI-infrastructure work on the same
jobs. That stopped holding when Stage R's selector was cut — what remains of R
is an instrument fix, a caching measurement and a docs-zone path — and the order
now separates them by two positions. R left to **CAR-87**. CAR-81 bundled Stages
U and V as one class, a declaration nothing enforces; the order puts them six
positions apart, with U as the plan's `next`, so V left to **CAR-88**. Both new
issues carry 1 point: each parent held 2 across two halves, split one apiece.

## Stages

**`Order` is numbered and rewritten freely; `Stage` is lettered and never
changes.** The 2026-09-04 reordering is what that buys: eight stages changed
position and not one changed name, so every inbound reference still resolves.
Stage R is the clearest case — it holds order 22 and the letter it was allocated,
because [the CI cost census](../evidence/plan_162_stage_R_ci_cost_census_2026-09-04.md)
moved it to the end without making it a different stage.

| Order | Stage | Legacy | What it delivers | Closes | State | Issue |
|---:|:---:|:---:|---|---|---|---|
| 1 | [**A**](#stage-a-the-census) | 0 | The census | — | `done` | CAR-40 |
| 2 | [**B**](#stage-b-the-orphaned-suites) | 1 | The orphaned suites | G1, G2 | `done` | CAR-45 |
| 3 | [**C**](#stage-c-unblinding-coverage) | 2 | Unblind coverage | G10 | `done` | CAR-46 |
| 4 | [**D**](#stage-d-carries-a-constraint-worth-knowing-before-it-starts) | 3 | The two health-sensor censuses read one declared source | Plan 139 Stage H | `done` | CAR-47 |
| 5 | [**E**](#stage-e-splitting-the-267s-dbt-build--test-job) | 4 | Split the 267s `dbt build + test` job | Plan 139 Stages B, C | `done` | CAR-48 |
| 6 | [**F**](#stage-f-the-mechanical-sweeps) | 5 | The mechanical sweeps | G4, G11, G13 | `done` | CAR-49 |
| 7 | [**G**](#stage-g-what-the-split-is-and-why-a-directory-rather-than-a-list) | 5b | Separate production scripts from spent ones | — | `done` | CAR-55 |
| 8 | [**H**](#stage-h-container_healths-test-home-and-every-route-reached) | 6 | Route coverage, and `container_health`'s test home | G6, G9 | `done` | CAR-50 |
| 9 | [**J**](#stage-j-was-added-by-the-failure-this-plan-predicted) | 6b | Encoding-sensitive I/O, mechanised | G13's class | `done` | CAR-60 |
| 10 | [**K**](#stage-k-was-added-by-a-deploy-not-by-the-suite) | 6c | Every service contract produces an intent row the database accepts | — | `done` | CAR-66 |
| 11 | [**L**](#stage-l-grew-two-gaps-while-closing-one) | 7 | SQL execution, from both directions | G14; G5 to 15 | `done` | CAR-51 |
| 12 | [**M**](#stage-m-narrowed-and-g7-now-names-a-different-gap) | 8 | `scraper`'s floor, and the Layer 2 suite that asserts nothing | G7, G8 | `done` | CAR-52 |
| 13 | [**N**](#stage-n-the-dag-trees-sql-convention) | 9 | `airflow/dags` and the `.sql` convention | G12 | `done` | CAR-53 |
| 14 | [**P**](#stage-p-dbt-builds-against-production-shaped-data) | 10 | dbt builds against production-shaped data | — | `done` | CAR-54 |
| 15 | [**U**](#stage-u-every-skip-in-ci-is-declared-or-the-run-fails) | 13 | Every skip in CI is declared, or the run fails | — | `done` | CAR-81 |
| 16 | [**X**](#stage-x-a-test-may-not-author-sql-either) | 16 | A test may not author SQL either, and what text ran against which engine | — | `done` | CAR-83 |
| 17 | [**S**](#stage-s-answers-a-question-plan-161-did-not-ask) | 11 | Branch coverage for the dbt models, and what leaves the SQL census | G16 | `next` | CAR-79 |
| 18 | [**T**](#stage-t-exists-because-this-plan-grew-the-suite) | 12 | Shared fixtures: what the suite duplicates at 3,988 tests | — | `—` | CAR-80 |
| 19 | [**W**](#stage-w-a-test-may-not-supply-both-halves-of-a-contract) | 15 | A test may not supply both halves of a contract | — | `—` | CAR-82 |
| 20 | [**Q**](#stage-q-cis-services-are-productions-in-definition-and-in-contents) | 10b | CI's services are production's, in definition and in contents | — | `—` | CAR-78 |
| 21 | [**V**](#stage-v-a-variable-the-environment-documents-reaches-the-service-that-reads-it) | 14 | A variable the environment documents reaches the service that reads it | — | `—` | CAR-88 |
| 22 | [**R**](#stage-r-ci-selection-and-the-instrument-that-has-to-precede-it) | 10c | CI selection, and the instrument that has to precede it | Plan 139 Stage E | `—` | CAR-87 |

`State` takes the five values [the plan-document
contract](../PLAN_DOCUMENT.md#stages-and-order) defines — `—`, `next`,
`blocked`, `done`, `canceled` — and exactly one stage carries `next`.
**The stage sections below run in letter order, not work order**, so a stage is
found by its name rather than by remembering where it sits today. `Order` is the
only thing that says what comes next, which is the point of it being a column.

**4 + 50 + 12 + 56 = 122**, from Stages B, F, H and L respectively. The stages
account for the whole waiver list; no entry is left without a stage that deletes
it. Stage L later raised its own share from 56 to 66 + 23 across two new rules —
see [Stage L grew two gaps](#stage-l-grew-two-gaps-while-closing-one), and note
that a stage discovering more than it was scoped for is the instrument working,
not the arithmetic failing. Those figures record what each stage was **scoped to
drain** and are deliberately not restated as work lands; what the tuples hold
today is [measured in the case](#the-case) and is 37.

### Stage A: the census

**Legacy:** Stage 0 · **Issue:** CAR-40 · **State:** `done`

**What it was.** Measure the whole suite against the contract Plan 161 had
just landed, replace this plan's XL placeholder with a real estimate, and cut
the follow-on work into issues that could actually be filed.

**Exit.** The census is scoped and the effort estimate is real. Slicing the
work into further issues is the outcome, not this stage — the specifics did
not exist until Plan 161 had answered its nine questions.

### Stage B: the orphaned suites

**Legacy:** Stage 1 · **Issue:** CAR-45 · **State:** `done`

**What it was.** 73 integration-marked tests in 11 files that no CI step had
ever invoked, `tests/integration/processing/` — 58 of them — never having
appeared in `ci.yml` in its history. Whether they still passed was the one
input the L estimate rested on that the census could not settle, which is why
this ran first of the remaining stages.

**Exit.** The 11 orphaned files are executed and their pass/fail state
recorded; passing suites are invoked by named steps in `ci.yml`;
`tests/integration/lakehouse/` is declared dormant against G2 rather than
waived; `CI_INVOCATION_WAIVERS` is empty and
`test_every_integration_suite_is_invoked_by_a_ci_step` passes without it; and
the plan's L estimate is confirmed or revised against what the run found.

### Stage C: unblinding coverage

**Legacy:** Stage 2 · **Issue:** CAR-46 · **State:** `done`

**What it was.** `[tool.coverage.run] source` named six packages and omitted
`container_health`, `dashboard`, `scripts` and `airflow/dags` — so the two
services furthest below the floor were the two the instrument could not see.
Sequenced second because coverage is the instrument the rest of the work
reads.

**Exit.** `[tool.coverage.run] source` names every service directory; the unit
job's `--cov --cov-report=term-missing` output is consumed by a threshold, an
artifact, or both, rather than measured and discarded; and an assertion fails
when a service directory is missing from `source`.

### Stage D carries a constraint worth knowing before it starts

**Legacy:** Stage 3 · **Issue:** CAR-47 · **State:** `done`

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

**Exit.** Both health-sensor censuses read one declared source, that source does not import Airflow, and adding or deleting a sensor updates one place with both tests following.

### Stage E: splitting the 267s `dbt build + test` job

**Legacy:** Stage 4 · **Issue:** CAR-48 · **State:** `done`

**What it was.** One 267-second job running eight sequential suites against
one Postgres and one MinIO, named for one of the eight. This is Plan 139
Stages B and C, which moved here when 139 was archived.

**Exit.** The eight suites are split into jobs named for what each runs; the
`pip install apache-airflow==3.2.0` venv build no longer runs on every
invocation of unrelated suites; Plan 139 Stage C's question — what the 92s
step is actually doing — is answered on the record; and wall clock is
measured against the 267s baseline and recorded. That last is success
criterion 3, and it is measured rather than asserted.

### Stage F: the mechanical sweeps

**Legacy:** Stage 5 · **Issue:** CAR-49 · **State:** `done`

**What it was.** 34 files patching with something other than `mocker` and 16
`Layer N` mentions carrying Plan 84's numbering — 50 of the plan's 120
waivers, at near-zero judgement, each verified by deleting a waiver.

**Exit.** The venv fix goes first: `tests/integration/airflow/`'s venv does
not install `pytest-mock`, and two of the 34 files are blocked on it. All 34
files are converted; the 16 `Layer N` mentions across `tests/` and `ci.yml`
match the contract's headings; and `MOCKER_WAIVERS` and
`LAYER_NUMBER_WAIVERS` are both empty.

### Stage G: what the split is, and why a directory rather than a list

**Legacy:** Stage 5b · **Issue:** CAR-55 · **State:** `done`

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
instance of the impact selection Stage P generalises, against code that
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
import crosses a bucket boundary. The lesson is the one Stage A already
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
to **77.8%**. Stage M's dashboard repair — 309 statements, 280 of them
currently missed — moves the total by **+1.27 points today and +1.87 after**,
so the ratchet becomes about **1.5× more responsive**. Real, and worth having
before Stages H through N are graded; nowhere near large enough to justify
paying the Stage F waiver collision to get it sooner. That arithmetic is why
this stage is placed on the waiver argument rather than the coverage one.

An earlier draft of this section put the bucket at 19 scripts and 7,019
statements. It was close by accident and wrong in composition: it counted
`spike_iceberg_lakehouse.py`, `run_dbt_spark.py`, `verify_dialect_datediff.py`
and both `compare_gate_*_parity.py` against Plan 125, and
`estimate_dictionary_savings.py` against Plan 129 — **six scripts belonging to
plans that are still open.** Spent is a property of the owning plan's state, not
of how finished a script looks.

**Exit.** `scripts/ops/` and `scripts/oneoff/` exist with `tests/` mirroring them and production unmoved; every script is classified by the archive join, with the four declaring no plan read and placed by hand; `[tool.coverage.run] source` excludes `scripts/oneoff/` while the tests under it still run; `ci_change_scope.py` treats an `oneoff/`-only changeset as needing lint and unit tests only; the contract's *Where the newer suites sit* table gains rows for the two new test directories; and an assertion fails when a script directory is unclassified.

### Stage H: `container_health`'s test home, and every route reached

**Legacy:** Stage 6 · **Issue:** CAR-50 · **State:** `done`

**What it was.** 12 of 87 routes reached through no routing table, four of
them `container_health`'s — a service with no `tests/` directory and no
`TestClient` anywhere. G6 and G9 are one stage because G6's four routes are
uncoverable until G9 builds the home they would be tested from.

**Exit.** `container_health` has a `tests/container_health/` and a Layer 4
suite; the two misfiled unit tests move out of Layer 0's directory into the
Layer 1 home they belong in (G9); all 12 waived routes are reached through
their app's routing table by a test that asserts a status code; and
`ROUTE_WAIVERS` is empty. Health and readiness endpoints are not exempt —
they are what another service's drain logic reads, and the three coordination
routes are the surface whose drain hung Plan 142's first production deploy.

### Stage J was added by the failure this plan predicted

**Legacy:** Stage 6b · **Issue:** CAR-60 · **State:** `done`

**Added 2026-09-01.** Success criterion 2 records G13 as the weakest of its
three exceptions, and says why in a sentence worth reading back: *"the next
instance of G13's class will be found the way the last two were, by someone
running the suite somewhere CI does not."* That is precisely what happened, six
days later and while Stage H was being started.

`tests/scripts/test_build_public_roadmap.py` writes a synthetic plan document
containing an em-dash with `write_text` and no `encoding=`. The locale decides:
UTF-8 on Linux, cp1252 on Windows, where the character becomes the byte `0x97`.
`build_public_roadmap._first_heading` reads it back as UTF-8 — correctly — and
raises. **The suite was green in CI and red on a developer machine**, which is
the benign direction of the harness rule and the same shape as `21333ab`.

**What makes it a stage rather than a second one-file repair is that four
independent guards were in a position to catch it and none could.** Measured
on 2026-09-01:

1. **No encoding rule is configured.** `[tool.ruff.lint] select` is
   `["E", "F", "I"]` — nothing that reads an encoding argument.
2. **The rule that would is preview-gated.** `--select PLW1514` alone answers
   *"Selection `PLW1514` has no effect because preview is not enabled"*, so it
   is off twice over and silently.
3. **Enabling it fully would still not have caught this.** With
   `--select PLW1514 --preview` the repository has **22 violations and not one
   of them is the line that broke master.** The rule fires only on a
   directly-constructed receiver: `Path("b.md").write_text(...)` is flagged,
   `(tmp_path / "a.md").write_text(...)` is not — with or without a `Path`
   annotation on the fixture. It is blind to the idiom nearly every
   fixture-writing test in this repository uses.
4. **CI is `ubuntu-latest` in all ten jobs**, so this failure direction is
   invisible by construction — the constraint G13's exception already named.

The near-miss is the instructive part. *The harness must not decide the
outcome* is written for exactly this class and even carries a Windows example,
but its checkable rule is about **mocks** of filesystem, clock, platform or
path primitives. A missing `encoding=` is not a mock, so Stage F's sweep — the
pass that read every patch in the suite — went straight past it. The prose
covered this; no mechanism could.

**So the stage is not "turn on the ruff rule".** Finding 3 is the whole reason
it needs designing: the available tool cleans 22 real sites, several in
production code (`ops/routers/admin.py`, `dbt_runner/app.py`, three
`archiver/processors/` modules), and still would not have stopped the defect
that prompted it. Closing the class means a rule that reads the calls the way
the route rule reads request literals, a Windows job, or an argued case that
neither is worth it — recorded as a decision either way.

**It sits after Stage H and before Stage L.** After 6 because CAR-50 is already
in flight and re-cutting it buys nothing. Before 7 because **Stages L, M and N
author more new tests than the rest of the plan combined**, and a guard that
lands first is one those stages get for free rather than one that has to sweep
what they wrote. That is the same argument that put Stage C ahead of the stages
it measures.

**The old suffix was positional, not topical.** This stage has nothing to do
with route coverage; it was numbered 6b for the reason 5b was, and CAR-52 — closed
under the old namespace — still reads `Stage 8` in its title.

**Exit.** The 22 `PLW1514` violations are resolved or explicitly waived, the production ones included; a mechanism fails on `(tmp_path / "a.md").write_text("—")` — the exact shape ruff cannot see — **or** the plan records why neither an AST rule nor a Windows runner is worth building; and success criterion 2 names precisely which part of G13's class remains unmechanisable rather than leaving the exception standing whole.

### Stage K was added by a deploy, not by the suite

**Legacy:** Stage 6c · **Issue:** CAR-66 · **State:** `done`

**Added 2026-09-01.** Stage J was added by a failure this plan predicted. This
one was added by a failure it did not, found during Plan 138 Stage 2's
production deploy — and the shape is the reason it belongs here rather than in
Plan 138.

`POST /deploy/start` with `{"targets":["dashboard"]}` returns **503
`{"detail":"Database unavailable."}`**. Postgres was healthy throughout. The
database was never the problem.

`ops/coordination_contract.py` maps `dashboard` and `pgadmin` to `frozenset()`
— they are the only two services in `SERVICE_CONTRACTS` with **no surfaces**.
`_set_intent` therefore writes `phase='requested'`, `targets='["dashboard"]'`
and `scope='[]'`, against a constraint that forbids exactly that pair
(`db/migrations/V043__coordination_state.sql:27`):

```sql
CHECK (
    (phase =  'none' AND kind IS NULL     AND targets =  '[]'::jsonb AND scope =  '[]'::jsonb)
    OR
    (phase <> 'none' AND kind IS NOT NULL AND targets <> '[]'::jsonb AND scope <> '[]'::jsonb)
)
```

**So two services can never be deployed alone**, and the failure is structural
rather than intermittent. The workaround is to name a scoped service in the
same command — `bash scripts/redeploy.sh ops dashboard` — because the union is
then non-empty. That is a real property of `redeploy.sh`, which takes a service
list, and it is what unblocked the deploy.

**Three guards were in a position to catch this and none could.** Measured
2026-09-01:

1. **The contract suite never asserts the value that breaks.**
   `tests/ops/test_coordination_contract.py` exercises `expand_targets` and the
   string `scope` does not appear anywhere in the file. It asserts the mapping
   is *well-formed*, never that its output is *writable*.
2. **The constraint is in a Flyway migration, and the contract is in Python.**
   Neither half is wrong on its own; the defect exists only in their
   composition, and no layer in this repository composes them. This is the same
   division Stage D closed for the health-sensor censuses — two sources that
   must agree, with nothing asserting that they do.
3. **The error message actively misdirects.** `_set_intent` catches bare
   `Exception` and returns `"error"`, which `ops/routers/deploy.py:248` renders
   as 503 "Database unavailable." The constraint violation never reaches the
   response or the log, so the symptom points at the one component that was
   healthy.

**The stage is therefore two things, and the second is not optional.** An
assertion that every service in `SERVICE_CONTRACTS` yields a `(targets, scope)`
pair the constraint accepts closes the defect class. Unmasking the exception is
what stops the *next* unrelated failure in this path costing the same
diagnosis, and finding 3 is the whole reason a passing deploy script is not
sufficient evidence here.

**It sits after Stage J for Stage J's own reason** — Stages M and N author more new
tests than the rest of the plan combined, and this is a guard those stages get
for free rather than one that has to sweep what they wrote. **The old suffix was
positional, not topical**, as it was for Stages G and J.

*Written on 2026-09-01 as "after Stage J and before Stage L", by a deploy that
did not know Stage L was in flight on another branch. Stage L completed the same
day, so this stage gets Stages M and N rather than L, M and N — the argument is
unchanged and the count is not.*

#### Finding 3 was corroborated the same day, on the same endpoint

**Added 2026-09-01 while merging Stage L.** Stage L broke `POST /deploy/start`
too, independently and for an unrelated reason: it moved
`set_deploy_intent.sql` into a file and wrote an explanatory comment that
quoted the statement's own placeholder, and **psycopg2 counts placeholders
across the whole string, comments included**. The statement then expected four
parameters where `deploy.py` passes three.

**The symptom was identical — 503 `Database unavailable` — and for exactly the
reason finding 3 gives.** `_set_intent` catches bare `Exception`, so a
`psycopg2` parameter error and a `CHECK` violation are indistinguishable at the
response, in the log, and to the operator. Two unrelated defects, one day
apart, wearing the same misleading face.

That is the strongest evidence this stage has for its second half, and it
arrived from outside it. **The assertion half would not have caught Stage L's
defect** — the contract's `(targets, scope)` pair was fine — but **the
unmasking half would have named it immediately**, instead of it being found by
seven Layer 4 failures in CI and diagnosed from a log. A reader comparing the
two should not conclude they share a cause: [Stage L's
evidence](#stage-l--sql-execution-from-both-directions)
records the placeholder defect and Rule 5e, which is what stops that one
recurring; this stage owns the masking that made both of them expensive.

**Exit.** A test enumerates `SERVICE_CONTRACTS` and fails for any service whose lone-deploy `(targets, scope)` pair violates `V043__coordination_state.sql:27`; `_set_intent`'s exception path surfaces the underlying SQL error, so a constraint violation no longer renders as 503 "Database unavailable"; and `bash scripts/redeploy.sh dashboard` either succeeds or fails naming the actual cause.

### Stage L grew two gaps while closing one

**Legacy:** Stage 7 · **Issue:** CAR-51 · **State:** `done`

**Added 2026-09-01, mid-stage.** Stage L was scoped at 56 Layer 2 waivers and
"G5's ten modules". Both numbers were wrong, and both were wrong the same way
the census was wrong about G14: **the measure was fitted to the code in front
of it.**

G5 measured 66 sites in 15 modules, not 10. The gap list's own stated measure —
"`.execute(` with a literal first argument" — cannot see
`execute_values(cur, sql, rows)`, which carries its statement second, and
`ops/routers/maintenance.py:152` is a literal `INSERT` sitting exactly there.
Two of the named ten did not belong: `shared/db.py`'s only match is inside
`db_cursor`'s docstring, and `shared/duckdb_s3.py`'s seven are `INSTALL` /
`LOAD` / `SET` session setup, which name no schema and so cannot drift from
one. Closed the same day; `INLINE_SQL_WAIVERS` is `()`.

**G15 is what closing G5 revealed.** A statement bound to a name and executed
from there is invisible to both instruments at once: Rule 5b does not fire
because it is not at the call site, and Rule 5's denominator cannot count it
because there is no `.sql` file. Stage L extracted six of these by hand and
only because someone happened to read the files; the measured cost of that
blind spot was 23 more in 11 modules, six of them in `ops/routers/admin.py`, a
router the stage never touched precisely because every one of its statements is
assigned before it is executed.

**The scan surface was the third instance of the same error.** Both new rules
scan `service_packages()`, which is the right predicate for "what is a service"
and the wrong one for "what is production Python". `airflow/` and `scripts/`
hold neither an `__init__.py` nor, therefore, any rule — and they hold 26 more
sites, 22 of them in Plan 125's Iceberg and Spark scripts, which Gates C and D
productionize. The repair is a second derivation reading Stage G's declared
bucket table, **not** an `__init__.py`: `service_packages()` drives seven rules,
and making `scripts` a package would demand an "enough" row for something that
is not a service and send the route rule looking for `scripts.app`.

The lesson is the one this plan keeps relearning about its own instruments, and
it is worth stating as a design rule rather than a third anecdote: **a
denominator that is listed, or scoped to what exists when it is written, will
be wrong.** G14 was undercounted at 54, G5 at 10, the scan surface at eight
packages, and `executemany` was left out because it matched nothing that day.
The rules that have never been wrong are the derived ones —
`service_packages()`, `_test_directories()`, `production_sql_files()`.

**Exit.** All 54 uncovered `.sql` files are executed by a Layer 2 test; the two paraphrasing test files are repaired; the 10 modules holding inline SQL at `.execute()` call sites move to `shared.query_loader` or expose a module-level `(sql, params)` builder; and `LAYER_2_WAIVERS` is empty.

### Stage M narrowed, and G7 now names a different gap

**Legacy:** Stage 8 · **Issue:** CAR-52 · **State:** `done`

**Rescoped 2026-09-02, before the stage started.** Stage M was "the services
below the floor", G7 and G8 together, and G7 was "`dashboard/`: 7 modules, 0
test files". Reading the service settled that G7 as written is not this plan's
work, and that a better gap was sitting underneath it unnamed.

**What the dashboard's Python actually contains.** 483 lines under `pages/`,
of which roughly 430 are `st.*` and `px.*` presentation calls and roughly 35
are logic — a filter-clause builder, three division-by-zero guards and two
granularity maps. Read end to end, it holds no defect: `app.py:29`'s unguarded
`.iloc[0]` is safe because `mart_freshness.sql` is a bare aggregate that always
returns one row, and the filter builder's placeholder/parameter pairing is
correct and locally coupled. What is there is redundancy — a tautological
`elif`, two different zero-guards for one metric, one granularity map that
round-trips through an intermediate encoding to reach what the other produces
directly. None of it is worth a test, and testing the other 430 lines means
asserting that `st.metric` was called with a particular string, which fails on a
renamed header and never on a wrong number.

**The gap that was underneath it.** `tests/integration/sql/test_dashboard_queries.py`
is 25 tests with **zero assertions** — the only Layer 2 suite with none, against
116 in `test_ops_queries.py` and 65 in `test_processing_queries.py`. Every test
executes a statement and discards the result. That satisfies Layer 2's first
clause and not its second: the contract says statements execute *"and return the
columns the caller expects"*, and nothing here checks a column. Every page
indexes by name, so a renamed mart column passes green and `KeyError`s in
production. **The pattern that closes it already exists in the same directory,
against the same `duckdb_con` fixture** — `test_analytics_snapshot_queries.py`
asserts `result.description` against a declared column tuple. `dbt_runner` has
that contract; `dashboard` does not.

It pays a second dividend on the way. Writing down the columns the caller uses
surfaces the ones it does not: `data_health_block_rate.sql` selects
`block_rate_pct`, `total_block_events` and `max_attempts_seen`, and
`data_health.py` reads none of the three — it recomputes the rate in pandas
after aggregating, correctly, because percentages do not average. Three dead
columns in production SQL that nothing can currently see.

**So G7 is redefined rather than deferred**, and the Python becomes **G18, owned
by Plan 150**. Two rows, not one, because they are two gaps with two owners and
two triggers, and folding either into the other hides one of them. Dropping the
Python row entirely was the alternative and it is the Plan 84 move exactly:
`docs/TESTING.md`'s Testing Strategy section was accurate the day it was written
too. G18 states the blocker in the row — the suite cannot import
`dashboard.pages.*` at all, because `streamlit` and `plotly` are declared in
`dashboard/requirements.txt` and nowhere else and production imports are bare —
so the row records a measurement rather than an intention.

**What this plan is not taking on, stated so it is a decision.** Asserting that
the dashboard's queries return *correct values* is Layer 3's shape — known
inputs, known outputs — not Layer 4's, and Stage M does not attempt it. Three
reasons, in order of weight:

1. **The correctness is already asserted where the data is made.** All eight
   marts the dashboard reads carry dbt unit tests with seeded inputs and
   expected rows in `dbt/models/marts/unit_tests.yml`, plus `not_null` and
   `unique` data tests in their `.schema.yml` files.
2. **Most of the SQL adds nothing to assert.** 24 files, 218 lines, averaging
   nine. Scanned for `JOIN|CASE|COALESCE|NULLIF|GROUP BY`, 13 add none of them
   — `SELECT COUNT(*) AS cnt FROM mart_deal_scores` and its kin — 10 add a
   `GROUP BY` over a tested mart, and one carries real logic.
3. **The fixture cannot do it.** `duckdb_con` is a read-only connection to
   whatever `dbt build --target duckdb` produced from CI's seed. There is no
   seeding, so there are no known inputs. Layer 3's `seed_and_build` has that
   machinery and runs `--target ci` into **Postgres**, so borrowing it means
   executing DuckDB-dialect SQL against another engine — `now() - INTERVAL '14
   days'` and the rest — which is a dialect question this repository already has
   scar tissue about.

**The one query where that reasoning does not hold is recorded rather than
built.** `data_health_block_rate.sql` LEFT JOINs `mart_block_rate` onto an
anchor of `mart_scrape_volume` with `COALESCE(..., 0)`, so hours with scrape
activity and no blocks read as zero rather than as gaps. `mart_block_rate.schema.yml`'s
own description says *"Join to mart_scrape_volume on hour to compute block rate
against observations"* — **the mart deliberately hands that join to its
consumer**, which is the one place mart-level unit testing structurally cannot
reach, and neither end of the handoff is tested. The cheap answer is probably
not a new fixture at all but moving the join into the mart, where Layer 3
already reaches it. That is a modeling change, it is what Plan 150 Stage 0c
means by *"served by extending an existing mart"*, and it is noted there rather
than done here.

**Exit.** `tests/integration/sql/test_dashboard_queries.py` asserts something, borrowing `test_analytics_snapshot_queries.py`'s `result.description` pattern rather than inventing one; a rule rather than only 25 assertions, so a Layer 2 test that executes a statement and asserts nothing about the result fails; `scraper/` meets the floor; and both services' rows in `docs/TESTING.md`'s "enough" table are updated to what is then true.

### Stage N: the DAG tree's `.sql` convention

**Legacy:** Stage 9 · **Issue:** CAR-53 · **State:** `done`

**What it was.** `airflow/dags/` was the only place in the repository where
the contract's rule that production SQL lives in a `.sql` file was
structurally impossible: no module under it imports `shared`, so
`shared.query_loader` was unavailable. That constraint is G12, and it is the
constraint rather than the violation — which is why this stage was permitted
to close without an assertion.

**Exit.** `airflow/dags/` can reach a `.sql` loading mechanism; the single
legitimate `ast` reader, `_sensor_constant()`, is either no longer forced or
is confirmed as still necessary with the reason recorded; and
`docs/TESTING.md` reflects the outcome — G12 closed, or a third exemption
stated as a decision with its reasoning. **This is the only stage in the plan
that changes production import structure rather than test structure.**

### Stage P: dbt builds against production-shaped data

**Legacy:** Stage 10 · **Issue:** CAR-54 · **State:** `done`

**What it is.** An isolated CI job whose only work is `dbt build` against a
Plan 120 production-derived snapshot, on its own runner with its own Postgres
and MinIO, gated to changes that can affect a dbt build.

**Why it is not the fixture we already have.**
[`seed_lake_snapshot_fixture.py`](../../scripts/seed_lake_snapshot_fixture.py)
seeds authored business-state scenarios, and authored rows are well-behaved by
construction. dbt unit tests are semantic and run on inputs their author chose.
Neither can surface a `unique` violation, a `not_null` violation, a cast
failure or a duplicate join key that exists in production **because no
production row is ever in the denominator.** A `dbt build` over a real snapshot
is the only instrument here that can, and it answers the question before a
deploy rather than after one.

**Why a separate job rather than a step in `dbt-models`.** The fixture lives in
a reserved `obs_year=2099` partition specifically so it cannot collide with the
empty-schema compilation seed. A production snapshot lands in real partitions,
under the same globs, as a third dataset — so sharing a runner would mean the
existing equivalence assertions run over fixture-plus-production. Separate
GitHub Actions jobs get independent runners with no shared filesystem or
network, which dissolves the collision rather than managing it, and parallelism
keeps the wall clock at `max()` rather than `sum()`, protecting [success
criterion 3](#success-criteria).

**Why it may be path-gated from the start, though [Stage R](#stage-r-ci-selection-and-the-instrument-that-has-to-precede-it) may
not.** Plan 139 Stage E requires an observation window before a selector is
promoted to skipping jobs, because a false positive in a *narrowing* selector
suppresses evidence that previously existed. A job that has never run suppresses
nothing; the worst case of a wrong trigger is coverage not gained. The
asymmetry does not bind on a net-new job, and this is the one place in the plan
where gating is free.

**Four things it needs that do not exist yet.**

1. **Two Postgres sources travel with the snapshot.** `sources.yml` declares six
   source tables. Four are MinIO Parquet and are exactly what
   [`seed_lake_snapshot.py`](../../scripts/seed_lake_snapshot.py) already
   uploads. The other two — `public.search_configs` and `ops.tracked_models` —
   resolve through `postgres_scan()` and so must be live rows in Postgres, not
   objects in MinIO. Left empty, `stg_search_configs` reads nothing,
   `int_active_make_models` inner-joins to nothing, and `mart_vehicle_snapshot`
   builds green over an empty world — in the job whose entire purpose is proving
   the build survives real data. Both tables are small enough to export whole,
   which is also the safe direction: full dimensions against a cohort fact set
   drop rows for cohort reasons only, never because a dimension row was left
   behind. Neither carries VIN or dealer data.
2. **The exporter grows those two tables**, and the seeder grows a Postgres
   write path, which it has never had — it uploads objects and nothing else.
   Its refusal to run against a production-looking target must extend to
   `POSTGRES_URL` at the same time: a seeder that can `INSERT` into any
   connection string it is handed is a different risk class from one that can
   only upload Parquet.
3. **The snapshot id is pinned in the repository**, not read from
   `latest.json`. Not because a moving pointer would produce false failures —
   it mostly would not, since prod-green plus CI-red on a fixed snapshot means
   the change did it — but because a pointer that moves between two runs of the
   same commit destroys re-runnability on the one check whose job is telling you
   what a change did. Pinning also turns "production data changed and a model
   now fails" into a snapshot-bump PR: a reviewable diff with an owner, rather
   than an ambient condition that lands on whoever opened a PR that morning.
4. **A trigger set wider than `dbt/`.** The pin itself, the seeder and
   downloader, the dbt version pins, and `db/migrations/` all change this
   build's outcome. Unclassified paths fail open, as
   [`ci_change_scope.py`](../../scripts/ci_change_scope.py) already does.

**What it deliberately does not do.** It does not run
`tests/integration/dbt/`; those keep their fixture and their job. It asserts
through dbt's own data tests, which is why the verb is `build` and not `run`.

**Three residuals, recorded rather than solved**, because each is a real limit
on what a green here proves. Production builds incrementally — `--full-refresh`
is conditional in [`dbt_runner/app.py`](../../dbt_runner/app.py) and off by
default — while a fresh DuckDB file takes every incremental model's cold path,
so the two exercise different code. A subset cannot invent a duplicate or a
null, so `unique` and `not_null` failures here are true positives about
production; `relationships` failures may be artifacts of incomplete cohort
closure, which Plan 120 records as the hard part and once got wrong. And a
snapshot captured after production's last dbt run can be red while production is
green only because production has not run yet — a correct finding, arriving
early, landing on an unrelated author.

**Exit.** A gated CI job builds the full dbt project against a pinned,
production-derived snapshot with all six sources populated, and fails on a
production row that violates a dbt data test. Demonstrated by a deliberate
violation, not asserted.

#### Stage P ships in two parts, and the reason is a cycle

**Split 2026-09-03, on the way to opening the PR.** The stage's own gate cannot
land in the change that introduces it. The job builds against a snapshot; that
snapshot can only be produced by an exporter carrying the two Postgres dimension
tables; and that exporter has to be merged and deployed before it can produce
one. Introduced together, the job is red on its own PR and stays red on master
until a pin bump lands — for two reasons that are both the job working exactly
as designed: no `CARTRACKER_SNAPSHOT_TOKEN` secret existed yet, and a
placeholder pin necessarily names a snapshot exported before the Postgres half,
which the seeder's `--require-non-empty` correctly refuses.

The alternatives were worse in the way this plan cares about. Merging one
known-red check leaves master red on a schedule nobody owns. Gating the job on
the secret's presence makes it skip silently the day that secret is rotated or
removed — a job that disappears when its credential does is the failure class
Stage B spent its budget making impossible, and it would have needed a
waiver-shaped justification to sit beside `DORMANT_SUITES`.

So:

* **Part 1** — what a production export needs: the exporter's two dimension
  tables, `shared/lake_snapshot_postgres.py` and its round-trip SQL pair, the
  export cache schema bump, and the seeder's Postgres write path with its
  stricter `POSTGRES_URL` guard. Merged as
  [#357](https://github.com/whitewalls86/new_car_tracker/pull/357) on
  2026-09-04, every job green.
* **Part 2** — the gate, arriving with a pin that resolves: the `snapshot-dbt`
  job, the `snapshot_dbt` classifier group and its trigger set,
  `.github/ci_lake_snapshot_pin.json` with a real snapshot id, and the
  deliberate violation the exit above demands.

**The pin travels with the job, not ahead of it.** A pin file in master naming
a snapshot no job reads, and which would fail if one did, is a file that lies
about which snapshot is authoritative — and the change that adds the job is
where it gets a real value anyway. The same reasoning moved the trigger set:
`SNAPSHOT_DBT_TRIGGERS` with no job consuming it is dead config in
`ci_change_scope.py`, the one file in this stage whose blast radius is every job
in the workflow.

#### The export DAG fails on a successful export

**Found 2026-09-03, pre-flighting the first `ci`-tier run.**
[`check_snapshot_result`](../../airflow/dags/export_ci_lake_snapshot.py) accepts
only `{"created"}` as a non-dry-run success status. The exporter returns
`"exported"`. A DAG-triggered export therefore publishes its archive and both
pointers, and then fails the task.

**Nothing caught it because the DAG has never run** — `airflow.dag_run` holds
zero rows for `export_ci_lake_snapshot`, and both snapshots in production before
this stage came from the `snapshot-worker` invocation `docker-compose.yml`
documents. The stage's own pre-flight is the only reason it is not still waiting
for whoever triggered the DAG first.

**It is a Layer 1 instance of the rule this plan keeps rediscovering.**
`tests/integration/airflow/test_export_ci_lake_snapshot_dag.py` seeds
`{"status": "created"}` and asserts the checker accepts it — a status string the
test author chose and the exporter never emits. Both halves of the contract are
written in the same file, so the test passes forever and proves nothing. It
belongs in *[a run that succeeds has done the work its success
implies](../TESTING.md#specified-here-not-yet-asserted)*, and the general shape
is worth naming: **a DAG-side checker keyed on a string a service returns needs
one test that reads the string from the service**, not from the test.

The repair is two lines and its test correction, and it is Part 2's rather than
a stage of its own — Part 2 is already the change that makes the export routine
instead of hand-run, so the DAG is the surface it lands on. Until it ships,
exports run through `snapshot-worker` directly.

#### The CI credential became Plan 173, not a stage here

Wiring Part 1's download needed a bearer token in CI for the first time, which
turned one shared string into a question about three callers — CI, a
developer's laptop, and the Plan 112 MLflow rehearsal. The *format* half landed
alongside this stage, because a credential format is cheapest to change while
nothing automated depends on it. The *storage* half became
[Plan 173](plan_173_machine_credential_lifecycle.md), which also records why
OAuth2's `client_credentials` and GitHub Actions OIDC were rejected and
deferred respectively.

Recorded here only so the trail from this stage to that plan is not lost. The
reasoning lives there and is deliberately not repeated.

#### Stage P was one row and is three

**Split 2026-09-03, while scoping it.** The row read "suites on real Compose
services, dbt against the Plan 120 snapshot, advisory CI impact selection" and
carried a 2-point estimate for six separable pieces, three of which the ticket
never listed. Nothing about them shares a mechanism, a file or a risk profile:
one adds an isolated job, one rewrites four existing ones, and one builds a
selector. Splitting them is not a rescope — every piece stays owned here — but
it stops a single issue from being able to read "done" on a third of its
content.

The numbering follows this plan's own precedent and the contract's exemption
for it. Stages S and T are already allocated, and [Stage G's
reasoning](#why-this-order) applies unchanged: inserting an
integer renumbers later stages and invalidates issues already filed against the
old numbers. So Stages Q and R, as with G, J and K.

### Stage Q: CI's services are production's, in definition and in contents

**Legacy:** Stage 10b · **Issue:** CAR-78 · **State:** `—`

**What it is.** Three questions with one thesis — CI's services are not
production's — approached from the definition, the contents and one named
instance.

**The definition.** Four jobs (`dbt-models`, `schema-contracts`,
`service-integration`, `lake-integration`) each declare their own `services:`
block and their own `docker://flyway/flyway` step: four copies of `postgres:16`,
three of `minio/minio:latest`, four hand-maintained Flyway argument lists. The
drift is already measurable. CI's Postgres omits Compose's `command: postgres -c
shared_buffers=2GB -c max_connections=100` and its `shm_size: 1gb`; CI's MinIO
omits the console, the OIDC identity configuration and
`MINIO_PROMETHEUS_AUTH_TYPE`; CI's Flyway omits `-baselineOnMigrate=true`.
Nothing asserts any of it. This is the general form of the CI-schema gap Plan
139 Stage F closed narrowly.

The shape is already precedented here.
[`docker-compose.lakehouse.ci.yml`](../../docker-compose.lakehouse.ci.yml) is a
CI-only override that makes `cartracker-net` non-external and substitutes
throwaway services, and
[`tests/test_lakehouse_compose_config.py`](../../tests/test_lakehouse_compose_config.py)
is its parity suite. A `docker-compose.ci.yml` needs the same two moves —
`cartracker-net` and `cartracker_pgdata` are both `external: true` in the base
file — and running Flyway as `docker compose run --rm flyway` makes CI execute
the identical command production executes, rather than a fourth transcription
of it.

**The contents.** CI's Postgres is greenfield and production's carries hundreds
of thousands of rows, so this stage measures *which* suites depend on an empty
database rather than assuming the answer. The rehearsal that would close it
needs a deployed stack and belongs to [Plan 121](plan_121_staging_environment.md);
this stage owes the measurement and the handoff, not the repair.

**The instance.** `tests/integration/airflow/` still points at
`sqlite:////tmp/airflow.db`. Plan 139 Stage F left it deliberately, to keep test
data out of the Postgres metadata schema the drain tests read. Now that a real
Airflow metadata schema exists in the same job, whether those suites should
share it is decided here — and it is the same question as the other two, one
service down.

**Why the three are one stage.** They touch the same four jobs. Splitting them
means editing those jobs two or three times, and the sqlite question cannot be
answered without knowing what the job's services are.

**Exit.** The four jobs' services come from the Compose definitions with a
mechanism that fails if a bare `services:` image returns; the greenfield
measurement is recorded with its Plan 121 handoff; and the sqlite question has
a decision with its reasoning.

#### Four decisions taken while scoping this stage, 2026-09-04

Reasoning in [the CI cost
census](../evidence/plan_162_stage_R_ci_cost_census_2026-09-04.md); these are what
changes about what gets built.

**1. It is five jobs, not four.** Stage P closed on 2026-09-04 and added
`snapshot-dbt`, which declares its own `postgres:16`, its own
`minio/minio:latest` and a sixth hand-transcribed Flyway argument list. The
count above was taken before it existed.

**2. The guard is a resolved-config diff, not a field-by-field parity test.**
`docker compose config --format json` resolves the whole merge chain — override
files, `${VAR}` interpolation, `extends`, `include` — and normalizes as it goes.
Verified against this repository: `command:` returns as a list where the file
holds a block scalar, `shm_size: 1gb` returns as `1073741824`, and it needs no
daemon state, no `cartracker-net` and no `cartracker_pgdata`. So the guard
resolves the base chain and the CI chain, **diffs the two documents**, and
requires the difference to equal a declared, commented allowlist. No field can
be missed for not having been thought of — which is the limitation
[`tests/test_lakehouse_compose_config.py`](../../tests/test_lakehouse_compose_config.py)
has, since it `yaml.safe_load`s single files and asserts only what it names.

That also settles the shape of the honest claim: **the exit is not "CI's
services are byte-identical to production's."** `cartracker-net` and
`cartracker_pgdata` are both `external: true`, the base file reads a dozen
unset `${...}` variables, and production's MinIO carries
`MINIO_IDENTITY_OPENID_*` pointing at Google. The end state is production's
definition plus a **declared and asserted** override set, which is what turns
the residue from accidental into visible.

**3. testcontainers is declined for this stage.** Raised by [the Stage X origin
note](../evidence/plan_162_stage_X_origin_2026-09-04.md) as directly addressing
this stage's problem statement, and evaluated properly rather than by taste. It
collides with [Stage R](#stage-r-ci-selection-and-the-instrument-that-has-to-precede-it):
`service-integration` runs five pytest steps against one shared Postgres and
each `run:` is its own session, so session-scoped containers mean five startups
per job or one collapsed invocation — and collapsing destroys the named-step
granularity the invocation rule reads. Every integration conftest also reads
`TEST_DATABASE_URL` at **module import time**, with
`tests/integration/archiver/conftest.py` stating the constraint outright
("Must run before `shared.db` is imported"), so containers would have to start
before conftest import in a root that the 2,212-test unit suite also loads. And
it wraps `docker compose up` without changing what the compose file says, so
`docker-compose.ci.yml` is needed either way. On this stage's own thesis the two
options tie; every tiebreaker after that is cost.

**4. The guard's own skip is Stage U's.** A `docker compose config` guard
shells out to the `docker` CLI, and the parity suite it is modelled on opens by
declaring "No live Docker required." It must skip cleanly when `docker` is
absent and be *required* in CI — which is exactly
[Stage U](#stage-u-every-skip-in-ci-is-declared-or-the-run-fails)'s mechanism,
and a dependency this stage should hand forward rather than solve locally.

**Two questions this stage still owes an answer**, both to be settled while
building rather than now: whether CI adopts production's `shared_buffers=2GB` /
`shm_size: 1gb` wholesale or overrides them with a stated reason — a 7GB runner
also hosting DuckDB, a dbt build and an Airflow venv is not the VM those numbers
were chosen for — and whether the unset variables arrive through a committed
`.env.ci` or through defaults in the override file.

### Stage R: CI selection, and the instrument that has to precede it

**Legacy:** Stage 10c · **Issue:** CAR-87 · **State:** `—`

**Rescoped 2026-09-04, and moved to run after Stage X.** The selector this
stage was built around is cut. [The CI cost
census](../evidence/plan_162_stage_R_ci_cost_census_2026-09-04.md) found that its
premises describe a workflow that no longer exists: the wall clock is set by a
single job (`schema-contracts`, 123s) and every other heavy job already
finishes inside its shadow, so **skipping any subset that excludes that job
saves exactly zero seconds**. Plan 139 Stage E requires "a benefit larger than
runner variance" before a selector is promoted; the available benefit is 0–30s
against ±10–20s of variance, so the rule cannot be satisfied at any precision
the selector could reach. Runner minutes are not a second justification — the
repository is public.

**What the stage is now.** Three pieces, in order:

1. **The instrument fix**, reduced to whatever [Stage U](#stage-u-every-skip-in-ci-is-declared-or-the-run-fails) has not already
   supplied. The defect is real and unchanged —
   `test_every_integration_suite_is_invoked_by_a_ci_step` asks whether a
   *directory* appears in a step's arguments, which is why 7 tests sat
   deselected until a coverage number caught them. But the fix as scoped below
   is a *static* reimplementation of pytest's own selection, and Stage U builds
   a `pytest_terminal_summary` hook across every job that observes what actually
   ran. Transcribing what a tool already knows is the same defect [Stage Q](#stage-q-cis-services-are-productions-in-definition-and-in-contents)
   exists to remove from the services blocks. **This stage runs after Stage U so
   it can read Stage U's output instead of simulating it.**
2. **Install caching, measured before adopted.** 98 of `schema-contracts`' 123
   seconds is infrastructure and dependency installs; the tests are 6. That is
   the compressible number, and unlike selection a cache miss costs time rather
   than correctness. Two candidates need measuring **both ways** rather than
   assuming: the 27s Airflow venv, where restoring several hundred MB may cost
   what installing it costs, and `setup-python`'s `cache: pip`, which caches
   downloads rather than installs. Having rejected the selector on measured
   grounds, this stage may not adopt caching on projected ones.
3. **`.claude/skills/**/*.md` joins the docs zone.** Prose edits of 12 and 14
   files are pulling the full heavy workflow, three dbt builds included, because
   `.claude/` is not in `DOCS_PREFIXES`. This is the fail-open direction and
   carries none of the risk the trigger sets were declined for.
   `.claude/settings.json` stays out: one such merge paired it with
   `tests/scripts/test_build_public_roadmap.py`, and hooks can change what runs.

**What it drops, each with the condition that would revive it.** A flat no
ossifies; these expire on checkable events.

| Dropped | Why | Revisit when |
|---|---|---|
| The advisory impact selector | 0s available under the `max()` ceiling | The ceiling falls far enough that job-level skipping beats runner variance |
| A trigger set for `dbt-models` | 57% of heavy runs would skip it, saving 0s; and narrowing an *existing* job suppresses evidence, unlike `snapshot-dbt` | Caching has promoted it to the critical path **and** Plan 125 Gate E has retired the dual-run, so its surface is stable |
| Incremental-diff classification | The plan's own four conditions, against ~2 minutes that the census shows is nearer zero | Not on current evidence |
| Content-addressed `docker-build` skipping | Called "the cheaper first win"; measures at 0s, since 96s sits under a 123s ceiling | Caching promotes `docker-build` to the critical path — which piece 2 above would do |

**Why it runs last.** Three mechanisms, not a general caution. Stage U
may subsume piece 1, as above. [Stage X](#stage-x-a-test-may-not-author-sql-either) creates a new SQL root with its
own census — a path class no invocation rule can know about yet. [Stage T](#stage-t-exists-because-this-plan-grew-the-suite) may move the suite
boundaries that piece 1's unit of analysis rests on.

**Exit.** The invocation rule distinguishes a suite from a directory, built on
Stage U's observation rather than a second implementation of it; the caching
candidates are measured both ways and only the winners adopted; the docs zone
covers `.claude/skills/**/*.md`; and each of the four dropped items carries its
revisit condition in the record.

**What it was scoped as**, kept below because the reasoning that produced the
four drops is worth reading against what replaced it.

**What it is.** Plan 139 Stage E's advisory impact selector, the two questions
Stage G raised and declined, and — first — the instrument both of them need.

**The instrument comes first, and the plan did not previously say so.**
`test_every_integration_suite_is_invoked_by_a_ci_step` asks whether a
*directory* appears in a step's arguments. A directory is not a suite, and the
rule cannot distinguish "this file runs in CI" from "the directory containing
this file is named in a `run:` line", nor either from "this file sits in
`tests/integration/` and needs nothing that makes it one".
[Stage F found this](../evidence/plan_162_stage_F_evidence.md#a-unit-test-filed-as-an-integration-test-and-two-wrong-answers-before-the-right-one)
and assigned it here. **A path-to-test-group selector cannot be built on top of
an instrument that does not know which tests a step runs**, so this is a
prerequisite rather than a companion, and the order inside the stage is: fix
the instrument, build the advisory selector, then decide the two questions on
top of it.

**The selector stays advisory**, on Plan 139 Stage E's terms: record what it
would have run, compare against every actual failure, and treat any failure
outside the predicted set as evidence against promotion rather than an
exception to allowlist. Promotion to job skipping requires a written observation
window with zero unexplained misses and a benefit larger than runner variance.
Plan 142's service graph is evidence for the selector and not the selector
itself — production asks which live work depends on a service, CI asks which
tests, images and environments can detect a changed path.

**The two questions it inherits** are
[below](#stage-p-inherits-a-question-stage-g-raised-and-declined): classifying
the incremental diff rather than the cumulative one, which Plan 139 Stage E's own rule
answers *not yet*, and content-addressed skipping for `docker-build`, which is
a claim about content rather than about run history and is the cheaper first
win.

**Exit.** The invocation rule distinguishes a suite from a directory; an
advisory selector emits its prediction on every full run without gating
anything; and both inherited questions have a recorded decision.

#### Stage P inherits a question Stage G raised and declined

**Scoped 2026-09-01, from a question asked while reviewing Stage G's CI
change. Recorded here rather than acted on, because it is Plan 139 Stage E's subject
and Plan 139 Stage E already has a rule for it.**

`ci_change_scope.py` classifies the **cumulative** PR diff — `base.sha` to
`head.sha` — so a documentation commit pushed onto a PR that has already gone
green re-runs the whole workflow, because the cumulative diff still contains
the production paths verified two pushes ago. Classifying the *incremental*
diff instead would skip it.

**The saving is about two minutes** — PR #325's full workflow was 127s wall
clock after Stage E. That is the number any design here has to beat, and it is
small.

**The cost is four conditions, not one diff.** A skip is only sound if all of
them hold, and three of them are invisible when they do not:

1. **The reference commit must be verified, not merely green.** The previous
   push may itself have skipped the heavy jobs, so the reference has to be the
   most recent ancestor where they actually concluded `success` rather than
   `skipped` — an Actions API walk with `actions: read`, or a marker written
   when heavy passes and read back later.
2. **The base must not have moved.** `actions/checkout` builds
   `refs/pull/N/merge` on a `pull_request` event, so the workflow tests
   `merge(base, head)` and not `head`. If `master` advances between the
   verified run and the new push, the merged tree differs even for a
   documentation-only diff and the earlier verdict does not carry. This is the
   dangerous one: green, fast, and not verifying the tree being merged.
3. **Rebases and force-pushes must fail closed**, via an ancestry check rather
   than a SHA equality that can match a commit no longer on the branch.
4. **The selection logic needs its own tests**, in Python beside
   `ci_change_scope.py` rather than in workflow shell, because it is riskier
   than the path classification it would sit on top of.

**Plan 139 Stage E's rule already decides this, and the answer is not yet.** Promotion
to job skipping requires an observation window with zero unexplained misses and
a benefit larger than runner variance; a false negative costs time and a false
positive suppresses evidence. Incremental gating is a false-positive risk by
construction. It also weakens exactly what Plan 139 Stage E told it not to: the current
fast path's proof is strong *because* it is cumulative — "every changed path in
this PR is under `docs/`" is a claim about a tree, and going incremental turns
it into a claim about a chain of runs.

**The cheaper target, if Stage P wants a win here first, is
content-addressed skipping for `docker-build`** — key the build on a hash of
the Dockerfiles, requirements and service sources and reuse the layer cache
when it matches. That is a claim about content rather than about run history,
so it carries none of the four conditions above, and `docker-build` is the job
`promtail-config` waits on.

### Stage S answers a question Plan 161 did not ask

**Legacy:** Stage 11 · **Issue:** CAR-79 · **State:** `—`

**Added 2026-09-01. Rewritten 2026-09-04, against a measurement that
contradicted its own premise.** [Plan 161](plan_161_testing_contract.md) asked
what a *service* owes before it ships and keyed the answer to a Python package.
The dbt project is not one: `dbt/` is a Dockerfile, SQL and YAML, `dbt_runner`
— the service that invokes dbt — has the "enough" row, and the models it builds
have none. `test_every_service_directory_has_a_row_in_the_enough_table` asserts
the table equals `service_packages()` in both directions, so **adding a `dbt`
row today fails as a phantom.** The obligation is not unmet; it is
inexpressible.

**The stage's original answer was a headcount, and the headcount is the wrong
instrument.** As written it measured 17 of 22 models with a dbt unit test and
required the five without to gain one. Re-measured 2026-09-04:

| | |
|---|---|
| Models on disk | **23**, not 22 |
| With at least one dbt unit test | 18 |
| Directly asserted by a fixture-driven real build | 7 |
| Asserted by **both** | 7 |
| Asserted by neither | 5 |

The fixture adds depth, not breadth — every model it asserts on already had a
unit test. But the count fails in the other direction too. `stg_observations`
has no unit test **and is not untested**:
[`scripts/seed_lake_snapshot_fixture.py`](../../scripts/seed_lake_snapshot_fixture.py)
seeds `ARTIFACT_NULL_VIN` and `ARTIFACT_SHORT_VIN` deliberately, so both reject
paths of its `vin17` guard and its accept path run against production-shaped
Parquet on every real build. The headcount scores it zero.

Set against branch counts the ranking inverts. Counting branch points in the
model SQL — a regex proxy, not a parse, and low by construction —
`int_listing_volatility_features` carries ~48 against 3 unit tests,
`int_listing_observation_fingerprints` ~37 against 5, `mart_deal_scores` ~33
against 4. **The models holding the most logic are the least proportionally
covered, and every instrument in this repository reports them as covered.**

**Three lists already claim to cover branches. None is derived from the models,
and no two are checked against each other.**

| List | Covers a branch with | Size |
|---|---|---|
| [`archiver/config/lake_snapshot_selectors.yml`](../../archiver/config/lake_snapshot_selectors.yml) | **real production rows** | 22 selectors, 18 SQL templates |
| [`scripts/seed_lake_snapshot_fixture.py`](../../scripts/seed_lake_snapshot_fixture.py) | **synthetic rows**, under the same scenario names | ~20 scenarios |
| `dbt/models/*/unit_tests.yml` | **mocked inputs** | 66 tests across 18 models |

The first is why this stage is a reconciliation rather than an invention. Its
own header already states the contract:

> Each entry names a dbt/PySpark branch or guard the snapshot must exercise,
> the source table(s) and filters used to find candidate entities in
> production, and the minimum representation required in the snapshot before it
> can be published.

The production snapshot is not a sample that happens to contain interesting
rows. It is **generated branch-first**, from real data, with `min_entities` as a
publication floor — `stable_state_run: 25`, `relisted_vin: 10`,
`invalid_or_null_vin`, `detail_beats_srp`, `srp_fallback`, `price_drop`,
`no_price_history`. The fixture mirrors those same scenario names
synthetically. Both were built to cover dbt branches; both are curated by hand
from somebody's reading of the models.

**So what the dbt project owes is branch coverage, and the missing artifact is
one: the branch list, derived from the model SQL itself.** With it in hand,
everything this stage wants is a comparison:

- **which branches no unit test covers** — against `unit_tests.yml`;
- **which branches production data never takes** — against the selector
  registry, and against which selectors actually fill `min_entities`. A branch
  no production row reaches is either dead code or a state never seen;
- **which constraints are decorative** — 161 column constraints are declared
  across the 23 models, and nothing demonstrates that removing a guard fails
  its constraint. A `not_null` is a claim about a branch guard, so the
  enumerator that finds the branch finds the constraint's subject too;
- **and the ratchet is free.** A derived list means a new model enters the
  denominator the moment it exists, and shows up missing in three places at
  once.

Today the only obligation this repository mechanically enforces on a model is
that it carries a cadence tag (`tests/dbt/test_cadence_tags.py`), which is a
*scheduling* rule. A new mart with no test of any kind ships green.

**G16 is untouched by the rewrite, and its argument is unchanged.**
`_SQL_EXEMPT_ROOTS` exempts `dbt/` from the Layer 2 census by design — correct,
because Layer 3 is dbt's instrument — so a `.sql` file whose logic moves into a
mart leaves a counted surface for an uncounted one, and **the count drops for
something that is not a repair.** That is the same failure as Stage F's
substring bug: the list shrinking for free. What the rewrite adds is the reason
the exemption is worth defending at all — it is only honest if the uncounted
population has a floor of its own, and branch coverage is that floor.

**One gap found while measuring, and small enough to close here.**
`--require-non-empty` proves all six dbt sources seeded rows before a snapshot
build, and its CI comment names the failure it prevents: *"left empty,
`stg_search_configs` reads nothing, `int_active_make_models` inner-joins to
nothing, and `mart_vehicle_snapshot` builds green over an empty world."* But the
list it checks — `LAKE_TABLES` plus `POSTGRES_SNAPSHOT_TABLES` — is hardcoded in
the seeder, and nothing asserts it agrees with `dbt/models/sources.yml`. A
seventh source would go unchecked and the gate would pass over exactly the empty
world it exists to catch. It is this plan's own recurring defect, sitting inside
the instrument this stage now depends on.

**The execution recorder left this stage on 2026-09-04, for
[Stage X](#stage-x-a-test-may-not-author-sql-either).** Recording what text ran
against which engine is repo-wide and not dbt's: production reaches an engine
through four client libraries — `psycopg2`, `asyncpg`, `duckdb` and
`pyspark.sql` — and the fixture-keyed design this section used to carry would
have recorded nothing for two of them, `scraper/sql/`'s statements included. X
is the stage that already makes every statement live in a file and validates it
against an engine, and it runs before this one, so the capture baseline's
deadline is served earlier there than it was here.

**Exit.**

1. **The branch list is derived from the model SQL**, not maintained. A model
   that gains a branch no unit test, no selector and no fixture scenario reaches
   fails the suite. Demonstrated by adding one, not asserted.
2. **The three lists are reconciled against it**, in both directions: every
   branch is claimed by at least one, and every entry in each list names a
   branch that exists.
3. **Every declared column constraint is shown to be load-bearing** — removing
   the guard that produces it fails its test. A constraint no mutation can
   break is recorded as decorative rather than left standing as coverage.
4. **G16 is asserted.** `production_sql_files()` may shrink only when the change
   names the dbt model that absorbed the statement; a silent shrink fails.
   Demonstrated by a silent shrink failing, not asserted.
5. **The non-empty gate derives its source list from `sources.yml`**, so a
   source added to the dbt project cannot go unchecked.

### Stage T exists because this plan grew the suite

**Legacy:** Stage 12 · **Issue:** CAR-80 · **State:** `—`

**Added 2026-09-01, at the maintainer's suggestion, during Stage L.** Plan 162
has spent nine stages adding tests -- Stage B put 73 orphaned ones into CI,
Stage H added Layer 4 for `container_health`, and Stage L alone took Layer 2
from 129 tests to 237. Nothing has yet looked at what that growth duplicated.

Measured on 2026-09-01, before the stage starts:

| | |
|---|---|
| Tests collected | 3,988 |
| Shared fixtures serving them | 11 |
| Ad-hoc `INSERT` statements inside test modules | 96 |
| Module-local seed helpers | 55 |
| Distinct read-back `SELECT`s written in tests | 161 |
| …of those, written more than once | **43** |
| Total retypings of those 43 | **145** |

**The duplication is by name, not merely by shape.** `_insert_artifact` is
defined separately in `tests/integration/ops/test_maintenance.py`,
`tests/integration/sql/test_ops_views.py` and
`tests/integration/sql/test_processing_queries.py`; `_insert_detail_claim` in
two files; `_seed` in three archiver modules; `_make_tar_zst` in three script
modules.

**And the assertions duplicate worse than the seeds.**
`SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid`
is written out **17 times**, across `tests/integration/processing/` and
`tests/integration/sql/test_ops_views.py`;
`SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid`
nine times; `SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid` six.

**~~These must not become `.sql` files.~~ Reversed by
[Stage X](#stage-x-a-test-may-not-author-sql-either), 2026-09-04**, and struck
rather than deleted because the mechanism it names is right and only the
conclusion was wrong. As written: a read-back assertion is the test's own half of
the work, not a statement production issues, and `shared/sql/` feeds
`production_sql_files()` -- filing one there would demand a Layer 2 test *for
an assertion*, which is circular, and would inflate the production census with
statements no service runs. **Every clause of that holds, and none of it requires
the file to live in `shared/sql/`.** A separate root with its own census answers
the circularity without keeping the exemption, which is what Stage X builds.

**Why this is the same defect as G17 and not a tidiness exercise.** A seed
`INSERT` written by hand inside a test is a statement that has to agree with
the schema, and there are 96 of them with no single definition. A column rename
means finding all 96, and nothing notices when 95 are found -- the surviving
one keeps passing against whatever it seeds. `tests/` is exempt from the Layer 2
census by design, and correctly so, because fixture seeds are not production
SQL. That exemption is what makes this invisible: the rules this plan built all
stop at the tests' own door.

**That paragraph is Stage X's thesis, written here first and scoped as
duplication.** It states the drift exactly -- 96 statements that must agree with
a schema, with no single definition and nothing that notices when 95 of them are
found -- and then reaches for a shared helper, which removes the retyping and
leaves the drift. **This stage keeps the half a file cannot answer**: the 55
module-local seed helpers, the three `_seed` definitions, `_make_tar_zst` in
three script modules. Two helpers doing the same thing is not a question a file
answers, which is what the instrument note below is about. The SQL is Stage X's.

**Two things the stage must not do**, stated now so they are decisions rather
than discoveries. It must not consolidate a fixture whose two callers want
different data -- a shared seed that grows parameters until it can serve
everyone is harder to read than the two helpers it replaced, and a test whose
setup lives three files away is worse at explaining its own failure. And it
must not touch `tests/scripts/oneoff/`, which Stage G declared spent: those
helpers duplicate each other freely and should, because the plans that own them
have archived.

**The instrument this stage needs does not exist yet**, which is why it is
scoped after [Stage X](#stage-x-a-test-may-not-author-sql-either) rather than
before it. "Two helpers do the same thing" is not a textual property -- `_seed`
and `_insert_queue_row` may be identical in effect and share no token -- so
unlike G5, G15 and G17 there is no cheap derived check waiting to be written.
The instrument that would answer it is the execution recorder, which records
what text each helper actually executed; it was Stage S's until 2026-09-04 and
is X's now, so this stage's dependency moved with it. The stage should say plainly whether it
found one or whether it leaves prose behind, per success criterion 2.

**Exit.** Scoped to the Python half; the SQL is Stage X's and is not re-measured
here. The helper duplication is re-measured against the same recipe that produced
the table above, and every delta is recorded — including the ones that did not
move. Shared test helpers replace the duplications that genuinely share intent;
a duplication whose callers want different data is left alone and that decision
is recorded rather than silently skipped. `tests/scripts/oneoff/` is untouched.
And the stage states plainly
whether it found a mechanical instrument for "two helpers do the same thing" or
leaves prose behind: concluding that none exists is a permitted outcome,
concluding nothing is not.

### Stage U: every skip in CI is declared, or the run fails

**Legacy:** Stage 13 · **Issue:** CAR-81 · **State:** `done`

**Found 2026-09-04, closing Stage P.** The run that proved the snapshot gate
works reported `3622 passed, 1 skipped`, and the skip took a paragraph to
explain — which is a paragraph nobody would have written if the number had not
been quoted in a record entry.

Measured across the whole run: **two skips, two jobs, two reasons, zero
mechanisms.** `test_every_sha_a_recap_names_is_a_real_commit` skips because
`actions/checkout@v4` clones at depth 1 and it needs real git history.
`test_dictionary_compressed_objects_and_packed_members_are_both_readable` skips
because `INTEGRATION_HTML_DICT_ID` is deliberately unset. Both are correct
decisions. Both are held in place by prose — a docstring and a `ci.yml` comment
— and a third would arrive the same way, silently.

**`REQUIRE_LAYER_2_EXECUTION` is narrower than its name.** It fails a run on any
skip in `tests/integration/sql/`, which is one suite in one job. The dictionary
skip is in `tests/integration/shared/` and the recap skip is a Layer 0 test in
the unit job; neither is in its reach. Its `pytest_terminal_summary` hook is the
right mechanism sitting at the wrong scope.

**The shape is `DORMANT_SUITES`, one level down.** That tuple made a deliberately
unrun *suite* declare itself, with an assertion that fails when an undeclared one
appears and a second that fails when a declared one starts running. The same two
directions apply to a deliberately skipped *test*, and the second direction is
the one that matters most here: a skip whose reason has stopped being true is
exactly the drift this plan exists against.

**Estimate: 1 point.**

**Exit:** every skip observed in a CI run is named in a declared-skips registry
with its reason and its condition; an undeclared skip fails the run; a declared
skip that stops skipping fails too; and the hook covers every job rather than
one suite. Demonstrated by an undeclared skip failing a run, not asserted.

### Stage V: a variable the environment documents reaches the service that reads it

**Legacy:** Stage 14 · **Issue:** CAR-88 · **State:** `—`

**Found 2026-09-04, deploying this plan's own change.** Stage P's credential
work added `SNAPSHOT_DOWNLOAD_TOKENS` to `.env.example` and to
`ops/routers/snapshots.py`, and never added it to `docker-compose.yml`. A
variable in `.env` reaches a container only if the service names it, so the new
one was inert: the router fell back to the legacy single token, the deploy
reported healthy, the route answered 200, and **a working rotation and a failed
one were indistinguishable from outside.** It surfaced only because the
container was asked what it had loaded rather than whether it was up. A `git
pull` is not a deploy, a healthy container is not a correct one, and neither the
deploy script's health gate nor the route's own 200 could tell the difference
here.

`.env.example` is the file that tells an operator what to set. A key it
documents that no service consumes is a lie in the one place someone reads
before touching production.

**Measured across the whole file: 37 keys, four never referenced by any
`docker-compose*.yml`.** One was the defect above. The other three are
pre-existing and are the reason this stage is an investigation before it is a
rule:

| Key | What has to be established |
|---|---|
| `FASTAPI_ADMIN_KEY` | whether anything still reads it, or it is dead and leaves `.env.example` |
| `MLFLOW_TRACKING_URI` | Plan 112's, and plausibly script-only — a variable a developer exports, never a container variable |
| `PROVENANCE_ENV` | same shape, same question |

**Waiving all three to make a new assertion pass is the move this stage exists
to refuse.** Each has a different correct answer — wire it, delete it, or
declare it script-only — and a ledger that absorbs three unexamined entries on
the day it is created is decoration. Nine stages of this plan have gone into
making waivers mean something.

**Then the rule.** Every key in `.env.example` is either referenced by a
`docker-compose*.yml`, or declared script-only with the consumer that reads it
named. The declaration carries the same two directions as `DORMANT_SUITES`: an
undeclared unwired key fails, and a key declared script-only that later appears
in compose fails too.

Not folded into [Stage U](#stage-u-every-skip-in-ci-is-declared-or-the-run-fails):
different subject, same class. That one is about a test that does not run; this
is about a variable that does not arrive. Sharing a stage would make the pair
read as one mechanism when they are two.

**Estimate: not sized.** The investigation is what sizes it — three keys with
three possibly different answers, and the rule is small only if none of them
turns out to be a real undelivered variable.

**Exit:** each of the three keys has an established answer and has been wired,
deleted, or declared; every remaining `.env.example` key is referenced by a
compose file or declared script-only with its consumer; an undeclared unwired
key fails; and a script-only declaration that stops being true fails.
Demonstrated by an unwired key failing, not asserted.

### Stage W: a test may not supply both halves of a contract

**Legacy:** Stage 15 · **Issue:** CAR-82 · **State:** `—`

**Found 2026-09-04, closing Stage P.** `check_snapshot_result` in the export
DAG accepted only `{"created"}` as a successful non-dry-run status. The exporter
returns `"exported"`. A DAG-triggered export would have published its archive
and both pointers and then failed the task, and it went unnoticed for as long as
it did because the DAG had never run.

**The test was the reason it could survive being written.**
`tests/integration/airflow/test_export_ci_lake_snapshot_dag.py` seeded
`{"status": "created"}` itself and asserted the checker accepted it. Both halves
of the contract were authored in one file, so the test passes for *any* string
its author picks — including one no service emits. It was not a weak test of the
right thing; it was a strong test of nothing.

**The class is narrower than the rule it sits under, and that is what makes it
reachable.** [*A run that succeeds has done the work its success
implies*](../TESTING.md#specified-here-not-yet-asserted) is recorded as having no
general form, and that is correct — "did this actually do the thing" is specific
to each thing. But *this* has a signature: **production enumerates a closed set
of values, and a test restates a member of that set as a literal rather than
deriving it.** Status sets, scope names, enum members, state vocabularies. The
same shape the `.sql` convention already solved for statements — defined once in
production, read by the test rather than retyped.

**That precedent is holed, and [Stage X](#stage-x-a-test-may-not-author-sql-either)
is the repair.** The `.sql` convention solved this for *production* statements.
Test statements were exempted by [Plan 161 question
3](plan_161_testing_contract.md#3-what-must-never-be-mocked), so a read-back
assertion retyped inside a test is the very thing this stage is about, in the
form this stage cites as already handled. Nothing here needs to wait on it —
the closed-set form is independent and the repair above stands — but the
sentence should not be read as saying the statement case is closed.

The repair for the instance is
`tests/airflow/test_export_ci_lake_snapshot_statuses.py`, which reads the DAG's
`acceptable` sets by AST and the exporter's `status=` literals by import, and
fails when the DAG accepts a status the exporter cannot produce. Both its
assertions were watched failing against the reintroduced bug. **That is one
instance and no mechanism**, which is why this stage exists rather than the
Record entry that named the shape being the end of it.

**The stage may conclude that only the narrow form is reachable**, and should
say so plainly rather than stretching for a general checker — the same licence
[Stage T](#stage-t-exists-because-this-plan-grew-the-suite) has. Enumerating
"a closed set in production restated in a test" is a static question. Deciding
whether an arbitrary fixture value should have come from somewhere is not, and a
rule that tried would fail on correct code.

**Three stages from one evening, all the same class.** Stages U, V and this
one were each found closing Stage P, and each is a declaration held by prose
that nothing enforces — a skip explained in a docstring, a variable documented in
`.env.example`, a status agreed in a comment. That they arrived together is not a
coincidence: the stage that closed was the one that asked what its own numbers
meant.

**Estimate: not sized.** Whether the narrow form is one rule or several is what
the first measurement answers.

**Exit:** the closed sets production enumerates and tests consume are
identified; a test that restates a member as a literal rather than deriving it
fails; and the stage states plainly which forms of the class the rule reaches
and which it does not. Demonstrated by a restated literal failing, not asserted.

### Stage X: a test may not author SQL either

**Legacy:** Stage 16 · **Issue:** CAR-83 · **State:** `next`

**Added 2026-09-04, from a review of what the SQL contract actually guarantees.**
The contract's claim is not that SQL *should* live in files. It is that SQL which
does not live in a file cannot be green. That property is total for production
statements and stops at `tests/`, and everything below follows from asking why.

This stage came from a conversation rather than an incident or a sweep, so its
reasoning — the prior-art comparison that prompted it, the affordance/enforcement
distinction it rests on, and the two positions argued and abandoned on the way —
is recorded in
[`docs/evidence/plan_162_stage_X_origin_2026-09-04.md`](../evidence/plan_162_stage_X_origin_2026-09-04.md).

**The exemption was reasoned, and its premise no longer holds.** [Plan 161
question 3](plan_161_testing_contract.md#3-what-must-never-be-mocked) settled that
paraphrase detection is judgement rather than mechanism, for one stated reason:
*fixture seeds are SQL in test files too*, and a checker that cannot tell a seed
from a paraphrase fails on correct code. That is true and it is the whole
argument. **If no SQL literal appears in a test file at all, the ambiguity has
nothing to live in** — any SQL-shaped literal under `tests/` is a violation, and
the rule stops needing judgement.

**So this stage removes a judgement rule rather than adding a mechanical one.**
The contract's split moves **7 mechanical / 4 judgement → 8 / 3**, and the rule
that leaves is the one [Plan 161 flagged as reading mechanical and not being
it](plan_161_testing_contract.md#7-what-does-the-agent-skill-check-and-what-can-it-not).
`.claude/skills/testing-contract/SKILL.md` loses its fourth judgement rule in the
same change; a skill that goes on refusing to certify something now asserted is
the stale-waiver defect in prose.

**The measurement already exists and belongs to
[Stage T](#stage-t-exists-because-this-plan-grew-the-suite)**, taken
2026-09-01: **96 ad-hoc `INSERT` statements inside test modules**, **161 distinct
read-back `SELECT`s**, 43 of them written more than once for **145 total
retypings** —
`SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid`
seventeen times. Stage T read those as duplication and reached for a shared
helper. A helper removes the retyping and leaves the drift: one definition that
still has to agree with a schema, with nothing asserting that it does.

**Nothing here needs inventing, which is why this is a stage.** Five mechanisms
exist and are pointed at a second root: `shared.query_loader` loads it,
`production_sql_files()` is the derivation pattern for the census, the Layer 2
execution rule is the assertion shape, the waiver tuples are the ratchet, and
[`verify_testing_contract_mutations.py`](../../scripts/verify_testing_contract_mutations.py)
is how the new rule earns trust before it is believed.

**The root is separate and its census is its own.** `tests/sql/` is loaded by the
same loader and is **not** in `production_sql_files()`. That answers Stage T's
circularity objection without keeping the exemption: a read-back assertion is
still not a production statement, still owes no Layer 2 test, and still does not
inflate the production denominator — it is simply no longer a literal typed
inside a test.

**`PREPARE` is likely the right instrument for validating them.**
`PREPARE stmt AS <sql>` parses and plans against the live catalogue of a
Flyway-migrated Postgres, so a renamed column fails loudly with no rows written
and nothing to clean up, and placeholders are native. It does not cover DDL and
does not catch constraint violations, so the real executions still happen in the
suites that own them; `PREPARE` is what schema-checks **every** test statement
whether or not a test using it ran. Today a seed is only checked if its own test
happens to execute — which is the same conditional coverage
[G14](../TESTING.md#the-gap-list) found on the production side.

**Two things to settle rather than discover.** Test DDL — temp tables and
scaffolding created inside tests — is neither production DDL that Flyway owns nor
a seed, and needs a stated position. And **the detector is itself an instrument
with a hostile failure surface**: docstrings, log lines and fixture text all
contain SQL keywords, and [G5's own measure](#stage-l-grew-two-gaps-while-closing-one)
matched a `SELECT` inside `db_cursor`'s docstring. This plan's rule applies to
this plan's newest rule — a denominator fitted to what exists when it is written
will be wrong — so the detector is mutation-tested before it is trusted, not
after.

**The execution recorder arrived here on 2026-09-04, from
[Stage S](#stage-s-answers-a-question-plan-161-did-not-ask).** It was scoped
there because it reads SQL and dbt was the surface that stage was defending,
and that was the wrong seam. Recording *what text executed against which
engine* is a claim about every statement in the repository, not about the dbt
project; this is the stage that already makes every statement live in a file
and validates it against an engine, so the engine half belongs beside the file
half.

**Its first design was an enumeration, and the repository caught it before it
was written.** Keying capture to the fixtures that hand out connections —
`cur`, `viewer_cur`, `duckdb_con`, `duckdb_s3_con` — misses two of the four
client libraries production actually reaches an engine through:

| Client | Where | A fixture-keyed recorder sees it |
|---|---|---|
| `psycopg2` | every service's `queries.py` path | yes |
| `duckdb` | `shared/duckdb_s3.py`, `dashboard`, `dbt_runner` | yes |
| `asyncpg` | `scraper/db.py`, exercised unmocked since Stage M | **no** |
| `pyspark.sql` | Plan 125's tooling | **no** |

It would have shipped recording nothing for `scraper/sql/`'s statements, and
gone on recording nothing when Spark arrives. That is `_SQL_CALL_NAMES` again,
which [Stage N](#stage-n-the-dag-trees-sql-convention) deleted rather than
lengthened.

**So the recorder is keyed on the client, and the client set is derived.** A
session-scoped plugin wraps each library at its entry point, so every
connection any fixture opens is recorded and the fixture list stops existing.
`production_db_clients()` — read from the imports across
`production_python_files()`, Stage N's derivation reused — is compared against
what the plugin instruments, equal in both directions, the same shape as
`service_packages()` against the "enough" table. **A new engine is a new
import, and a new import fails the suite until the recorder wraps it or the
contract says in writing why not.** That is how a future engine is made to
conform: not a rule someone remembers at Gate D, but a test that breaks when
the import lands.

dbt is the one execution surface that cannot be wrapped, running in a
subprocess — and does not need to be. It already writes what it executed to
`target/run/` beside `run_results.json`. A declared second mechanism, not a
hole.

**The `.format()` templates are the known hard part, and there are two ways
out.** A statement stored as a template records rendered, so attributing a
recorded string back to its `.sql` file is either a reverse match against the
template turned into a pattern — test-only, approximate, brittle on multi-line
placeholders — or a `str` subclass returned by `shared.query_loader.load_query()`
carrying its origin and preserving it through `.format()`, which is exact and
costs a production change made for a test instrument. All seven services load
through that one function. Decide it at the top of the stage rather than in the
middle of it.

**Two things it will not do, recorded now rather than discovered at Gate D.**
The recorder records *text*, so Spark's DataFrame API — not text at all, and a
`selectExpr` fragment leading with no verb — is invisible to it, exactly as
[G15](../TESTING.md#the-gap-list) already records for the static rule. And the
**cross-engine assertion** — "this ran on the engine production uses for it" —
is not in this stage: it needs two live engines to design honestly and belongs
to [Plan 125 Gate
D](plan_125_duckdb_to_iceberg_migration.md#gate-d-reader-migration). Building it
against one live engine and one hypothetical would fit the design to what
exists today.

**Capture has a deadline; aggregation does not.** A baseline taken after Gate D
is not a baseline, and capture is engine-local and cheap. The aggregation the
coverage upgrade needs — a per-job artifact and a gate job, because a statement
may be executed in any of five CI jobs — is what CAR-79 was filed blocked on
CAR-78 for, and Stages Q and R are what settle how those jobs are defined. The
baseline can be taken here and the aggregation can land with or after Q.

**What the aggregation buys, when it lands, is the weak reading this plan has
carried since Stage L.** `test_every_production_sql_file_is_touched_by_a_layer_2_test`
credits a file when a Layer 2 module *names* it as a whole word, which this
document has called the weakest available reading from the day it was written.
Replacing `_names(stem, text)` with "this file's text executed in this run"
turns it into the strongest, in one edit — and it is the half that cannot run
inside any single job, which is precisely why it waits for Q.

**Ordering: it must precede Stage T's SQL half, and now does.** Under the
numbering it ran after T, which would have built shared helpers that this stage
then converted to files — the [Stage F/G collision](#why-this-order) exactly.
That was resolved by scoping: T kept the Python helpers, this stage took the
SQL, and neither waited on the other. **The 2026-09-04 reordering resolves it
outright** — X is order 16 and T is order 18 — and the scoping split is kept
anyway, because two stages that cannot collide are cheaper to reason about than
two that merely do not.

**Estimate: 1 point, and it predates the recorder.** It was settled by the
paragraph above — nothing invented, five existing mechanisms pointed at a second
root — which held while this stage was only about where test SQL lives. The
recorder arrived on 2026-09-04 and *is* invented rather than pointed, so the
estimate is owed a revisit it has not had. The census of SQL literals under
`tests/` sets the waiver list this stage drains; it does not size the stage.

**Exit.** Two halves, the second of which arrived on 2026-09-04.

**Where test SQL lives:** no SQL literal appears in any file under `tests/`;
every statement they now hold lives under `tests/sql/`, loaded rather than typed,
and is validated against a Flyway-migrated Postgres whether or not the test
consuming it runs; the production census is unchanged in size by the move;
judgement rule 4 is struck from `docs/TESTING.md` and from the reviewer skill,
taking the split to 8/3; test DDL has a recorded position; and the detector has
been watched failing against a mutation of each shape it claims to catch.
Demonstrated by an inline statement failing the suite, not asserted.

**What ran against which engine:** capture records the text executed and the
client it executed through, with its baseline taken while DuckDB is still
authoritative; `production_db_clients()` is derived from production's imports
and asserted equal in both directions against what the recorder instruments, so
a new engine fails the suite until it is wrapped or exempted in writing;
`.format()` provenance is solved rather than noted; and dbt's subprocess is
captured from its own run artifacts rather than left uncovered. Two things are
explicitly **not** in this exit: the cross-engine assertion, which belongs to
Plan 125 Gate D, and the aggregation — a per-job artifact and a gate job — which
lands with or after Stage Q and takes the replacement of the Layer 2 name-match
reading with it.

## Success criteria

**1. The waiver list is empty.** All 120 entries deleted, each by the repair it
was waiting for rather than by being removed. The contract's own assertions make
this self-verifying: a waiver that no longer describes a violation fails as
loudly as an unwaived violation does.

**2. Every gap this plan closes leaves behind something that fails if it comes
back.** A repair with no assertion behind it is Plan 84 repeated exactly — real
tests, an accurate description, false within months, invisible because nothing
could tell. This is the criterion the six unmechanised gaps exist to be measured
against, and it is why Stage C comes before the stages that would otherwise be
graded by the instrument it repairs.

Three exceptions, stated here so they are decisions rather than omissions:

- **G7 stopped being an exception on 2026-09-02, and the rescoping is why.**
  As written it could not be asserted by the existing rules and needed an
  approach invented. Narrowed to the assertionless Layer 2 suite, it is
  ordinarily mechanisable — a Layer 2 test that executes a statement and asserts
  nothing about the result is a rule this suite can hold — and Stage M owes that
  rule, not just the 25 assertions. **The part that was genuinely exceptional
  left with G18**, which is Plan 150's, so this criterion no longer carries it.
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

  **That prediction came true on 2026-09-01 and the exception is now narrower.**
  A Windows-only encoding defect broke master, found exactly as forecast — by
  someone running the suite where CI does not. It is the third instance of the
  class, which is enough of a pattern to stop treating each one as a one-file
  repair, so [Stage J](#stage-j-was-added-by-the-failure-this-plan-predicted)
  now owns the class. **The exception stands only for the part Stage J concludes
  it cannot mechanise**, and Stage J is required to say which part that is rather than
  leaving it implied. What is already settled is that the obvious mechanism does
  not close it: `PLW1514` cannot see a `tmp_path / "name"` receiver, so the rule
  that looks like the answer would have passed this defect too.

  **Stage J answered this on 2026-09-01, and the exception is now one named
  behaviour rather than a whole rule.** Encoding is mechanised:
  `test_every_text_read_and_write_states_its_encoding` requires `encoding=` on
  every `read_text` and `write_text` in the repository, and fails on
  `(tmp_path / "a.md").write_text("—")` — the exact call ruff answers
  `All checks passed` on. It covers three shapes rather than one: the two
  `pathlib` methods, text-mode `subprocess`, and the logging handlers that open
  a file — the last two found by running PEP 597's `EncodingWarning` once, as a
  discovery tool, and then checked statically rather than at runtime for the
  reasons the decision record gives. **What remains unmechanisable is two
  things, not one.** The first is any encoding shape nobody has named yet: this
  rule sees what it is told to see, and the next unnamed shape will be found the
  way these were. The second is everything else the harness decides: path separators, line endings, case-insensitive filename
  collisions, and locale-dependent collation.** Those have no textual signature
  to match on — the code that breaks on them is not distinguishable, by reading,
  from code that does not — so the only instrument that sees them is an actual
  second platform, and [Stage J's decision
  record](#stage-j--mechanising-the-encoding-sensitive-io-guard)
  says why a Windows runner was declined rather than built. That is the residue,
  and it is now a list of four behaviours instead of an open-ended class.

**3. The `dbt build + test` job is no longer the critical path**, and what
replaced it is named for what it does. Measured in wall-clock seconds against
the 267s baseline, not asserted.

**Met by Stage E, 2026-09-01.** Across three runs of the final configuration
the workflow went 292s to 145-165s and the job's successor 267s to 118-134s,
in four jobs named for what they run.
[The precise reading](../evidence/plan_162_stage_E_evidence.md#success-criterion-3-is-met) matters more than the
headline: the dbt job's cost fell by 55% and stopped dominating, but it is
still the longest job in the workflow on both post-change runs. The criterion
was accepted as met on that basis.

**4. ~~Every suite in `tests/integration/` is either invoked by a named CI step
or declared dormant with a reason.~~ Met by Stage B (CAR-45), 2026-08-31.**
This was G1's repair and the one criterion already mechanically enforced when
the plan was written. `CI_INVOCATION_WAIVERS` is `()` and
`test_every_integration_suite_is_invoked_by_a_ci_step` fails against an empty
tuple the moment a suite appears unrun, with `tests/integration/lakehouse/`
declared in `DORMANT_SUITES` rather than waived.

*Struck in Stage E rather than deleted. The sentence went on describing "the
four current waivers" after Stage B had removed all four: the criterion was
already true and only its description had aged, which is the small version of
exactly what this plan exists to stop.*

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

## Intersections

### Plan 161 — the testing contract

Archived. It decided the rules and built the mechanism that measures them; this
plan closes the distance. `docs/TESTING.md`'s gap list names Plan 162 as the
owner of thirteen entries — twelve at the census, plus G13, re-owned here on
2026-08-31 — and an assertion fails if that owner is ever an archived plan — so this plan cannot be quietly abandoned without the suite
saying so.

**This document was written as a deliberate stub on 2026-08-30**, when Plan 161
had not yet decided the standard this plan measures against. Writing the stages
before the standard existed would have been scoping work against a rule nobody
had agreed. That blocker is gone: 161's contract landed, was asserted, and is
archived.

### Plans 103 and 107 — coverage

Superseded, and **their targets are this plan's inheritance rather than their
own**: 103's per-file coverage gaps and 107's testing-rubric third are what the
gap list now measures. Read them for the gap list they assembled, not for their
numbers.

### Plan 120 — CI lake snapshot

Complete, and it produced **two** artifacts this document had been conflating.
Stage E checked, because a claim about what CI builds against should not rest
on a sentence:

- **The synthetic fixture**, `scripts/seed_lake_snapshot_fixture.py` — its own
  docstring calls it *"the synthetic MinIO fixture used by the Plan 120
  lake-snapshot integration tests"*. It is seeded before every `dbt build` in
  CI and **is** read by it: `sources.yml` globs
  `silver_normalized/observations/**/*.parquet` with `hive_partitioning=true`,
  which picks up the fixture's reserved `obs_year=2099` partition, and the
  real-build tests assert on rows only a build over it can produce.
- **The production-derived snapshot** — the `snapshot-worker` →
  `snapshot.tar.zst` + `archive_manifest.json` →
  `ci_snapshots/adaptive_refresh/latest.json` → `download_lake_snapshot.py`
  pipeline. `download_lake_snapshot`, `snapshot.tar.zst` and `ci_snapshots`
  appear in no workflow and no Compose file. **Nothing pulls it**, and getting
  it into CI means a token and production VIN/dealer data on a GitHub runner.

This entry previously read that the fixture was *"unused for the dbt build it
was paid for"*. That is false of the fixture and true of the snapshot, which
is Stage P's.

### Plan 121 — staging environment

Owns the deployed-stack rehearsal that Stage P's greenfield-versus-populated
question cannot close from inside a CI job.

## Record

One entry per closed stage, oldest first. **Legacy** names the stage's old
number, so a commit, branch or ticket written before 2026-09-04 still resolves.
Where an entry states a cost as an `In Progress` window, that window is wall
clock reconstructed from the issue's Linear state history, not effort recorded
when the stage closed.

### Stage A — the census

**Legacy:** Stage 0 · **Issue:** CAR-40 · **Closed:** 2026-08-31

Both exit conditions met. Commit `dfa55ae`. Estimate 2, actual 1.

The census ran against the instrument Plan 161 built rather than by hand, which
is the whole reason it cost 1 rather than the XL this plan was sized at. What
it produced is [above](#the-case): 120 waived violations across
five mechanically checked rules, twelve gaps, and a stage per repair.

**Every by-eye reading this document had carried since 2026-08-30 was an
undercount, three times out of three**, and the gap inspection missed entirely
— G14, 56 of 76 `.sql` files executed by no Layer 2 test — is now the largest
single item in the plan. The direction of that error is the reusable finding,
not the individual numbers.

**`dashboard/` is Streamlit, not FastAPI.** G7 therefore cannot be reached by
the route rule or by the "enough" floor's first clause: the rule imports
`<service>.app` and reads its OpenAPI schema, and there is no schema to read.
This was not known when the gap list was written, and it means Stage M must
invent an approach rather than drain a waiver list.

*Read on 2026-09-02 as a conclusion about the wrong subject, and left standing
because it was the right conclusion about the one it had. Inventing an approach
is what the Streamlit Python needs, and that is now G18 and Plan 150's; the
dashboard gap this plan kept — a Layer 2 suite with 25 tests and no assertions
— needed no invention at all, and the census never looked for it because it was
counting test files rather than reading one. See [Stage M
narrowed](#stage-m-narrowed-and-g7-now-names-a-different-gap).*

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
That is Stage B, and it is the one input the L estimate rests on that remains
unmeasured.

### Stage B — the orphaned suites

**Legacy:** Stage 1 · **Issue:** CAR-45 · **Closed:** 2026-08-31

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
design change.** Stage B was scoped to declare `tests/integration/lakehouse/`
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

**The L estimate is confirmed.** Stage B was the one input it rested on that
the census could not settle, and it resolved the favourable way: no production
defects, no rot in the covered areas, and Stages L and M do not get worse. The
two test defects cost minutes, not the days a genuine failure would have. What
Stage B adds to the estimate is not effort but a warning about its shape — both
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
Stage B (CAR-42)**, scoped to the `PYTHONPATH` half. That half shipped; the
`Documentation tests` step sets `PYTHONPATH` today, and `21333ab` had already
repaired the other named instance. So the G13 row described finished work while
naming an owner in closeout that owes no code, and the live instance — found and
deliberately left by Plan 161 — had no owner at all.

**G13 is therefore re-owned to Plan 162, Stage F**, and the row rewritten to
describe what is actually left. Stage F rather than Stage D: Stage D is a
specific two-venv census fix and "also small" is not a category, while Stage F
is already the pass that reads every patch in the suite. What that costs is
honest and recorded above — [a third exception](#success-criteria) to success
criterion 2, and the weakest of the three.

### Stage C — unblinding coverage

**Legacy:** Stage 2 · **Issue:** CAR-46 · **Closed:** 2026-08-31

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
percentage"* clause holding under its first real test, and it re-scopes Stage H:
G9 is a test-home and routing problem, not a coverage one. `dashboard/` at 9%
is the genuine gap, and it is Stage M's.

**The threshold is a ratchet, not a target, and `scripts/` is the caveat on
it.** At 10,488 statements it is over half the denominator and largely spent
one-off code, so it damps the movement the service stages produce. That is
[Stage G](#stages), scoped from this measurement.

**Two of this stage's own claims were wrong and were corrected by measuring.**
The gap list said unblinding would expose "the two services below the floor"; it
exposed one badly covered service, one well-covered one, and a `scripts/`
denominator nobody had counted. The first sizing of Stage G then repeated the
error in miniature — see its section for what the archive join corrected.

**Landed alongside Stage B and reconciled to it.** Stage B established the
convention for repaired gap entries — row deleted, preamble names what closed
it, history here, letters never reused — while this branch was open; G10's
closure was rewritten to follow it. Stage B's own deletions had left three
mutations in `scripts/verify_testing_contract_mutations.py` anchored on the
removed G1 and G2 rows, so the script aborted rather than ran; its staleness
guard is what said so. Re-anchored, and Stage B's new dormancy rule was given
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
cannot see the instance Stage F owns, exactly as its row says. The single
Linux skip is unrelated — `test_every_sha_a_recap_names_is_a_real_commit`,
which skips on a shallow clone.

### Stage D — one declared source for the health-sensor censuses

**Legacy:** Stage 3 · **Issue:** CAR-47 · **Closed:** 2026-08-31

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

### Stage E — splitting the 267s dbt job

**Legacy:** Stage 4 · **Issue:** CAR-48 · **Closed:** 2026-09-01

All four exit conditions met. Commits `5dd6bb7`, `8c54915`, `70d2411`,
`9f21f87`, [PR #321](https://github.com/whitewalls86/new_car_tracker/pull/321).
Estimate 2 points. `In Progress` ran 02:32–03:56 UTC on 2026-09-01, 1h23m.

**The whole result, measured across nine CI runs rather than asserted:**

| | Workflow wall clock | The long job | `tests/integration/dbt/` |
|---|---:|---:|---:|
| Baseline, three consecutive runs | 277 / 297 / 303s | `dbt build + test` 254 / 272 / 282s | 88 / 89 / 61s |
| Split into four jobs | 232s | `dbt model tests (real build)` 210s | 89s |
| — plus per-job dependency trim | 201s | 177s | 86s |
| — plus in-process dbt, three runs | **145 / 156 / 165s** | **118 / 134 / 128s** | **25 / 37 / 36s** |

**292s to about 155s is a cut of a little under half, and the 269s job's
successor runs around 127s.** Three changes did it, and only the first was the
stage as scoped.

The final configuration is quoted as three runs rather than a number because
the spread is real and worth knowing before a later stage optimises against
it: **the wall clock moved 20 seconds across three consecutive runs of
identical code**, and the equivalence step moved 25s to 37s. Job durations are
steadier than wall clock, which also carries queueing — run 3's `lint` waited
14s for a runner. A change worth less than about 20 seconds cannot be
demonstrated here without more runs than it is worth.

**The full record is [`docs/evidence/plan_162_stage_E_evidence.md`](../evidence/plan_162_stage_E_evidence.md)**, 7 sections:

1. What the job was, and why the cut is by prerequisite
2. Plan 139 Stage C's question, answered
3. A property nearly lost, then re-established somewhere else
4. Two findings that were not scoped and cost nothing
5. Success criterion 3 is met
6. What was deliberately not done
7. Cost, and one regression worth recording

### Stage F — the mechanical sweeps

**Legacy:** Stage 5 · **Issue:** CAR-49 · **Closed:** 2026-09-01

**Cost:** estimate 2 points. `In Progress` ran 04:01–05:09 UTC on 2026-09-01, 1h08m.

All four exit conditions met. **The waiver list went 116 to 68** — the 50 the
stage was scoped to delete, and 2 added back because moving two test files
[exposed a defect in the Layer 2 checker](../evidence/plan_162_stage_F_evidence.md#the-instrument-was-weaker-than-its-own-docstring).
Both tuples the stage owns are now `()`. 51 test files, three workflow lines
and one contract document changed; the production tree was not touched.

| | Before | After |
|---|---:|---:|
| `MOCKER_WAIVERS` | 34 | **0** |
| `LAYER_NUMBER_WAIVERS` | 16 | **0** |
| `ROUTE_WAIVERS` (Stage H) | 12 | 12 |
| `LAYER_2_WAIVERS` (Stage L) | 54 | **56** |
| **Total** | **116** | **68** |

G4, G11 and G13 are deleted from `docs/TESTING.md`'s gap list, which is now
seven rows: G5, G6, G7, G8, G9, G12 and G14.

**The full record is [`docs/evidence/plan_162_stage_F_evidence.md`](../evidence/plan_162_stage_F_evidence.md)**, 10 sections:

1. The venv fix, which really was one argument
2. Two of the 16 were a move, not a rename
3. Four `Layer N` cross-references, which the rule cannot see
4. G13: an interpreter path, replaced by a shell builtin
5. One conversion that is not mechanical, and says why in the file
6. How it was done, and what that cost
7. Three process-state patches stayed monkeypatch, correctly
8. A unit test filed as an integration test, and two wrong answers before the right one
9. The instrument was weaker than its own docstring
10. Coverage is a unit-test instrument, and the integration answer is already built

### Stage G — separating production scripts from spent ones

**Legacy:** Stage 5b · **Issue:** CAR-55 · **Closed:** 2026-09-01

**Cost:** estimate 2 points. `In Progress` ran 05:11–06:02 UTC on 2026-09-01, 51m.

**Fourteen scripts and seven test files moved, as renames.** `scripts/oneoff/`
holds the work whose owning plan has archived; `tests/scripts/oneoff/` mirrors
it. Everything else stayed at `scripts/`, and `git log --follow` carries the
history rather than a manifest recording it. Shipped in `e954306`, PR #325.

| | before | after |
|---|---|---|
| Statements in the denominator | 19,733 | **13,588** |
| Reported coverage | 75.91% | **77.66%** |
| `--cov-fail-under` | 74 | **75** |

[Run 33474748500](https://github.com/whitewalls86/new_car_tracker/actions/runs/33474748500),
all ten jobs green in 127s: `Required test coverage of 75% reached. Total
coverage: 77.66%`, with `Documentation tests` the only skip — which is the
correct classification for a branch that touches production paths, and the
first exercise of the new `heavy` gate.

**Linux and this checkout read the same number**, 13,588 statements and 3,035
missed on both. Stage C found seven statements that differed between Linux and
Windows and set the floor two points low to absorb them; that spread did not
appear here, and the two points of headroom carried forward on the same
reasoning rather than on a new measurement.

**The full record is [`docs/evidence/plan_162_stage_G_evidence.md`](../evidence/plan_162_stage_G_evidence.md)**, 7 sections:

1. The classification needed a third step the design did not name
2. Three claims in the design section above were wrong
3. The coupling finding ran the other way a second time
4. Three repairs found on the way
5. What the contract gained
6. The CI zones compose, which was a second pass
7. What was deliberately not done

### Stage H — route coverage and `container_health`'s test home

**Legacy:** Stage 6 · **Issue:** CAR-50 · **Closed:** 2026-09-01

**Cost:** estimate 2 points. `In Progress` ran 13:15–15:17 UTC on 2026-09-01, 2h01m.

`ROUTE_WAIVERS` is `()`. G6 and G9 are deleted from the gap list. All twelve
routes are reached through their app's routing table by a test that asserts a
status code, `container_health` has both a `tests/container_health/` and a
Layer 4 suite, and the two misfiled unit tests are in the former.

**The full record is [`docs/evidence/plan_162_stage_H_evidence.md`](../evidence/plan_162_stage_H_evidence.md)**, 7 sections:

1. Five of the twelve were never uncovered
2. `container_health` had nowhere to put a `TestClient`, which is why G6 and G9 were one stage
3. The Layer 4 suite has no database, and the substitute is a recording
4. What the recording cannot see, and who owns that
5. What was deliberately not done
6. What CI said, and what only CI could have said
7. Three times the same mistake: citing a precedent and copying half of it

### Stage J — mechanising the encoding-sensitive I/O guard

**Legacy:** Stage 6b · **Issue:** CAR-60 · **Closed:** 2026-09-01

**Cost:** estimate 1 point. `In Progress` ran 16:37–18:08 UTC on 2026-09-01, 1h31m.

The stage was filed to close G13's *class* rather than repair another instance
of it, and it was allowed to conclude that no mechanism was worth building. It
did not conclude that. A mechanism exists, it fails on the exact call that broke
master, and the residue it cannot reach is now four named behaviours rather than
an open-ended exception.

**The full record is [`docs/evidence/plan_162_stage_J_evidence.md`](../evidence/plan_162_stage_J_evidence.md)**, 8 sections:

1. The measurement that decided the design
2. The class was dormant, not live, and that changed the cost argument
3. Why the rule is a test and not a ruff setting
4. The runtime check that found them, and why it is not in CI
5. The exit criterion, demonstrated rather than asserted
6. What was swept, and why the sweep is safe rather than merely large
7. What CI said, and what only CI could have said
8. What was deliberately not done

### Stage L — SQL execution from both directions

**Legacy:** Stage 7 · **Issue:** CAR-51 · **Closed:** 2026-09-01

**G14 is closed and `LAYER_2_WAIVERS` is `()`.** With it, the whole of the
original waiver list: the plan's own arithmetic was 4 + 50 + 12 + 56 = 122, and
G14's 56 was the last column standing. Every waiver that remains is one this
stage found.

| Ledger | Start | End |
|---|---|---|
| `LAYER_2_WAIVERS` (G14) | 56 | **0** |
| `INLINE_SQL_WAIVERS` (G5) | rule did not exist | 15 |
| `SQL_LITERAL_WAIVERS` (G15) | gap did not exist | 19 |
| `DUPLICATE_SQL_WAIVERS` (G17) | gap did not exist | 1, waived with a reason |
| production `.sql` files | 76 | 141 |
| production `.py` scanned for SQL | ~100, across 8 packages | 156 |
| Layer 2 tests | 129 | **242, all executed in CI** |

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

**The full record is [`docs/evidence/plan_162_stage_L_evidence.md`](../evidence/plan_162_stage_L_evidence.md)**, 10 sections:

1. The finding that matters most: two production defects only execution found
2. Three gaps this stage opened
3. A merge that would have broken deploys silently
4. The scan surface was the third instance of one mistake
5. 18 files were never uncovered, and the ruler was the problem
6. One file left the census, under G16's rule
7. A guard the instrument itself needed
8. What was deliberately not done
9. The deploy, and the failure it was watched for
10. Cost

### Stage K — a service that pauses no surface can be deployed alone

**Legacy:** Stage 6c · **Issue:** CAR-66 · **Closed:** 2026-09-02

**The defect is closed in production.** `V050` applied 2026-09-02, and `bash
scripts/redeploy.sh dashboard` — the exact command that returned 503
`{"detail":"Database unavailable."}` on 2026-09-01 — now completes end to end.

| Deploy | Drain | Healthy | Exit |
|---|---|---|---|
| `ops` | 5s | 6s | 0 |
| `dashboard` alone | **0s** | 6s | 0 |
| `archiver pack-worker processing scraper dbt_runner` | 1s | 8s | 0 |

**"Drain confirmed after 0s" is this stage's own prediction, observed.** V050's
comment argued that an empty scope is a true statement rather than a missing
one, because `required_drain_sources(frozenset())` is empty and every source
reports not-applicable. The `dashboard` deploy drained in zero seconds where
every scoped deploy above it took one to five. The readers already agreed; only
the constraint did not.

**The constraint keeps the invariant worth keeping.** Verified against
production `pg_constraint` after the migration: the `scope <> '[]'` clause is
gone and `targets <> '[]'` remains, so an active record must still name what it
coordinates. Coordination advanced generation 59 → 65 across the three deploys
and returned to `phase='none'` after each.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

#### The rollout found a gap in the deploy service list, of this stage's own kind

`shared/db.py` changed, and every service that bakes it needed rebuilding. The
recorded list was archiver, pack-worker, snapshot-worker, processing,
april-processor, scraper and ops. Measured against the tree, it was wrong in
both directions:

- **`dbt_runner` bakes `shared/` and was absent from the list** —
  `dbt_runner/Dockerfile:19` copies it to `/usr/app/shared/`, and the service
  is `restart: unless-stopped`. It would have kept the old module indefinitely.
- **`snapshot-worker` and `april-processor` are `profiles:`-gated** and were
  not running, so they are not deploy targets at all; they load new files on
  their next invocation, as Stage L recorded for `docker compose run --rm`.

`container_health` copies only its own package and `lakehouse` is not a Compose
service, so neither is affected. **This is Stage K's defect one layer out** —
a contract (which images bake `shared/`) and its consumer (the list an operator
types) with nothing composing them, and the same failure mode: the list looked
right and was never asserted against the tree.

#### Cost

Estimate 1 point, actual 1. The stage was sized before the diagnosis was
written down and still landed on its estimate, which is worth recording as
plainly as an overrun would be: the expensive half was already spent finding
the cause on 2026-09-01, and what remained — one migration, two exception
paths, one shell function and the assertion — was the cheap half. **The
unmasking cost almost nothing and is the part that pays later**, since the next
unrelated failure on this path will name itself.

### Stage M — the assertionless suite and the scraper's write path

**Legacy:** Stage 8 · **Issue:** CAR-52 · **Closed:** 2026-09-02

**G7 and G8 are both closed**, and the gap list is down to seven rows. Confirmed
in [run 33665172964](https://github.com/whitewalls86/new_car_tracker/actions/runs/33665172964)
on PR #347, all jobs green.

| Ledger | Start | End |
|---|---|---|
| Assertions in `test_dashboard_queries.py` | **0** | 26 tests, all asserting |
| Assertionless tests under `tests/integration/sql/` | **29** | **0** |
| Layer 2 tests executed in CI | 242 | **244** |
| `scraper` Layer 4 files | 1, Layer 2-shaped | **2, both unmocked** |
| Mutations the harness actually runs | 7 of 24 | **24 of 24** |
| Unit coverage | 78% | **78.68%** against a floor of 75 |

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

**The full record is [`docs/evidence/plan_162_stage_M_evidence.md`](../evidence/plan_162_stage_M_evidence.md)**, 9 sections:

1. The rule found four violations no reading of the suite would have
2. Writing the contract down found five dead columns, not three
3. G8 was not the file count
4. The pacing seam is keyed to the origin, and the direction was the decision
5. The fixture had to be page 1, and the code was right
6. The mutation harness had been aborting for two stages
7. Two production changes, and the deploy that carried them
8. Deployed 2026-09-02, and confirmed
9. Cost

### Stage N — the DAG tree's `.sql` convention

**Legacy:** Stage 9 · **Issue:** CAR-53 · **Closed:** 2026-09-02

**Cost:** estimate 2 points. `In Progress` ran 20:22 UTC on 2026-09-02 to 02:59 UTC on 2026-09-03, 6h36m.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work. Neither surface describes where SQL lives or what the
drain reads, no DAG was added — `dag_queries.py` builds none, which the dagbag
census confirms — and both still say "More than 3,000 tests run in CI", which
3,523 satisfies.

**Two of G12's three claims were already stale when the stage opened**, and
finding that out changed the work. The gap row said `airflow/dags/` "has no
`.sql` convention and cannot reach one" and that this "is what forces the
single legitimate `ast` reader, `_sensor_constant()`". Measured:

- `airflow/sql/` has existed since Stage L, holds two files, and is bind-mounted
  to `/opt/airflow/sql` beside `/opt/airflow/dags` in `x-airflow-common`. Both
  consumers already loaded from it.
- **`_sensor_constant()` does not exist.** Stage L deleted it when it moved
  `GATE_OBSERVATION_SQL` into `airflow/sql/record_gate_observation.sql`;
  `test_ops_queries.py` reads the file and says so in a comment. So the
  stage's second exit criterion was met by a sibling stage and needed
  recording, not repair.

This is the fourth time in this plan that a measure was fitted to the code in
front of it, and the first time the *gap row itself* was the thing out of date.
A row that names a function is falsifiable and this one had gone false, which
is the argument for measures over prose stated from the other side.

**The full record is [`docs/evidence/plan_162_stage_N_evidence.md`](../evidence/plan_162_stage_N_evidence.md)**, 13 sections:

1. What actually remained was one statement, invisible to three instruments
2. The exemption is from the loader clause, not the file rule
3. The name had to be `dag_queries`, and the suite proved it
4. Both guards were mutation-tested rather than assumed
5. The Layer 2 tests were run against a real Postgres, and three failed first
6. What Stage N did not do
7. Rules 5b and 5c became one rule, and that reverses a Stage L decision
8. The detour worth recording: an exemption that decided nothing
9. Not reinventing a wheel, and the check is recorded
10. What the change cost, and what was verified
11. The two builders were rewritten, not waived
12. Four things the rewrite broke, each worth naming
13. The Layer 2 census demanded the new files, immediately

### Stage P — dbt builds against production-shaped data

**Legacy:** Stage 10 · **Issue:** CAR-54 · **Closed:** 2026-09-04

**Cost:** estimate 2 points. `In Progress` ran 20:31 UTC on 2026-09-03 to 07:14 UTC on 2026-09-04, 10h43m.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work. Neither describes CI's job set or which snapshot it
reads, and both still say "More than 3,000 tests run in CI", which CI's
`3622 passed, 1 skipped` satisfies.

The one skip is `test_every_sha_a_recap_names_is_a_real_commit`, and it is
declared rather than incidental: it resolves recap SHAs against real git
history, `actions/checkout@v4` clones at depth 1, and the test detects the
shallow repository and skips. Its docstring predicts exactly this and locates
its value locally, in the run `plan-week` makes after writing a recap. Noted
because a bare count hides it, and because nothing mechanical holds it there —
`REQUIRE_LAYER_2_EXECUTION` fails a run on any skip in
`tests/integration/sql/`, but this is a Layer 0 test in the unit job and
outside that guard's reach. What stops one declared skip becoming three is the
docstring, which is the same shape as the gaps this plan has been closing and
is left open here deliberately: the fix is a general declared-skip rule, not
something Stage P should grow.

**The gate was shown failing on a production row, not asserted to.** The exit
demanded a demonstration because a green build proves the instrument runs and
says nothing about whether it can fail — the failure mode this plan is named
after.

Recipe, both runs on PR #358, job `dbt build against a production snapshot`,
against pinned snapshot `adaptive-refresh-2026-09-04-002234`:

- **Green** — run [33830401797](https://github.com/whitewalls86/new_car_tracker/actions/runs/33830401797).
  `Done. PASS=251 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=251`, covering 7
  incremental models, 12 table models, 4 views, 161 data tests and 66 unit
  tests in 9.95s. The seed reported `postgres_rows_by_table:
  {public.search_configs: 13, ops.tracked_models: 13}` with `postgres_skipped:
  []`, so all six sources were populated and `--require-non-empty` had
  something to check.
- **Red** — run [33830916950](https://github.com/whitewalls86/new_car_tracker/actions/runs/33830916950),
  identical but for one statement run against the seeded database between the
  seed and the build:

      UPDATE public.search_configs SET params = params - 'makes'
      WHERE search_key = (SELECT min(search_key) FROM public.search_configs);

  Result: `21 of 250 FAIL 1 not_null_stg_search_configs_make_slug`, "Got 1
  result, configured to fail if != 0", `Done. PASS=214 WARN=0 ERROR=1 SKIP=36
  NO-OP=0 TOTAL=251`. The 36 skips are downstream models declining to build on
  a failed ancestor.

**One statement, one failing test, one row.** The mutation was routed through
`public.search_configs` deliberately rather than through a Parquet source: a
violation dbt catches there also proves the two `postgres_scan()` sources are
load-bearing, since a snapshot without them builds this same project green over
an empty world. One run answers both questions.

**`dbt model tests (real build)` stayed green on the red run**, which is the
job-separation argument holding: the synthetic fixture in its reserved
`obs_year=2099` partition and the production snapshot in real partitions are two
datasets on two runners, and corrupting one did not reach the other.

**161 dbt data tests ran against 808,069 production silver rows and found
nothing** — no `unique` violation, no `not_null` violation, no cast failure, no
duplicate join key anywhere in the pinned cohort. That is a result, not an
absence of one: nothing had ever asked the question before.

Two limits on what the green half proves, both already recorded in the stage
above and neither retired by this run: the whole build took 9.95s because the
cohort is 5,127 VINs against production's 313,291, and a fresh DuckDB file takes
every incremental model's cold path where production builds incrementally.

### Stage U — every skip in CI is declared, or the run fails

**Legacy:** Stage 13 · **Issue:** CAR-81 · **Closed:** 2026-09-04

**Cost:** estimate 2 points on CAR-81, which was sized while it still carried
Stage V; the stage section sized U alone at 1. Actual 1.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

**This closes the loop [Stage P's entry](#stage-p--dbt-builds-against-production-shaped-data)
left open**, in the terms that entry set: it recorded one declared skip held in
place by a docstring, noted that `REQUIRE_LAYER_2_EXECUTION` could not reach it,
and said the fix was a general declared-skip rule rather than something Stage P
should grow. `tests/plugins/declared_skips.py` is that rule.

**The hook moved scope without changing shape.** The same
`pytest_terminal_summary` mechanism now runs repo-wide under a workflow-level
`REQUIRE_DECLARED_SKIPS`, failing an undeclared skip, a declared skip that was
selected and did not skip, and one that skipped for text its declared
`condition` does not match. It reports on green runs too, because a job where
the plugin failed to load is otherwise indistinguishable from one where it was
satisfied.

**Registered through `addopts`, not a conftest, and that is forced rather than
chosen.** `docs-tests` runs `pytest --noconftest` — it installs three packages
and cannot import `tests/conftest.py` — and it is one of the two jobs holding a
skip. `-p` loads through `--noconftest`; `pythonpath = ["."]` makes the module
importable at plugin-registration time, before the collection that would
otherwise put the repository root on `sys.path`.

**`REQUIRE_LAYER_2_EXECUTION` is retired, and Layer 2 was kept absolute by
deriving the rule rather than listing the path.** The general gate offers a door
the suite-scoped hook did not: a Layer 2 skip used to be unconditionally fatal,
and under a registry someone could make one legal in four lines.
`test_no_declared_skip_sits_at_a_layer_that_admits_none` nails that door shut
through `_layer_of`, which reads the contract's own headings — so a second Layer
2 root is strict the day the contract declares it. A path list would have been a
fresh instance of the enumeration
[Stage N](#stage-n--the-dag-trees-sql-convention) deleted.

**Nothing had guarded the variable it replaces.** `REQUIRE_LAYER_2_EXECUTION`
was one line of YAML, and deleting it would have restored the blind spot with no
test failing — still true of `REQUIRE_DUCKDB`, `REQUIRE_MINIO` and
`REQUIRE_AIRFLOW_SCHEMA`, which the new gate check now covers as a side effect.
Four checks stand behind the hook: every entry names a test that exists, none
sits at a layer admitting none, the gate and the `-p` registration both survive,
and `DECLARED_SKIP_CEILING` makes a third declaration move a number rather than
append to a tuple. All four are in
[`scripts/verify_testing_contract_mutations.py`](../../scripts/verify_testing_contract_mutations.py),
28/28 caught.

**Demonstrated across three runs on PR #374, not asserted:**

- **Green** — [33914747213](https://github.com/whitewalls86/new_car_tracker/actions/runs/33914747213)
  (`03553fb`). The hook reported in 9 pytest steps across 4 jobs. Both
  declarations accepted for the condition each names — the recap skip in
  `Unit tests (pytest)`, the dictionary skip in `Lake integration tests (MinIO)`.
- **Red** — [33915286492](https://github.com/whitewalls86/new_car_tracker/actions/runs/33915286492)
  (`fcec5ac`), one deliberately undeclared skip in
  `tests/test_stage_u_demonstration.py`. `Unit tests (pytest)` failed alone;
  eleven jobs stayed green; the summary read `3767 passed, 2 skipped, 662
  deselected` with no `FAILED` line, and the step exited 1 on the `Declared
  skips` section alone. **A skip is not a failure, which is why nothing noticed
  before this stage.** The same section carried the refusal and an acceptance
  together.
- **Green again** — [33915566312](https://github.com/whitewalls86/new_car_tracker/actions/runs/33915566312)
  (`7b8ea40`), the file deleted. Deleting rather than declaring is the
  registry's intended move: fixing the cause is the default and a declaration is
  the exception that has to be argued for.

**The drift direction was demonstrated too, locally rather than in CI.**
`PYTHONPATH=. REQUIRE_DECLARED_SKIPS=1 pytest --noconftest tests/test_planning_docs.py -q`
on a full clone reports `1 declared skip(s) ran instead of skipping … (declared
2026-09-04: shallow clone)` and exits 1 with 52 tests passing. CI cannot
demonstrate it — every job clones at depth 1, so the condition is always true
there — and this is the same invocation shape `docs-tests` uses, so it doubles
as proof the plugin loads under `--noconftest`.

**One job has still never run under the gate.**
[`scripts/ci_change_scope.py`](../../scripts/ci_change_scope.py) makes
`docs_tests` true only for a docs-only changeset, and all three runs were code
changes, so `docs-tests` was skipped in each. The commit carrying this record
entry is docs-only and is therefore the run that exercises it. That is why the
plugin prints its accepted declarations rather than staying silent: for that
job, the evidence is a green log rather than a watched failure.

### Stage X — a test may not author SQL either, and what ran against which engine

**Legacy:** Stage 16 · **Issue:** CAR-83 · **Closed:** 2026-09-06

**Cost:** estimate 1 point, actual 2. CAR-83 recorded before the work that its
estimate predated the recorder and was owed a revisit it never had; the
recorder, the aggregation gate and `SqlText` were all invented here.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work.

**Every production statement in this repository executes against a real engine
in CI**, on the strongest available reading — not that a test names the file,
but that the file's text reached a database client. No waiver list: one landed
with the gate and was deleted rather than kept empty, because an empty ledger
and no ledger differ in exactly what the next statement that executes nowhere
costs to repair.

| | Start | End |
|---|---|---|
| SQL literals under `tests/` | 506 | **0** |
| Statements under `tests/sql/` | 0 | 381 |
| Production `.sql` files | 161 | 163 |
| …recorded executing in CI | never measured | **163 of 163** |
| `TEST_SQL_TEMPLATE_WAIVERS` (G19) | gap did not exist | 1 |
| `DBT_CONTRACT_WAIVERS` (G20) | gap did not exist | 23, seeded full for Stage S |
| `INLINE_SQL_WAIVERS` (G5) | 15 | 14 |
| Judgement rules in the contract | 4 | **3** |

**The aggregation was not in this exit and landed anyway.** The exit placed it
"with or after Stage Q"; it is here, so Stage Q inherits less than its section
claims — worth knowing before that stage is scoped.

**Three readings below 161 were the instrument, not the repository, and the gate
found all three** — CI discarding its own execution record, two loaders
returning a plain `str`, and fourteen archiver selectors reading as dead while
running nested inside `wrap_candidate_query.sql`. The last was fixed in the type
rather than in the gate.

**The full record is
[`plan_162_stage_X_evidence.md`](../evidence/plan_162_stage_X_evidence.md)**, 9 sections:

1. Where test SQL went, and the provenance decision taken at the top
2. The aggregation this exit had deferred, and what Stage Q inherits now
3. Three instrument defects the gate found, and two holes under the denominator
4. A count the stage was scoped by was already wrong
5. G19 drained 25 → 1 — seven never templates, seventeen the call site states
6. One waiver that was prose, and why the predicate was left alone
7. Plan 129's statements, and the obligation that forced a testability seam
8. An authoring gap seen from outside, and the skill that answers it
9. A failure this stage caused, and the guard that fixes it

Its two companions stay as written:
[the origin](../evidence/plan_162_stage_X_origin_2026-09-04.md), with its
2026-09-06 correction, and [the recorder
baseline](../evidence/plan_162_stage_X_recorder_baseline_2026-09-05.md), with
the open contract-drift findings.
