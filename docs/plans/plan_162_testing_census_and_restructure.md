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

**Three times, now.** Read again on 2026-09-07 while scoping the contract stages
below, by the same method: **the live total is 25**, and the table above is
missing two tuples entirely. `TEST_SQL_TEMPLATE_WAIVERS` (5) arrived with
Stage X and `DBT_CONTRACT_WAIVERS` arrived with Stage S, was seeded at 23 and
drained to 0 in two days. The live split is `INLINE_SQL_WAIVERS` 13,
`SQL_LITERAL_WAIVERS` 6, `TEST_SQL_TEMPLATE_WAIVERS` 5, `DUPLICATE_SQL_WAIVERS`
1, everything else empty.

The third instance is not the same error as the first two. Those were a number
that had moved; this is a *shape* that had moved — new rules brought new tuples,
and a hand-written table cannot know about a tuple nobody told it about. The
repair is the same either way and it is the one this plan keeps arriving at:
read the tuples, do not read the paragraph.

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
| 17 | [**S**](#stage-s-answers-a-question-plan-161-did-not-ask) | 11 | Branch coverage for the dbt models, and what leaves the SQL census | G16 | `done` | CAR-79 |
| 18 | [**T**](#stage-t-exists-because-this-plan-grew-the-suite) | 12 | Shared fixtures: what the suite duplicates at 3,988 tests | — | `done` | CAR-80 |
| 19 | [**W**](#stage-w-a-test-may-not-supply-both-halves-of-a-contract) | 15 | A test may not supply both halves of a contract | — | `done` | CAR-82 |
| 20 | [**V**](#stage-v-a-variable-the-environment-documents-reaches-the-service-that-reads-it) | 14 | A variable the environment documents reaches the service that reads it | — | `done` | CAR-88 |
| 21 | [**Y**](#stage-y-grew-its-rule-passes-a-route-that-reports-work-it-did-not-do) | — | A route declares its statuses, observes its own effects, and exercises both | G21, G27, G28 | `done` | CAR-104 |
| 22 | [**AF**](#stage-af-the-harness-that-proves-the-rules-is-proved-by-nothing) | — | The harness that proves the rules is proved by nothing | G29 | `done` | CAR-114 |
| 23 | [**Q**](#stage-q-cis-services-are-productions-in-definition-and-in-contents) | 10b | CI's services are production's, in definition and in contents | — | `next` | CAR-78 |
| 24 | [**AC**](#stage-ac-the-database-makes-a-stale-read-loud) | — | The database makes a stale read loud | G25 | `—` | CAR-105 |
| 25 | [**AB**](#stage-ab-what-we-do-not-own-is-recorded-and-replayed) | — | What we do not own is recorded and replayed | G24 | `—` | CAR-106 |
| 26 | [**Z**](#stage-z-the-contract-is-generated-committed-and-gated) | — | The contract is generated, committed and gated | G22 | `—` | CAR-107 |
| 27 | [**AA**](#stage-aa-a-test-may-not-invent-another-services-response) | — | A test may not invent another service's response | G23 | `—` | CAR-107 |
| 28 | [**AE**](#stage-ae-configuration-is-what-compose-delivers-and-everything-else-is-a-constant) | — | Configuration is what Compose delivers, and everything else is a constant | — | `—` | CAR-109 |
| 29 | [**AD**](#stage-ad-a-fixture-cannot-fabricate-a-row-the-database-would-reject) | — | A fixture cannot fabricate a row the database would reject | G26 | `—` | CAR-108 |
| 30 | [**R**](#stage-r-ci-selection-and-the-instrument-that-has-to-precede-it) | 10c | CI selection, and the instrument that has to precede it | Plan 139 Stage E | `—` | CAR-87 |
| 31 | [**AG**](#stage-ag-rules-live-in-a-directory-and-an-unregistered-one-cannot-exist) | — | Rules live in a directory, and an unregistered one cannot exist | G30 | `—` | CAR-115 |
| 32 | [**AH**](#stage-ah-every-rule-has-a-skill-that-helps-an-agent-obey-it) | — | Every rule has a skill that helps an agent obey it | G31 | `—` | CAR-116 |

`State` takes the five values [the plan-document
contract](../PLAN_DOCUMENT.md#stages-and-order) defines — `—`, `next`,
`blocked`, `done`, `canceled` — and exactly one stage carries `next`.

**The 2026-09-08 ordering, and what fixes it.** Six stages entered the table at
once and three constraints decided where, only one of which is a preference.

**Y before Z before AA is a hard chain.** Z generates a contract artifact from
each service's schema; run today it would faithfully record the false claim that
every endpoint returns `200` or `422`, because that is all any route declares.
AA needs Z's artifact as the place a caller goes to look. Nothing about that
order is negotiable.

**Q before AC is an argument rather than a block.** AC is a schema migration and
CI's Postgres is a fourth hand-maintained transcription of production's —
Stage Q's whole subject. Landing a migration against a database defined the way
production defines it is worth one stage of delay.

**AC is not urgent in the way its gap entry reads.** [G25](../TESTING.md#the-gap-list)
is a latent hole, not an actively failing one: it costs the next time somebody
renames a constrained value, and nothing is renaming one today.
[Stage W](#stage-w-a-test-may-not-supply-both-halves-of-a-contract) also
partially mitigates it, because a rename must now pass through
`shared/db_vocabularies.py`, where the coupling is at least visible. So it sits
after Y and Q rather than first, and the risk of that choice is stated here
rather than discovered later.

**V is early because it is the same class as U and W** — a declaration held by
prose that nothing enforces — and finishing that thread before opening the
contract programme keeps the plan legible.

**R stays last, and its first piece may have evaporated.** [The CI cost
census](../evidence/plan_162_stage_R_ci_cost_census_2026-09-04.md) moved it to
the end and cut its selector; what remained was an instrument fix *"reduced to
whatever Stage U has not already supplied"*. Stage U has since shipped. Whether
anything is left is the first question that stage asks, not an assumption this
table should make for it.

**The six new stages carry no `Legacy`.** They were not in the 2026-09-04
renumbering. The `Issue` cells were empty when this table was written and are
filled as the issue set lands — grouped by
[`plan-start`](../../.claude/skills/plan-start/SKILL.md)'''s rule, one issue per
deploy-requiring stage and one per bundled run of locally-verified ones.

*This paragraph first claimed the issues are created only when a stage starts,
citing `ticket-now` for it. **That skill says no such thing and the claim is
backwards** — `plan-start` creates a plan'''s whole issue set up front, in
`Backlog` with no cycle, and `fill-cycle` seeds them into one later. A
fabricated citation inside the plan whose subject is documents that drift from
their mechanisms, left on the record rather than quietly deleted.*
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

**Legacy:** Stage 11 · **Issue:** CAR-79 · **State:** `done`

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

Set against branch counts the ranking inverts. Scoping counted branch points
with a regex proxy — not a parse, and expected to be low by construction — and
put `int_listing_volatility_features` at ~48 against 3 unit tests,
`int_listing_observation_fingerprints` at ~37 against 5, and `mart_deal_scores`
at ~33 against 4. **Re-counted 2026-09-06 from a real parse of dbt's compiled
SQL: 46, 33 and 34 respectively, against a total of 216 branch points across
the 23 models on a cold compile, and 311 once both compile phases are
counted.** The proxy was close and its ranking was right, which is worth
recording because it is the rarer outcome in this plan — three of the four
other numbers this stage was scoped by did not survive measurement.

**A fifth did not either, and it was this section's own.** Scoping put the
total at 250 by counting with a regex; the enumerator says 216. The proxy had
counted every `filter (where ...)` twice, once as an aggregate filter and again
as a `where` conjunct. It is recorded here rather than quietly corrected
because the error is this plan's recurring subject, committed by this plan's
own author, and caught by the instrument the stage was building. **The models
holding the most logic are the least proportionally covered, and every
instrument in this repository reports them as covered.**

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

**The column contract arrived here from
[Stage X](#stage-x-a-test-may-not-author-sql-either) on 2026-09-05**, travelling
the other way. X needed a trustworthy declaration of each model's shape — to
stop a test inventing one — and found there is none. `schema.yml` is
documentation: nothing makes it agree with the model it describes, and no column
in it carries a type. The drift is already in the tree, in three fixtures that
hand-declare stand-ins for real models and have diverged from them —
`int_listing_state_fingerprints` declared 5 columns against the model's 8,
`int_listing_state_runs` 1 against 11, `int_listing_observation_fingerprints` 1
against 10. Nothing noticed, because nothing was comparing them.
[`int_latest_observation.sql`](../../dbt/models/intermediate/int_latest_observation.sql)
already records the production half of the same defect in its own prose: *"a
column added to stg_observations must be added here too, or it silently stops
appearing downstream. Nothing currently catches that drift automatically … this
model's schema file documents only vin17/source/make, not the full column list,
so it is not a backstop."* X landed the ledger that makes this visible — G20,
seeded at 23, one waiver per model — and the name-only half of the fixture rule;
**the retype half is recorded there as a stated limit and is struck when this
stage closes.**

**The scoping counts were wrong in three places, and re-measuring them is the
first thing this stage did.** CAR-79 named six models with partial column lists
and put the shortfall at ~101 columns. Measured against each model's final
`SELECT` on 2026-09-06:

| Model | Declared | Emitted | Undocumented |
|---|---:|---:|---:|
| `mart_deal_scores` | 4 | 39 | **35** |
| `int_latest_observation` | 3 | 33 | 30 |
| `stg_observations` | 6 | 33 | 27 |
| `mart_vehicle_snapshot` | 5 | 29 | 24 |
| `stg_price_events` | 6 | 10 | 4 |
| | | | **120** |

**Five models, not six, and the largest gap was not on the list.**
`int_listing_volatility_features` was cited at 27/31 and `mart_block_rate` at
6/8; both document every column they emit. `mart_deal_scores` — 4 declared
against 39 emitted, because its final `SELECT` is `select *` over a CTE that
projects 38 — was cited nowhere, and it is the worst case in the project. So 18
of 23 models are complete rather than 17, the shortfall is **120 columns rather
than ~101**, and the completed declaration is **307 columns rather than 187**,
which is the denominator the type contract below actually has to fill. This is
the third time in this plan that a stage's scoping number was wrong in the
direction that made the stage look smaller, and the second time in two stages —
see [Stage X](#stage-x-a-test-may-not-author-sql-either), whose evidence records
the same thing under §4.

#### The three lists answer three questions, not one

**Amended 2026-09-07, against a measurement taken after the enumerator
existed.** This section previously treated the three lists as three ways of
doing one job, and asked which branches each of them covered. With the branch
list in hand that question could finally be asked of the data, and the answer
says the framing was wrong. Of 308 measurable branch points:

| | branches |
|---|---:|
| Covered by a dbt unit test | **148** |
| Covered by the fixture-driven real build | 95 |
| Union | 200 |
| **Only** the fixture reaches | **22** |
| **Only** a unit test reaches | 75 |
| Both reach | 73 |
| Neither reaches | 108 |

**Unit tests are already the stronger branch instrument by half again**, and
73 branches are covered twice over. But the 22 the fixture alone reaches are
not a random remainder — they are almost exactly the surface a unit test
*cannot* express:

- **12 are phase-tagged**, `@full` or `@incremental` — and this bullet said
  something false when it was written, which is worth leaving visible. It read
  *"a dbt unit test never materializes a relation, so `is_incremental()`
  machinery is structurally out of its reach — no quantity of unit testing gets
  these."* **Corrected 2026-09-07: a unit test reaches them.** It needs
  `overrides: {macros: {is_incremental: true}}` and a mocked `- input: this`,
  and on a clean warehouse that fails — dbt runs a model's unit tests *before*
  materializing it, so the test needs a relation the test is blocking. That
  reads as a deadlock and is not one: dbt's documentation prescribes
  `dbt run --empty` first, which creates the relations without running tests.
  With that step a clean warehouse builds `PASS=282 ERROR=0` and all 19 of the
  incremental-only branch points are unit-tested. **The claim was inferred from
  a failure rather than from the documentation, and it was inferred twice —
  once by an agent, once by the author checking it.** It is corrected here
  rather than deleted because the shape of the error is the plan's own subject:
  a measurement taken without a precondition, read as a property of the world.
- **2 are `stg_observations`' `vin17` guard**, which is this section's own
  headline anecdote arriving from the other direction: the model with zero unit
  tests, whose reject paths run on every build because the fixture seeds
  `ARTIFACT_NULL_VIN` and `ARTIFACT_SHORT_VIN` deliberately.
- **4 are cooldown bucket boundaries** in `mart_cooldown_cohorts` and
  `mart_cooldown_event_funnel`, mapping one-to-one onto the selector registry's
  `cooldown_bucket_3_4`, `_5_10` and `_11_plus`. The branch-first snapshot
  design, seen from the far end.
- **4 are deep CTE predicates** in `int_listing_volatility_features`, several
  joins past anything a mocked `ref()` reaches.

**So the lists are not redundant, they are differently shaped, and grading them
all on one number was the error.** The work divides:

| List | Owes |
|---|---|
| `dbt/models/*/unit_tests.yml` | **Branch exhaustiveness — 100%, both arms, every branch**, including ones another instrument already reaches |
| the fixture-driven build | **Non-vacuity**: every model materializes rows, cold and warm |
| the snapshot selectors | **Relevance**: which branches production actually reaches |

**Unit tests take exhaustiveness because they are the only list that can
construct a state production has never produced.** A selector finds rows; it
cannot find the absence of rows, and it cannot find a state the business has
never entered. Keying coverage to production data would make the branch list
hostage to whatever production happens to contain.

**The fixture is released from branch coverage entirely, and that is what makes
it maintainable.** Its data stops being pinned in place by an obligation to
reach particular branches, and answers one question instead: does a build over
this data produce a world where every model is actually populated? Nothing else
can ask that. No unit test would ever have found that five of the 23 models
build to zero rows — see [the empty
models](#five-models-build-over-an-empty-world) — and a production-shaped
snapshot would not either, because production has the rows that fixture lacks.

**And the third question inverts.** A branch production never takes is not a
coverage gap somebody must close by inventing data. It is a finding: dead code,
or a state never yet seen. Nothing in this repository currently answers it.

#### Five models build over an empty world

**Measured 2026-09-06.** `int_active_make_models`, `int_benchmarks`,
`mart_deal_scores`, `mart_price_freshness_trend` and `mart_vehicle_snapshot`
materialize **zero rows** against the fixture, and the build reports success.
Their data tests pass vacuously — `not_null` over an empty relation is
trivially true — so roughly thirty declared constraints currently assert
against nothing at all.

The cause is one line of provenance: dbt has six sources, two of which are
Postgres tables read through `postgres_scan`, and
[`scripts/seed_lake_snapshot_fixture.py`](../../scripts/seed_lake_snapshot_fixture.py)
seeds **MinIO only**. `ops.tracked_models` is written by the processing service
at runtime and by nothing in the dbt path, so it is empty;
`int_active_make_models` inner-joins it and yields nothing; `mart_vehicle_snapshot`
inner-joins that; `mart_deal_scores` and `mart_price_freshness_trend` read the
mart. `int_benchmarks` is empty for an unrelated reason — its join between
`int_latest_observation` and `int_price_history` survives no rows under the
`current_price > 0` filter.

**The instrument for this already exists and is pointed at the other job.**
`--require-non-empty` names this exact cascade in its own CI comment — *"left
empty, `stg_search_configs` reads nothing, `int_active_make_models` inner-joins
to nothing, and `mart_vehicle_snapshot` builds green over an empty world"* — but
it runs in `snapshot-dbt`, against the production snapshot, and guards
*sources*. The fixture build in `dbt-models` has no such gate, and no gate at
all on *models*.

**There is no waiver list, and the case against one is a live defect rather
than a principle.** `mart_vehicle_snapshot.sql:35` reads
`case when ph.last_seen_at >= {{ now_ts() }} - interval '7 days' then 'active'
else 'unlisted' end`, and the fixture's timestamps are absolute — `2026-07-26`
and neighbours. That arm was covered when it was written and has been dead for
weeks, because wall-clock time moved past the fixture and nothing was watching.
A waiver list is precisely where that would have been absorbed: a red gate
nobody can explain, a line reading "legitimately empty in the fixture", and rot
recorded instead of repaired. The cost of refusing one is that this gate will
one day fail for a reason no commit caused; the answer to that is to anchor the
fixture's dates relative to `now()`, which is work this stage owes and a waiver
would have hidden.

#### Six decisions taken while scoping this stage, 2026-09-06

Each was settled against a measurement taken first, in a throwaway dbt project
running the pinned CI versions (`dbt-core==1.10.20`, `dbt-duckdb==1.10.1`).

**1. The branch list is parsed, not matched, and its identity is positional.**
`sqlglot` on the duckdb dialect parses **184 of 184** files under
`target/compiled/` — all 23 models and all 161 data tests — with no failures,
`arg_max`, `filter (where …)`, `qualify`, `::numeric` and windows included.
Compiled rather than raw, because `regex_matches()`, `parquet_source()` and
`datediff_days()` sit inside the expressions the enumerator has to see, and a
Jinja stub that rendered one of them branchless would undercount without
failing. A branch is keyed `model.<output column>.<kind>.<ordinal>`; the
predicate text rides along as a fingerprint the reconciliation prints for
review but never keys on, because a text key silently detaches every claim on
the next edit. The accepted cost is that reordering `CASE` arms transfers a
claim between them — which changes semantics anyway and so is already review's
business.

**2. Coverage is observed, not claimed.** The alternative was nominal: each of
the three lists declares branch ids and the gate checks both directions. That
relocates the hand-curation this stage exists to close, and a typed claim can
be wrong forever without failing. It was rejected once
`target/compiled/…/unit_tests.yml/` turned out to hold **the model's own SQL
with each `ref()` replaced by a `__dbt__cte__` CTE of the `given` rows** — a
unit test is the model with fixed inputs, so one probe mechanism reaches all
three lists. A probe is `count(*) filter (where <predicate>)` and its negation,
evaluated in the branch's own scope; a branch is covered when both arms come
back non-zero. Demonstrated on `stg_dealers`, where
`test_dealers_most_recent_attributes_win` takes only the true arm and
`test_dealers_null_customer_id_excluded` takes both. Two limits are accepted:
branches inside windows, `qualify` or aggregate arguments have no row-level
scope to attach to and are recorded unprobeable with the reason, and unit-test
probes must ride in the `dbt-models` job rather than a bare compile, because
`get_fixture_sql` reads the real relation's columns and errors without it.

**3. Every branch counts the same.** 73 of the 216 a cold compile yields are
`coalesce` fallbacks, and 48 of those are `coalesce(field, '')` field
normalizations inside the two fingerprint concats, which raised the option of filtering them out or
weighting them by kind. Both were rejected: a filter is a judgement that
shrinks the denominator, which is the defect this plan has now found in four
separate instruments, and a field that is never null in the fixture is a field
the fingerprint has never been shown to distinguish on. The obligation is both
arms of every one of them.

**4. The constraint gate mutates only inside the model that declares the
constraint.** Mutating across models and rebuilding the downstream subtree was
considered and dropped. `not_null_mart_vehicle_snapshot_vin` cannot be broken
from inside `mart_vehicle_snapshot` — the guard is `where vin17 is not null` at
[`int_latest_observation.sql:39`](../../dbt/models/intermediate/int_latest_observation.sql),
and the mart takes it as its driving table. Calling that constraint decorative
is **correct, not a false verdict**: it restates an invariant established
upstream, where `not_null_int_latest_observation_vin17` sits and is locally
load-bearing. Generalised: a propagated constraint is either mirrored upstream,
where local mutation finds it, or it is not — and then the finding is that the
model establishing the invariant fails to declare it, which is the more useful
one and comes free. So two classes, not three, with a decorative verdict
carrying the upstream guard's branch id as its reason. The stated limit is that
mutation measures the code as it stands and cannot tell "decorative because
redundant" from "decorative but a useful regression barrier" — which is why the
exit records these rather than deleting them.

**5. G16 gets a manifest, not a count.** A high-water count plus the absorption
ledger was the cheaper design and is rejected on a hole in exactly the thing
being asserted: delete `foo.sql` and add `bar.sql` in one commit and the count
never moves, so the departure goes unrecorded. The objection to the manifest
was churn, and the churn was measured — production `.sql` add/delete events run
2, 6 and 6 in non-sweep months against 112, 28 and 95 in the three months that
were Plan 120's selector extraction and this plan's own Stages L and X. **About
five lines a month, from sweeps that are now finished.** That does not buy a
correctness hole in a rule whose whole subject is silent departure. No
`--update` flag: a manifest that regenerates itself is a rubber stamp, and the
diff someone reads is the entire mechanism. `SQL_ABSORBED_BY_DBT` stays out of
`ALL_WAIVERS` — it is permanent record rather than a draining queue, and
[`test_no_waiver_outlives_the_plan_that_owns_it`](../../tests/test_testing_contract.py)
would turn the whole ledger red the day this plan archives.

**6. The column contract is derived from a build, and its Spark half leaves.**
Names and types both come from `DESCRIBE` against the built relations, so
exits 6 and 7 close in one operation and nobody hand-transcribes a column list
— the failure that produced the three stale fixtures Stage X found. Contracts
were confirmed enforced on `table` and on `view` (4 of the 23 are views), with
a precise diagnostic naming the column, both types and the mismatch reason, and
`string` was confirmed accepted by dbt-duckdb and normalized to `VARCHAR`. The
spellings are DuckDB's; both Spark questions this stage uncovered are
[Plan 125](plan_125_duckdb_to_iceberg_migration.md)'s and are recorded there.

**Exit.**

1. **The branch list is derived from the model SQL**, not maintained. A model
   that gains a branch nothing exercises fails the suite. Demonstrated by adding
   one, not asserted.
2. **Every branch is exercised in both directions by a dbt unit test** — all of
   them, including branches some other instrument already reaches. See
   [the division of labour](#the-three-lists-answer-three-questions-not-one)
   for why duplication is the point rather than waste.
3. **The fixture's obligation is non-vacuity, and it is the whole of the
   fixture's obligation.** Every model materializes at least one row, on a cold
   build and on an incremental one. No waiver list: an empty model is a defect
   in the fixture, never a fact to be recorded.
4. **Every declared column constraint is shown to be load-bearing** — removing
   the guard that produces it fails its test. A constraint no mutation can
   break is recorded as decorative rather than left standing as coverage.
5. **G16 is asserted.** `production_sql_files()` may shrink only when the change
   names the dbt model that absorbed the statement; a silent shrink fails.
   Demonstrated by a silent shrink failing, not asserted.
6. **The non-empty gate derives its source list from `sources.yml`**, so a
   source added to the dbt project cannot go unchecked.
7. **`schema.yml` is complete.** The 5 partial column lists are filled — 120
   columns — so 23 of 23 models document every column their final `SELECT`
   emits.
8. **Every column carries a `data_type`, under `contract: {enforced: true}`** —
   0 of 187 do today, and 307 will be declared once exit 6 lands — so dbt fails
   the build when a model's output stops matching its declaration. Spellings
   valid on both engines per [Plan 125's
   audit](../reference/plan_125_portability_audit.md): `varchar` is a hard Spark
   parse error, `string` is DuckDB's alias and Spark's native name, *"verified
   on both"*. A model that cannot carry an enforced contract has the reason
   recorded rather than being skipped.
9. **G20's waiver ledger is empty**, ratcheting down from the 23 Stage X seeded,
   one per model.

### Stage T exists because this plan grew the suite

**Legacy:** Stage 12 · **Issue:** CAR-80 · **State:** `done`

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

**Legacy:** Stage 14 · **Issue:** CAR-88 · **State:** `done`

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

**Legacy:** Stage 15 · **Issue:** CAR-82 · **State:** `done`

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

**Legacy:** Stage 16 · **Issue:** CAR-83 · **State:** `done`

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

### The service data contracts, Stages Y to AD

**Added 2026-09-07, from the conversation that closed Stage W.** Six stages,
one subject, and the subject is the one Stage W turned out to be the first
instance of: **a test can only avoid authoring both halves of a contract if
there is a place it must go to find out what its options are.**
`shared/db_vocabularies.py` is that place for the words the database owns.
Nothing is that place for anything else.

**Every measurement below was taken on 2026-09-07** and none of it is recorded
anywhere else; the numbers are the reason these are six stages rather than one
sentence in Stage W's record.

**Each seeds its own waiver tuple and drains it**, which is Stage S's pattern —
`DBT_CONTRACT_WAIVERS` was seeded fully waived at 23 of 23 on 2026-09-05 and
drained to 0 on 2026-09-07. Stage W did not do this: it repaired all 33 of its
sites outright and so contributed nothing to the plan's progress meter, which
is the only reason the completion criterion looked unable to describe this
work. It can. The stages below use it.

**The lettering runs past Z into AA.** `I` and `O` were skipped as always.

### Stage Y grew: its rule passes a route that reports work it did not do

**Issue:** CAR-104 · **State:** `next` · **Gap:** G21, G27, G28

**Re-measured 2026-09-08 against `d6e3a6d`: 100 routes across the six importable
services, 46 of which produce a status code they never declare.** The census
said 85 and 42 on 2026-09-07; the difference is grain, not drift — six `ops`
handlers are `api_route(..., methods=["GET", "HEAD"])` and serve two routes
each, which the census counted once. `dashboard` contributes nothing because it
still cannot be imported ([G18](../TESTING.md#the-gap-list)). The codes are 503
(15), 303 (13), 409 (11), 500 (10), 404 (7), 400 (5), 422 (3), 403, 307 and 308.
**Not one route in the repository declares a single code today.**

**The OpenAPI schema is not a weak contract here, it is a false one.** Every
service declares exactly `200` and `422` — FastAPI's defaults — while the suite
asserts eleven distinct codes across **137 assertions**. Every real code is
raised inside a handler body and surfaces nowhere a machine can read, and a
contract artifact generated today would faithfully record the falsehood, which
is why Stage Z cannot precede this.

**Then writing the declarations down found routes with nothing true to
declare, and the original rule passes every one of them.** `toggle_search`
executes `UPDATE search_configs SET enabled = NOT enabled WHERE search_key = %s`,
never reads `rowcount`, and returns 303 unconditionally. It produces exactly
one code. It declares exactly one code. **Declared equals produced, and the
route reports success for work it did not do** — a search key that does not
exist gets the same redirect as one that does. A rule that certifies that is
measuring the wrong thing, so the stage grew to cover the two gaps that finding
opened.

**The mechanism is a test that could not have failed, and it is this plan's own
subject one layer up.** `test_toggle_search_ok` mocks the cursor and asserts
`303`. A `MagicMock`'s `rowcount` is a `MagicMock` — never `0` — so the test
environment cannot express the failing condition at all, and the assertion is
satisfied by construction: it is equally true of a handler whose body has been
deleted. Stage M's rule already passes it, because that rule asks whether an
assertion **exists** and this one does. Nothing asks whether it could ever have
been false.

**G27 — a handler does not observe the effects it causes. 13 functions.** A
`WHERE`-bearing `UPDATE`/`DELETE` with no `RETURNING` and no `rowcount` read,
across `ops/routers/` (admin, users, scrape, snapshots, maintenance),
`processing/` and `archiver/`. **There is no member where checking is worse**,
which is why it carries no ledger: `_record_last_used` touching a token row that
is gone is exactly when you want to know, and `_reap_stuck_processing` updates a
row it just read by primary key, so `rowcount` of 0 there is a live
concurrent-modification race that currently passes silently. The same defect
arrives a second way — an effect whose exception is swallowed and falls through
to the success path. 34 such sites; **11 cover a mutation** and the rest cover a
read or a parse, which degrades honestly. Five of the eleven are the dead
`dbt_runner` calls and belong to [Stage AA](#stage-aa-a-test-may-not-invent-another-services-response),
which resolves them. `scraper/processors/scrape_detail.py:196` is the control
case and stays: it binds `minio_write_error` into the returned artifact, so the
caller can tell. `revoke_user` binds nothing and returns the identical redirect,
for an access-control operation, whether the row matched, did not exist, or the
database raised.

**G28 — a declared code that nothing exercises. 68 (route, code) pairs, 46
asserted, at most 22 not.** That ceiling is soft in the direction this plan has
been caught by before: `tests/ops/routers/test_coordination.py:156` asserts
`status_code == status_code` with the code `parametrize`-injected, which a
literal scan reads as no assertion — the same blindness Stage H fixed for paths,
whose `_parametrized_strings` this reuses, so the true figure is lower. **This
is not a new rule but Stage H's, strengthened.** `docs/TESTING.md` says every
route is reached *"and the test asserts the status code"*, singular; every
vacuous 303 test satisfies it. Only this stage's declarations make "every
declared code" expressible, and that is the edit that answers whether the test
could have failed — not by grading an assertion, but by requiring the declared
set to be covered. Where a code's trigger is a database outcome it must be
asserted at a real-engine layer, because a mocked `rowcount = 0` is the author
asserting a belief about psycopg2 rather than observing one.

**The three repairs are ordered, because the declarations and the behaviour
cannot move at once.** Handlers observe their effects first — that is where the
admin and user CRUD routes gain a 404 — then every route declares, then every
declared code is covered. Reversed, the denominator shifts while it is being
filled.

**42 judgement calls was the wrong price.** The stage has none of the shape the
census predicted: there are no `Depends()` and no `@app.exception_handler`
anywhere in the six services — auth is Caddy's `forward_auth` — so a handler
body is the whole of what a route can produce, with no dependency graph to
walk. And the 500s argue for themselves in prose their own docstrings already
carry (`archiver/app.py:241`, `dbt_runner/app.py:155`), with Airflow DAGs
branching on them. They are contract.

**One ledger survives, and it is ownerless.** FastAPI injects `422` on the
presence of a parameter, never its fallibility — `fastapi/openapi/utils.py:418`,
byte-identical in 0.128 and 0.141.1, so no upgrade removes it. 41 routes declare
it; **33 can produce it and 8 cannot**, their only parameters being unconstrained
strings. Six of the eight are repaired by G27 or resolved by Stage AA. The two
that remain are `GET` and `HEAD /recaps/{slug}`, where the guard already exists
as `_SLUG_RE` in the body and moving it into `Path(pattern=...)` would turn
today's 404 into a 422 on a public route — a worse answer for "no such page".
That is a decision with no owner and no expiry, so it is shaped like
`DORMANT_SUITES` rather than a waiver, which would fail the day this plan
archives and take the reason with it.

**Exit:**

- Every route's declared codes equal the codes its handler can produce, both
  directions — an undeclared code fails, and a declared code nothing raises
  fails too.
- No route executes a mutation whose effect it does not observe, and no handler
  swallows an effect's exception without changing what the caller observes. The
  first carries no ledger. The second carries only calls into the dead
  `dbt_runner` panel, waived to Stage AA, and drains when that stage resolves
  it.
- Every declared code is asserted by a test, and a database-triggered code is
  asserted at a real-engine layer.
- The phantom-422 ledger holds exactly the two `/recaps/{slug}` routes, with no
  owner and no expiry.
- Demonstrated, not asserted, by mutation rather than by argument: keeping
  `toggle_search`'s `UPDATE` and dropping its `rowcount` check fails the
  observation rule; gutting its body to the redirect fails the declaration rule,
  because it then declares a 404 and a 503 it can no longer answer. **The
  original wording of this clause predicted one mutation failing three rules and
  was wrong**, which is worth keeping rather than quietly correcting: gutting
  the body removes the `UPDATE` as well, so the observation rule has nothing
  left to object to and the coverage rule sees a route producing only 303. The
  two halves need two mutations, and the second is the better demonstration
  because it exercises the over-declaration direction -- the half that stops
  declarations rotting into a description of what the routes used to do.

### Stage AF: the harness that proves the rules is proved by nothing

**Issue:** CAR-114 · **State:** `done` · **Gap:** G29

**Every "demonstrated by X failing" exit in this plan rests on
`scripts/verify_testing_contract_mutations.py`, and nothing guards it.** Its own
docstring says so in the one sentence that matters: *"This is not a CI step."*
So its 59 anchors are literal strings in 22 files that can stop matching at any
time, and nobody finds out until a human runs it — which happens when somebody
remembers, and the whole plan exists because *"a check you must remember is
weaker than one you cannot forget."*

**Measured 2026-09-09.** 61 rules are named in `docs/TESTING.md`'s `Asserted by`
column. **41 are proved by a mutation and 20 are not.** The harness holds 59
mutations across 49 distinct rules, ten of which carry two; the 8 it names that
the column does not are the waiver-hygiene rules, which predate that table and
belong to it rather than to a gap.

**Stage Y is the argument, and it is a measurement rather than a conviction.**
Six rules landed there. **Every one shipped with a bug that made the repository
look healthier than it was, and not one of those bugs failed a test.** The
observation rule keyed on the `execute` call's arguments and so never saw the
three handlers that bind their statement first — *it passed on a tree with a
`rowcount` check deleted*. The coverage rule let a wildcard match a literal
segment and took **38 of 89 handlers out of scope while its failure list still
read four**. A response rule nearly shipped an escape clause that was itself the
defect. Each was caught by writing the mutation and watching it not fail.

**The mutations themselves cannot be derived, and that is the finding rather
than an excuse.** `mutmut` and `cosmic-ray` generate edits automatically and
prove the suite is sensitive *somewhere*; they cannot say that *this rule*
catches *the defect it was written for*. The value of an entry is its
description — *"a route stops reading the rowcount of the UPDATE it performs"* —
and stating what a mutation is supposed to prove is what caught the two worst
bugs above. The prose is the artifact, and a generator does not write it.

**Two things around it derive cleanly, and both are cheap.**

**The obligation.** Every rule named in the `Asserted by` column owes a
mutation. That column already exists and is already asserted the other way by
`test_every_asserted_rule_names_a_real_test`, so this is one table with a second
obligation rather than a new registry. It seeds at 20.

**The anchors.** Each `_edit` anchor must still match exactly once in the file it
names, checkable with a string search per entry and no mutation run at all.
**This is not hypothetical.** Stage Y broke five waivers keyed on
`admin.py:135:_fetch_dbt_context` by adding lines above them while declaring
that file's codes; the waivers failed loudly because a waiver is asserted, and
the anchors would not have. They have the same fragility and none of the
protection.

**What this stage does not do is run the harness in CI.** It costs a pytest
subprocess per mutation and the tree has to be mutated and restored, which is a
minute or two of wall clock against a workflow Stage R exists to shrink. Making
the *anchors* checkable is a second's work and catches the rot; making the
*mutations* run every time is a cost decision this stage records rather than
takes.

**Exit:** every rule in the `Asserted by` column has a mutation, seeded at 20 and
drained to 0; every `_edit` anchor is asserted to match its file exactly once,
in CI; the harness's docstring says which of its two halves runs there and why
the other does not; demonstrated by a stale anchor failing.

### Stage Z: the contract is generated, committed and gated

**Issue:** unassigned · **State:** `backlog` · **Gap:** G22 · **Blocked by:** Stage Y

**A contract nobody generates is a document, and this plan exists because
`ARCHITECTURE.md:179` was accurate in April 2026 and quietly false by August.**
A certified hand-written contract is that failure wearing a suit.

So the artifact is **generated from each running app and committed**, and a
job regenerates and diffs it. You cannot forget to update a generated file; you
can only fail to notice it changed, and the diff is what makes noticing
mandatory. Adding a route, changing a response model, changing a status code
all move the file, and the reviewer's job is to say "yes, I meant that".

**The mechanism already exists here in miniature** and its docstring makes the
argument: `scripts/public_surface_gate.py` holds a commit that edits README or
`info.html` until the surface has been read, keyed on a digest of the staged
content so re-staging reopens the gate. *"A check you must remember is weaker
than one you cannot forget."* This is that, pointed at seven services instead
of two files.

**The fiddly part is normalisation.** The schema carries operation IDs and
ordering that move for reasons nobody cares about — `ops/routers/public.py`
already emits duplicate-operation-ID warnings — so the committed artifact is a
normalised projection, and what it drops is a decision to record rather than a
default to inherit.

**Exit:** every service has a committed contract artifact; a change to any
service that is not reflected in its artifact fails; the normalisation names
what it drops and why; demonstrated by an unreflected route change failing.

### Stage AA: a test may not invent another service's response

**Issue:** unassigned · **State:** `backlog` · **Gap:** G23 · **Blocked by:** Stage Z

**37 fabricated HTTP status codes across 6 test modules** — `{200: 26, 403: 9,
400: 1, 500: 1}` — and the seams they replace are `ops.coordination_drain.
requests.get`, `ops.coordination_release.requests.get`,
`scrape_listings.requests.post` and `notifications.requests.post`.

**This is Stage W's defect at the HTTP boundary**, and it behaves the same way.
A test of *our own* endpoint asserting an impossible code is self-correcting —
`TestClient` runs the real route and the assertion goes red. A test of a
*caller* invents both the status code and the body of a service it never calls,
so it passes for any pair its author picks. If `archiver` starts returning 202,
`ops`'s test still fabricates 200, still passes, and production breaks.

The rule is Stage W's third rule one layer up: the caller's fabricated codes
must be codes the callee can produce, with the pairing derived rather than
declared — the caller names the service in a `<NAME>_URL` constant, and Stage W
proved that pairing is derivable and that an undetermined owner must fail
rather than be skipped.

**28 of the 37 are ours. The other 9 are `cars.com`**, and they are Stage AB.

**Two of this stage's own claims were corrected before it opened**, by Stage Y's
census on 2026-09-08.

**The seam list is short, and the missing seam is the one that matters.**
`tests/ops/routers/test_admin.py` fabricates at `ops/routers/admin.py`'s request
seam — `mock_requests["delete"].return_value.status_code = 200` in a test named
`test_dbt_intent_delete_ok` — and its `mock_dbt_context` fixture returns
`{"lock": {}, "intents": {}, "docs_available": False}`, a fabricated *shape*
rather than a code, which the census counted at neither. So 28 is a floor and
this stage re-measures rather than inheriting it.

**And the rule as written would pass what that seam is hiding.**
`ops/routers/admin.py` calls five endpoints on `dbt_runner` that do not exist:
`GET /dbt/lock`, `GET` and `POST /dbt/intents`, `DELETE /dbt/intents/{intent_name}`
and `GET /logs`. Two commits removed them and left every caller standing —
`9f08336` on 2026-04-28 took the four dbt ones while rebuilding the dbt layer
(301 deletions), and `d88a41e` on 2026-05-05 took `/logs` while standardising
logging. Each call site is wrapped in `except Exception: pass` and followed by an
unconditional 303, so the admin dbt panel and the log viewer have done nothing
since April and nothing has reported it. **Keyed on codes alone this stage
passes it**: the test fabricates a 200, and `dbt_runner` can certainly produce a
200 — on `/health`, `/ready` and `/dbt/build`. A route that does not exist is
invisible to a code-only comparison, so the pairing is **(path, code)**, which
is exactly what Stage Z's committed artifact supplies and the reason this stage
is blocked by it.

**Resolving those five calls belongs here rather than to Stage Y**, which found
them: whether the intent-management UI is deleted or `dbt_runner` regains the
endpoints is the same decision as what the repaired test asserts. The four
handlers also hold four of the swallowed-mutation sites Stage Y repairs
elsewhere, and they leave that set with whichever answer this stage takes.

**A second instance, and it is a field rather than a route.**
`airflow/dags/scrape_detail_pages.py:143` logs `result.get("status")` from
`POST /scrape/claims/release`, and that endpoint has never returned a `status`
key — it answers `run_id`, `total`, `errors` and `fetches_recorded`. Every run
logs `status=None`. Found by Stage Y on 2026-09-08 while correcting the same
endpoint's counts, and left here deliberately: **the fix is not to add the
field.** A caller reading a key the callee does not produce is this stage's
defect one level below the status code, and it says the pairing has to reach
the response *body* as well as the code — which is a scoping question for this
stage to answer, not a line to patch in the DAG.

**Exit:** no test fabricates a response for a service this repository owns; the
fabricated-response ledger re-measured at this stage's start rather than seeded
from the census, and drained to 0; the pairing keyed on (path, code); the five
dead `dbt_runner` calls resolved; demonstrated by a fabricated code the callee
cannot return failing, and by a fabricated response for a path it does not
serve failing.

### Stage AB: what we do not own is recorded and replayed

**Issue:** unassigned · **State:** `backlog` · **Gap:** G24

**9 fabricated `cars.com` responses**, mostly the 403 in
`tests/scraper/processors/test_scrape_detail.py`. There is no code in this
repository to derive them from, so Stage AA's rule cannot reach them.

**The pattern is already established here, twice**:
`scripts/verify_promtail_contract.py` and
`scripts/verify_container_health_docker_contract.py` each replay a recorded
corpus through the real thing in a dedicated CI job — *"one corpus, two
consumers, neither importing the other"*. The fast tests get the recording; the
job catches the real service changing underneath it.

**Airflow is the cheap half and should not wait.** `airflow/dags/
notifications.py:71` compares a task state against the literal `"failed"`, a
word Airflow owns. CI already installs real Airflow in a venv and runs a suite
in it, so asserting `"failed"` is a real member of `TaskInstanceState` is three
lines in an existing job.

**`git` and `markdown-it` are deliberately excluded.** Their vocabularies are
restated too — `fetch.prune`, `heading_open` — but a stale one there makes a
script error out. This class is about silence, and those are not silent.

**Exit:** every external vocabulary this repository depends on is either
replayed against the real thing in CI or declared out of scope with the reason;
demonstrated by a recorded corpus that has drifted failing.

### Stage AC: the database makes a stale read loud

**Issue:** unassigned · **State:** `backlog` · **Gap:** G25

**The hole this closes is live and was measured, not inferred.** On 2026-09-07
the Layer 2 suite was run against a Flyway-migrated Postgres with
`artifacts_queue.status`'s `retry` renamed to `retry_later` in the migration
and in `shared/db_vocabularies.py`, and with the test seeds updated so the run
got past its own fixtures. **240 passed.** Five production statements were
still filtering on `'retry'` — `processing/sql/claim_artifact.sql`,
`claim_artifacts.sql`, `archiver/sql/delete_cleanup_candidates.sql` and
`get_queue_cleanup_candidates.sql` among them — which in production means the
claimer silently stops picking up retry artifacts and the cleanup job silently
stops clearing stuck rows, with a green suite.

**The pattern behind that result generalises.** Writes are caught, because the
constraint rejects them. Rowcount-asserted updates are caught: the same
experiment against `coordination_state.phase` failed two tests on
`assert cur.rowcount == 1`. **Read filters are caught by nothing.**

**92 such literals across 52 `.sql` files**, 22 of them in `dbt/models/`, which
the local run could not reach at all — those 35 skips are the DuckDB half.

**A checker is the wrong instrument, and this was verified rather than
assumed.** Against a `text` column with a `CHECK`, a stale filter returns
`(0 rows)`. Against a Postgres `ENUM`, it raises `invalid input value for enum
phase: "draining"` — parameterised as well as literal. Converting the 18
constrained columns to enum types closes this for `.sql` files, for dbt models,
and for code nobody has written yet, at the database rather than in a linter.

**It does not subsume Stage W, and the reason is measured.** psycopg2 returns
an enum column to Python as a plain `str`, so `state["phase"] == "draining"`
stays silently false after a rename, enum or no enum. The enum makes the
*query* loud and does nothing about the *comparison*. Those are the 33 sites
Stage W repaired.

**This stage must update Stage W's reader**, which parses `CHECK (<column> IN
(...))` and will match nothing once the columns are enum types.
`test_the_check_constraint_corpus_is_not_empty` is what stops that being
silent — the corpus would fall to 0 against a floor of 10 and fail loudly — but
the reader is part of this stage's work, not a surprise for it to discover.

**Exit:** the 18 constrained columns are enum-typed; Stage W's corpus reader
reads `CREATE TYPE … AS ENUM`; a stale literal in a `.sql` file or a dbt model
fails; demonstrated by the `retry_later` mutation above now failing where it
passed.

### Stage AD: a fixture cannot fabricate a row the database would reject

**Issue:** unassigned · **State:** `backlog` · **Gap:** G26

**481 test-side copies of a database-owned value, and the split is the
finding**: **183 (38%)** are in `tests/integration/`, and they are **already
policed** — during Stage AC's experiment, stale seeds were rejected with
`CheckViolation` before the test could assert anything. The database refuses to
let those tests exist outside the contract. **298 (61%) are unit tests** that
build a dict in memory, and nothing checks them at all.

**That is where the useless test actually lives.** CAR-82's instance was an
in-memory dict with a made-up status. So the class is not "a test that retypes a
value" — it is **a test that fabricates data the database would have
rejected**, which is 298 sites and not 481.

The heaviest are `tests/scripts/oneoff/test_reconcile_april_detail.py` (84),
`tests/ops/routers/test_coordination.py` (38), `tests/scripts/
test_host_maintenance.py` (31) and `tests/processing/test_batch_functions.py`
(20).

**The mechanism is probably a fixture factory rather than a rule** — rows built
through something that validates against the vocabulary, so a fabricated row
that could not exist fails at construction rather than being linted afterwards.
That is a design question this stage opens rather than one it inherits.

**Seeding 298 waivers takes the plan's live total from 25 to roughly 400 before
it falls.** That is honest — the census opened at 120 — but it makes progress
read as a spike rather than a slope, and anyone glancing at the number
mid-programme will misread it. If that is too coarse, the natural split is by
whether the fixture crosses a service boundary, because that is where the
useless tests are.

**Exit:** a unit fixture cannot carry a value the owning column forbids;
`FABRICATED_ROW_WAIVERS` seeded at its measured count and drained to 0;
demonstrated by a fabricated row failing at construction.


### Stage AE: configuration is what Compose delivers, and everything else is a constant

**Issue:** CAR-109 · **State:** `backlog`

**Found 2026-09-08, closing Stage V.** That stage asserts `.env.example`
against `docker-compose*.yml` in both directions, and its corpus is those two
files. It therefore says nothing about a default chosen in Python, which is
where most of this repository's configuration actually lives.

**Measured across production Python: 76 distinct environment names read, and
39 that neither `.env.example` nor any Compose file mentions.** Twenty are
covered by Stage V's rule. Seventeen more are set as *literals* in a Compose
`environment:` block -- `PGHOST`, `DATABASE_URL`, `SCRAPER_URL` -- so they are
delivered, just not operator-configurable. The remaining 39 exist only in the
code that reads them.

**None of them is a provisioning defect, and that was measured rather than
assumed.** Every one has a concrete fallback and **not one is read through
`os.environ[...]`**, so no absent variable can fail a fresh provision. Two that
first looked like exceptions were not: `DISK_USAGE_ROOT_PREFIX` and
`DISK_USAGE_VOLUME_PREFIX` reach their defaults through the
`os.environ.get(X) or DEFAULT_X` idiom at `disk_usage.py:287-290` rather than a
`.get` default. So this stage is not about a variable that fails to arrive. It
is about where configuration is allowed to live.

**The 39 are not one kind of thing, and treating them as one is what makes the
split feel unmaintainable.** Twelve are configuration by any reading: six
topology defaults that are simply the deployed value compiled into Python
(`LOKI_URL = http://loki:3100` in `ops/coordination_release.py`,
`PROMETHEUS_URL`, `CONTAINER_HEALTH_URL`, `AIRFLOW_HOME`, `RAW_BASE`,
`LOG_PATH`), two archiver safety flags that gate behaviour per service, and
four DuckDB memory and thread limits that are the real levers on a struggling
VM. The rest are calibration constants and cadences -- pack and prune batch
sizes, byte targets, progress intervals.

**`PACK_PRUNE_INODES_PER_OBJECT` is the clearest of those and is worth stating
exactly**, because it is what shows the two kinds apart. MinIO stores each
object as a directory plus an `xl.meta`, so deleting one frees about two
inodes; Plan 131 Stage 0a measured the figure across the bucket at **2.24**. It
is used once, at `delete_packed_source_html.py:632`, to print
`inodes_freed_estimated` beside the `inodes_freed_measured` the filesystem
actually reports. A measured constant feeding one cosmetic estimate is not a
knob, and exposing it as an environment variable invites someone to change a
number whose derivation lives in a plan document.

**The line, stated so that it needs no judgement to apply:** configuration is
what Compose delivers. Everything else is a constant and stops being an
environment variable at all. Membership is decided by whether a service
delivers the value, not by anyone's view of whether an operator might want it,
which is what makes the split survive the people who made it.

**Moving a default is not a safe edit, and the order matters.** A variable left
as `os.environ.get("X", "2GB")` while Compose gains `X: ${X:-}` returns the
**empty string**, not `"2GB"` -- the key now exists. That is Stage P's failure
mode a third time: a value that looks wired, a container that comes up healthy,
and a wrong value invisible from outside. So a variable that moves has its
default removed from Python and its read made strict **in the same change**,
with Compose the single owner. Thirty-six of the 39 use the fragile
`.get(name, default)` form today; only `disk_usage.py` uses the idiom that
survives an empty value.

**Then the rule, and it is one rule rather than a policy.** No production
module reads an environment variable with an inline default: AST-walk
`production_python_files()` -- the corpus the SQL and import rules already use
-- and fail on `os.environ.get(X, d)` and `getenv(X, d)`. What survives is
`os.environ[X]`, and the second clause is that a variable read strictly must be
delivered by its service's Compose block. That clause reuses the interpolation
parser [Stage V](#stage-v-a-variable-the-environment-documents-reaches-the-service-that-reads-it)
committed rather than deriving Compose references a second time, because two
parsers that could disagree about what Compose delivers is the defect this
plan keeps finding in other clothes.

**The waiver is seeded, drained, and then deleted.** The rule has 39 violations
the day it is written, so it lands with a waiver tuple at that count and the
stage drains it to zero -- Stage S's pattern, and this plan does not leave
things on waivers. What is decided here is what happens to the empty tuple, and
this plan has done it both ways deliberately: `CI_INVOCATION_WAIVERS` stays
empty so a new violation fails on append, while the SQL-execution gate's ledger
was deleted so that restoring the escape hatch is a diff that has to argue for
itself. **This one is deleted**, for the reason that gate gave: an empty ledger
and no ledger differ in exactly one way, which is what the next violation costs
to admit, and here it should cost an argument rather than a tuple append.

**The cost is the module-scope reads, not the line count.** These are module
level -- `DUCKDB_MEMORY_LIMIT = os.environ.get(...)` at
`delete_packed_source_html.py:123`. Made strict, a missing variable raises
`KeyError` at **import**, which breaks every test and tool that imports the
module rather than only those exercising the behaviour. Either a conftest
supplies the service defaults or the read defers into the function. That choice
is this stage's design question and it is larger than the 39 edits.

**Estimate: not sized.** The triage is done -- roughly twelve move and the rest
are deletions -- but the module-scope decision above is what sizes it, and the
twelve that become Compose configuration can only be proven by a deploy, so
this stage is production-gated in the way [Stage V](#stage-v-a-variable-the-environment-documents-reaches-the-service-that-reads-it)
was expected to be and was not.

**Exit:** no production module reads an environment variable with an inline
default; every variable read strictly is delivered by its service's Compose
block; each of the 39 has been moved to Compose or reduced to a constant, with
its read made strict in the same change as its default moved; the waiver tuple
has drained to zero and been deleted; and the twelve that moved are verified by
asking a deployed container what it loaded, not by asking whether it is up.
Demonstrated by an inline default failing, not asserted.

### Stage AG: rules live in a directory, and an unregistered one cannot exist

**Issue:** CAR-115 · **State:** `—` · **Gap:** G30

**Found by Stage Q, in the shape this plan keeps finding things.** That stage
wrote four rules, ran the suite, and reported **3,899 passed** — with all four
rules unregistered, unmutated, and one of them *broken*. The obligation
[Stage AF](#stage-af-the-harness-that-proves-the-rules-is-proved-by-nothing)
added reads `docs/TESTING.md`'s `Asserted by` column, so a rule named in no row
owes no mutation, and a green run said only that nothing had been claimed.
Registering them made the obligation fire, and writing the mutation for
`test_every_heavy_job_starts_the_compose_services` found that it joined a job's
every `run:` step into one string — the Flyway step's own mention of the
override file satisfied it with the `up` step deleted. **A rule that was named,
implemented and green did not work**, which is Stage Y's finding arriving twice.

**The reverse direction was declined for a reason that has expired.**
`test_every_asserted_rule_names_a_real_test` asserts table → test and says
plainly why it does not assert test → table: the waiver-hygiene checks have no
row, and enumerating them "would need exactly the curated list this file
refuses to keep." Measured 2026-09-09, that list is **7 items**, four of which
already carry mutations, and Stage AF's own note says the waiver-hygiene rules
"belong to" the column rather than to a gap.

**Detection cannot be derived from shape, and this was measured rather than
assumed.** If a rule were "a test that asserts about the repository rather than
exercising its code", the scope would be **411 of the 421** definitions at the
top level of `tests/` — sweeping in 132 observability-config tests, 43
planning-docs tests and 39 deploy-script tests. Something has to be declared.

**So the declaration moves from per test to per module, and becomes a
directory.** The repository already reasons this way: Layer is assigned by
directory, and [Stage G](#stage-g-what-the-split-is-and-why-a-directory-rather-than-a-list)
is titled for the argument. A new file's author chooses where it lives, once,
and the mechanism reads the filesystem instead of a table.

**The boundary is "asserts a repo-wide invariant", not "is about testing."**
The rules are the guardrails anything operating in this repository works
inside, so `test_planning_docs.py` belongs beside `test_testing_contract.py`
however different their subjects look. That puts **155 definitions** in scope
against **266** that stay at the top level as Layer 0 config tests — the ones
whose subject is one artifact's configuration rather than a convention. That
distinction is where the boundary will be argued next, and it is written here
so the argument starts from a stated line.

**The migration is small and self-verifying.** Three modules move (79
definitions), carrying 34 path references in the mutation harness and one real
import in `scripts/check_sql_execution_coverage.py`. A botched move is loud
rather than silent, because Stage AF's `test_every_mutation_anchor_still_matches_its_file`
fails on any anchor that stops matching exactly once.

**Exit.** Rules live in their own directory; every test there is named in the
`Asserted by` column and proved by a mutation, seeded at ~83 and drained to 0;
a test added to that directory with no row fails, and so does a rule module
that never joins it; the 266 that stay are stated with the reason they are not
rules; demonstrated by an unregistered rule failing, not asserted.

### Stage AH: every rule has a skill that helps an agent obey it

**Issue:** CAR-116 · **State:** `—` · **Gap:** G31

**A rule with no skill is a rule an agent discovers by failing CI.** Stage AG
makes the rule set complete and provable; this makes it *reachable* from inside
the work. The two halves are the same claim from opposite ends — one says every
rule is enforced, the other says every rule is learnable before it is enforced.

**Partial coverage already exists and shows the shape.** On the code side
[`add-sql`](../../.claude/skills/add-sql/SKILL.md) carries the SQL rules: where
a statement lives, what loads it, and the Layer 2 test that discharges its
coverage obligation. On the documentation side the plan-document family —
`plans`, `plan-draft`, `plan-start`, `stage-close`, `close-out`,
`note-evidence` — carries what `test_planning_docs.py` asserts. Both were
written because the rule alone was not enough to act on.

**The uncovered rules are the ones an agent trips over without knowing they
exist**: patching is `mocker` everywhere, encoding-sensitive I/O states its
encoding, a route declares the statuses it can return, `.env.example` wiring in
both directions, and — added by Stage Q — CI's services come from the Compose
definitions. Each is a rule whose first contact is a red CI run.

**The mapping is asserted, not maintained.** A rule's row gains a skill
reference the same way it gains a mutation, and the same rule that fails on a
missing mutation fails on a missing skill — otherwise this becomes another
column that drifts, which is the defect the whole plan is named for.

**Exit.** Every rule in the `Asserted by` column names a skill that helps an
agent comply with it; a rule with no skill fails; the skills that do not yet
exist are written; demonstrated by a rule losing its skill reference failing.

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

### Stage S — branch coverage for the dbt models

**Legacy:** Stage 11 · **Issue:** CAR-79 · **Closed:** 2026-09-07

**Cost:** estimate 2 points. `In Progress` 2026-09-06 18:30Z → 2026-09-07
18:52Z. The issue itself records that *"the estimate predates this rewrite and
has not been revisited"*; the actual is the maintainer's to set.

All nine exits met. The dbt project's obligation is now stated as branch
coverage and held by three gates that fail loudly rather than by any list
someone maintains.

| | Start | End |
|---|---|---|
| Measurable branch points | 308 | **297** (11 removed as dead) |
| …covered both arms | never measured | **297 of 297** |
| `UNIT_TEST_WAIVERS` | 160, seeded full | **0** |
| `BRANCH_COVERAGE_WAIVERS` | 142, seeded full | **0** |
| `UNPROBEABLE_BRANCHES` | 12 | **0** |
| `UNREACHABLE_BRANCHES` | 11 | **0** |
| `DBT_CONTRACT_WAIVERS` (G20) | 23, seeded by Stage X | **0** |
| dbt unit tests | 66, in 3 files | **105**, in 10 — 39 added here |
| Models building zero rows | 5 of 23 | **0** |
| Constraints shown load-bearing | never measured | **15 of 161**; 146 decorative |
| `schema.yml` columns declared | 187 | **307**, all typed, 23/23 enforced |
| `TEST_SQL_TEMPLATE_WAIVERS` (G19) | 1 | **5** |
| `ALL_WAIVERS` | 44 | **25** |

**"Unreachable" is not a category — it is a symptom of three different
defects, and the ledger for it should never have existed.** Eleven branch
points could be taken by no fixture row and no unit test. Read as a property
of the code they would have been a permanent ledger nobody could drain. Read
as a symptom they resolved into: **six guards against states the surrounding
SQL already makes impossible** (dead code, deleted — `nullif(count(*), 0)`
under a GROUP BY, a `coalesce` around an expression whose first argument is
the literal 0, a LEFT JOIN whose two sides are grouped from the same relation
on the same key), **one fixture too old to reach a 7-day recency window**, and
**one genuine production bug**. Every entry the ledger ever held was removable.
It is empty because the code went, not because anything was excused.

**The bug it found is what the stage is for.** `mart_vehicle_snapshot`'s
`listing_state` falls back to a recency test whose header says "seen on SRP
within 7 days"; it read `int_price_history.last_seen_at`, which is
`max(event_at)` over *price events*. A VIN seen an hour ago that never carried
a price was published `'unlisted'`; a listing still on SRP aged into
`'unlisted'` as soon as its price stopped moving. It had been wrong for as long
as the model existed, `dbt build` was green throughout, and the fixture's own
`not_null` constraint on the column passed vacuously because the model built
zero rows.

**The fixture had rotted on the calendar, inside the stage measuring it.**
Every `_ts()` literal was an absolute 2026 date, so the newest fixture row aged
away from `now()` as time passed. The 7-day arm above was covered the day it
was written and had been silently uncoverable for weeks. `FIXTURE_EPOCH` plus a
whole-day shift fixes it: every relationship the fixture encodes is a
*difference* between two timestamps and a constant shift preserves all of them,
so the scenarios survive and only the distance to `now()` changes. **A test
that was true when written and quietly stopped being true is the failure mode
this plan keeps rediscovering** — see Stage A's undercounts and Stage X's three
instrument defects.

**Five models built to zero rows with ~30 constraints asserting vacuously**,
from two independent causes: `scripts/seed_lake_snapshot_fixture.py` seeded
MinIO only, so `ops.tracked_models` was empty and `int_active_make_models`
inner-joined nothing; and the recency arm above. `--require-non-empty` named
this exact cascade in its own CI comment while guarding sources rather than
models, in a different job. The non-vacuity gate has **no waiver list**, by
decision: a list is exactly where these five would have been recorded instead
of repaired.

**146 of 161 declared constraints are decorative** — no mutation of the guard
that produces them fails their test. That is not a defect to fix here; it is a
measurement nobody had, and the ledger now records which 15 hold something up.

**`dbt run --empty` is a prerequisite, not an optimisation, and the plan
asserted the opposite.** §Stage S said unit tests could not reach
`is_incremental()` machinery — *"structurally out of its reach"*. They can:
`overrides: {macros: {is_incremental: true}}` with a mocked `- input: this`,
after `dbt run --empty` creates the relations dbt needs to type the mocked
rows. **The false claim was inferred from a failure rather than read from dbt's
documentation, and inferred twice — once by an agent, once by the author
checking the agent.** It cost eight correct unit tests, deleted on the bad
diagnosis and restored when the maintainer asked for the public docs to be
read. The bullet is corrected in place above rather than deleted, because the
shape of the error — a measurement taken without a precondition, read as a
property of the world — is this plan's own subject. 19 branch points are
reachable no other way.

**Attribution between the cold and warm compiles took four attempts**, and
three of the four failures were surfaced by an agent or the maintainer rather
than by the author. Tagging by compile root, then by predicate text, then by
rendered form (which moved the gap from 0 to 25), then the rule that holds:
unphased, or same condition, or same rendering.

**Two instrument defects worth carrying forward.** Filing an execution error as
`unprobeable` made a missing S3 config read as 143 legitimately excused
branches *and the gate reported an improvement* — an instrument that cannot
distinguish "no probe can express this" from "the probe broke" will always
report progress when it breaks. `error` is now a separate state with its own
assertion. And `sqlglot` 30 renamed the AST arg keys `from` and `with` to
`from_` and `with_`, which silently made 212 branches read as having no source
scope.

**The gate was audited by mutation after it was built, and had a hole.** Ten
anti-patterns were applied to a clean tree one at a time. Seven failed loudly:
deleting a gate file, deleting a gate's CI step while keeping the `--ignore`,
turning `enforced: true` off, dropping a `data_type` (dbt itself, `ERROR=1`),
and adding a new untested branch to a model, which is exit 1 demonstrated
rather than asserted. Two were correct passes — deleting a unit test that had
become redundant when `mart_vehicle_snapshot` was fixed, and re-adding an
absolute `datetime` to the fixture, which the shift makes harmless.

**One was a real failure of the gate.** `UNPROBEABLE_BRANCHES` and
`UNREACHABLE_BRANCHES` are asserted as exact sets, so neither can take an
untrue entry. The two *waiver* ledgers had only the staleness half — a waived
id must still name a live branch — and nothing asked whether the branch still
needed waiving, so a waiver naming a fully covered branch passed 6 of 6.
`test_no_coverage_waiver_names_a_branch_that_is_already_covered` closes it,
verified by both mutations failing on it. That the ledger the whole stage
drained to zero could be silently refilled is the same one-directional
checking this plan keeps finding elsewhere, this time in its own instrument.

**The one ledger that grew is `TEST_SQL_TEMPLATE_WAIVERS`, 1 → 5** — three
constraint-mutation templates and one non-vacuity template whose renderings are
generated SQL that Layer 0 cannot enumerate. Recorded as debt, not as a
convenience.

**A scoping count this stage was sized by was wrong**, again: 250 branch points
became 216 became 297 as the enumerator learned that `count(*) filter (where
…)` is one branch and not two, and that both compile phases must be counted.
Stage A's finding — *"the direction of that error is the reusable finding"* —
now holds three stages out of three where a by-eye count was checked.

**Addendum, 2026-09-07, after review: the instrument was corrected, and the
stage's numbers survive it at 295 of 295.** The PR #379 review found probes
counting arms over rows the scope's own WHERE discards; corrected (and with
the gates reordered ahead of the reseeding suite), the stricter read named
exactly two branches — both dead code in `mart_deal_scores`, deleted per this
stage's own precedent — and one fixture gap, repaired as data, so the
fingerprint `unique`s stay demonstrably load-bearing and the 15-of-161 split
is unchanged. All four ledgers are still empty and no exit reopens; 295 is
derived (297 minus the two deletions — the CI log proves the gates pass but
does not print the count). Full account with the recipe in
[plan_162_stage_S_evidence.md](../evidence/plan_162_stage_S_evidence.md); CI
run 34162171648 at `5b96948`.

### Stage T — shared fixtures: what the suite duplicates

**Legacy:** Stage 12 · **Issue:** CAR-80 · **Closed:** 2026-09-07

**Cost:** estimate 2 points → **actual 1** — one session, one commit.

All exits met, one with a confession attached: the exit requires re-measuring
"against the same recipe that produced the table," and that recipe was never
written down. It was re-derived — Plan 138 Stage 9's precedent — and is now
recorded verbatim in
[plan_162_stage_T_evidence.md](../evidence/plan_162_stage_T_evidence.md),
which carries everything bulky about this stage. One row cross-checks exactly:
the re-derived predicate reads 55 module-local seed helpers on the pre-stage
tree, the number measured 2026-09-01.

| | 2026-09-01 | before | after |
|---|---:|---:|---:|
| Tests collected | 3,988 | 4,483 | 4,483 |
| Ad-hoc `INSERT`s in test modules | 96 | 2* | 2* |
| Distinct read-back `SELECT`s | 161 | 0 | 0 |
| Module-local seed helpers | 55 | 55 | **47** |
| Helper names defined in >1 module | — | 52 | **43** |
| …byte-identical | — | 19 | **12** |

\* Stage X's deltas, observed rather than re-litigated; neither survivor is a
seed (one is the contract's own mutation fixture, one a docstring).

**Seven groups consolidated**, each into an existing natural home — the
largest single deletion was `_get_conn` ×3, which re-derived a fixture the
root conftest already had. Six duplicate seed `.sql` files deduped into
conftest-owned copies on the way. **The 12 identical groups that remain are
each a recorded decision**: twin test modules mirroring the parser that
production itself duplicates, 2–5-line helpers below the consolidation floor,
and one dormant suite. The ticket's flagship, `_insert_artifact` ×3, dissolved
under Stage X into three visibly different statements — callers wanting
different data, left alone.

**The instrument the exit demands an answer about was found, and it is
static, not the recorder**: Stage X's extraction made "two helpers execute the
same statement" a textual property — normalized content equality over
`tests/sql/` (43 identical groups across 96 of 385 files, 7 sharing no
filename). Recorded as available, not made a rule: per-module seeds are Stage
X's deliberate convention, and PREPARE already fails every copy on drift. For
helpers that execute no SQL, no mechanical instrument exists; that half leaves
prose behind.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work — the collection count did not move.

PR #383, CI run 34181659530 green at `b695283`; locally 441 passed / 35
skipped across the touched suites against a Flyway-migrated postgres:16.

### Stage W — a test may not supply both halves of a contract

**Legacy:** Stage 15 · **Issue:** CAR-82 · **Closed:** 2026-09-08

**Cost:** estimate 2 points → **actual 1** — one session, four commits, and a
full rebuild after the first design was rejected.

*Numbers, transcripts, the rejected design and the two stated limits are in
[plan_162_stage_W_evidence.md](../evidence/plan_162_stage_W_evidence.md).*

**Built once as a registry and rejected, and the rejection is the useful part.**
The first implementation was a curated tuple of cross-module contracts, each
naming a producer, a consumer and a derivation, with one parametrized meta-test
per entry. It worked and it was demonstrated failing. It was also **the thing
this plan exists against**: `DORMANT_SUITES` and `DECLARED_SKIPS` are lists that
work because each is compared against a *derived population* — directories on
disk, skips pytest reported — so an unlisted member fails. The registry had no
derived population, so nothing could say a third contract existed. One instance
and no mechanism, at n=2. Commit `ca74ba3`, reset away, reachable in the reflog.

**The rebuild's first derived pass found the third instance two files from one
the registry covered.** `airflow/dags/sensors.py:95` checks the coordination
phases, owned by a Flyway `CHECK` in `V043`, guarded only by
`test_coordination_admission.py` asserting the literal appears in `sensors.py`'s
*source text* — the paraphrase shape the contract warns about.

**The census was wrong before it was right, and the error was the subject.** It
first counted closed sets *in production* — 110, with 104 having a member
restated under `tests/` — and asked who restates them; structurally that gives
87 "cross-module contracts" dominated by `ok`, `error`, `year`, `price`. From
that reading a curated list looks inevitable. The right subject is the **guard**:
where a module tests an incoming value against a locally written literal.

**262 such comparisons, in three buckets, and only one reachable.** Owned by git,
markdown-it or Airflow — unreachable, nothing here owns them. Owned by the
module's own package — reachable but nothing can drift. **Owned by another
artifact here — the defect class**, and its owner corpus is derivable: **18
`CHECK` columns in `db/migrations/` holding 14 distinct vocabularies**. Of 83
comparisons naming a constrained column, **33 restate a member**; the other 50
are `ok`, `success`, `unknown`, `locked` and must not be touched.

**Membership is the whole discriminator, and the alternative was watched
failing.** By column name alone the rule produced 9 false positives —
`result.status == "ok"` reading as a claim about `artifacts_queue.status`. A
module→constant→`.sql`→table derivation fixed those and cost a chain of
machinery while **missing 11 sites in `scripts/host_maintenance.py`**, which
reads the vocabulary over HTTP. Inverting the test to "the literal is a member"
removed all 9 and recovered the 11, so the machinery was deleted.

**What ships is two rules that do not work apart**, plus
`shared/db_vocabularies.py` holding each vocabulary once as a `StrEnum`. One
compares that module to the migrations in both directions and requires equality;
the other fails a bare literal that is a member. A migration rename leaves the
second green — the literal stops being a member, so the comparison leaves scope
— and that vacuity was observed, not argued. **All 33 sites were repaired**
across 8 modules, and the completed rename then propagated with **zero call-site
edits**: two migration files and one enum member.

**A third rule closes the instance the stage came from**, where the owner is a
service and not a constraint. `airflow/dags/` is the one place here where the
import that would remove the copy is impossible — compose mounts three
directories into the Airflow containers — so both halves are derived instead:
the service from the module's own `<NAME>_URL` constant, the module from the
shared basename. Reintroducing `acceptable = {"created"}` fails it, naming the
exporter's five statuses. The known reader hole is closed by a guard rather than
a wider scan: a `status=` expression the reader cannot follow **fails** instead
of narrowing the set silently.

**Two limits stated rather than closed.** The tests hold their own copies — 115
comparisons and 366 seeds across 49 modules. *This entry first read that those
were "duplication, not the both-halves defect", on the strength of eight tests
failing loudly under the rename mutation. Re-measured 2026-09-07 while scoping
[Stage AD](#stage-ad-a-fixture-cannot-fabricate-a-row-the-database-would-reject),
that is true of 183 of them and false of 298.* The **183 in
`tests/integration/`** are policed already, and by the contract rather than by
this stage: stale seeds are rejected with `CheckViolation` before the test can
assert anything. The **298 unit tests** build a dict in memory and are checked
by nothing — which is exactly where CAR-82's own instance lived. The class is
not *a test that retypes a value*; it is **a test that fabricates data the
database would have rejected**, and this stage does not reach it. And
`sensors.py` reads its phases positionally out of a query result, so no column
name appears; the link is derivable through `deploy_intent_gate.sql`'s `SELECT`
list, the loose membership alternative was tried and produces a false positive
on `notifications.py`, and closing it properly is the next stage's.

**What this stage covers, as a matrix rather than as prose**, because the prose
above was written before the surfaces were separated and reads as though the
class were closed. One row of it is.

| Where a stale word hides | `CHECK` today | ENUM (Stage AC) | Stage W |
|---|---|---|---|
| A Python comparison on a fetched value | silent | **silent** | **caught** |
| A Python write of a bad value | loud | loud | — |
| A read filter in a `.sql` file | **silent** | **loud** | no |
| A read filter in a dbt model | **silent** | **loud** | no |
| Any write, anywhere | loud | loud | — |
| An in-memory test fixture | silent | silent | no |

**The first row is why this stage is not made redundant by
[Stage AC](#stage-ac-the-database-makes-a-stale-read-loud), and it was measured
rather than argued.** psycopg2 returns an enum column to Python as a plain
`str`, so after a rename `state["phase"] == "draining"` evaluates `False` with
no error, while the same filter inside SQL raises `invalid input value for
enum`. The database can make the *query* loud and can do nothing about the
*comparison*. Those are the 33 sites this stage repaired.

**The coupling runs the other way too.** Stage AC must update this stage's
corpus reader, which parses `CHECK (<column> IN (...))` and will match nothing
once those columns are enum types. It cannot do so silently:
`test_the_check_constraint_corpus_is_not_empty` would find 0 against a floor of
10 and fail. That guard went in on general principle and this is the first
concrete thing it catches.

**And this stage should have seeded a waiver tuple rather than repairing all 33
sites outright.** Repairing them was right for the work; contributing nothing to
the plan's progress meter was not, and it is the only reason the completion
criterion looked unable to describe the contract stages. Stages Y to AD each
seed and drain, which is Stage S's pattern and this plan's own answer.

**Exit met, with clause 2 narrowed rather than dropped.** *"A test that restates
a member as a literal rather than deriving it fails"* holds for the forms this
stage declares reachable and not otherwise: the **183** copies in
`tests/integration/` are policed by the constraint itself — a stale seed is
rejected with `CheckViolation` before the test can assert anything, observed
during the Stage AC experiment — and the **298** in-memory copies are policed by
nothing. That residue is [G26](../TESTING.md#the-gap-list) and
[Stage AD](#stage-ad-a-fixture-cannot-fabricate-a-row-the-database-would-reject).
Clause 3 is what permits the narrowing, and the matrix above is the plain
statement it asks for.

**A regression this stage introduced, and caught before merge rather than after.**
The repair replaced a hand-ordered list with `sorted(RequestableRole)`, which is
alphabetical, silently reordering the access-request form's role dropdown from
least-privileged-first to `observer, power_user, viewer`. That list is the
`roles` context for seven template responses. **Nothing asserts the order, so
the suite was green and CI was green** — a silent regression shipped by a change
whose entire subject is silent regressions. It was found by asking what the
repair had changed that no test looks at, before pushing. Fixed at the
declaration rather than the call site: `RequestableRole` now declares its own
members least-privileged first and the router takes `list(RequestableRole)`, so
the order and the completeness are one fact instead of two that can disagree.

Public surfaces: no mechanism, name or quantity either surface states was
changed by this work — both still read *"More than 3,000 tests run in CI"* at
3,801, and this stage added no migration against their *"40+ versioned Flyway
migrations"*.

PR #389, CI run 34190151416 green at `d2a6293` — including the jobs no local
run reaches: the Layer 2 suite's DuckDB half, `dbt model tests (real build)`,
`Service integration tests (Postgres)`, the Airflow metadata contracts and the
SQL execution coverage gate. Locally 3,801 passed / 694 deselected.

### Stage V — a variable the environment documents reaches the service that reads it

**Legacy:** Stage 14 · **Issue:** CAR-88 · **Closed:** 2026-09-08

**Cost:** estimate 1 point → **actual 1** — one session, three commits, and a
scope change mid-stage that added the second direction.

*The census both ways, the answer established for each key, and the six
mutations watched failing are in
[plan_162_stage_V_evidence.md](../evidence/plan_162_stage_V_evidence.md).*

**Both directions landed, and the second was the larger defect.** The stage was
written for Stage P's direction — a key `.env.example` documents that no Compose
service delivers. Three failed it, with three different answers.
`FASTAPI_ADMIN_KEY` was **deleted**: `git log -S` across all history shows it
entered in `eb96c41` (2026-04-09) and never appeared in any file but
`.env.example` and plan documents, so it was never wired because it was never
read. `MLFLOW_TRACKING_URI` and `PROVENANCE_ENV` left the template as
in-development Plan 112 Gate B variables, both read only by
`scripts/log_lakehouse_experiment_provenance.py`, both with working defaults.

**The reverse direction was added mid-stage, and Plan 142 had already found
it.** `plan_142_planned_host_maintenance.md:812` records that `.env.example`
documents none of the seven Airflow variables `docker-compose.yml` requires,
counts "12 of the 42 variables Compose interpolates are missing in total", and
declines the fix — wanting "either a full pass over the template or a test
asserting every interpolated variable is documented". This stage did both.
**Eight secrets required with no default were documented nowhere**, so a fresh
provision got an empty Fernet key, an empty JWT secret and an empty Grafana
admin password rather than a failure. `METRICS_DB_PASSWORD` is the sharpest:
`docker-compose.yml:84-91` substitutes six role passwords into Flyway
placeholders, creating each role with whatever is set, and the template
documented three of the six.

**References are read from parsed YAML values, never file text, and that is not
a style choice.** `docker-compose.yml:169` names `SNAPSHOT_DOWNLOAD_TOKENS` in a
comment three lines above the reference at `:172` that delivers it. A grep
counts the comment, so a text-matching rule would have passed Stage P's defect
on the day it was written — the mechanism meant to catch the class would have
had the class built into it.

**`$$` is Compose's escape, and missing it produced the census's one false
positive.** `$${HOSTNAME}` in two Airflow health checks passes a literal
`${HOSTNAME}` through to the container's own shell. Plan 142's 12 counted it;
the 14 measured here does not.

**The declared tier is named for what its members are, not for what the stage
predicted.** The stage specified "script-only". Its one member,
`SCRAPER_RESULTS_BASE_URL`, is read by `scraper/processors/scrape_results.py:34`
— a production module inside a Compose service, taking a default production must
never override — so the tier is `Undelivered`: documented, read by something,
delivered to no container. It entered the corpus at all only because
commented-out keys are counted, on the grounds that a `#` does not stop a file
being read.

**Both ledgers carry a third direction beyond the two `DORMANT_SUITES`
established.** A declaration whose named consumer does not contain the key, or
whose named Compose file does not interpolate the variable, fails rather than
pointing nowhere. Each carries a ceiling so a new entry cannot be a quiet tuple
append.

**The stage did not turn production-gated**, which CAR-88 warned it might. Every
documented-but-undelivered key resolved to delete-or-declare, nothing needed
wiring into `docker-compose.yml`, and no Dockerfile copies `docs/` or
`.env.example` — the merge is the whole delivery.

**What it does not prove: delivery is not arrival.** The rule asserts that a
service *names* the variable, which is the link Stage P broke, and cannot assert
that the running container loaded a value. A key wired into Compose and left
unset in the VM's `.env` still arrives empty with this file green.

Public surfaces: `README.md`'s local quick start listed five things to edit
before `docker compose up -d`, and the Airflow and Grafana secrets this stage
added to the template were not among them. The line was corrected with this
stage.
### Stage Y — step 1 in production: the mutations observed, and a number that did not move

Measurements and recipes: [`plan_162_stage_Y_evidence.md`](../evidence/plan_162_stage_Y_evidence.md),
which accumulates across the stage's three steps. This entry is step 1.

**The census that opened the stage, re-measured against `d6e3a6d`: 100 routes
across the six importable services, 46 of which produce a code they never
declare, and not one route declaring a single code today.** The 2026-09-07
figure of 85 and 42 differs by grain rather than drift -- six `ops` handlers
serve two routes each through `api_route(methods=["GET", "HEAD"])`. Two findings
made the stage cheaper than its own estimate: there is no `Depends()` and no
`@app.exception_handler` anywhere in the six services, so a handler body is the
whole of what a route can produce; and the 500s the census called judgement
calls argue for themselves in docstrings their callers already branch on.

**G27 measured at 13 blind mutations, and only eight were repairs.** Three were
already gated by a preceding read in the same function, two already observed
`RETURNING`, and one -- `_record_last_used` -- was restructured rather than
repaired, because its guard lived one frame up in `_resolve_machine_token` with
nothing connecting the two. **That the other five needed nothing is what stops
the rule's third clause being an escape hatch invented for the awkward case.**

**Deployed 2026-09-08 and verified inside the containers rather than inferred
from the checkout** -- `processing` recreated at 22:37:50Z, `ops` at 00:27:34Z,
both through `scripts/redeploy.sh` with drain confirmed at 0s and intent
released.

**Eleven consecutive `results_processing` runs moved 1,200 detail artifacts and
reported `status_write_failures: 0` on every one**, with no ERROR line and no
traceback across the window. So the stale-claim case `StatusWriteFailed` was
built for does not occur under normal operation at this cadence. **The honest
counterpart is that its handling path has therefore only ever run in tests** --
the `continue`, the counter and the log line are unproven in production, and a
zero is what records that rather than hides it.

**All six guards answered correctly against production `ops`**, and every one of
them answered 303 before this stage: five 404s for an identity matching no row,
and 400 for a role the service does not have. Non-mutating by construction.

**Plan 147's loop guard came back clean on its first run under the new code**
-- 400 released, 400 claims deleted, 400 fetches recorded, the three agreeing.
That is not proof the guard never under-records; it is one run in which the
failure did not occur, and the thing actually established is that the counts are
now *capable* of disagreeing, where `len(fetched_ids)` would have read 400
whatever the database did. The same log line carries `status=None`, which is
Stage AA's second instance observed in production rather than inferred.

**Against the exit:** this bears on the second clause alone -- no route executes
a mutation whose effect it does not observe. It is evidence that the repair is
live and quiet, not that the clause is met: what would meet it is the rule that
makes the next blind mutation fail, and that rule is not written yet.

**The rules, written after the repairs and against them.** Six across the three
gaps, each proved by a mutation. **Every one shipped with a bug that made the
repository look healthier than it was, and not one of those bugs failed a test**
-- the observation rule keyed on the `execute` call's arguments and so passed on
a tree with a rowcount check deleted; the coverage rule let a wildcard match a
literal segment and took 38 of 89 handlers out of scope while its failure list
still read four. That is the argument for the mutations and the floors as a
measurement rather than a principle, and it is the reason the floors assert
population counts rather than merely non-emptiness.

**One rule was nearly written with an escape clause, and the escape clause was
the bug.** Six coordination helpers never read a response status and were
correct anyway, because each subscripts the parsed payload and an error body
raises into a handler returning `unknown`. Crediting that shape is an inference
one step from crediting a 200 that happens to parse, so **the conforming code was
changed instead** -- six explicit `raise_for_status()` calls, free because
`HTTPError` subclasses the `RequestException` those gates already catch, in
exchange for a rule that states one thing and cannot be argued around.

**G21 drained 52 to 0.** The rule was written before any declaration existed so
that its failure list was the work; the census had estimated 46 and the reader
found 52, the difference being that every admin route's 404 and 503 arrive
through a response helper. The schema now carries eleven codes where every
service claimed two.

**G28 drained 11 to 0, and two of the eleven were this stage's own.**
`approve_access_request` and `deny_access_request` gained a 503 when their
swallowed database errors were repaired that morning, and neither got a test
until the rule written after them said so.

**What the stage could not close is one decision, not fourteen repairs.** Every
live waiver across the four G27 clauses and the phantom-422 ledger is a call into
the dead `dbt_runner` admin panel -- five endpoints deleted in April and May with
every caller left standing. Stage AA owns whether that panel is removed or the
endpoints return, and all fourteen entries drain on that answer.

**Deployed 2026-09-09**, in two commands: the five services sharing an image
with a changed file at 13:44:23Z -- `pack-worker` among them because it shares
`archiver`'s digest rather than because it changed -- and `ops` alone at
13:49:19Z. All six healthy, and the loaded code read back from inside each
container rather than inferred from the checkout.

**Cost: estimated 2, actual 2.** The stage tripled its gap count while it ran and
still landed on its estimate, which is worth recording precisely because a delta
of zero is the evidence that the scale works.

Measurements, recipes and the full table of rule bugs:
[`plan_162_stage_Y_evidence.md`](../evidence/plan_162_stage_Y_evidence.md).

### Stage AF — the harness that proves the rules is proved by nothing

**Issue:** CAR-114 · **Closed:** 2026-09-09

**The instrument every other stage's evidence rested on had none of its own.**
`scripts/verify_testing_contract_mutations.py` is what every *"demonstrated by X
failing"* exit in this plan cites, and its docstring said why nothing guarded it:
*"This is not a CI step."* So its anchors were literal strings living in other
people's files, and they stop matching the way any literal does -- silently, and
only where nobody is looking.

**Two halves, split on cost rather than on value.** The anchors are asserted in
CI by `test_every_mutation_anchor_still_matches_its_file`, with
`test_the_mutation_harness_corpus_is_not_empty` as its floor. The obligation is
asserted by `test_every_asserted_rule_is_proved_by_a_mutation`, which reads
`docs/TESTING.md`'s `Asserted by` column for a second duty rather than opening a
new registry -- the column was already asserted the other way by
`test_every_asserted_rule_names_a_real_test`, and one reader now serves both so
the table's shape cannot be understood two ways. Running the mutations stays a
deliberate command, and the docstring now states that split rather than leaving
it implied.

**G29 drained 20 to 0**, and the harness went from 59 mutations to 82. Three of
the 22 new entries prove the rules this stage itself added, so the instrument is
measured by the instrument.

**The anchors were already rotting, which is the argument rather than a
prediction.** The rule found two ambiguous anchors on the day it landed.
`Waiver(subject, gap="G5", owner=162)` matched twice because
`test_no_waiver_outlives_the_plan_that_owns_it` quotes it in its own docstring as
*"literal source text"* -- the sentence documenting the anchor is what made the
anchor ambiguous. The `int_listing_state_runs` `CREATE` had grown a second
occurrence in a fixture. Both had been mutating the earlier site by position
rather than by intent, and `_edit`'s own guard cannot see that case at all: it
raises on an anchor it cannot find and says nothing about one it finds twice.
That asymmetry is why the rule asserts *exactly once* rather than *present*.

**Writing the mutations found a false CAUGHT, which is the method defending
itself.** Each new entry was checked to fail on *its own assertion* rather than
merely to fail. One did not: the `UNDOCUMENTED` ledger append landed outside its
tuple and failed on a `SyntaxError`, and the harness had reported that as a
success. It is the failure the harness's own comment warns about -- a mutation
measured against a suite that never collected its assertion -- and nothing but
reading the failure would have caught it.

**One rule of the twenty needed a live engine, and the cost of that turned out
to be the deciding measurement.**
`test_every_test_statement_plans_against_the_migrated_schema` `PREPARE`s every
`tests/sql/` statement against the live catalogue, so a fixture left behind by a
renamed column is a condition no static reading can express. Two alternatives
were considered and both rejected: a declared exception ledger, which the
obligation rule's own docstring argues against in the same breath as forbidding
one, and moving the proof to a Layer 2 CI gate beside
`tests/integration/dbt/test_constraint_mutation.py`, which is a stage of its own.
Measurement settled it -- **3s to a ready `postgres:16` and 4s to apply 51
migrations**, both images already cached. The harness now provisions a throwaway
database on port 55432, migrates it with `ci.yml`'s placeholder set, and destroys
it. The port is not 5432 deliberately: a script whose whole discipline is not
disturbing the tree it runs in must not shadow a running stack either.

**The DSN alone is not trusted.** The engine-bound node must pass *unmutated*
before its mutation is judged, because an unmigrated database fails every
`PREPARE` and would report CAUGHT having proved nothing. With no engine the entry
reports `UNPROVEN HERE` and the run does not fail -- no engine is a fact about
the machine, not a finding about the repository, and failing there would make the
honest answer indistinguishable from a rule that had actually gone quiet.

**This exceeds what the stage was written to do, and is recorded rather than
applied quietly.** The *"explicitly not in scope"* clause holds: the mutations
still do not run in CI. But "the harness starts and destroys a Docker container"
is a new fact about a script that previously touched nothing but the working
tree, and it belongs in the record next to an exit it was not part of.

**A Windows-only defect in the harness fell out of the work.** The child encoded
its stdout with the console codepage while the parent decoded UTF-8, so the
em-dash in `test_the_encoding_rule_sees_the_shape_ruff_cannot`'s own source
raised `UnicodeDecodeError` before any mutation could be judged. That is the same
locale-dependent defect the rule exists to catch, arriving in the harness that
proves it, and it was invisible on the Linux half of this repository's two boxes.

**Verified 2026-09-09.** The harness: 82 of 82 CAUGHT, zero missed, zero
unproven; baseline and restore both 86 passed; the throwaway container destroyed;
115s wall clock including provisioning and teardown. Locally, `ruff` clean and
3,895 unit tests pass. **Observed in CI** on run `34373255590` for
[#404](https://github.com/whitewalls86/new_car_tracker/pull/404) -- all fourteen
jobs green with two scope-skipped, and `Unit tests (pytest)` is where the three
new rules actually ran, which is what makes the exit's *"in CI"* an observation
rather than a reading of `ci.yml`.

**Public surfaces:** no mechanism, name or quantity either surface states was
changed by this work. `README.md:317` and `info.html:855` both say *"More than
3,000 tests run in CI"*, which stays true at 3,895.

**Cost: no estimate carried, actual 1.** The issue was created without one, so
there is no delta to learn from here -- which is itself the finding, since an
unestimated closed issue undercounts its cycle silently.
