# Plan 161: The Testing Strategy Is A Description, Not A Contract

## Status

Written 2026-08-30, out of a planning conversation that started as a test-suite
refactor and stopped when the refactor turned out to be unwritable. The
restructuring work is real and is described in
[what this unblocks](#what-this-unblocks); it cannot be specified yet, because
there is no standard for it to restructure toward.

Priority and effort are proposed in [`docs/PLANS.md`](../PLANS.md), which owns
both; this document does not choose them.

## The problem

`docs/ARCHITECTURE.md:179` already carries a **Testing Strategy** section:
three layers plus unit tests, each with a stated goal, a pattern, and the
directories it governs. [Plan 84](plan_84_integration_testing.md) built it and
is archived.

It is a **description of what was built in April, not a contract anything is
held to.** Nothing asserts it, so it has drifted, and the drift is invisible
because there is no mechanism that could see it.

### It is already false in specific, checkable ways

Measured against the code on 2026-08-30:

| What the section says | What is true |
|---|---|
| "All three run in CI against a real Postgres instance with Flyway migrations applied" | CI's database does not create Airflow's schema. An entire class of `ops` query cannot be executed by any layer — [Plan 139 Stage F](plan_139_test_suite_maintenance.md) |
| Unit tests "mock DB connections (psycopg2 mock pattern via `mock_db_conn` fixture)" | Across 157 test files: **52 use pytest-mock, 37 use `unittest.mock`, 25 use `monkeypatch`, and 21 use two styles in the same file** |
| `tests/integration/sql/` runs "every query the ops service runs against Postgres" (its own docstring) | **16 service modules still carry inline SQL.** Seven test files cover five areas |
| CI ordering is `unit → Layer 1 → Layer 2 → Layer 3` | True, and it is the 267s serial monolith the restructuring wants to break. The document encodes the current shape as the intended one |

Plan 84 also closed with a deferral that was never revisited — *"dbt_runner and
scraper deferred — lower risk, meaningful coverage achieved."* That sentence is
four months old and is still load-bearing.

### The cost is not hypothetical

Two incidents this month came through gaps the document does not describe:

1. **The `search_path` bug that hung a deploy.** `ops/coordination_drain.py`
   queried `task_instance` unqualified and `public.detail_scrape_claims`
   instead of `ops.`. All three failed in production;`_database_count` returned
   `unknown`; unknown fails closed; the first production deploy of Plan 142's
   coordination gate drained forever. It escaped because
   `tests/ops/test_coordination_drain.py` patches `_database_count` itself, so
   **no drain query string reaches a database anywhere in the unit suite** —
   one test exercises it with a literal `"SELECT evidence"` against a mock
   cursor.
2. **The half-deployed resume gate.** On 2026-08-30 `container-health` ran an
   image five days stale while `ops` had moved ahead, and
   `/project-status/{project}` 404d for roughly eleven hours with nothing
   reporting it. **That endpoint and `/oneoff-processes` are the only two
   routes in the repository with no reference anywhere in `tests/`** — 32 of 35
   routes are at least named by a test; those two are not. The two untested
   endpoints are the two that silently went missing.

Neither is a test that failed. Both are tests that could not exist, in a suite
whose stated strategy says they should.

### Why a census could not fix this on its own

The measurements above are drift from a description. They cannot say which
direction is correct: nothing in the repository makes `mocker` right and
`unittest.mock` wrong, or the reverse. An audit against an unenforceable
document produces a list of differences and no decisions — which is where the
conversation that produced this plan actually stalled.

**The missing artifact is the standard, not the audit.**

## What this plan is

A decision-and-codify plan. It produces the testing contract for this codebase
in two forms that are the same contract:

1. **For people** — what the layers are, what each is *for*, what belongs in
   each, the one convention per concern (mocking, fixtures, SQL location,
   endpoint coverage, what a service owes before it ships), and what "enough"
   means for each service.
2. **For a coding agent** — a skill under `.claude/skills/` that reviews a
   change against the same contract before it is committed, in the shape of the
   five skills already in this repository.

And a third thing that makes it a contract rather than a preference:

3. **A test that asserts it.** `tests/test_planning_docs.py` holds the planning
   rules to 33 assertions; `tests/airflow/test_coordination_admission.py`
   requires every DAG module to declare its admission surfaces or be classified
   as support. **The testing contract needs the same treatment**, or it becomes
   the next `ARCHITECTURE.md:179` — true when written, quietly false a quarter
   later, with no mechanism able to notice.

This is the same shape as [Plan 141](plan_141_structured_log_ingestion_contract.md)
(log ingestion contract) and [Plan 142](plan_142_planned_host_maintenance.md)
(coordination contract): a checked-in registry plus tests asserting the registry
matches reality. The pattern is established here; this applies it to testing.

## Non-goals

- **Fixing the violations.** The moment this plan starts converting 21
  mixed-mock files it stops being the thing that unblocks the other two. It
  decides the rules and records the gap; closing the gap is the follow-on work.
- **Restructuring CI.** Splitting the 267s job, building dbt against the Plan
  120 snapshot, and running the suites against real Compose services are the
  follow-on plan. They need this one's answers first.
- **Choosing the CI selector design.** [Plan 139 Stage E](plan_139_test_suite_maintenance.md)
  already specifies advisory impact selection, including the reason Plan 142's
  service graph is evidence for it rather than the selector itself. This plan
  does not redo that thinking; it may decide where Stage E belongs.
- **Raising a coverage number.** Coverage is one instrument among several, and
  Plans 103 and 107 already sit in the backlog behind Plan 139 Stage D's gate
  decision.

## Open questions this plan must answer

1. **What are the layers, now?** The existing four (Layer 1 SQL smoke, Layer 2
   dbt logic, Layer 3 API integration, unit) predate the archiver, the
   coordination contract, the Airflow 3 migration and the DAG-level suites. Are
   they still the right cut, and where do `tests/airflow/`,
   `tests/integration/airflow/` and the DAG-module tests sit in them?
2. **One mocking convention, or one per layer?** 21 files already mix two. The
   decision is not which library is better but which is *required where*, and
   whether the boundary is the layer or the thing being mocked.
3. **What must never be mocked?** The `search_path` bug escaped because a unit
   test patched the function that builds the query. A rule of the form "a query
   string must reach a real database in some layer" would have caught it —
   whether that rule is expressible is the question.
4. **Where does SQL live?** 384 `.sql` files exist and 16 modules still hold
   inline SQL. Is "all production SQL is a separately executable file, executed
   as-is by `tests/integration/sql/`" the rule, and what is the exemption for
   trivially generated statements?
5. **What does a service owe before it ships?** Every route exercised is one
   candidate answer; `container_health`'s two unreferenced endpoints are the
   argument for it. Whether "exercised" means named, called, or asserted-on is
   the actual decision — the 32/35 figure above uses the weakest reading.
6. **What is "enough" per service, and who says so?** `container_health` has 5
   source files and 2 test files; `ops` has 23 and 29. Plan 84's deferral of
   `dbt_runner` and `scraper` was a judgement that was never re-examined.
7. **What does the agent skill check, and what can it not?** Some rules are
   mechanical (SQL location, mock library, a route with no test). Others need
   judgement. The skill should refuse to bless what it cannot check rather than
   implying coverage it does not have.
8. **What asserts the contract in CI, and what happens on violation?** A
   failing test, an advisory report, or a required review step — and whether
   existing violations are grandfathered explicitly or block the merge.
9. **What happens to `ARCHITECTURE.md:179`?** It is either rewritten as the
   contract or replaced by a pointer to it. Leaving both recreates exactly the
   drift this plan exists to end.

## The answers

**Answered 2026-08-31 (CAR-33), measured against the repository that day.** The
contract itself is [`docs/TESTING.md`](../TESTING.md); this section records the
decisions and the evidence behind them, so a later reader can see *why* rather
than only *what*. Where a question was answered "no rule", that is written as
such — a deliberate absence reads differently from an oversight.

### 1. What are the layers, now?

**Five, and the existing cut survives.** Layer 0 (config and contract tests) is
new only as a name: `tests/test_planning_docs.py`,
`tests/test_observability_config.py` and the four `test_*_compose_config.py`
files were already a distinct kind — facts about the repository that need no
service running — governed by nothing. Layers 1, 2, 3 and unit (now Layer 4)
are unchanged.

The newer suites were already instances of the existing layers and are now
named as such. The one that needed a real decision was `tests/airflow/` versus
`tests/integration/airflow/`, and the answer is **the interpreter**, not the
subject: `tests/integration/airflow/` runs from the isolated
`apache-airflow==3.2.0` venv the CI job builds, and `tests/airflow/` runs in the
main venv and therefore must not import `airflow`. Both existing files respect
that already — `test_notifications.py` by import discipline,
`test_coordination_admission.py` by reading DAG modules with `ast`.

`tests/integration/lakehouse/` is **dormant by decision** (Plan 125 pulled the
job in `863a2f2` rather than patch its fixture problem) and the contract makes
dormancy declarable, because an undeclared dormant suite is indistinguishable
from an orphaned one — see [G2](../TESTING.md#the-gap-list).

### 2. One mocking convention, or one per layer?

**One convention, and the boundary is neither the layer nor the thing being
mocked — it is which interpreter the suite runs in.**

In the main venv, patching is `mocker` (pytest-mock). `tests/integration/airflow/`
is exempt because its venv installs `apache-airflow`, `pytest`,
`psycopg2-binary` and `requests` and **pytest-mock is not among them** — the
`mocker` fixture does not exist in that interpreter. That is a cause CI states
and a test can read, not a preference.

Two clarifications shrink the apparent drift considerably. `from unittest.mock
import MagicMock` is a **value constructor, not a patching mechanism**; 37 files
import it and none of them are violations. And `monkeypatch` legitimately owns
process state (`setenv`, `delenv`, `setitem`, `chdir`), where `mocker` is the
wrong tool. Re-measured against those distinctions, the census is **10 files
mixing two patching mechanisms and 10 using `unittest.mock.patch`, of which 2
are the sanctioned Airflow exemption** — not the 21 mixed files this document
originally reported.

The tiebreak, where one was needed: every shared fixture in `tests/conftest.py`
is already built on `mocker`, so a test that uses `mock_cursor_context` and
then reaches for `unittest.mock.patch` has two patch stacks unwinding in an
order nobody chose.

### 3. What must never be mocked?

**Yes, the rule is expressible, in three parts of descending mechanical
strength — and the strongest one is already implemented in the tree.**

1. **A production SQL string must reach a real engine in some layer.** Patching
   the function that *runs* a query stays legitimate; what the rule forbids is
   that being the only thing that ever happens to a statement. The repair
   pattern already exists: `ops/coordination_drain.py` lifted its statements to
   module scope (`RUNNING_DETAIL_CLAIMS_SQL`, `task_instance_query()`,
   `gate_observation_query()`) specifically so
   `tests/integration/sql/test_ops_queries.py` can execute the real thing, and
   `_sensor_constant()` in that same file reads a statement out of
   `airflow/dags/sensors.py` with `ast` because the Layer 1 venv cannot import
   it. The checkable half is the converse: **a `SELECT` in a test file that
   appears in no `.sql` file and no production module is a paraphrase**, and a
   paraphrase is worse than no test.
2. **A route must be reachable through the app's routing table.** See question 5.
3. **The thing under test is not the thing you mock.** Judgement. The skill
   states it and refuses to certify it.

### 4. Where does SQL live?

**Yes — production SQL is a separately executable `.sql` file, loaded by
`shared.query_loader.load_query` and exposed through the service's
`queries.py`.** This was not invented here; six modules already do it and
Layer 1 already executes what they expose.

**Two exemptions.** *Structurally generated statements*, where the shape
depends on the arguments — `task_instance_query()`'s `VALUES` list is `(%s, %s)`
repeated per admitted task, which no file can hold. The exemption has a price:
the builder must be a **module-level function returning `(sql, params)`** so
Layer 1 can call it and execute the result. And *DDL and one-shot maintenance*,
which Flyway and `scripts/` own. An `f`-string interpolating a value into SQL
is neither exemption; it is a bug.

Re-measured at `.execute()` call sites rather than by counting `SELECT`-shaped
lines, the violation is **10 production modules**, not 16 — listed as
[G5](../TESTING.md#the-gap-list).

### 5. What does a service owe before it ships?

**Every route reached through the app's routing table by at least one test,
asserting the status code.** Of the three readings this document offered —
named, called, asserted-on — the decision is the middle one, made specific:
not "the handler function is called" but "the request goes through the app".

The evidence for that precision is stronger than the 32/35 figure this document
originally cited, and different from it. The real counts are **87 routes, 83 of
which are named somewhere in `tests/`** — and the weakest reading is worthless,
because it is cleared by the two endpoints that actually vanished in
production. `container_health`'s `/oneoff-processes` and `/project-status/{project}`
**do** have tests as of `4d6ed4a` (2026-08-26): `tests/test_container_health_app.py`
calls `app.active_oneoff_processes()` and `app.project_status()` directly.
There is **no `TestClient` anywhere in the repository for that service**, so
those tests passed throughout the eleven hours `/project-status/{project}` was
returning 404. A handler-level test cannot catch a routing failure, and did not.

Health and readiness endpoints are not exempt — they are precisely what another
service's drain logic reads.

### 6. What is "enough" per service, and who says so?

**Not a coverage percentage.** The floor is: every route reached through the
app, every production statement executed against a real engine, and every
failure branch another service's behaviour depends on. Coverage is an
instrument for finding gaps, not the definition of one.

**Who says so is a derived table**, in [`docs/TESTING.md`](../TESTING.md), with
one row per service directory on disk — so a new service with no row is a
contract violation rather than an omission from a list. Two services sit below
the floor: `container_health` (4 modules, no Layer 3, no `TestClient`) and
`dashboard` (7 modules, **zero test files anywhere**; its SQL is covered by
Layer 1 through `dashboard.queries`, its Python by nothing).

**Plan 84's four-month-old deferral is closed, and it splits.** `dbt_runner`'s
deferral was already lifted in practice — it has a Layer 3 suite and its
serving-snapshot SQL executes in Layer 1 against the real DuckDB artifact; it
meets the floor. `scraper`'s stands and is the largest genuine gap after
`dashboard`: ten source modules, one integration file, and that file is itself
invoked by no CI step.

### 7. What does the agent skill check, and what can it not?

Five rules are mechanical and are the skill's and the test's shared scope: CI
invokes every integration directory or it is declared dormant; patching matches
the directory-to-venv map; every route is reached through `app.routes`; every
service directory has a row in the "enough" table; no test file contains a
`SELECT` absent from every `.sql` file and production module. Each is derived
from the repository rather than from a checked-in inventory.

Three are judgement and the skill must **say so rather than imply coverage it
does not have**: whether the thing under test is the thing being mocked,
whether a failure branch matters to another service, and whether an assertion
is meaningful. Building both is CAR-34.

### 8. What asserts the contract in CI, and what happens on violation?

**A failing test, with a dated, owned waiver list that only shrinks.** Not an
advisory report — an advisory report is exactly what `ARCHITECTURE.md`'s
Testing Strategy section already was, and its failure mode is the reason this
plan exists.

Existing violations are grandfathered **explicitly**: each entry names its
owner plan and its date, the test fails on anything not in the list, and an
entry whose owner plan has closed is itself a failure. This keeps the contract
mergeable today without turning CAR-34 into the fix-everything work this plan's
non-goals defer, and it inherits `test_planning_docs.py`'s doctrine — coverage
is asserted, not enumerated; the waiver list is the one sanctioned exception
and is visible and countable.

### 9. What happens to `ARCHITECTURE.md:179`?

**Replaced by a pointer**, not rewritten in place. The contract is longer than
an architecture reference should carry, and it is the source of truth for a
skill and a test as well as for a reader — that argues for one addressable
document, `docs/TESTING.md`.

The section that remains is a pointer plus the two facts that are properties of
the *system* rather than of the suite, and so belong in an architecture
reference: **CI's database is not production's database** (bare `postgres:16`
and `minio/minio:latest` rather than the Compose definitions, which is why
Airflow's schema is absent), and **Airflow is tested in its own interpreter**,
mirroring its own container in production. No summary of the contract was left
behind.

## Corrections to this document's own measurements

The four figures below were measured on 2026-08-30 and are restated here rather
than edited above, because the reasoning that used them is still sound and the
difference between the two passes is itself informative.

| Stated 2026-08-30 | Measured 2026-08-31 | Why it moved |
|---|---|---|
| 21 files mix two mocking styles | **10 files mix two patching mechanisms**; 10 more use `unittest.mock.patch`, 2 of them legitimately | The first pass counted `from unittest.mock import MagicMock` as a competing style. It is a value constructor, not a patching mechanism |
| 16 modules still carry inline SQL | **10 modules** carry inline SQL at an `.execute()` call site | The first pass counted `SELECT`-shaped lines, which matched docstrings and comments |
| `/project-status/{project}` and `/oneoff-processes` are the only two routes with no reference anywhere in `tests/` | Both have had unit tests since `4d6ed4a` (2026-08-26). **Four routes are reached by no test through any routing table**, those two among them | Fixed between the planning session and this one — and the correction *strengthens* the argument, because the tests that exist call the handlers directly and were green throughout the 404 |
| 32 of 35 routes are at least named by a test | **83 of 87** | The first pass counted a subset of services |

One measurement the first pass did not make at all, and the largest single
finding of this ticket:

> **73 integration-marked tests across 11 files are invoked by no CI step.**
> `tests/integration/processing/` — 58 tests in 6 files, last touched
> 2026-08-30 — has **never** appeared in `.github/workflows/ci.yml`
> (`git log -S`, no results). `tests/integration/scraper/` (4) and
> `tests/integration/shared/` (4) are orphaned the same way;
> `tests/integration/lakehouse/` (7) is dormant by decision but undeclared.
> Whether any of them still pass is unknown, and finding out is Plan 162's
> census, not this ticket's.

This is the sharpest available argument for criterion 3. Nothing failed. The
tests were written, reviewed, merged and maintained, and no mechanism existed
that could notice they never ran.

## What this unblocks

Two pieces of work are waiting on this plan's answers, and both were scoped in
the conversation that produced it.

**The test-suite restructuring** (no plan document yet — deliberately, it cannot
be written until the questions above are answered):

- Split the `dbt build + test` job. It is **267s of a ~4 minute CI wall clock —
  the critical path** — and it is eight sequential suites sharing one Postgres
  and one MinIO, one of which builds an entire Airflow venv inline on every run.
  The job's name describes one of the eight.
- **Build dbt against the Plan 120 lake snapshot.** The fixture already exists
  and is already seeded (`scripts/seed_lake_snapshot_fixture.py`), and the step
  immediately after it is *"dbt build (DuckDB — empty data compilation check)"*.
  The production-shaped fixture is paid for and unused for the thing it was
  built for.
- **Run the suites against real Compose services**, locally before a PR and
  ideally in CI. CI uses bare `postgres:16` and `minio/minio:latest` service
  containers rather than the Compose definitions, which is why CI's database is
  not production's database — the same root as question 3.

**[Plan 139](plan_139_test_suite_maintenance.md)**, whose remaining stages are
maintenance against a standard that does not exist yet. Stage C profiles the
92s step, Stage D decides the coverage gate, Stage E designs the selector, and
each is easier to answer once this plan has said what the suite is *for*. Stage
F is the exception and should not wait — see below.

## Sequencing note

**Plan 139 Stage F should ship without waiting for this plan.** It closes the
CI-schema gap that produced incident 1, it is S-effort, and Plan 142 Stage 4
runs a maintenance window driven by exactly the coordination queries that gap
hides. It is also the worked example this plan's question 3 reasons from, so
landing it first makes that question concrete rather than theoretical.

Everything else in Plan 139 waits.

## Success criteria

1. A written testing contract exists, in one place, that a person can read and
   act on — and `ARCHITECTURE.md`'s Testing Strategy section either *is* it or
   points at it, with no second description left behind.
2. A skill under `.claude/skills/` reviews a change against that contract, and
   is explicit about which of its rules are mechanical and which are not.
3. A test in `tests/` fails when the contract and the repository disagree, in
   the shape of `test_planning_docs.py` and `test_coordination_admission.py`.
4. Every open question above is answered in writing, including the ones answered
   "no rule" — a deliberate absence is a decision and reads differently from an
   oversight.
5. The known violations are recorded as a gap list with an owner plan, not
   fixed here and not left implicit.

## Intersections

### Plan 139 — test suite maintenance

The plan this was nearly written into, and should not be. Plan 139 is
maintenance — recover the critical path, add markers, profile a step, fix the
CI schema gap. This plan sets the standard that maintenance is measured
against. Folding them together would give 139 two incompatible scopes.

Open: whether Stage C (profiling) and Stage E (impact selection) move to the
restructuring plan when it is written, leaving 139 as D, F and H. That is a
scope decision on an existing plan and is not taken here.

### Plan 84 — integration testing

Archived, and the source of the current layer model. Its explicit deferral of
`dbt_runner` and `scraper` is one of the decisions question 6 must re-examine.

### Plans 141 and 142 — the contract pattern

Not dependencies; precedents. Both pair a checked-in registry with tests that
assert reality matches it, which is the mechanism criterion 3 asks for.

### Plan 120 — CI lake snapshot

Supplies the fixture the restructuring needs. Complete, and its output is
already seeded in CI without being used for dbt.
