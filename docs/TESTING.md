# The Testing Contract

**Owner:** [Plan 161](plans/plan_161_testing_contract.md). **Measured against
the repository on 2026-08-31.**

This is the standard, not a description of the suite. Where the two disagree
the repository is wrong, and [the gap list](#the-gap-list) says so by name,
with an owner and a date.

`docs/ARCHITECTURE.md` points here and holds no second copy. That is
deliberate: the section this replaced was accurate in April 2026 and quietly
false by August, because nothing could tell the difference.

Three things carry this contract, and they are the same contract:

| Form | Where | Owner |
|---|---|---|
| For a person | this document | Plan 161 / CAR-33 |
| For a coding agent | [`.claude/skills/testing-contract/`](../.claude/skills/testing-contract/SKILL.md) | Plan 161 / CAR-34 |
| For CI | [`tests/test_testing_contract.py`](../tests/test_testing_contract.py) | Plan 161 / CAR-34 |

---

## The first rule

**Coverage is asserted, not enumerated.** A test you can silence by appending
to a list reproduces the defect it was written to catch. This is already the
house doctrine — `tests/test_planning_docs.py` and
`tests/test_observability_config.py::TestServiceHealthCoverage` both derive
their subject from the repository rather than from a checked-in inventory —
and everything below inherits it.

The one sanctioned exception is [the waiver list](#waivers), which is dated,
owned, and only ever shrinks.

---

## The layers

**Five, numbered by what they need in order to run.** A layer's number is its
dependency cost: 0 and 1 need nothing, and each step up adds a dependency that
has to exist before the layer can execute at all. That is the only ordering
that makes the numbers mean something, and it is what lets CI fail on the
cheapest thing first.

Plan 84's numbering did not do this. It gave the three integration tiers
numbers and left unit tests unnumbered beside them, so the fastest,
dependency-free tier read as the *last* one. The fix is a **+1 shift**: every
tier Plan 84 numbered keeps its position relative to the others and gains one,
and unit tests take their real place at the cheap end.

| Plan 84 | Here | Layer |
|---|---|---|
| — | **0** | Config and contract |
| unnumbered | **1** | Unit |
| Layer 1 | **2** | SQL smoke |
| Layer 2 | **3** | dbt model logic |
| Layer 3 | **4** | Service API integration |

The docstrings in `tests/` and the step names in `ci.yml` still carry Plan 84's
numbers. That sweep is mechanical and is [G11](#the-gap-list) — and the
asserting test checks it, so the two cannot drift apart again.

Layers 3 and 4 keep Plan 84's relative order on its own fail-fast rationale
rather than on strict cost, and Plan 162's job split may revisit it.

### Layer 0 — Config and contract tests

**For:** facts about the repository that are true or false without running any
service — Compose wiring, a config file the production image must accept, a
declared-versus-actual coverage claim.

**Runs in:** the unit job, and `tests/test_planning_docs.py` additionally runs
in the standalone **Documentation tests** job with `--noconftest`, so a broken
service import cannot take the planning assertions down with it.

**Lives in:** `tests/*.py` (top level) — `test_planning_docs.py`,
`test_observability_config.py`, `test_*_compose_config.py`,
`test_maintenance_running_set.py`, `test_deploy_script.py`.

**Needs:** nothing. No database, no MinIO, no service.

The top level of `tests/` is Layer 0's home and is not a dumping ground.
`tests/test_container_health_app.py` and `tests/test_container_health_collector.py`
sit there today and are unit tests of a service — see
[the gap list](#the-gap-list).

### Layer 1 — Unit tests

**For:** fast feedback on logic that needs no real dependency. Parser tests use
real HTML fixtures and mock nothing.

**Runs in:** its own CI job, ahead of everything that needs a service —
`pytest tests/ -m "not integration"`. Note what that selector means: it
collects **every** directory under `tests/` and runs whatever is not marked
`integration`, so a file's location does not decide whether it runs here. The
marker does.

**Needs:** nothing.

**Lives in:** `tests/<service>/`.

### Layer 2 — SQL smoke tests

**For:** catching schema breakage. Every statement the services issue executes
against a real engine with the migrations applied, and returns the columns the
caller expects. No business-logic assertions.

**Runs against two engines, not one:** Postgres with Flyway migrations applied
(`ops`, `processing`, the views), and the DuckDB file that
`dbt build --target duckdb` produced earlier in the same job (`dashboard`,
`dbt_runner`'s serving snapshots).

**Pattern:** per-test rollback. Each test opens a transaction, seeds minimal
rows, runs the query, asserts columns, rolls back. Nothing is committed.

```python
@pytest.fixture()
def db_conn(db_conn_factory):
    conn = db_conn_factory()
    conn.autocommit = False
    yield conn
    conn.rollback()   # teardown — no committed state left behind
    conn.close()
```

**Lives in:** `tests/integration/sql/`.

### Layer 3 — dbt model logic

**For:** transformation correctness. Known inputs produce known outputs.

dbt runs in a subprocess and cannot see an open transaction, so the isolation
strategy differs from Layer 2:

1. Seed source data on an **autocommit** connection, committed once for the
   whole session in the `seed_and_build` session-scoped autouse fixture.
2. `dbt build --select <selector> --target ci`, writing to `analytics_ci`.
3. Assert against `analytics_ci.<model>` through the `analytics_ci_cur`
   fixture.
4. Module-level teardown `TRUNCATE`s the source tables.

Individual test modules hold assertions only — no per-test seeding and no
per-test dbt invocation. The **ID scheme** keeps primary keys from colliding
across groups (100s VIN mapping, 200s price percentiles, 300s ops/staleness,
400s deal scores, 500s vehicle attributes, 600s price history, 700s days on
market, 800s price-event dedup, 900s vehicle snapshot).

**Lives in:** `tests/integration/dbt/`.

### Layer 4 — Service API integration

**For:** endpoint behaviour against a real database. FastAPI `TestClient`, the
full request path — router, business logic, SQL, DB. **No mocked DB
connections in this layer**; a `verify_cur` autocommit fixture reads committed
state after a request without sharing its transaction.

Auth is exercised for real: `AUTH_EMAIL_SALT` is set in the test environment
and `auth_email_hash` computes the hash the middleware will look for.

**Lives in:** `tests/integration/<service>/`.

### Where the newer suites sit

The layers Plan 84 wrote down predate the archiver, the coordination contract,
the Airflow 3 migration and the DAG-level suites. They are still the right cut;
the newer suites were already instances of them and are now named as such.

| Directory | Layer | Note |
|---|---|---|
| `tests/dbt/` | 0 | The dbt project's cadence tags and selectors, checked as YAML and filesystem facts. No dbt invocation, no database — Layer 0 despite not sitting at the top level, which is why it needs a row here at all |
| `tests/airflow/` | 1 | Unit tests of DAG modules. Runs in the **main** venv, so it must not import `airflow` — `test_notifications.py` and `test_coordination_admission.py` both avoid it deliberately, by import discipline and by AST reading |
| `tests/integration/airflow/` | 4 | Runs in the **isolated Airflow venv** (`apache-airflow==3.2.0`), because Airflow's starlette pin conflicts with the FastAPI services' |
| `tests/lakehouse/` | 1 | Unit tests of `scripts/` lakehouse tooling; the source is not a `lakehouse/` package |
| `tests/integration/lakehouse/` | 4 | **Dormant by decision** — Plan 125 pulled the job (`863a2f2`) rather than patch its fixture problem |
| `tests/scripts/` | 1 | Unit tests of one-shot and recovery scripts |
| `tests/integration/scripts/` | 4 | Recovery scripts against a real database — rollback, receipt and protected-table properties |

---

## One convention per concern

### Mocking: `mocker`, everywhere, no exemptions

**Patching is `mocker` (pytest-mock). There is no layer, directory, or venv
where something else is correct.** Every shared fixture in `tests/conftest.py`
already is: `mock_db_conn`, `mock_cursor_context`, `mock_db_sql_error`,
`mock_requests`. Adopting anything else in a test that uses one of them means
two patch stacks unwinding in an order nobody chose.

Two clarifications dissolve most of the apparent drift:

- **`from unittest.mock import MagicMock` is fine and always was.** It is a
  value constructor, not a patching mechanism. 37 files import it; that is not
  a violation and never was. **`unittest.mock.patch` is the violation.**
- **`monkeypatch` owns process state**: `setenv`, `delenv`, `setitem`, `chdir`.
  It is the right tool there and `mocker` is not. `monkeypatch.setattr` on an
  application object is the violation — that is `mocker.patch`'s job.

#### The one place `mocker` is unavailable is a defect, not an exemption

`tests/integration/airflow/` runs from the isolated venv the CI job builds, and
that venv installs `apache-airflow`, `pytest`, `psycopg2-binary` and
`requests` — **pytest-mock is not among them**, so the `mocker` fixture does
not exist in that interpreter and its two files use `unittest.mock.patch`.

That is a missing line in one `pip install`, and the contract does not bend
around it. `pytest-mock` declares exactly one dependency — `pytest` — which
that venv already installs, so none of the starlette/fastapi conflict that
forced the venv's existence in the first place applies to it. The real cause is
that the venv was built to run Airflow, and its *test* dependencies were never
thought about separately.

Nor do those two files need `unittest.mock` for anything: what they patch is
`requests.post`, `requests.get`, `time.sleep`, and one `patch.object` on a DAG
module's `post_json`. All four are ordinary `mocker.patch` calls.

**The fix is to install pytest-mock in that venv and convert the two files** —
[G4](#the-gap-list), owned by Plan 162. Until then the asserting test waives
those two files by name, with that gap as the reason. A waiver is how a defect
waits its turn; it is not a second convention.

The general form, because this will come up again: **"the environment cannot do
it" is a fact about the environment, and the first question is always whether
the environment is right.** A convention that forks to accommodate a tooling
gap has stopped being a convention.

### What must never be mocked

Three rules, in descending order of how mechanically checkable they are.

**1. A production SQL string must reach a real engine in some layer.**

This is the rule the `search_path` incident wanted. `ops/coordination_drain.py`
queried `task_instance` unqualified and `public.detail_scrape_claims` instead
of `ops.`; all three statements failed in production, `_database_count`
returned `unknown`, unknown fails closed, and the first production deploy of
Plan 142's coordination gate drained forever. It escaped because
`tests/ops/test_coordination_drain.py` patches `_database_count` itself — the
only string that ever reached a cursor was the literal `"SELECT evidence"`.

Patching the function that *runs* a query is legitimate and stays legitimate;
it is how the evidence-aggregation logic is testable at all. What the rule
forbids is that being the **only** thing that ever happens to a statement.

The repair is already in the tree and is the pattern to copy: the drain's
statements were lifted to module scope — `RUNNING_DETAIL_CLAIMS_SQL`,
`task_instance_query()`, `gate_observation_query()` — precisely so
`tests/integration/sql/test_ops_queries.py` can import and execute the real
thing.

**A paraphrase of production SQL in a test file is worse than no test.** A
paraphrase passes forever; it is a copy that cannot notice the original
changed. So the statement a test executes must be *the* statement, obtained in
one of three ways, in this order of preference:

1. **Load the `.sql` file.** No import of the owning module, no dependencies,
   works from any interpreter. This is what [where SQL lives](#where-sql-lives)
   buys, and it is why that rule exists.
2. **Import the module-level constant or `(sql, params)` builder**, where the
   statement is generated and no file can hold it.
3. **Read it out of the source with `ast`** — last resort.

#### `ast` should be rare, and each use names a defect elsewhere

`_sensor_constant()` in `test_ops_queries.py` reads `GATE_OBSERVATION_SQL` out
of `airflow/dags/sensors.py` this way. It is correct, and it is worth being
precise about *why* it was necessary, because the reason is not "some
statements can't be imported" as a fact of life.

`sensors.py` imports `airflow.providers.postgres.hooks.postgres` and
`airflow.sdk.bases.sensor` **at module scope**, and the SQL-smoke suite runs in
the main venv, which has no Airflow. So `import sensors` raises before reaching
the constant. Nothing about the *statement* resists import — a module around it
does.

The underlying cause is narrower still: **`airflow/dags/` has no `.sql`
convention and cannot reach one.** No module under it imports `shared` at all,
so `shared.query_loader` — the thing every other service uses — is not
available there, and the DAG tree is the only part of the repository where
option 1 is structurally impossible. That is [G12](#the-gap-list).

So the answer to "should `ast` be used more widely" is **no, and a growing
count of `ast` readers is a signal to act on, not a pattern to spread.** Each
one marks a statement that ought to live in a file and does not. Where it is
genuinely the only option, the reader carries a comment naming the import that
forced it, so the next person can tell a constraint from an accident —
`_sensor_constant()`'s docstring already does.

The mechanically checkable half of this rule is one direction only: **every
`.sql` file and every module-level statement is executed by Layer 2.** The
other direction — deciding whether a `SELECT` in a test file is a paraphrase of
production or legitimate fixture seeding — is judgement, and the skill flags it
rather than failing on it.

**2. A route must be reachable through the app's routing table.**

Calling the handler function proves the function works. It proves nothing about
the URL, the method, the prefix, or whether the router was included at all.

`container_health` is the worked example. Its `/project-status/{project}` 404d
for roughly eleven hours on 2026-08-30 with nothing reporting it — and
`tests/test_container_health_app.py` *does* cover both of its interesting
endpoints, by calling `app.active_oneoff_processes()` and `app.project_status()`
directly. There is no `TestClient` anywhere in the repository for that service.
A handler-level test could not have caught the failure, and did not.

**3. The thing under test is not the thing you mock.**

Judgement, not mechanism. The skill states it and refuses to certify it.

### The harness must not decide the outcome

**A test's result must be determined by the code under test, not by the
environment it happens to run in.** Where a test genuinely depends on an
environmental capability, it declares that dependency — a skip with a reason, a
mocked boundary with a comment saying which capability forced it — rather than
letting the environment silently decide.

This is a separate rule from the three above, and it has bitten this repository
twice in two weeks in opposite directions.

**The benign direction — it fails where it should pass.**
`tests/airflow/test_prune_task_logs_dag.py` built a real symlink, which needs
elevated privileges on Windows, so the test failed on a developer machine for a
reason that had nothing to do with pruning. `21333ab` is the repair and is the
pattern to copy: mock `Path.is_symlink` and say why in a comment —
*"the behavior owned here is our refusal to traverse a run directory that the
filesystem classifies as a symlink, not pathlib's OS integration."* That
sentence is the rule working. It draws the line between the capability the test
depends on and the behaviour the test owns, and it makes rule 3 above come out
right rather than wrong: the code owns its *reaction* to `is_symlink`, not
`is_symlink` itself.

**The dangerous direction — it passes where it should fail.**
`tests/test_planning_docs.py` imports `scripts`, which resolves only when the
repository root is on `sys.path`. The root carries an `__init__.py`, so pytest
walks up for the package root — and whether it stops at the repo or climbs past
it depends on **whether the checkout directory name is a valid Python
identifier.** Measured on one machine, one OS, one commit, changing only the
directory:

| Checkout directory | `pytest --noconftest tests/test_planning_docs.py` |
|---|---|
| `cartracker-scraper` | 35 passed |
| `new_car_tracker` (what CI uses) | **2 failed** |

Nothing about the code differed. The harness decided.

This one is worse than the symlink case for two compounding reasons, and both
are the subject of rules elsewhere in this document: it produced a **false
green** on the developer machine, and the only job that would have caught it —
`Documentation tests` — runs solely on docs-only changesets, so it was skipped
in 29 of the last 40 CI runs. A test that cannot fail where it is run is the
[G1](#the-gap-list) failure in miniature.

**The practical rules, then:**

- A test that depends on an environmental capability declares it. An unexplained
  mock of a filesystem, clock, platform or path primitive is a finding.
- A green run on a developer machine is not evidence the test passes in CI, and
  a CI job that is usually *skipped* is not evidence of anything at all. Check
  `conclusion`, never just the run's colour.
- Where an environment difference is reproducible, reproduce it — a throwaway
  worktree at a matching path costs a minute and settles the question that
  speculation cannot.

### Where SQL lives

**Production SQL is a separately executable `.sql` file, loaded by
`shared.query_loader.load_query` and exposed through the service's
`queries.py`.** Six modules already do this — `ops`, `archiver`, `processing`,
`scraper`, `dbt_runner`, and `archiver/processors/lake_snapshot_selectors.py` —
and `tests/integration/sql/` executes what they expose.

Two exemptions, and no others:

- **Structurally generated statements.** Where the shape depends on the
  arguments — `task_instance_query()`'s `VALUES` list is `(%s, %s)` repeated
  per admitted task — a file cannot hold it. The builder is then a **module-level
  function returning `(sql, params)`**, so Layer 2 can call it and execute the
  result. That is the standard the drain now meets.
- **DDL and one-shot maintenance** inside a migration or a script that runs
  once. Flyway owns `db/migrations/`.

An `f`-string interpolating a value into SQL is neither exemption; it is a bug.

**`airflow/dags/` is the one tree where this rule cannot currently be
followed** — no module under it imports `shared`, so `query_loader` is not
reachable and there is no `.sql` directory to put a statement in. That is a
gap ([G12](#the-gap-list)), not a third exemption, and it is what forces the
tree's only `ast` reader.

### Endpoint coverage: what a service owes before it ships

**Every route is reached through the app's routing table by at least one test,
and the test asserts the status code.**

"Named somewhere in `tests/`" is the weakest reading and is not the rule — 83
of 87 routes clear that bar today, including the two that vanished in
production. "Reached through the routing table" is the rule because it is the
only reading that fails when a route stops existing.

Health and readiness endpoints are not exempt. They are the endpoints another
service's drain logic depends on.

### What "enough" means, per service

Not a coverage percentage. **The floor is: every route reached through the app,
every production statement executed against a real engine, and every failure
branch that another service's behaviour depends on.** Coverage percentage is an
instrument for finding gaps, not the definition of one.

Measured on 2026-08-31 (`*.py` excluding `__init__.py`):

| Service | Src | Layer 1 | Layer 4 | Standing |
|---|---|---|---|---|
| `ops` | 19 | 18 | 9 | Meets the floor bar the two `/maintenance` routes |
| `archiver` | 20 | 15 | 9 | Meets the floor |
| `processing` | 11 | 10 | 6 | Layer 4 exists and **runs nowhere** |
| `scraper` | 10 | 9 | 1 | Plan 84 deferred it; **re-examined below** |
| `dbt_runner` | 4 | 3 | 1 | Plan 84 deferred it; **re-examined below** |
| `shared` | 14 | 11 | 1 | Meets the floor |
| `container_health` | 4 | 2 (misplaced) | 0 | **Below the floor** — no `TestClient`, no Layer 4 |
| `dashboard` | 7 | 0 | 0 | **Below the floor** — its SQL is covered by Layer 2, its Python by nothing |

**Plan 84's deferral of `dbt_runner` and `scraper` is closed, split:**

- **`dbt_runner` — deferral lifted, and it was already lifted in practice.** It
  has a Layer 4 suite and its serving-snapshot SQL is executed in Layer 2
  against the real DuckDB artifact. It meets the floor.
- **`scraper` — deferral lifted, and it is the largest genuine gap after
  `dashboard`.** Ten source modules, one integration file
  (`test_blocked_cooldown.py`), which itself runs in no CI step. Six of its
  eight routes are the fetch path the whole pipeline sits on. Owner: Plan 162.

**Who says so:** this table. It is derived, not curated — a new service
directory with no row is a violation of the contract, not an omission from a
list.

---

## What CI asserts, and what happens on violation

**[`tests/test_testing_contract.py`](../tests/test_testing_contract.py) fails
when this document and the repository disagree.** Not an advisory report. An
advisory report is what `ARCHITECTURE.md`'s Testing Strategy section already
was.

It runs in the unit job, needs nothing, and is therefore Layer 0 — a fact about
the repository, like the rest of that layer.

Mechanically checked today. **The `Asserted by` column is the load-bearing
one** — `test_every_asserted_rule_names_a_real_test` reads it and fails if a
cell names a function that does not exist, so this table cannot claim a check
the suite does not implement:

| Rule | Asserted by | How it is checked without a curated list |
|---|---|---|
| Every `tests/integration/<dir>` is invoked by a named CI step, or declared dormant with a reason | `test_every_integration_suite_is_invoked_by_a_ci_step` | Parse `.github/workflows/ci.yml` for `pytest tests/integration/...` invocations; compare to the directories on disk |
| Patching is `mocker`, everywhere | `test_patching_is_mocker_everywhere` | AST-walk for `unittest.mock.patch` and for `monkeypatch.setattr` on an application object. No venv carve-out — the two Airflow files are waived by name against [G4](#the-gap-list) |
| Every route is reached through the app's routing table | `test_every_route_is_reached_through_the_apps_routing_table`, `test_no_route_is_hidden_from_the_schema_this_rule_reads` | Walk each FastAPI app's `app.routes`; compare to the verb/path literals tests actually request |
| Every service directory has a row in the "enough" table | `test_every_service_directory_has_a_row_in_the_enough_table` | Compare this document's table to the service directories on disk |
| Every `.sql` file is executed by a Layer 2 test | `test_every_production_sql_file_is_touched_by_a_layer_2_test` | Collect what `tests/integration/sql/` imports and executes; compare to what `queries.py` exposes |
| Every `Layer N` mention in `tests/` and `ci.yml` matches this document | `test_every_layer_number_in_the_code_matches_the_contract`, `test_every_test_directory_is_assigned_a_layer` | Regex both, compare to the headings here — this is what stops [G11](#the-gap-list) recurring |
| Every pytest invocation in `ci.yml` sets `PYTHONPATH` | `test_every_pytest_invocation_in_ci_sets_pythonpath` | Parse the workflow's `run:` steps; a pytest step without it is [G13](#the-gap-list)'s failure |

### Specified here, not yet asserted

Rules this document commits to that **no test implements yet**. They are not
waivers — a waiver grandfathers a known violation of a rule that is enforced,
while these are rules with no enforcement at all, so a violation of one is
invisible rather than counted.

| Rule | Why it is not asserted | Owner |
|---|---|---|
| Every module-level SQL statement in a production module is executed by a Layer 2 test | The Layer 2 check reads `.sql` files. Inline SQL at an `.execute()` call site is the opposite direction and is measured by nothing — [G5](#the-gap-list)'s ten modules | Plan 162 |

Until 2026-08-31 this rule was not in this section. It was a clause inside the
Layer 2 row above — *"every `.sql` file **and module-level statement**"* — which
read as enforced, was never implemented, and was found by running the suite
during Plan 162's first measurement rather than by anything failing. That is
the drift this whole document exists to make impossible, occurring inside the
document itself, which is why the `Asserted by` column now exists.

Not mechanically checkable, and the skill must **say so rather than imply
coverage it does not have**: whether the thing under test is the thing being
mocked, whether a failure branch matters to another service, whether an
assertion is meaningful, and whether a `SELECT` in a test file is a paraphrase
of production or legitimate fixture seeding. The last of those looks
mechanical and is not — fixture seeds are SQL in test files too, and a checker
that cannot tell them apart would fail on correct code.

### Waivers

Today's violations do not block the next merge. They sit in **one dated waiver
list, each entry with an owner plan** — the `*_WAIVERS` tuples in
[`tests/test_testing_contract.py`](../tests/test_testing_contract.py) — and the
asserting test fails on anything not in it. The list only shrinks; adding to it
is a decision, not a convenience, and an entry whose owner plan has closed is
itself a failure.

Three properties make that more than a promise, and each is its own assertion:

- **A waiver that no longer describes a violation fails.** Without it, repairs
  pile up behind a list that still claims they are outstanding, and the list
  stops describing anything. This is what forces the deletion.
- **A waiver whose owner plan has been archived fails**, checked against
  `docs/planning/completed_plans.md`. A waiver that outlives its owner is
  waiting for nobody.
- **A waiver naming a gap entry not in the list below fails.** The gap entry is
  the reason; delete the reason and the waiver has none.

**Dormancy is declared through the same list, not a second mechanism.** A
waiver already carries a reason, an owner and a date, and it already only
shrinks. `tests/integration/lakehouse/` is waived against [G2](#the-gap-list)
where the orphaned suites are waived against [G1](#the-gap-list), which is
exactly what G2 asked for: dormant and orphaned, told apart by inspection.

---

## The gap list

Every entry is a measured violation of the contract above, as of 2026-08-31.
Recorded here, fixed elsewhere — Plan 161's non-goals hold.

| # | Violation | Measure | Owner |
|---|---|---|---|
| G1 | **73 integration-marked tests in 11 files that no CI step invokes.** `tests/integration/processing/` (58 tests, 6 files) has **never** appeared in `ci.yml`, and was last touched 2026-08-30. `tests/integration/scraper/` (4) and `tests/integration/shared/` (4) are orphaned the same way. Whether they still pass is unknown | `git log -S` over `ci.yml`; `pytest` invocations in `ci.yml` versus `tests/integration/*/` | Plan 162 |
| G2 | `tests/integration/lakehouse/` (7 tests) is dormant **by decision** — Plan 125 pulled the job in `863a2f2`. Correct, but undeclared; it is indistinguishable from G1 by inspection | — | Plan 162 |
| G4 | **34 test files patch with something other than `mocker`** — 17 import `patch` from `unittest.mock`, 17 use `monkeypatch.setattr`, and two do both. The second half is what the 2026-08-31 census did not count: every one of those 17 targets a module object rather than process state, so `mocker.patch` is the tool for all of them. Two of the 34 are `tests/integration/airflow/`, whose venv **does not install pytest-mock** — a missing argument on one `pip install` line in `ci.yml`, not a conflict: `pytest-mock` depends only on `pytest`, which that venv already has. Fix the venv first, then convert all 34 | `tests/test_testing_contract.py`, AST census | Plan 162 |
| G5 | **Inline SQL at `.execute()` call sites in 10 production modules**, against six that use the loader: `ops/routers/{coordination,deploy,scrape,users}.py`, `archiver/processors/{pack_bronze_html,delete_packed_source_html,flush_silver_observations,flush_staging_events}.py`, `shared/db.py`, `shared/duckdb_s3.py` | `.execute(` with a literal first argument | Plan 162 |
| G6 | **Twelve routes reached by no test through any routing table**, not the four measured by eye: all four of `container_health`'s, including `/health` and `/metrics` (there is no `TestClient` in that service at all), and eight of `ops`' — the two `/maintenance` routes already named, plus `GET /coordination/status`, `POST /coordination/begin-validation`, `POST /coordination/cancel` and the three `/admin/snapshots/adaptive-refresh/` reads. The three coordination routes are the same surface whose drain hung Plan 142's first deploy | `tests/test_testing_contract.py`: each app's real `app.routes` versus the request literals in that service's own test directories | Plan 162 |
| G7 | **`dashboard/`: 7 modules, 0 test files.** Its SQL is covered by Layer 2 through `dashboard.queries`; its Python is covered by nothing | — | Plan 162 |
| G8 | **`scraper/`: Plan 84's four-month-old deferral, now lifted.** One integration file, which is itself in G1 | — | Plan 162 |
| G9 | `tests/test_container_health_app.py` and `tests/test_container_health_collector.py` are Layer 1 tests sitting in Layer 0's directory. `container_health` has no `tests/container_health/` and no Layer 4 at all | — | Plan 162 |
| G13 | **Two known harness-decides-the-outcome tests.** `Documentation tests` fails on every docs-only changeset because it is the only pytest step in `ci.yml` without `PYTHONPATH` — latent since `bf989fc` (2026-08-31 09:00 CDT) and first run ~7h later by PR #299, which is why it went unnoticed. `tests/airflow/test_prune_task_logs_dag.py` is the same class already repaired, in `21333ab`, and is the pattern | Reproduced on master's tree in a worktree named `new_car_tracker`: 2 failed, 33 passed | **Plan 146 Stage 1 (CAR-42)** for the CI fix; the rule itself is here |
| G11 | **The layer numbers in the code are Plan 84's, not this document's.** Docstrings across `tests/` and two step names in `ci.yml` say "Layer 1 — SQL smoke" and "Layer 3 — API integration", which are Layers 2 and 4 here. Mechanical sweep; the asserting test covers it afterwards so the two cannot drift again | `grep -rn 'Layer [0-9]' tests/ .github/` | Plan 162 |
| G12 | **`airflow/dags/` has no `.sql` convention and cannot reach one.** No module under it imports `shared`, so `shared.query_loader` is unavailable and the DAG tree is the only place in the repository where "production SQL is a `.sql` file" is structurally impossible. This is what forces the single legitimate `ast` reader, `_sensor_constant()` | `grep -rn 'from shared' airflow/dags/` returns nothing | Plan 162 |
| G14 | **54 of 76 production `.sql` files are named by no Layer 2 test.** All 19 under `processing/sql/`, all 8 under `ops/sql/`, all 3 under `scraper/sql/`, 17 of `archiver/`'s, the 6 `dashboard/sql/data_health_*` files and `airflow/sql/delete_stale_emails.sql`. `test_ops_queries.py` and `test_processing_queries.py` are named for the services whose statements they should execute, import nothing from either `queries.py`, and **paraphrase the SQL instead** — which the rule above calls worse than no test, because a paraphrase passes forever | `tests/test_testing_contract.py`, at the weakest reading of "executed": a file counts as covered if Layer 2 so much as names it. A stricter check can only find more | Plan 162 |
| G10 | **The unit job's coverage is measured and discarded** — `--cov --cov-report=term-missing`, no threshold, no artifact. Worse, `[tool.coverage.run] source` names six packages and omits `container_health`, `dashboard`, `scripts` and `airflow/dags`, so **the two services below the floor are the two the instrument cannot see** | `ci.yml:131`, `pyproject.toml` | Plan 162 — Stage D, reassigned 2026-08-31 |

---

## What this contract does not decide

- **The CI restructure.** Splitting the 267s `dbt build + test` job, building
  dbt against the Plan 120 lake snapshot, and running the suites against the
  real Compose definitions are [Plan 162](plans/plan_162_testing_census_and_restructure.md).
- **A coverage threshold.** [Plan 162](plans/plan_162_testing_census_and_restructure.md)
  owns whether there is a gate at all, and whether `airflow/dags/` and
  `dashboard/` join the coverage configuration — Stage D moved there entire
  when Plan 139 archived on 2026-08-31. This document says only that a
  percentage is not the definition of enough.
- **Impact-based test selection.** Plan 139 Stage E specified it and has since
  moved to Plan 162; nothing here changes its design.
- **Fixing anything in the gap list.**
