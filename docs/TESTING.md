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
| For a coding agent | `.claude/skills/` reviewer | Plan 161 / CAR-34 |
| For CI | a test in `tests/` that fails when this document and the repository disagree | Plan 161 / CAR-34 |

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

**A test in `tests/` fails when this document and the repository disagree.**
Not an advisory report. An advisory report is what `ARCHITECTURE.md`'s Testing
Strategy section already was.

Mechanically checkable today, and therefore CAR-34's scope:

| Rule | How it is checked without a curated list |
|---|---|
| Every `tests/integration/<dir>` is invoked by a named CI step, or declared dormant with a reason | Parse `.github/workflows/ci.yml` for `pytest tests/integration/...` invocations; compare to the directories on disk |
| Patching is `mocker`, everywhere | AST-walk for `unittest.mock.patch` and for `monkeypatch.setattr` on an application object. No venv carve-out — the two Airflow files are waived by name against [G4](#the-gap-list) |
| Every route is reached through the app's routing table | Walk each FastAPI app's `app.routes`; compare to the verb/path literals tests actually request |
| Every service directory has a row in the "enough" table | Compare this document's table to the service directories on disk |
| Every `.sql` file and module-level statement is executed by a Layer 2 test | Collect what `tests/integration/sql/` imports and executes; compare to what `queries.py` exposes |
| Every `Layer N` mention in `tests/` and `ci.yml` matches this document | Regex both, compare to the headings here — this is what stops [G11](#the-gap-list) recurring |

Not mechanically checkable, and the skill must **say so rather than imply
coverage it does not have**: whether the thing under test is the thing being
mocked, whether a failure branch matters to another service, whether an
assertion is meaningful, and whether a `SELECT` in a test file is a paraphrase
of production or legitimate fixture seeding. The last of those looks
mechanical and is not — fixture seeds are SQL in test files too, and a checker
that cannot tell them apart would fail on correct code.

### Waivers

Today's violations do not block the next merge. They sit in **one dated waiver
list, each entry with an owner plan**, and the asserting test fails on anything
not in it. The list only shrinks; adding to it is a decision, not a
convenience, and an entry whose owner plan has closed is itself a failure.

---

## The gap list

Every entry is a measured violation of the contract above, as of 2026-08-31.
Recorded here, fixed elsewhere — Plan 161's non-goals hold.

| # | Violation | Measure | Owner |
|---|---|---|---|
| G1 | **73 integration-marked tests in 11 files that no CI step invokes.** `tests/integration/processing/` (58 tests, 6 files) has **never** appeared in `ci.yml`, and was last touched 2026-08-30. `tests/integration/scraper/` (4) and `tests/integration/shared/` (4) are orphaned the same way. Whether they still pass is unknown | `git log -S` over `ci.yml`; `pytest` invocations in `ci.yml` versus `tests/integration/*/` | Plan 162 |
| G2 | `tests/integration/lakehouse/` (7 tests) is dormant **by decision** — Plan 125 pulled the job in `863a2f2`. Correct, but undeclared; it is indistinguishable from G1 by inspection | — | Plan 162 |
| G3 | **CI's database does not create Airflow's schema**, so the drain's `airflow.task_instance` and `airflow.dag_run` statements cannot be executed by any layer — the exact class of bug that hung Plan 142's first deploy | — | **Plan 139 Stage F / CAR-36**, shipping ahead of Plan 162 |
| G4 | **10 test files mix two patching mechanisms; 10 more use `unittest.mock.patch`.** Eight are `tests/scripts/` (7) and `tests/airflow/test_notifications.py`, all in the main venv where `mocker` is available. The other two are `tests/integration/airflow/`, whose venv **does not install pytest-mock** — a missing argument on one `pip install` line in `ci.yml`, not a conflict: `pytest-mock` depends only on `pytest`, which that venv already has. Fix the venv first, then convert all 20 | AST census, 2026-08-31 | Plan 162 |
| G5 | **Inline SQL at `.execute()` call sites in 10 production modules**, against six that use the loader: `ops/routers/{coordination,deploy,scrape,users}.py`, `archiver/processors/{pack_bronze_html,delete_packed_source_html,flush_silver_observations,flush_staging_events}.py`, `shared/db.py`, `shared/duckdb_s3.py` | `.execute(` with a literal first argument | Plan 162 |
| G6 | **Four routes reached by no test through any routing table**: `container_health`'s `/oneoff-processes` and `/project-status/{project}` (handler-level tests only, no `TestClient` in the service), and `ops`' `POST /maintenance/evict-delisted-cooldowns` and `POST /maintenance/reconcile-cooldown-cohorts` | `app.routes` versus test request literals | Plan 162 |
| G7 | **`dashboard/`: 7 modules, 0 test files.** Its SQL is covered by Layer 2 through `dashboard.queries`; its Python is covered by nothing | — | Plan 162 |
| G8 | **`scraper/`: Plan 84's four-month-old deferral, now lifted.** One integration file, which is itself in G1 | — | Plan 162 |
| G9 | `tests/test_container_health_app.py` and `tests/test_container_health_collector.py` are Layer 1 tests sitting in Layer 0's directory. `container_health` has no `tests/container_health/` and no Layer 4 at all | — | Plan 162 |
| G11 | **The layer numbers in the code are Plan 84's, not this document's.** Docstrings across `tests/` and two step names in `ci.yml` say "Layer 1 — SQL smoke" and "Layer 3 — API integration", which are Layers 2 and 4 here. Mechanical sweep; the asserting test covers it afterwards so the two cannot drift again | `grep -rn 'Layer [0-9]' tests/ .github/` | Plan 162 |
| G12 | **`airflow/dags/` has no `.sql` convention and cannot reach one.** No module under it imports `shared`, so `shared.query_loader` is unavailable and the DAG tree is the only place in the repository where "production SQL is a `.sql` file" is structurally impossible. This is what forces the single legitimate `ast` reader, `_sensor_constant()` | `grep -rn 'from shared' airflow/dags/` returns nothing | Plan 162 |
| G10 | **The unit job's coverage is measured and discarded** — `--cov --cov-report=term-missing`, no threshold, no artifact. Worse, `[tool.coverage.run] source` names six packages and omits `container_health`, `dashboard`, `scripts` and `airflow/dags`, so **the two services below the floor are the two the instrument cannot see** | `ci.yml:131`, `pyproject.toml` | Plan 139 Stage D |

---

## What this contract does not decide

- **The CI restructure.** Splitting the 267s `dbt build + test` job, building
  dbt against the Plan 120 lake snapshot, and running the suites against the
  real Compose definitions are [Plan 162](plans/plan_162_testing_census_and_restructure.md).
- **A coverage threshold.** Plan 139 Stage D owns whether there is a gate at
  all; this document says a percentage is not the definition of enough.
- **Impact-based test selection.** Plan 139 Stage E specified it; nothing here
  changes it.
- **Fixing anything in the gap list.**
