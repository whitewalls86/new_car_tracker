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

Five, not four. The fourth and fifth existed before this document and were
governed by nothing.

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

### Layer 1 — SQL smoke tests

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

### Layer 2 — dbt model logic

**For:** transformation correctness. Known inputs produce known outputs.

dbt runs in a subprocess and cannot see an open transaction, so the isolation
strategy differs from Layer 1:

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

### Layer 3 — Service API integration

**For:** endpoint behaviour against a real database. FastAPI `TestClient`, the
full request path — router, business logic, SQL, DB. **No mocked DB
connections in this layer**; a `verify_cur` autocommit fixture reads committed
state after a request without sharing its transaction.

Auth is exercised for real: `AUTH_EMAIL_SALT` is set in the test environment
and `auth_email_hash` computes the hash the middleware will look for.

**Lives in:** `tests/integration/<service>/`.

### Layer 4 — Unit tests

**For:** fast feedback on logic that needs no real dependency. Parser tests use
real HTML fixtures and mock nothing.

**Lives in:** `tests/<service>/`.

### Where the newer suites sit

The four layers Plan 84 wrote down predate the archiver, the coordination
contract, the Airflow 3 migration and the DAG-level suites. They are still the
right cut; the newer suites were already instances of them and are now named as
such.

| Directory | Layer | Note |
|---|---|---|
| `tests/airflow/` | 4 | Unit tests of DAG modules. Runs in the **main** venv, so it must not import `airflow` — `test_notifications.py` and `test_coordination_admission.py` both avoid it deliberately, by import discipline and by AST reading |
| `tests/integration/airflow/` | 3 | Runs in the **isolated Airflow venv** (`apache-airflow==3.2.0`), because Airflow's starlette pin conflicts with the FastAPI services' |
| `tests/lakehouse/` | 4 | Unit tests of `scripts/` lakehouse tooling; the source is not a `lakehouse/` package |
| `tests/integration/lakehouse/` | 3 | **Dormant by decision** — Plan 125 pulled the job (`863a2f2`) rather than patch its fixture problem |
| `tests/scripts/` | 4 | Unit tests of one-shot and recovery scripts |
| `tests/integration/scripts/` | 3 | Recovery scripts against a real database — rollback, receipt and protected-table properties |

---

## One convention per concern

### Mocking: `mocker`, and the boundary is the interpreter

**In the main venv — every suite except `tests/integration/airflow/` — patching
is `mocker` (pytest-mock).** Every shared fixture in `tests/conftest.py`
already is: `mock_db_conn`, `mock_cursor_context`, `mock_db_sql_error`,
`mock_requests`. Adopting anything else in a test that uses one of them means
two patch stacks unwinding in an order nobody chose.

Three clarifications that dissolve most of the apparent drift:

- **`from unittest.mock import MagicMock` is fine and always was.** It is a
  value constructor, not a patching mechanism. 37 files import it; that is not
  a violation and never was. **`unittest.mock.patch` is the violation.**
- **`monkeypatch` owns process state**: `setenv`, `delenv`, `setitem`, `chdir`.
  It is the right tool there and `mocker` is not. `monkeypatch.setattr` on an
  application object is the violation — that is `mocker.patch`'s job.
- **`tests/integration/airflow/` is exempt, on a real cause.** Its venv
  installs `apache-airflow`, `pytest`, `psycopg2-binary` and `requests` —
  **pytest-mock is not there**, so `mocker` does not exist in that
  interpreter. `unittest.mock.patch` is correct in that directory. If the venv
  ever gains pytest-mock, the exemption ends with it.

So the boundary is neither "the layer" nor "the thing being mocked". It is
**which interpreter the suite runs in**, which is a fact CI states and a test
can read.

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
thing. Where a statement cannot be imported, read it out of its module with
`ast` rather than retyping it; `_sensor_constant()` in that same file does this
for `airflow/dags/sensors.py`, which the Layer 1 venv cannot import.

**A paraphrase of production SQL in a test file is worse than no test**, and
this is the mechanically checkable half: a test file that contains a `SELECT`
which does not appear in any `.sql` file or production module is a finding.

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
  function returning `(sql, params)`**, so Layer 1 can call it and execute the
  result. That is the standard the drain now meets.
- **DDL and one-shot maintenance** inside a migration or a script that runs
  once. Flyway owns `db/migrations/`.

An `f`-string interpolating a value into SQL is neither exemption; it is a bug.

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

| Service | Src | Layer 4 | Layer 3 | Standing |
|---|---|---|---|---|
| `ops` | 19 | 18 | 9 | Meets the floor bar the two `/maintenance` routes |
| `archiver` | 20 | 15 | 9 | Meets the floor |
| `processing` | 11 | 10 | 6 | Layer 3 exists and **runs nowhere** |
| `scraper` | 10 | 9 | 1 | Plan 84 deferred it; **re-examined below** |
| `dbt_runner` | 4 | 3 | 1 | Plan 84 deferred it; **re-examined below** |
| `shared` | 14 | 11 | 1 | Meets the floor |
| `container_health` | 4 | 2 (misplaced) | 0 | **Below the floor** — no `TestClient`, no Layer 3 |
| `dashboard` | 7 | 0 | 0 | **Below the floor** — its SQL is covered by Layer 1, its Python by nothing |

**Plan 84's deferral of `dbt_runner` and `scraper` is closed, split:**

- **`dbt_runner` — deferral lifted, and it was already lifted in practice.** It
  has a Layer 3 suite and its serving-snapshot SQL is executed in Layer 1
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
| Patching uses `mocker` outside the Airflow venv | Grep the AST for `unittest.mock.patch` / `monkeypatch.setattr` against the directory-to-venv map CI itself declares |
| Every route is reached through the app's routing table | Walk each FastAPI app's `app.routes`; compare to the verb/path literals tests actually request |
| Every service directory has a row in the "enough" table | Compare `docs/TESTING.md`'s table to the service directories on disk |
| No test file contains a `SELECT` absent from every `.sql` file and production module | AST-walk test string constants |

Not mechanically checkable, and the skill must **say so rather than imply
coverage it does not have**: whether the thing under test is the thing being
mocked, whether a failure branch matters to another service, and whether an
assertion is meaningful.

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
| G4 | **10 test files mix two patching mechanisms; 10 more use `unittest.mock.patch` where `mocker` is available.** Two of the latter are the sanctioned `tests/integration/airflow/` exemption; the other eight are `tests/scripts/` (7) and `tests/airflow/test_notifications.py` | AST census, 2026-08-31 | Plan 162 |
| G5 | **Inline SQL at `.execute()` call sites in 10 production modules**, against six that use the loader: `ops/routers/{coordination,deploy,scrape,users}.py`, `archiver/processors/{pack_bronze_html,delete_packed_source_html,flush_silver_observations,flush_staging_events}.py`, `shared/db.py`, `shared/duckdb_s3.py` | `.execute(` with a literal first argument | Plan 162 |
| G6 | **Four routes reached by no test through any routing table**: `container_health`'s `/oneoff-processes` and `/project-status/{project}` (handler-level tests only, no `TestClient` in the service), and `ops`' `POST /maintenance/evict-delisted-cooldowns` and `POST /maintenance/reconcile-cooldown-cohorts` | `app.routes` versus test request literals | Plan 162 |
| G7 | **`dashboard/`: 7 modules, 0 test files.** Its SQL is covered by Layer 1 through `dashboard.queries`; its Python is covered by nothing | — | Plan 162 |
| G8 | **`scraper/`: Plan 84's four-month-old deferral, now lifted.** One integration file, which is itself in G1 | — | Plan 162 |
| G9 | `tests/test_container_health_app.py` and `tests/test_container_health_collector.py` are Layer 4 tests sitting in Layer 0's directory. `container_health` has no `tests/container_health/` and no Layer 3 at all | — | Plan 162 |
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
