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

The docstrings in `tests/` and the step names in `ci.yml` carried Plan 84's
numbers until Plan 162 Stage F swept all 16 of them on 2026-09-01 (CAR-49).
`test_every_layer_number_in_the_code_matches_the_contract` waives nothing now,
so the two cannot drift apart again.

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

The top level of `tests/` is Layer 0's home and is not a dumping ground. The
two `container_health` unit tests that sat here until Stage 6 (CAR-50) moved to
`tests/container_health/` on 2026-09-01, which was G9. Where a test lives is
not filing tidiness: the route rule attributes a test to a service by its
directory, so those files could not have counted for `container_health` even
once they grew a `TestClient`.

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
`dbt_runner`'s serving snapshots). Needing both is why this suite runs in the
`SQL + Airflow metadata contracts` job, which builds dbt for the 21 DuckDB
tests and runs `airflow db migrate` for the one file that reads Airflow's own
`task_instance` and `dag_run`.

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

**Executes production SQL from these suites.** The rule that every `.sql` file
is executed reads all of them, and this table is the declared source it reads —
a suite absent here is invisible to that rule however much SQL it runs, so
adding one is an edit here rather than a change to the checker.

| Suite | Engine | Why it executes production SQL |
|---|---|---|
| `tests/integration/sql/` | Postgres, DuckDB | Layer 2 proper: imports each service's `queries.py` constants and executes them |
| `tests/integration/archiver/` | DuckDB over MinIO | The Plan 120 lake-snapshot selectors run against real Parquet through `run_lake_selectors`, which no Postgres/DuckDB fixture in Layer 2 can reach. They live beside their service because they are also that service's integration tests |

**Why a declared table and not a glob over `tests/integration/`.** Measured
2026-09-01, a glob would credit 35 of the then-46 uncovered files on a name
match alone, several of them from suites that mention a statement without
running it. The check matches a filename stem in a test's *text*; it cannot
tell execution from mention, so widening what it reads has to be a decision
somebody made rather than a directory that happened to exist.

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
| `tests/integration/lakehouse/` | 4 | **Dormant by decision** — Plan 125 pulled the job (`863a2f2`) rather than patch its fixture problem. Declared in `DORMANT_SUITES`, not waived; see [Waivers](#waivers) for why that distinction is structural |
| `tests/scripts/` | 1 | Unit tests of `scripts/` — production tooling that CI, an image, a Compose file or an ops route invokes |
| `tests/scripts/oneoff/` | 1 | Unit tests of `scripts/oneoff/`, whose owning plans have archived. **Runs in the unit job like any other Layer 1 suite**; what its subjects leave is the coverage denominator, not the suite. Spent is a statement about a plan's state, never a licence to stop testing — `reconcile_april_detail.py` is well covered *because* it deleted 14.6 GB of production data |
| `tests/integration/scripts/` | 4 | Recovery scripts against a real database — rollback, receipt and protected-table properties |

### Where scripts sit, and what the directory declares

`scripts/` is two things, and the path is the declaration — there is no
manifest to keep in step, `git log --follow` records a reclassification, and
`[tool.coverage.run]` and `scripts/ci_change_scope.py` each read the prefix for
free.

| Directory | In the coverage denominator? | What belongs there |
|---|---|---|
| `scripts/` | yes | Production. Invoked by CI, an image, a Compose file, an ops route or a harness hook — or imported by something that is |
| `scripts/oneoff/` | **no** | Spent. The owning plan has archived and nothing binding names it. An entry here should cite that plan in its docstring, so the bucket cannot outlive its reasons |

**The safe failure direction is the point.** A new script lands in
production-land and is measured **by default**; leaving the instrument takes a
deliberate move. Nobody drops something out of coverage by forgetting.

**A script directory the contract does not classify fails
`test_every_script_directory_is_classified`.** An unplaced directory is not a
documentation gap: coverage, the CI scope classifier and a reader all have to
guess what it is, and they can guess differently.

**Spent is a property of the owning plan's state, not of how finished a script
looks.** The `audit_`, `estimate_` and `spike_` prefixes classify nothing:
`audit_adaptive_refresh_features.py` reads as forensics and is baked into
`dbt_runner/Dockerfile`. Classification is the archive join —
docstring plan number against
[`completed_plans.md`](planning/completed_plans.md), the script's own name
grepped back through the archive and the plan documents when it declares none —
and a binding reference or an import from production wins over both.

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

#### The one place `mocker` was unavailable was a defect, not an exemption

`tests/integration/airflow/` runs from the isolated venv the CI job builds, and
until 2026-08-31 that venv installed `apache-airflow`, `pytest`,
`psycopg2-binary` and `requests` — **pytest-mock was not among them**, so the
`mocker` fixture did not exist in that interpreter and its two files used
`unittest.mock.patch`.

That was a missing argument on one `pip install`, and the contract did not bend
around it. `pytest-mock` declares exactly one dependency — `pytest` — which
that venv already installs, so none of the starlette/fastapi conflict that
forced the venv's existence in the first place applied to it. The real cause was
that the venv was built to run Airflow, and its *test* dependencies were never
thought about separately.

Nor did those two files need `unittest.mock` for anything: what they patch is
`requests.post`, `requests.get`, `time.sleep`, and one `patch.object` on a DAG
module's `post_json`. All four are ordinary `mocker.patch` calls.

**Plan 162 Stage F added `pytest-mock` to that `pip install` and converted both
files** (CAR-49, 2026-09-01). The rule now has no waivers and no venv carve-out.

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
statements all live in `ops/sql/` and are executed by
`tests/integration/sql/test_ops_queries.py`. Two of them were module-level
*builders* until 2026-09-02 — `task_instance_query()` and
`gate_observation_query()` — which was the second-best option under the
preference order above, and Plan 162 Stage N moved them up to the first.

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

**As of Plan 162 Stage N no test reads a SQL statement out of source, and the
history of the two that did is the argument for the rule.**

`_sensor_constant()` in `test_ops_queries.py` used to read
`GATE_OBSERVATION_SQL` out of `airflow/dags/sensors.py` this way, and
`test_coordination_admission.py` scraped the admission `SELECT` out of
`_DeployIntentSensor` to check its column order. Both were correct given where
the statements lived, and both stopped being necessary the moment the
statements moved into `airflow/sql/` — Stage 7 for the first, Stage 9 for the
second. Neither was a fact of life about statements that resist import.

The stated cause was always a module, not a statement. `sensors.py` imports
`airflow.providers.postgres.hooks.postgres` and `airflow.sdk.bases.sensor` **at
module scope**, and the SQL-smoke suite runs in the main venv, which has no
Airflow, so `import sensors` raised before reaching the constant. A `.sql` file
has no imports, which is why option 1 outranks option 2.

That the second reader survived two stages past its cause is the part worth
keeping. It asserted `"scope ? 'host'" in sensor_source` — a substring match
against Python text, which passes forever and cannot notice a reworded
statement. **An `ast` reader is a paraphrase with extra steps**, and it ages
into one silently.

So the answer to "should `ast` be used more widely" is **no, and a growing
count of `ast` readers is a signal to act on, not a pattern to spread.** Each
one marks a statement that ought to live in a file and does not. Where it is
genuinely the only option, the reader carries a comment naming the import that
forced it, so the next person can tell a constraint from an accident. The
remaining `ast` in `tests/airflow/` reads *code shape* — which DAG declares
which sensor, what keywords a factory passes — which is a different question
and the right tool for it.

The mechanically checkable half of this rule is one direction only: **every
`.sql` file and every module-level statement is executed by Layer 2.** The
other direction — deciding whether a `SELECT` in a test file is a paraphrase of
production or legitimate fixture seeding — is judgement, and the skill flags it
rather than failing on it.

**2. A route must be reachable through the app's routing table.**

Calling the handler function proves the function works. It proves nothing about
the URL, the method, the prefix, or whether the router was included at all.

`container_health` is the worked example. Its `/project-status/{project}` 404d
for roughly eleven hours on 2026-08-30 with nothing reporting it — and its unit
tests *did* cover both of its interesting endpoints, by calling
`app.active_oneoff_processes()` and `app.project_status()` directly. A
handler-level test could not have caught the failure, and did not.

**Repaired in Stage 6 (CAR-50), 2026-09-01**, and the shape of the repair is
the part worth copying. The service has no database, so its Layer 4 suite
stands up a strict fake of `docker-socket-proxy` on loopback serving responses
recorded from the real proxy: nothing is mocked, and the whole path runs
through `TestClient` → router → `DockerApi` → `urllib` → HTTP. What a recording
cannot notice is the real API changing shape underneath it, so
`scripts/verify_container_health_docker_contract.py` replays the same corpus
through the real proxy in its own CI job — the split Plan 141 already uses for
Promtail. One corpus, two consumers, neither importing the other.

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
orphaned-suite failure in miniature — the one G1 recorded, where 73 tests were
maintained for months by a CI job that never invoked them.

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

- **Structurally generated statements**, where the *text* genuinely depends on
  the arguments — a dynamic identifier, which SQL cannot parameterise. The
  builder is then a **module-level function returning `(sql, params)`**, so
  Layer 2 can call it and execute the result.

  **This exemption currently has no members, and the two it used to name never
  qualified.** `task_instance_query()` and `gate_observation_query()` built
  `(VALUES …)` and `IN (…)` lists sized to their arguments, which is *value*
  variability, not structural — only the number of placeholders changed, never
  an identifier. `unnest(…::text[])` and `= ANY(…)` take those values as arrays
  and the text stops varying at all; both rewrites were verified equivalent
  against `postgres:16` on every admission scope before landing. The rewrite
  also removed a special case rather than adding one: `(VALUES )` with no rows
  is a syntax error, so both builders returned `None` on an empty scope and
  every call site translated that back into zero, while the array form answers
  zero from the engine.

  So the bar is high and the burden is on the statement: *which identifier*
  cannot be a parameter? "The shape depends on the arguments" was too loose,
  and it survived two stages as the stated reason two ordinary statements sat
  outside the file convention.
- **DDL and one-shot maintenance** inside a migration or a script that runs
  once. Flyway owns `db/migrations/`.

An `f`-string interpolating a value into SQL is neither exemption; it is a bug.

**`airflow/dags/` follows the file rule through its own loader.** The two
exemptions above are from *"is a `.sql` file"*; this is a decided exemption
from a different clause — *"loaded by `shared.query_loader`"* — and it is the
only one. Statements live in `airflow/sql/`, are loaded once at import
by `airflow/dags/dag_queries.py`, and are executed by
`tests/integration/sql/test_airflow_dag_queries.py`. Everything the rule is
*for* holds: one copy of each statement, in a file, executed against a real
engine by a test that imports no Airflow.

What is exempted is the shared import, and the reason is that reaching it costs
more than it buys. `shared/query_loader.py` is two lines, but the DAG tree has
no `shared` on its path: the Airflow image is `apache/airflow:3.2.0` plus four
providers, and compose mounts only `dags/` and `sql/` into it. Mounting
`shared/` would put `minio`, `duckdb_s3`, `iceberg_catalog` and `packfile` —
which need boto3, duckdb and pyiceberg — on the DAG tree's import path with
none of them installed, so an import that resolves in the main venv would fail
**at DAG-parse time in production**. That isolation is the same one forcing a
separate Airflow venv in CI. Two duplicated lines is the cheaper side.

The exemption is narrow and deliberately so: it licenses a local `load_query`,
not local SQL. Inline SQL in a DAG fails
`test_no_production_module_holds_a_sql_statement` like anywhere
else — `airflow/dags` is in `production_python_roots()`, and Stage 9 added the
Airflow hook and operator names to the scanned call set so the rule can
actually see the tree. This closed [G12](#the-gap-list).

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
| `scraper` | 10 | 9 | 2 | Meets the floor — Stage 8 put both fetch paths through Layer 4 unmocked |
| `dbt_runner` | 4 | 3 | 1 | Plan 84 deferred it; **re-examined below** |
| `shared` | 14 | 11 | 1 | Meets the floor |
| `container_health` | 4 | 2 | 2 | Meets the floor — every route reached, Layer 4 added in Stage 6 |
| `dashboard` | 7 | 0 | 0 | **Below the floor** — its 24 statements are executed *and asserted* in Layer 2 since Stage 8, but its Python is still **not importable by the suite at all** ([G18](#the-gap-list)) |

**Plan 84's deferral of `dbt_runner` and `scraper` is closed, split:**

- **`dbt_runner` — deferral lifted, and it was already lifted in practice.** It
  has a Layer 4 suite and its serving-snapshot SQL is executed in Layer 2
  against the real DuckDB artifact. It meets the floor.
- **`scraper` — deferral lifted and repaid by Stage 8 (CAR-52), 2026-09-02.**
  It had ten source modules and one integration file, `test_blocked_cooldown.py`,
  which executed SQL constants against a cursor — Layer 2's shape, in a Layer 4
  directory. So the service's *writing* half had never run: every route was
  reached only from `tests/scraper/`, whose autouse fixture patches
  `shared.db.get_conn` and `shared.minio.write_html`. Both fetch paths now run
  end to end against a loopback origin serving real captures, to real MinIO and
  real Postgres, with nothing mocked — `test_detail_fetch_path.py` for
  `POST /scrape_detail` and `test_results_fetch_path.py` for the background SRP
  job. The old file was deleted; the one assertion in it that Layer 2 did not
  already cover moved to `test_processing_queries.py`.

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
| Patching is `mocker`, everywhere | `test_patching_is_mocker_everywhere` | AST-walk for `unittest.mock.patch` and for `monkeypatch.setattr` on an application object. No venv carve-out and, since Stage 5, no waivers: every interpreter that runs a test here installs `pytest-mock` |
| Every route is reached through the app's routing table | `test_every_route_is_reached_through_the_apps_routing_table`, `test_no_route_is_hidden_from_the_schema_this_rule_reads` | Walk each FastAPI app's `app.routes`; compare to the verb/path literals tests actually request |
| Every service directory has a row in the "enough" table | `test_every_service_directory_has_a_row_in_the_enough_table` | Compare this document's table to the service directories on disk |
| Every `.sql` file is executed by a Layer 2 test | `test_every_production_sql_file_is_touched_by_a_layer_2_test` | Collect what `tests/integration/sql/` imports and executes; compare to what `queries.py` exposes |
| A Layer 2 test asserts something about its result | `test_no_layer_2_test_executes_a_statement_without_asserting_on_the_result`, `test_the_assertionless_rule_sees_a_test_that_only_executes` | AST-walk every test in a directory `_layer_of` places at Layer 2 and require an `assert`, a `pytest.raises`, or a call to an `assert`-named helper. This is Layer 2's **second** clause — the row above is the first, and a test that executes a statement and discards the result satisfies that one while checking nothing. Added by Plan 162 Stage M after `test_dashboard_queries.py` was found to be 25 tests and zero assertions; it found four more in suites whose other tests assert. It checks that an assertion **exists**, never that it is meaningful — `assert True` passes, and that half stays judgement in the section below |
| Every `Layer N` mention in `tests/` and `ci.yml` matches this document | `test_every_layer_number_in_the_code_matches_the_contract`, `test_every_test_directory_is_assigned_a_layer` | Regex both, compare to the headings here — this is what stops Plan 84's numbering coming back |
| Every pytest invocation in `ci.yml` sets `PYTHONPATH` | `test_every_pytest_invocation_in_ci_sets_pythonpath` | Parse the workflow's `run:` steps. One of two mechanically checkable clauses of *the harness must not decide the outcome*; the rest of that rule is judgement, and the row below says so |
| Every text read and write names its encoding | `test_every_text_read_and_write_states_its_encoding`, `test_the_encoding_rule_sees_the_shape_ruff_cannot` | AST-walk every `read_text`/`write_text` call and require `encoding=`. The second clause of the same rule, added by Plan 162 Stage J after a missing `encoding=` made a fixture read UTF-8 on Linux and cp1252 on Windows. `open` and `NamedTemporaryFile` are ruff's `PLW1514` instead, which reads them by type; this rule reads the two `pathlib` methods by name, because ruff resolves a receiver by type and is blind to `(tmp_path / "a.md").write_text(...)` |
| Coverage measures every service directory, and the number it produces is consumed | `test_every_service_directory_is_measured_by_coverage`, `test_the_coverage_number_the_unit_job_produces_is_consumed` | Compare `[tool.coverage.run] source` to the service directories on disk; require `--cov-fail-under` on every `ci.yml` step that passes `--cov` |
| Every `scripts/` directory is classified, and the classification is what coverage does | `test_every_script_directory_is_classified`, `test_every_unmeasured_script_bucket_is_omitted_from_coverage` | Compare *Where scripts sit* to the subdirectories on disk, both directions; then compare each bucket's stated answer to `[tool.coverage.run] omit` |

### Specified here, not yet asserted

Rules this document commits to that **no test implements yet**. They are not
waivers — a waiver grandfathers a known violation of a rule that is enforced,
while these are rules with no enforcement at all, so a violation of one is
invisible rather than counted.

| Rule | Why it is not asserted | Owner |
|---|---|---|
| Every module-level SQL statement in a production module is executed by a Layer 2 test | ~~Inline SQL at an `.execute()` call site is the opposite direction and is measured by nothing~~ — **mechanised 2026-09-01 by Plan 162 Stage L.** [G5](#the-gap-list) and [G15](#the-gap-list) are asserted together by `test_no_production_module_holds_a_sql_statement`, which since 2026-09-02 is **one rule keyed on the statement rather than two keyed on its container**. It reads `production_python_files()` — a second derivation, deliberately **not** `service_packages()`, which answers "is this a service" rather than "is this production Python" and stops at the package boundary. It admits `airflow/dags` and the `scripts/` buckets the contract declares measured, and excludes the ones it declares spent, so a new bucket is covered by editing that table rather than this file. What stays unasserted is Spark: a SQL *fragment* (`selectExpr`, `expr`, a string `filter`) leads with no verb, and the DataFrame API is not text at all, so neither is visible to a static read | Plan 162 |
| A run that succeeds has done the work its success implies | **The rule this whole document is about, stated once instead of rediscovered.** A paraphrased test passes forever. A skipped test executes nothing. A `dbt build --target spark` that writes to `spark_catalog` instead of Iceberg **exits 0 having written nothing** — `scripts/run_dbt_spark.py` documents that trap and answers it with `--verify-table`, which is a convention someone must remember, not a mechanism. Three instances, one class, and only the second is mechanised: `REQUIRE_LAYER_2_EXECUTION` fails a Layer 2 run that skipped. There is no general form, because "did this actually do the thing" is specific to each thing | Plan 162, and [Plan 125](plans/plan_125_duckdb_to_iceberg_migration.md) for the Spark instance |

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

**Dormancy is not a waiver, and Plan 162 Stage B is why.** It was written as
one — a waiver already carries a reason, an owner and a date, so reusing it
looked like the frugal choice. The third property above is what breaks it: a
waiver dies when its owner plan archives. `tests/integration/lakehouse/` is not
waiting on Plan 162 or on any plan; it is a suite deliberately not run. Held as
a waiver it would have failed the day Plan 162 archived, and the only way to
quiet it would have been to delete the record of why the suite is not running —
losing exactly the fact that asking for the declaration was meant to preserve.

So dormancy lives in `DORMANT_SUITES`, beside the waivers and shaped like them,
with no owner and no expiry. Two assertions hold it in place: an undeclared,
uninvoked suite fails `test_every_integration_suite_is_invoked_by_a_ci_step`,
and a declared suite that *is* invoked fails
`test_no_dormant_suite_is_quietly_running` — because a dormancy reason that has
stopped being true is the drift this document exists to prevent.

---

## The gap list

Every entry is a measured violation of the contract above, as of 2026-08-31.
Recorded here, fixed elsewhere — Plan 161's non-goals hold.

An entry is deleted when it is repaired, not marked closed: a violations table
that keeps its dead rows is a list you have to read twice to use. Ten rows
have gone that way, all to Plan 162: **G1 and G2** to Stage B (CAR-45) and
**G10** to Stage 2 (CAR-46) on 2026-08-31, **G4, G11 and G13** to Stage 5
(CAR-49) on 2026-09-01, **G6 and G9** to Stage 6 (CAR-50) on 2026-09-01, and
**G7 and G8** to Stage 8 (CAR-52) on 2026-09-02.
What the run of those 73 tests found, what unblinding coverage exposed, what
the 34 mock conversions and 16 layer renames turned up, why five of G6's
twelve routes turned out never to have been uncovered at all, and which four
assertionless Layer 2 tests were hiding in suites that otherwise assert are in
[`docs/plans/plan_162_testing_census_and_restructure.md`](plans/plan_162_testing_census_and_restructure.md),
§Stage 1, §Stage 2, §Stage 5, §Stage 6 and §Stage 8. The numbering never reuses
a letter, so a deleted row leaves a gap in the sequence and the plan documents
stay the place the history lives.

| # | Violation | Measure | Owner |
|---|---|---|---|
| G5 | **Inline SQL at a SQL-taking call site.** 66 sites in 15 modules across the eight service packages, seeded and drained 2026-09-01. **Reopened at 22 the same day** when the scan surface was widened from `service_packages()` to production Python: `airflow/` and `scripts/` are not packages and so had no rule at all, and 16 of the 22 are in Plan 125's Iceberg and Spark tooling, which Gates C and D productionize. Measured by the rule below. The census said ten modules and named two that do not belong: `shared/db.py`'s only match is the usage example in `db_cursor`'s docstring, and `shared/duckdb_s3.py`'s seven are `INSTALL`/`LOAD`/`SET` session setup, which name no schema to drift from. Eight modules it never named do belong, including `ops/routers/maintenance.py:152` — a literal `INSERT` passed to `execute_values` as its *second* argument, which the measure below was originally written to miss | ~~A SQL verb leading any argument of `execute`, `executemany`, `execute_values` and their kin~~ — **superseded 2026-09-02 by Plan 162 Stage N.** That measure was an inventory of database-client method names, and a rule reading one is escapable by calling anything the list has not heard of: `ops/coordination_drain.py:77` hands a production SELECT to `_database_count(...)`, a helper defined in the same file, and no list length would ever have found it. Now asserted by `test_no_production_module_holds_a_sql_statement`, which asks only whether the literal **is** a statement — a SQL verb followed by a clause keyword — and never where it sits. This row is kept rather than deleted because live waivers name it | Plan 162 |
| G15 | **23 SQL statements in 11 production modules are kept in a Python literal**, bound to a name and executed from there — 8 in `archiver/processors/lake_snapshot_cohort.py` and 6 in `ops/routers/admin.py`, a router Stage 7 never touched because every one of its statements is assigned before it is executed. **Down to 7 in 5 modules on 2026-09-03**, alongside Stage 10: the cohort's 8, the selector and export wrappers, the source audit, the blocked-cooldown reconcile, the deploy-intent pause and the silver-observations insert are now files under `archiver/sql/lake_snapshot/`, `ops/sql/`, `processing/sql/` and `shared/sql/`, executed by `tests/integration/archiver/test_lake_snapshot_queries.py` and the three Layer 2 modules. Every one of the fourteen was verified byte-identical to the f-string it replaced after comment and whitespace normalisation, so the move changed no statement. What remains is two archiver pack/delete processors, three `scripts/` measurement tools, and — untouched by design — Plan 125's Spark and Iceberg tooling, whose suite is `DORMANT_SUITES`-declared until Gate C returns the services, so a `.sql` file for it would sit unexecuted and fail G14 instead. Found by closing G5: a literal at a call site cannot be imported, so G5 fires; a literal bound to a name **is** importable, so G5 does not — and it is in no `.sql` file, so G14's denominator cannot count it either. It satisfies the letter of both instruments while sitting outside both | ~~A SQL verb leading the value of an assignment or a `return`~~ — **superseded 2026-09-02 by Plan 162 Stage N**, and for the same reason as G5's: assignment-or-return is itself an enumeration, of shapes rather than of names. `scripts/compare_gate_b_parity.py` holds two statements as **dict values** in `TIE_QUERIES`, which is neither, so both rules were blind to it at once. Now asserted by `test_no_production_module_holds_a_sql_statement` | Plan 162 |
| G16 | **The dbt project owes nothing this document can state.** Questions 5 and 6 of [Plan 161](plans/plan_161_testing_contract.md) asked what a *service* owes, and the mechanism is keyed to a Python package: the "enough" table's rows must equal `service_packages()` in both directions, so a `dbt` row fails as a phantom. `dbt_runner` — the service that *invokes* dbt — has a row; the 23 models it builds have none. **Re-measured 2026-09-04: 18 of 23 carry a dbt unit test, 7 are directly asserted by a fixture-driven real build, 5 by neither — and the headcount is the wrong instrument.** `stg_observations` scores zero on it while both reject paths of its `vin17` guard run against production-shaped Parquet on every build, and the models holding the most branches are the least proportionally covered. What the project owes is branch coverage, and three lists already claim to provide it — the snapshot's selectors, the synthetic fixture and the unit tests — none derived from the models, no two checked against each other. The only obligation enforced on a model today is that it carries a cadence tag, which is a scheduling rule. Because `dbt/` is a named exemption from the rule above, logic moving out of a `.sql` file and into a mart leaves a counted surface for an uncounted one, and the count drops for something that is not a repair | Not yet asserted. Plan 162 Stage S owes the branch-coverage mechanism and G16's own rule: a `.sql` file may leave `production_sql_files()` only by naming the model that absorbed it | Plan 162 |
| G17 | **One statement filed twice.** `mark_artifact_status`, `insert_artifact_event` and `insert_blocked_cooldown_cleared_event` each existed byte-identically under both `ops/sql/` and `processing/sql/`. Both services issue them against the same tables, so the schema already coupled the two — the copies decoupled nothing and only made a second place to edit. Worse, the rule above credits a file when Layer 2 names its **stem**, and the pairs shared one, so a test of `processing`'s copy silently credited `ops`'s: three files reported covered by a test that never executed them. Consolidated into `shared/sql/` on 2026-09-01 and re-exported by both services' `queries.py`, so no call site changed | Every production statement compared against every other, normalised for comments and whitespace — asserted by `test_no_two_production_sql_files_hold_the_same_statement`. One waived pair, `cancel_coordination_state` and `release_deploy_coordination`, which are two policies that agree rather than one statement | Plan 162 |
| G18 | **`dashboard/`: 7 modules, 0 test files, and the suite cannot reach them.** Not under-tested — *unreachable*: `streamlit` and `plotly` are declared in `dashboard/requirements.txt` and nowhere else, and production imports are bare (`from queries import`, `from db import`) because the Dockerfile does `WORKDIR /app; COPY dashboard/ .`, while Layer 2 imports `dashboard.queries`. `import dashboard.pages.deals` raises `ModuleNotFoundError` today. That is the whole of the 9% reading — only `queries.py` is importable, ~30 of 309 statements. Closing it means a CI venv, a resolution of the dual import identity, and a render harness, in that order; the first two are structural changes to a service whose role is undecided. **Of the 483 lines under `pages/`, ~430 are `st.*`/`px.*` presentation and ~35 are logic** | — | Plan 150 |
| G12 | ~~**`airflow/dags/` has no `.sql` convention and cannot reach one.**~~ — **closed 2026-09-02 by Plan 162 Stage N.** The tree reaches the convention through its own `airflow/dags/dag_queries.py`, a decided exemption from the `shared.query_loader` clause and not from the file rule — see [where SQL lives](#where-sql-lives) for why mounting `shared/` into the Airflow image costs more than the two lines it saves. Two of the row's three claims were already stale when the stage opened: `airflow/sql/` had existed since Stage 7, and `_sensor_constant()` had been deleted by it. What actually remained was **one statement** — the admission `SELECT` inline at `sensors.py`'s `hook.get_first`, invisible to all three instruments at once: G5 did not scan Airflow hook methods, G14 counts only `.sql` files, and the only thing asserting on it was an `ast` substring match | `grep -rn 'from shared' airflow/dags/` still returns nothing, and that is now the intended state. What is asserted instead: the statement is in `airflow/sql/deploy_intent_gate.sql`, executed by `tests/integration/sql/test_airflow_dag_queries.py` including its column *order*, which `poke()` indexes positionally; and a new inline statement in a DAG fails `test_no_production_module_holds_a_sql_statement`. Stage 9 first closed this by adding six Airflow method names to `_SQL_CALL_NAMES`, then deleted that list outright — lengthening an inventory is not a fix for an inventory | Plan 162 |
| G14 | ~~**56 of 76 production `.sql` files are named by no Layer 2 test.**~~ — **closed 2026-09-01 by Plan 162 Stage L**, `LAYER_2_WAIVERS` is `()`. Was All 19 under `processing/sql/`, all 8 under `ops/sql/`, all 3 under `scraper/sql/`, 19 of `archiver/`'s, the 6 `dashboard/sql/data_health_*` files and `airflow/sql/delete_stale_emails.sql`. `test_ops_queries.py` and `test_processing_queries.py` are named for the services whose statements they should execute, import nothing from either `queries.py`, and **paraphrase the SQL instead** — which the rule above calls worse than no test, because a paraphrase passes forever | `tests/test_testing_contract.py`, at the weakest reading of "executed": a file counts as covered if Layer 2 names it **as a whole word**. Was 54 until 2026-09-01, when Stage 5 found the match was a bare substring and three files were being credited by identifiers that merely contained their stem. Stage 7 drained it: 132 files gained a test importing the constant production imports, 18 lake-snapshot selectors were already executed by `tests/integration/archiver/` and needed the reading widened rather than new tests, and one file was deleted under [G16](#the-gap-list)'s rule because the statement that absorbed it could be named | Plan 162 |

---

## What this contract does not decide

- **The CI restructure.** Building dbt against the Plan 120 lake snapshot and
  running the suites against the real Compose definitions are
  [Plan 162](plans/plan_162_testing_census_and_restructure.md), Stage P. The
  first half — splitting the 267s `dbt build + test` job into four jobs named
  for what they run — shipped as Stage 4 on 2026-09-01.
- **What the coverage threshold should be.** Whether there is a gate at all was
  decided by [Plan 162](plans/plan_162_testing_census_and_restructure.md)
  Stage 2 on 2026-08-31: there is one, `--cov-fail-under` on the unit job, over
  all ten production directories including `airflow/dags/`, `dashboard/` and
  `scripts/`. It is a ratchet against regression, raised by the stages that
  raise the number. **The number it is set to is not a target**, and this
  document's position is unchanged: a percentage is not the definition of
  enough, and clearing the floor is not evidence that a service meets it.
- **Impact-based test selection.** Plan 139 Stage E specified it and has since
  moved to Plan 162; nothing here changes its design.
- **Fixing anything in the gap list.**
