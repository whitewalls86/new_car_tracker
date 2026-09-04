# Plan 162 — the CI cost census, 2026-09-04

This records the measurements taken while scoping [Stage R](../plans/plan_162_testing_census_and_restructure.md#stage-r-ci-selection-and-the-instrument-that-has-to-precede-it),
which moved it behind Stages S–X. The stage was scoped in the plan's opening
pass, before Stage E split the 267s dbt job and before Stage P added
`snapshot-dbt`, so its premises were about a workflow that no longer exists.

**This file holds the measurements; the rescope they produced is in the plan
document.** Stage R keeps its number and its position in the table — the
convention that stage numbers carry the order is a legacy defect [Plan
172](../plans/plan_172_plan_authoring_skill.md) names and deliberately does not
sweep, and reconciling this document with the contract is a separate pass.

Nothing here is a production measurement; every number is a GitHub Actions
reading or a `git log` walk, and each is reproducible from the recipe beside
it.

The short version: **CI selection has no wall clock left to recover**, because
the workflow's cost is set by one job and every other job already finishes
inside its shadow.

## The full-run census

**Recipe.** `gh api repos/whitewalls86/new_car_tracker/actions/runs/<id>/jobs`,
differencing each job's `started_at` and `completed_at`. Run
`33832093164`, `master`, 2026-09-04, 153s wall clock, all jobs green.

| Job | Duration |
|---|---:|
| **SQL + Airflow metadata contracts** | **123s** |
| dbt model tests (real build) | 103s |
| Docker build (all services) | 96s |
| Unit tests (pytest) | 94s |
| Service integration tests (Postgres) | 75s |
| dbt build against a production snapshot | 73s |
| Lake integration tests (MinIO) | 63s |
| Promtail config (real image) | 19s |
| Lint (ruff) | 11s |
| container_health Docker contract | 11s |
| Classify changed paths | 7s |
| Documentation tests | skipped |

**The critical path is `changes (7s) → lint (11s) → schema-contracts (123s)`**,
which is 141s against an observed 153s — the remaining 12s is scheduling
overhead. Every other heavy job finishes before `schema-contracts` does.

## 80% of the longest job is infrastructure, and 5% is tests

Step timings for `schema-contracts`, the job that sets the wall clock:

| Category | Steps | Time |
|---|---|---:|
| Runner and containers | set up 1s, pull flyway 10s, initialize containers 20s, checkout 3s, stop containers 2s, complete 1s | **37s** |
| Dependency installs | deps 22s, dbt 8s, `dbt deps` 3s, DuckDB extensions 1s, **Airflow venv 27s** | **61s** |
| Setup work | flyway migrate 2s, seeds 1s, dbt build 12s, `airflow db migrate` 2s | **17s** |
| **The tests themselves** | SQL smoke 3s, Airflow integration 3s | **6s** |

98 of 121 accounted seconds — 80% of the 123s job — is infrastructure and
installs. **The tests a selector would skip are six seconds.**

## Why selection cannot pay: `max()`, not `sum()`

`dbt-models`, `service-integration`, `lake-integration` and `snapshot-dbt` all
hang off the same `heavy` output as `schema-contracts`, so they are skipped or
run together. Since all four are shorter than 123s, **skipping any subset that
excludes `schema-contracts` saves exactly zero wall clock.**

Skipping `schema-contracts` itself would promote `dbt-models` (103s) to the
ceiling, for a total near 133s — a 20s saving on a job that answers whether
every service can still reach the schema it depends on. Even omniscient
selection across every heavy job floors the workflow at roughly 114s against
today's 153s.

**Plan 139 Stage E's promotion rule therefore cannot be satisfied.** It requires
"a benefit larger than runner variance," and runner variance across these jobs
is comfortably ±10–20s against an available benefit of 0–30s. The advisory
selector would be an instrument built to answer a question the arithmetic has
already closed.

Runner minutes are not a second justification: the repository is public, so
GitHub-hosted minutes are free.

## The `dbt-models` trigger set, derived and then declined

`heavy` is a fail-open default rather than a gate — it is `true` for everything
that is not docs-or-oneoff, so `dbt-models` runs on a `Caddyfile` change. Only
`snapshot_dbt` has a real allowlist. The question of whether `dbt-models`
deserves one was asked properly and answered no.

**The input surface**, derived from `tests/integration/dbt/`'s first-party
imports plus the job's own steps rather than guessed:

```
dbt/                                    models, profiles, packages
db/migrations/                          on-run-start hook + postgres_scan sources
tests/integration/dbt/                  the suite itself
archiver/                               lake_snapshot_selectors, lake_source_audit
shared/                                 duckdb_s3
scripts/seed_lake_snapshot_fixture.py   the fixture the build materializes against
.github/scripts/                        create_ci_bucket, seed_ci_bronze_schemas
.github/workflows/ci.yml                the job and its dbt pins
requirements-dev.txt                    the installs
scripts/ci_change_scope.py              a gate must see its own edit
```

That excludes `ops/`, `dashboard/`, `scraper/`, `processing/`, `airflow/`,
`dbt_runner/`, `caddy/`, `grafana/` and `.claude/`.

**How often it would fire.** Walking 196 merge commits since 2026-08-01,
counting only those that run the heavy workflow today:

| | |
|---|---:|
| Merges with a diff | 196 |
| Already fast-passed (docs/oneoff only) | 66 |
| Run the heavy workflow today | 130 |
| …of those, would skip `dbt-models` | **75 (57%)** |

57% is a real fraction of a zero. Under the `max()` finding above, those 75 runs
would not have finished any sooner.

**Three reasons it was declined**, in increasing order of weight:

1. **No wall clock to gain**, per the census above.
2. **Narrowing an existing job suppresses evidence.** The asymmetry the plan
   [records for `snapshot-dbt`](../plans/plan_162_testing_census_and_restructure.md#stage-p-dbt-builds-against-production-shaped-data)
   — that a wrong trigger on a net-new job costs coverage never gained — does
   not hold here. `dbt-models` has history, so a missing trigger silently
   removes coverage that exists today.
3. **The safety mechanism degrades exactly where it is needed.** The proposed
   guard was to derive the trigger set from the suite's imports and assert it,
   so a new import fails the contract test until the trigger set catches up.
   That covers **Python imports only**. [Plan
   125](../plans/plan_125_duckdb_to_iceberg_migration.md)'s testing strategy
   adds a DuckDB↔Iceberg parity comparison, a dashboard reader smoke and a Plan
   143 serving-snapshot smoke — dependencies that arrive as compose files,
   catalog configuration, a runtime-selected reader backend and Grafana rules,
   almost none of them as an `import` statement. The guard would stay green
   while going blind to the dependency class the migration introduces.

**On Plan 125's actual effect**, since the premise is easy to get wrong: its
testing strategy says "a **separate, isolated** dbt-spark job with its own venv
[...] **Do not add Spark to the existing `dbt build + test` job**," and DuckDB
tests keep running to Gate E as the adjudicating specification during dual-run.
So `dbt-models` itself does not absorb Spark. What grows more cross-service is
the dbt testing surface around it, which is what reason 3 turns on.

## What survives: caching, not skipping

The 61s of installs is the compressible number, and unlike selection a cache
miss costs time rather than correctness.

Projected, and deliberately **not** promised — having rejected the selector on
measured grounds, the stage should not adopt caching on projected ones:

- Cache `schema-contracts`' installs → job falls to ~80s, ceiling moves to
  `dbt-models` at 103s, workflow ≈ 121s.
- Cache `dbt-models`' 27s of installs too → ceiling becomes `docker-build` at
  96s, workflow ≈ 114s.

Two caveats that have to be measured both ways rather than assumed:

- **The Airflow venv (27s) may be a wash.** `apache-airflow==3.2.0` plus
  requirements is a large tree, and restoring several hundred MB from
  `actions/cache` can cost as much as installing from a warm PyPI CDN.
- **`setup-python`'s `cache: pip` caches downloads, not installs**, so it takes
  a bite out of the 22s rather than removing it.

**And it revives one shelved decision.** Caching promotes `docker-build` to the
critical path. At 96s it is currently third and invisible; once the two jobs
ahead of it are cached it becomes the ceiling, and content-addressed
`docker-build` skipping — which the plan called "the cheaper first win" and
which measures at 0s benefit today — becomes live. That is why its decision
below carries an expiry condition rather than a flat no.

## A cheaper finding, independent of all of the above

Several merges in the walk are `.claude/skills/*.md` — prose, 12 and 14 files at
a time — pulling the entire heavy workflow, three dbt builds included, because
`.claude/` is not in the docs zone. Adding `.claude/skills/**/*.md` to
`DOCS_PREFIXES` is the fail-open direction (prose joining the prose zone) and
carries none of the miss risk the trigger sets were declined for.

**`.claude/settings.json` stays out.** One of those merges paired it with
`tests/scripts/test_build_public_roadmap.py`, and hooks can change what runs.

## testcontainers, asked and answered

[The Stage X origin note](plan_162_stage_X_origin_2026-09-04.md) lists
testcontainers-python as "directly addresses Layer 2's premise and Stage Q's
problem statement." It was evaluated here and declined for Stage Q, on
grounds specific to this repository rather than to the library:

1. **It collides with the invocation instrument.** `service-integration` runs
   five pytest steps against one shared Postgres, `lake-integration` three,
   `schema-contracts` two. Each `run:` is its own pytest session, so
   session-scoped containers mean five startups per job — or collapsing the
   steps into one invocation to amortize them. Named steps are the data the
   invocation rule reads; collapsing them destroys the per-suite granularity in
   the same plan that is trying to sharpen it.
2. **The env coupling is tighter than it looks.** Every integration conftest
   reads `TEST_DATABASE_URL` at **module import time**, not in a fixture —
   `tests/integration/ops/conftest.py`, `.../processing/conftest.py`,
   `.../scripts/conftest.py` — and `tests/integration/archiver/conftest.py`
   says so out loud: "Must run before `shared.db` is imported." Session
   fixtures run after conftest import, so containers would have to start in
   `pytest_configure` or root-conftest module scope — which the unit suite also
   imports, and which must never start a database for its 2,212 tests.
3. **It does not remove the work.** It wraps `docker compose up`; it does not
   change what the compose file says. `docker-compose.ci.yml` — the external
   network and volume flips, the unset `${...}` variables, the MinIO OIDC
   override — is Stage Q, and is needed either way.

On Stage Q's own thesis the two options tie: both source services from the
same Compose definitions. Every tiebreaker after that is cost.

**Where it would have been right**, recorded so the question is not reopened
from scratch: a repository with one pytest invocation per job, or a suite with
no existing service contract. Its genuine wins here — automatic teardown and
dynamic ports — are worth nothing on a runner that is destroyed anyway and on
jobs that each own a VM.

## What Stage Q should take from this

One thing, and it is a strengthening rather than a rescope. `docker compose
config --format json` resolves the whole merge chain — override files, `${VAR}`
interpolation, `extends`, `include` — and emits a normalized document. Verified
against this repository on 2026-09-04: `command:` came back as a list where the
file has a block scalar, and `shm_size: 1gb` came back as `1073741824`, with no
daemon state, no `cartracker-net` and no `cartracker_pgdata` required.

That makes a **complete** parity assertion possible rather than a sampled one:
resolve `-f docker-compose.yml` and `-f docker-compose.yml -f
docker-compose.ci.yml`, diff the two documents, and require the diff to equal a
declared, commented allowlist. Compose does the normalization, so the comparison
is semantic rather than syntactic, and no field can be missed by not having been
thought of — which is the failure mode
[`tests/test_lakehouse_compose_config.py`](../../tests/test_lakehouse_compose_config.py)
has, since it `yaml.safe_load`s individual files and can only assert what it
names.

**One cost**: it shells out to the `docker` CLI, and that parity suite opens by
declaring "No live Docker required." A guard built this way must skip cleanly
when `docker` is absent and be *required* in CI — which is
[Stage U](../plans/plan_162_testing_census_and_restructure.md#stage-u-every-skip-in-ci-is-declared-or-the-run-fails)'s
subject, and one more reason the two stages sequence the way they now do.

## Why Stage R waits for 11–16

Three specific mechanisms, not a general caution. Each is a reason the stage's
final content is better decided after those stages than before them, which is
why this file stops at what the measurements rule out.

1. **Stage U may subsume the instrument fix.** It builds a
   `pytest_terminal_summary` hook covering every job, reporting every skip with
   its declared reason. That is a *runtime* observation of what CI actually ran.
   The instrument fix as scoped is a *static* reimplementation — AST-parse
   `pytestmark`, parse `-m` expressions, simulate pytest's own selection — which
   is the same anti-pattern as transcribing Compose service definitions into
   `services:` blocks. If Stage U's hook is already running everywhere,
   "collected and deselected" and "this directory contributed zero tests to any
   job" become observable facts, and the instrument may reduce to reading its
   output.
2. **Stage X creates a new SQL root with its own census.** A tree of `.sql`
   files outside `shared/sql/` is a new path class that any invocation rule
   would have to know about, and it does not exist yet.
3. **Stage T may move the suite boundaries.** It consolidates 96 ad-hoc
   `INSERT`s, 55 module-local seed helpers and 43 duplicated `SELECT`s into
   shared fixtures. The invocation rule's unit of analysis is the suite
   directory; consolidation across directories changes what that unit means.
