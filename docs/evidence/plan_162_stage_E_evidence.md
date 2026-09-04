# Plan 162 Stage E — splitting the 267s dbt job

**Legacy:** Stage 4 · **Issue:** CAR-48 · **Closed:** 2026-09-01

The record entry this belongs to is [`plan_162` §Record](../plans/plan_162_testing_census_and_restructure.md#record), under Stage E. It carries the summary; the sections below are the detail.

---

#### What the job was, and why the cut is by prerequisite

Eight suites ran in series on one runner behind one Flyway migration, one
`pip install`, one `dbt build` and one Airflow venv build. The suites shared
a runner and nothing else, so the split is by the only thing they *did*
share:

| Job | The prerequisite it pays for | Suites |
|---|---|---|
| `dbt model tests (real build)` | a real `dbt build --target duckdb` | `dbt/` |
| `SQL + Airflow metadata contracts` | that build **and** the `airflow` schema | `sql/`, `airflow/` |
| `Service integration tests (Postgres)` | a migrated Postgres, nothing else | `ops/`, `scripts/`, `processing/`, `scraper/`, `dbt_runner/` |
| `Lake integration tests (MinIO)` | the Plan 120 fixture in object storage | `shared/`, `archiver/` |

`tests/integration/sql/` is the only suite needing two prerequisites — 21
tests read dbt's marts out of DuckDB, 92 read Postgres, one file reads
Airflow's own `task_instance`/`dag_run`. So `schema-contracts` runs its own
`dbt build` rather than waiting on `dbt-models`. Passing the DuckDB file
between jobs as an artifact was costed and rejected: it puts the two longest
jobs in series, which is the thing the split exists to stop. The second build
costs ~31s in a job that is not on the critical path.

The Airflow venv now lives in the one job whose suites import Airflow. It ran
ahead of all eight before, six of which never import it.

#### Plan 139 Stage C's question, answered

**The 92s step was never running tests. It was starting Python.**

`--durations=20` in CI: 16 tests, 93.99s. Seven real-build tests were 93.25s
of it — **99.2%** — and the nine selector-equivalence tests were 0.44s
together. Those seven drive 21 `dbt build --select` subprocesses at a mean
**4.44s**, while dbt's own report for one such build reads *"1 incremental
model, 1 project hook, 12 data tests, 4 unit tests in 0.61 seconds"*.

The decomposition, measured in the job rather than reasoned from adapter
internals:

| | |
|---|---:|
| `import dbt.cli.main` | 1.37s, once per process |
| `dbtRunner().invoke(build)` | 1.19 – 1.24s |
| `dbtRunner(manifest=...).invoke(build)` | 1.04 – 1.14s |
| the same build as a subprocess | ~4.2s |

So roughly 3s of every 4.4s was Python starting and importing dbt, paid 21
times: **~80s of the 93s, against ~13s of actual dbt work.**

**Plan 139's hypothesis was half right, and the wrong half is the useful
one.** It guessed "each test drives its own real dbt invocation and they could
share one build". The first clause holds. The second cannot: each test's
subject *is* a sequence of incremental builds with fixture data seeded
between them, so there is no single build to share. Only the startup was
shareable. Sharing it took the suite to **24.26s** — the seven from 93.25s to
23.83s, every test between 2.33s and 4.92s where the cheapest had been
12.31s. Both of those are pytest's own timer on one run each, which is the
only way to get a per-test breakdown; the CI *step* around them reads 86-95s
before and 25-37s after, across three runs each.

Manifest reuse was measured and **not** taken: ~0.15s per invocation, about
three seconds total, in exchange for running a configuration the numbers
above were not measured against.

#### A property nearly lost, then re-established somewhere else

All three readers in `tests/integration/dbt/` opened DuckDB with
`read_only=True`, so no assertion could mutate the warehouse it inspected.
dbt-duckdb caches its environment across invocations — precisely why an
invoke is 1.2s rather than 4.4s — so it holds the file open read-write for
the life of the pytest process, and a read-only *connection* is no longer
available. Two ways of keeping one were tried against duckdb 1.5.5, the
version CI runs, and both are closed:

```
connect(path, read_only=True)   ConnectionException: Can't open a connection to
                                same database file with a different
                                configuration than existing connections
:memory: + ATTACH (READ_ONLY)   BinderException: Unique file handle conflict
```

A third, reading a copy of the file, works and was rejected on meaning rather
than mechanism: every assertion would then describe a snapshot instead of the
warehouse the build actually wrote.

**So the guard moved from the connection to the statement**, which is a
different mechanism for the same property and on one axis a stricter one. It
was always the *statements* that were the risk, and a read-only connection
never had an opinion about `COPY ... TO`, which reads the warehouse and writes
the filesystem. `ReadOnlyConnection` classifies every statement with
`duckdb.extract_statements` — the engine's own parser, so no regex over SQL
text and no special case for `WITH ... SELECT` — and allows only `SELECT` and
`EXPLAIN`. **Deny by default**: a statement type a future DuckDB adds is
refused until someone reads it and adds it.

This is success criterion 2 applied to the stage's own work, so it has an
assertion behind it rather than a paragraph.
`test_analytics_connection_guard.py` is 16 tests and runs unmarked in the unit
job, because it needs no MinIO, no Postgres and no dbt. Verified by breaking
the guard four ways:

| Mutation | Result |
|---|---|
| `INSERT`/`DELETE` added to the allowlist | 5 fail |
| only the first statement of a multi-statement string classified | 1 fails |
| the check moved after execution instead of before | 1 fails |
| the check removed entirely | 11 fail |

**The first run of that mutation set was wrong, and how it was wrong is worth
more than the table.** Restoring each mutation with `cp` set the source
mtime inside the same second as the `.pyc` written moments earlier, so Python
kept the *mutated bytecode* and ran it against restored source. The visible
symptom was a test failing while `inspect.getsource` showed correct code —
`refuse_writes` provably first in the file, and a `DELETE` reaching the
database anyway. It was diagnosed by wrapping the connection in a spy that
logged every statement reaching it, which showed the execute happening
*before* the refusal.

Nothing was wrong with the guard. **The verification tooling was lying, in a
way that read exactly like a defect in the thing under test** — which is this
plan's own recurring finding pointed at itself, and the reason the mutation
script now clears `__pycache__` around every edit rather than trusting a
timestamp.

**What it does not cover, stated so the limit is known:** a caller that
reaches past the wrapper for a raw connection. Nothing in the directory does
today.

The change reaches further than the two files that build: all three share one
pytest process, so the selector suite's reads moved too, though it invokes dbt
never.

#### Two findings that were not scoped and cost nothing

**The `loki` service was dead weight.** The old job started three service
containers; nothing under `tests/integration/` has ever opened port 3100. It
arrived with `cf216c8`'s log-aggregation work and outlived it. Removing it
took **"Initialize containers" from 44–56s to 12–16s** — far more than
expected, because the Loki image pull was most of it.

**Every job installed every service's dependencies, and none of them needed
to.** ~220MB of wheels per job, because the single job it replaced genuinely
did need all seven files. Measured by walking each suite's imports including
its first-party ones:

- **`dashboard/` is ~50MB** of streamlit, pydeck, pandas, plotly and pillow
  that no integration suite reaches. `tests/integration/sql/` imports
  `dashboard.queries`, and that module imports `pathlib` and
  `shared.query_loader`.
- **`scraper/` brings curl_cffi (13.5MB) and asyncpg**, and the scraper suite
  reaches neither: it imports `processing.queries` and `scraper.queries`, both
  pure `load_query` modules.

`Install dependencies` went **44s to 17–20s** in the three jobs that dropped
`dashboard/`, and 46s to 41s in `service-integration`, which still needs four
files. The failure direction is the safe one — a distribution that turns out
to be needed is an ImportError at collection, named and immediate.

#### Success criterion 3 is met

The criterion is *"the `dbt build + test` job is no longer the critical path,
and what replaced it is named for what it does"*. The job is gone; the four
that replaced it are named for what they run; the workflow went 292s to
145-165s and the job's successor 267s to 118-134s across three runs.

The third clause is the one worth stating precisely rather than rounding, and
**the first attempt to state it here was wrong on one reading — which is this
plan's own recurring finding, committed by this plan.** On the first run after
the in-process change, `dbt model tests (real build)` was 118s against `Unit
tests (pytest)` at 112s, and this section said the two were "within runner
variance of each other". The next run said 134s against 97s:

| | run 33465499674 | run 33466934324 | run 33467252321 |
|---|---:|---:|---:|
| workflow wall clock | 145s | 156s | 165s |
| `dbt model tests (real build)` | 118s | 134s | 128s |
| `Unit tests (pytest)` | 112s | 97s | 97s |
| gap | 6s | **37s** | **31s** |

**Three readings, and the dbt job is the longest in all three.** It is no
longer a 267-second critical path; it is still the critical path. The honest
form of the claim is that the job's cost fell by about half and stopped
dominating, not that something else overtook it — and one run was never enough
to say which, on a runner whose variance moves a 25s step to 37s.

**The third run is the one that settles it, and it settles it the other way
from the guess.** It was taken after the temporary measurement step was
deleted, which the previous draft of this section expected to be worth 7-11s;
the job came back at 128s and the workflow at its slowest of the three. The
rig was never the variable. Runner variance was, and it is larger than the
thing being subtracted — which is the argument for quoting three runs
everywhere above rather than the best one.

**The criterion was accepted as met by the maintainer on 2026-09-01 with this
reading on the record**, which is a decision about what "no longer the critical
path" was worth in practice, not a measurement that says something else is
slower. A stage that wants the dbt job genuinely off the top has a sized
target waiting: its remaining `Install dependencies` is 18s and its `dbt build`
16s, against 25-37s of tests.

#### What was deliberately not done

- **No test file moved and no step was renamed.** Two `ci.yml` step names are
  subjects in `LAYER_NUMBER_WAIVERS`, and `tests/integration/sql/`'s files
  appear there too; renaming or moving either would have meant rewriting
  waivers that Stage F's G11 sweep deletes outright — the same collision
  [Stage G's placement argument](../plans/plan_162_testing_census_and_restructure.md#stage-g-what-the-split-is-and-why-a-directory-rather-than-a-list)
  is built on. The waiver list is untouched at 116.
- **`docs/PLANS.md` was moved through the `plans` skill**, not edited here.
  Build-order row 1 keeps its position; only its slice pointer advanced to
  Stage F, since Stages F through P remain. This plan's non-goal is authoring
  that file mid-stage, and the skill's boundary is what kept the two apart:
  both authored cells were proposed with sources and approved before the row
  was touched.
- **The measurement rig is gone.** `.github/scripts/measure_dbt_invocation_cost.py`
  and its `ci.yml` step existed to answer Stage C and were deleted once the
  answer was recorded above. The numbers it produced are in this section; the
  script is in the history at `e3b4c82` if a later stage wants to re-run it.


**Stage K's closeout rode in on this PR.** `f6a7077` was already on the branch
when Stage M started — the same pattern as Stage L on PR #344 — so #347 also
carried `docs/PLANS.md` and `ops/static_ops/generated/project-updates.json`,
the generated roadmap the landing page renders. Neither is Stage M's work and
neither is in `public-surface-check`'s scope, which covers the two authored
surfaces only. It is recorded because a published surface moved inside a PR
reviewed as a testing change, and that is the shape worth noticing rather than
the content, which was 6c's and correct.

**One incidental finding.** `scripts/redeploy.sh` is `-rw-rw-r--` in the
checkout, so `./scripts/redeploy.sh` is `Permission denied` and it has to be
invoked as `bash scripts/redeploy.sh`. Not this stage's to fix, and recorded
because the next person to deploy will hit it.

#### Cost, and one regression worth recording

The stage cost one red CI run, and it was self-inflicted in a way the suite
was already built to catch: the split commit wrote `apache-airflow==3.2.0`
into two YAML *comments*, and
`test_ci_pins_the_airflow_version_production_runs` greps the whole workflow
for that pattern and requires exactly one distinct match. It found three —
`3.2.0`, ``3.2.0` `` and ``3.2.0`.`` — having swept up markdown backticks.
The test was right and the comments were wrong; a version in prose is a second
declaration that drifts. **It was invisible locally because the assertion is
integration-marked and runs inside the isolated Airflow venv, which a
`-m "not integration"` run never reaches** — the same shape as Stage D's
finding, and the second time in two stages that pre-push verification proved
only what the main venv could see.

One harmless consequence of in-process dbt, recorded so it is not mistaken
for a defect later: dbt-core's own Click deprecation warnings now surface in
this suite's output, because dbt is imported into the pytest process instead
of hidden inside a subprocess.
