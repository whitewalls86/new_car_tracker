# Plan 139: Test Suite Construction and Maintenance

## Status

**STAGES A+B COMPLETE 2026-08-18** — merged as PR #213 (`4fa6c7d`). Surfaced
2026-08-17 during Plan 135 Stage 4 development, when the unit suite's
wall-clock time prompted the question "are we missing a mock somewhere?" The
answer was no. Re-measured 2026-08-18 against **real CI job timings**, which
overturned the original draft's central assumption and moved one of its steps
into another plan.

Coverage now reports on every CI run at an 88% baseline with no gate, and the
critical path is a stable ~260s against the 333s/345s/278s baseline. Both A+B
have left the build order. Stages C and D remain queued at their lower
build-order positions.

## What was actually measured

### The local unit suite (2026-08-17, warm)

| Measure | Value |
|---|---|
| Unit tests | **2,212** in **~10.8s** (`-m "not integration"`) |
| Per test | **~4.9ms** |
| Collection | 1.4s of that |
| Slowest single test | **0.29s** (`test_html_sections.py`, real detail-page parsing) |
| Top 25 slowest combined | ~2.5s |
| Setup/teardown phases over 20ms | **zero** |
| Coverage (6 service packages) | **88%** — 6,834 statements, 845 missed |
| Integration tests | 41 files, run in CI's `dbt` job across 6 serialized pytest invocations |

**There is no missing mock.** The empty setup/teardown list is the proof: a
fixture blocking on real I/O lands in *setup*, normally at 100ms+. Nothing does.
The residual ~7s is roughly 2,187 tests at ~3ms each — pytest's per-test
overhead, not work anyone wrote. ~11s is close to the floor for this suite as
structured.

One correction worth recording: a 15.0s figure quoted earlier that day was a
**cold** run populating `__pycache__`. Warm local runs are consistently
10.7–10.9s. **CI is always cold**, and the same suite takes **22s** there — so
the local number is the wrong one to reason about CI with, and both appear below.

### The full test-area breakdown (2,212 total)

The original draft's table omitted four areas. Corrected and complete:

| Area | Tests | Share |
|---|---:|---:|
| **`tests/scripts/`** | **517** | **23%** |
| `tests/archiver/` | 485 | 22% |
| `tests/ops/` | 244 | 11% |
| `tests/scraper/` | 224 | 10% |
| `tests/processing/` | 197 | 9% |
| `tests/shared/` | 176 | 8% |
| `tests/lakehouse/` | 164 | 7% |
| **`tests/` (root, compose/observability config)** | **130** | **6%** |
| `tests/dbt_runner/` | 39 | 2% |
| `tests/airflow/` | 31 | 1% |
| `tests/dbt/` | 5 | <1% |

The 130 root-level tests matter to name, because they are the *config-coverage
assertion* tests — `test_observability_config.py`,
`test_pack_worker_compose_config.py`, and friends — and
[Plan 140](plan_140_service_health_contract.md) Stage 3 extends exactly that set.

### CI critical path (run `32090798835`, 2026-08-18, representative of three
consecutive successful runs)

This is the measurement the original draft was missing, and it is the one that
matters.

```
lint (13s) ──> unit-tests (62s) ──> dbt (253s)          total: 5m33s
docker-build (91s) ─────────────────────────────────    (parallel, off path)
```

Step-level, for the two jobs on the critical path:

| Job | Step | Time |
|---|---|---:|
| unit-tests | checkout | 2s |
| unit-tests | **pip install** | **35s** |
| unit-tests | **pytest (2,212 tests, cold, `-v`)** | **22s** |
| dbt | pull flyway image | 7s |
| dbt | **initialize 3 service containers** | **45s** |
| dbt | flyway migrate | 3s |
| dbt | **pip install** | **33s** |
| dbt | install dbt + deps + duckdb ext | 11s |
| dbt | seed MinIO fixtures | 2s |
| dbt | dbt build | 15s |
| dbt | SQL smoke (Layer 1) | 2s |
| dbt | API integration (Layer 3) | 2s |
| dbt | **isolated Airflow venv install** | **25s** |
| dbt | Airflow integration | 4s |
| dbt | Archiver integration | 6s |
| dbt | **selector/dbt equivalence (`tests/integration/dbt/`)** | **92s** |
| dbt | dbt_runner integration | 0s |

Two facts fall out of this table and neither was visible before:

1. **Dependency installation is 103s of the 333s critical path — 31%.** Four
   separate uncached `pip install` steps. Nothing is cached; `actions/setup-python`
   is used without `cache: pip`.
2. **The six integration invocations are not six comparable costs.** Five of them
   total 14s. One — `tests/integration/dbt/`, **16 tests** that shell out to real
   dbt builds — takes **92s**, 87% of all integration test time and 28% of the
   entire critical path.

---

## Question 1 — Should script tests run on every build?

**Decided: no split on the `scripts/` boundary. Split on "shipped or run by CI"
vs "one-off measurement", and only after fixing a misfiled script.**

The draft framed this as scripts-vs-production and suspected the real axis was
load-bearing-vs-one-off. That suspicion was right, and the evidence is sharper
than expected.

**`scripts/` already contains production code.**
`scripts/report_dbt_run_results.py` is `COPY`'d into the dbt_runner image
(`dbt_runner/Dockerfile:21`). It ships. Its 14 tests are not optional and the
directory it lives in is simply wrong.

**And `scripts/` is not uniformly over-tested — it is unevenly tested.** There are
**31 scripts and 14 test files.** The 517 tests concentrate on fewer than half the
scripts; 17 have no tests at all. "Scripts are over-tested" is not the finding.
"Testing effort in `scripts/` is uncorrelated with what the script is for" is.

Per-file, largest first:

| Test file | Tests | Classification |
|---|---:|---|
| `test_rewrite_parquet_layout.py` | 109 | one-off (Plan 110 layout migration, done) |
| `test_audit_parquet_layout.py` | 78 | one-off measurement |
| **`test_audit_sectioned_html_storage.py`** | **65** | **one-off for [Plan 114](plan_114_sectioned_html_artifact_audit.md) — measured and rejected** |
| `test_recompress_bronze_html.py` | 46 | one-off (Plan 129 backfill) |
| `test_estimate_dictionary_savings.py` | 40 | one-off measurement |
| `test_estimate_recompression_savings.py` | 36 | one-off; spends tests on argparse error paths |
| `test_run_local_lakehouse_rehearsal.py` | 27 | **load-bearing** — documented local dev entry point |
| `test_lake_snapshot_common.py` | 23 | **load-bearing** — imported by 3 other scripts |
| `test_estimate_pack_savings.py` | 23 | one-off measurement |
| `test_audit_adaptive_refresh_features.py` | 16 | one-off measurement |
| `test_train_html_dictionary.py` | 14 | one-off, but reruns if dict v2 is trained |
| **`test_report_dbt_run_results.py`** | **14** | **ships in the dbt_runner image** |
| `test_download_lake_snapshot.py` | 14 | **load-bearing** — Plan 120 local/CI snapshot path |
| `test_seed_lake_snapshot.py` | 12 | **load-bearing** — CI runs `seed_lake_snapshot_fixture.py` |

**65 tests gate every pull request on a script written to measure an approach the
project measured and rejected.** That is the concrete version of the draft's "some
of it is thin," and it is the clearest single candidate to move behind a marker.

**Decision:** add a `oneoff` marker, apply it to the six audit/estimate/migrate
files (~299 tests, 14% of the suite), keep them runnable on demand and in a
nightly or pre-release run, and deselect them from the per-PR job. Move
`report_dbt_run_results.py` out of `scripts/` into `dbt_runner/` where it is
already deployed.

**But note the payoff honestly:** 299 fewer tests saves roughly 1.5s of a 22s
pytest step inside a 62s job inside a 333s pipeline — **under 0.5% of the
critical path.** This is a *legibility and intent* change, not a speed change,
and the plan should not pretend otherwise. It ranks last for that reason.

## Question 2 — Can we parallelize?

**Answer changed on measurement. `pytest-xdist` is real but it is the fifth-best
lever, not the first.**

The draft said "the wall-clock prize is not the 11s job" and then ranked xdist
third anyway. With CI step timings in hand, the levers rank cleanly by payoff
against the 333s critical path:

| # | Lever | Saves | Share of path | Effort | Risk |
|---|---|---:|---:|---|---|
| 1 | **Cache pip** (`cache: pip` on all 3 `setup-python` steps + the Airflow venv) | ~60–75s | **~20%** | XS | none |
| 2 | **Drop `needs: unit-tests` from the `dbt` job** | ~78s | **~23%** | XS (one line) | see below |
| 3 | **Profile the 92s `tests/integration/dbt/` step** | unknown, up to 90s | up to 27% | S | none to measure |
| 4 | **Trim the 45s container initialization** | up to ~20s | ~6% | S | image pinning |
| 5 | `pytest-xdist -n auto` on unit tests | ~14s (22s → ~8s) | **~4%** | S | shared-state, see below |

Levers 1 and 2 together are **~40% of CI wall-clock for two lines of YAML.**

**On lever 2 — `needs:` is a scheduling edge, not an enforcement one.** This is
the part worth being precise about, because "remove a `needs:`" reads like
removing a safety gate and it is not. Both jobs still run, both still have to
pass, and the run is still red if either fails. Nothing merges that would not
have merged before. The only thing that changes is whether the 253s job starts at
T+15 or T+80.

There is also no data dependency between them: the `dbt` job does its own
checkout and its own `pip install`, and no artifact is uploaded by `unit-tests`
or downloaded by `dbt`. The edge is purely a fail-fast convention.

Measured 2026-08-18, the two things that convention was buying:

- **The repository is public**, so Actions minutes are free and unlimited. The
  "wastes four minutes of runner time on a broken PR" cost is **$0**.
- **The gate has never fired**: unit tests passed **40 of the last 40 runs.** The
  local suite is 11s, so it gets run before pushing and CI catches nothing new.

**The one genuine remaining cost is signal clarity.** If unit tests break, `dbt`
now fails too, with a wall of downstream integration noise that could pull
attention away from the actual fault. Small — the `unit-tests` job is still in the
checks list and still finishes first (62s against 253s) — but it is the real
argument, and it is the one to weigh. Keep `needs: lint` regardless; the 13s
syntax gate costs nothing and catches a genuinely different class of error.

> **The 40/40 is a dated measurement, not a property of the suite, and it is
> expected to stop holding.** It is 40/40 *because* local and CI currently run the
> same thing against the same inputs, so an 11s pre-push run catches everything
> first. [Plan 120](plan_120_ci_lake_snapshot_delivery.md) is deliberately
> changing that: its design pushes snapshot logic into the unit suite as
> "fast, MinIO-free unit tests" (`base_path=None` reads plain files), and
> `tests/scripts/test_download_lake_snapshot.py::TestDownloadApiAgainstOpsRouter`
> is already a real-app `TestClient` round trip **proving downloader/ops route
> wire compatibility**. That is a cross-component contract test living in the unit
> suite — the class of test that can pass against a developer's local state and
> fail in CI. As snapshotting integrates further, the unit job stops being a
> formality that always passes and starts being a place where CI genuinely learns
> something.
>
> This does not reverse the decision — the gate is still scheduling, not
> enforcement, and both jobs still block a merge either way. But it sets the
> **re-evaluation trigger**: if the unit job's CI-only failure rate becomes
> non-trivial (say it catches something local runs missed more than once or twice
> in a quarter), restore `needs: unit-tests`. At that point the fail-fast ordering
> is buying real debugging clarity rather than guarding against an event that has
> not happened. Re-measure the pass rate when Plan 120 closes out, not on a
> schedule.

**One interaction between levers 1 and 2.** Serialized, `unit-tests` populates the
pip cache and `dbt` restores it warm. Run in parallel, both cold-miss and race on
the same cache key. Across subsequent runs both restore from the prior run's
cache, so they compose — but the first run after any requirements change loses a
little. Expected paths against the 333s baseline: caching alone ~281s, dropping
`needs:` alone ~268s, both ~243s.

**On lever 5, the draft's hazard note stands and is now specific.** Several
suites mutate process-global state — `tests/archiver/test_app.py` has a
module-scoped autouse fixture flipping `archiver_app._ALLOW_PACK_JOBS` (8
occurrences in that file alone), and CI sets a single shared `LOG_PATH=/tmp/app.log`
that `tests/shared/test_logging_setup.py` writes to. Under xdist each worker is a
separate process, so the module-global is *probably* fine and the shared log path
*probably* is not. **Verify before enabling, do not assume**, and note that
`pytest-xdist` is not currently in `requirements-dev.txt` — it would be a new
dependency for a 4% gain.

**The draft's hypothesis that the six integration invocations could "share
setup" is disproved.** Setup is not their cost. Five of the six run in 14s
combined. The cost is 16 tests in one directory that each drive a real dbt build.
Sharing setup across them saves nothing; making those 16 tests cheaper might save
90s. That is lever 3, and it is a measurement task before it is a change.

## Question 3 — What's coverage like? Any holes?

**88% overall**, which is healthy. Three structural gaps matter more than the
number, and the third has a sequencing correction.

**a) Coverage is not measured in CI at all.** No `--cov` flag, no
`[tool.coverage]` config, no gate, no trend. The 88% above had to be computed by
hand for this doc. A number nobody watches will drift.

> **Correction to the draft:** it stated that "`pytest-cov` and `pytest-mock`
> are [installed]". `pytest-mock` is in `requirements-dev.txt`; **`pytest-cov` is
> not.** Only the `coverage` package exists, in the local `.venv`, installed by
> hand. CI installs from `requirements-dev.txt` and therefore has neither. Step 1
> is "add a dependency and configure it," not "add a flag."

**b) Two directories are outside coverage entirely** — `airflow/dags/` and
`dashboard/`. DAG logic is currently only exercised by the pure predicate
functions the DAGs expose (the `try/except ImportError` pattern), which is a
deliberate and good design, but it means DAG wiring is unmeasured.

**c) The worst-covered files, and which of them actually matter:**

| File | Cover | Note |
|---|---:|---|
| `archiver/processors/lake_source_audit.py` | 21% | |
| **`ops/metrics/duckdb_gauges.py`** | **25%** | **see below — ultimately transferred to Plan 143** |
| `archiver/processors/lake_snapshot_cohort.py` | 44% | largest single gap, 183 statements |
| `archiver/processors/lake_snapshot_selectors.py` | 53% | |
| `scraper/app.py` | 65% | already Plan 103's P2 (recorded there as 69%) |

### The `duckdb_gauges.py` correction — this step must not be done here

The draft ranked "cover `ops/metrics/duckdb_gauges.py`" as step 2, reasoning that
**"Plan 136 is about to depend on it."** That is backwards, and doing it in this
plan would actively waste work.

[Plan 136](plan_136_solver_recycle_and_liveness.md) **Stage 1a rewrites this
module's failure semantics**: on any refresh failure the gauges will be set to
`float('nan')` instead of retaining their previous value. The module is 132 lines
of seven nested `try/except` blocks, each of which currently swallows its error
and leaves a gauge holding a stale reading — which is Plan 136's defect D2 and the
cause of the 8-hour blind spot.

Writing coverage tests against the module as it stands today means writing
assertions like *"on a lock conflict, the gauge keeps its last value"* — pinning
in place the exact behavior Plan 136 Stage 1 exists to delete. The tests would be
written and then deleted within the same build order.

**Decision: this step first transferred to Plan 136 Stage 1 and is removed from
Plan 139.** Pre-PR architectural review then moved that whole stage to
[Plan 143](plan_143_analytics_serving_snapshot.md). The tests still belong with
the behavior change, encoding the new NaN/freshness convention rather than the
old silent-stale one; Plan 143 now owns both the replacement producer and its
coverage.

This also matters for prioritizing Plan 139 at all, because step 2 was the only
item in it with a dated dependency. Once it moves, nothing in this plan blocks
anything.

Note also that Plan 103 already owns part of this territory (P1 `ops/info.py`,
P2 `scraper/app.py`) and Plan 107 folds Plan 103 in. **This plan should not
duplicate them** — its distinct contribution is the CI measurement gap (a), the
uncovered directories (b), and the CI wall-clock work in Question 2.

---

## Goal

1. Coverage is a number CI computes and reports on every PR, so drift is visible
   before it is argued about.
2. CI's critical path loses the ~40% of its wall-clock that is pure packaging
   overhead and an unnecessary job dependency.
3. Test files declare their intent — shipped code, CI-load-bearing, or one-off
   measurement — rather than being sorted by which directory someone put them in.

Explicitly **not** a goal: a higher coverage percentage.

## Stages

Ordered by payoff-per-effort, which is not the draft's order.

### Stage A — Make coverage visible (XS)

1. Add `pytest-cov` to `requirements-dev.txt`. It is not currently there.
2. Add `[tool.coverage.run]` / `[tool.coverage.report]` to `pyproject.toml`,
   next to the existing `[tool.pytest.ini_options]`. Name the six service
   packages explicitly.
3. Add `--cov --cov-report=term-missing` to the CI unit-tests step.
   **Report only. No `--cov-fail-under`.**
4. Record the resulting CI number in this doc as the baseline, and note whether
   it differs from the hand-computed 88% (it will — CI's cold run and the
   `-m "not integration"` selection differ from how the local figure was taken).

**Verify:** the CI unit-tests job prints a coverage table; the job still passes;
the added time is under 3s.

**Deliberately no gate.** A threshold added before anyone has watched the number
for a few weeks is a number picked from the air, and its first effect is to block
an unrelated PR. Decide the gate in Stage D.

### Stage B — Recover the CI critical path (XS, highest payoff)

1. Add `cache: pip` to all three `actions/setup-python@v5` steps, with
   `cache-dependency-path` covering the requirements files each job installs.
   Cache the isolated Airflow venv install separately or accept its 25s.
2. Change the `dbt` job's `needs: unit-tests` to `needs: lint`. This preserves the
   13s syntax gate and lets the 253s job start ~65s earlier. Both jobs still run
   and both must still pass — see the enforcement-vs-scheduling note above.

**Verify:** compare three post-change runs against the three-run baseline recorded
above (333s, 345s, 278s critical path). Expect ~200–230s. If pip caching does not
help, say so and revert it rather than keeping a cache that only adds a restore
step.

**Explicit risk on step 2:** when unit tests fail, the `dbt` job now fails
alongside them with downstream integration noise. If that turns out to cost more
debugging attention than the 65s is worth, restore the edge — it is a one-line
revert with no migration.

#### Stages A+B implementation evidence (2026-08-18, PR #213)

The first CI coverage report measured **88%: 6,835 statements, 841 missed** over
the six configured service packages. This agrees with the hand-computed 88%; the
four-statement difference (6,834/845 by hand) is measurement-environment noise,
not a material baseline change. The selected CI suite was 2,224 passed and 399
deselected.

The default Coverage.py core initially increased the pytest step from 22s to
43.17s. Setting `COVERAGE_CORE=sysmon` on Python 3.13 reduced it to 22.82s on the
next run and about 20s on the following run, satisfying the under-3s overhead
criterion without changing the 88% result.

Three consecutive post-change critical paths, measured from workflow creation
through completion of the `dbt` job, were:

| Run | Critical path | `dbt` job | Note |
|---|---:|---:|---|
| 32103672753 | 307s | 292s | requirements changed; cold dependency key; default coverage core |
| 32104051715 attempt 1 | 261s | 245s | warm distributions; `sysmon` coverage |
| 32104051715 attempt 2 | 260s | 243s | warm distributions; `sysmon` coverage |

Against the 333s/345s/278s baseline, the scheduling-edge change removed the
unit-job wait and produced a stable warm path around 260s. The expected 200-230s
was not reached because the pip-cache hypothesis was wrong: even on a restored
cache, the shared dependency install remained 29-32s (33s before), since
`setup-python` caches downloaded distributions rather than the installed
environment. That saving is too small to clear the baseline's runner variance.
Per Stage B's verification rule, all three `cache: pip` additions were reverted;
the successful `needs: lint` change remains.

Checked against the stage specs after the merge: `pytest-cov` is in
[requirements-dev.txt](../requirements-dev.txt); `[tool.coverage.run]` names the
six service packages and `[tool.coverage.report]` sets `show_missing`;
[ci.yml](../.github/workflows/ci.yml) runs `--cov --cov-report=term-missing`
with **no `--cov-fail-under` anywhere in the repo**, as Stage A required; the
`dbt` job reads `needs: lint`; and no `cache: pip` remains. Every A+B item is
either delivered or deliberately reverted under the stage's own rule.

### Stage C — Understand the 92s step (S, measurement first)

`tests/integration/dbt/` is 16 tests and 28% of the CI critical path. Nobody has
looked at why.

Run it with `--durations=20` in CI and record the per-test breakdown here before
proposing any change. The likely finding is that each test drives its own real
dbt invocation and that they could share one build — but that is a hypothesis, and
the last hypothesis in this plan about integration setup was wrong.

**Constraint:** these tests exist as the coupling guard between the Plan 120
selector SQL and the real dbt models. Any change must keep them running against a
real dbt build. Per the project's standing rule, **this work is CI-only** — do not
pip-install dbt locally to iterate on it; extend
`scripts/seed_lake_snapshot_fixture.py` if fixture changes are needed.

### Stage D — Intent markers and the coverage decision (S, opportunistic)

1. Move `scripts/report_dbt_run_results.py` into `dbt_runner/`. It ships in that
   image; it is not a script.
2. Add a `oneoff` marker to `pyproject.toml`'s `markers` list, with a docstring
   defining it as "measurement written for a specific plan, kept for
   reproducibility, not gating."
3. Apply it to the six audit/estimate/migration test files (~299 tests). Add a
   nightly or on-demand workflow that runs `-m oneoff` so they do not silently rot.

   > **Mark per test class, not per file, and read each file before marking it.**
   > `tests/scripts/` is no longer uniformly one-off: `test_download_lake_snapshot.py`
   > mixes ordinary script tests with `TestDownloadApiAgainstOpsRouter`, a
   > downloader/ops **wire-compatibility** test that must keep gating. Question 1
   > classified that file load-bearing and so happens to be safe, but it was
   > whole-file reasoning and it got the right answer by luck. As
   > [Plan 120](plan_120_ci_lake_snapshot_delivery.md) puts more cross-component
   > contract tests in the unit suite, whole-file marking will eventually
   > de-gate one of them silently — which is the exact failure this repo keeps
   > having, in a new place.
4. With several weeks of Stage A data in hand, decide: add `--cov-fail-under` at
   the observed floor, or record explicitly that there is no gate and why.
5. Decide whether `airflow/dags/` and `dashboard/` join the coverage
   configuration, **or** record deliberately that they are excluded and on what
   argument. Either answer is fine; the current state — unmeasured and unexplained
   — is not.
6. Optionally evaluate `pytest-xdist`, having first verified the `LOG_PATH` and
   `_ALLOW_PACK_JOBS` shared-state hazards under `-n auto`. 4% of the critical
   path for a new dependency; it may not be worth it, and "we measured it and
   declined" is an acceptable outcome to record.

## Success criteria

1. Every PR shows a coverage number in the CI log, and this doc records the
   baseline it started from.
2. Three consecutive post-Stage-B runs show a measured critical path reduction,
   recorded here against the 333s/345s/278s baseline.
3. `tests/integration/dbt/`'s 92s has a written per-test explanation, whether or
   not it gets faster.
4. Every test file in `tests/scripts/` is either load-bearing (unmarked, gating)
   or `oneoff` (marked, non-gating, still run on a schedule) — with no third
   category of "nobody looked."
5. The coverage gate question is answered in writing, in either direction.
6. The replacement analytics metric producer has tests — **written under Plan
   143, asserting the NaN and freshness conventions** — and this plan records
   that the original 25% gap was closed with the redesign.

## Risks

- **Stage B step 2 trades runner minutes for latency.** On a repo with frequent
  broken pushes this is the wrong trade. It is the right one here, but it is a
  preference, not a fact, and it should be reverted without ceremony if PR
  failures become common.
- **A coverage number invites a coverage target.** The moment 88% is on every PR
  someone (possibly me) will want it to be 90%. That is Plan 107's fight, on
  purpose, and this plan's explicit non-goal exists to keep Stage A from becoming
  it.
- **The `oneoff` marker can become a place to hide failing tests.** A marked test
  that nobody runs is worse than a deleted one, because it looks like coverage.
  Stage D's scheduled `-m oneoff` run is not optional garnish; without it, delete
  the tests instead of marking them.
- **Measuring CI by three runs is thin.** GitHub runners vary — the three baseline
  runs already span 278–345s, a 24% spread. Any claimed improvement smaller than
  that spread is not an improvement. Both Stage B's and Stage C's verification must
  clear it.

## Out of scope

- **`ops/metrics/duckdb_gauges.py` coverage.** Transferred through Plan 136 Stage
  1 to [Plan 143](plan_143_analytics_serving_snapshot.md), for the reason argued
  above. Listed here so the transfer is a decision on the record and not a
  dropped item.
- **`ops/info.py` and `scraper/app.py` coverage.** Owned by
  [Plan 103](plan_103_test_coverage.md), folded into
  [Plan 107](plan_107_quality_to_90.md).
- **Chasing 88% upward as a goal in itself.** The holes named above are worth
  closing because something depends on them, not to move a number.
- **Rewriting the integration layering.** [Plan 84](plan_84_integration_testing.md)
  established Layers 1-3 and the ordering constraint (SQL smoke before dbt); that
  design is not in question here.
- **Config-coverage assertions** (compose healthchecks, promtail job sets). Those
  are [Plan 140](plan_140_service_health_contract.md) Stage 3, which extends the
  same 130 root-level tests this plan counted. Plan 139 touches `ci.yml`; Plan 140
  touches `tests/test_observability_config.py`. They are adjacent, not overlapping.
- **Anything that makes the local suite slower to make CI tidier.**

## Where this belongs in the build order

**Recommendation: split it. Stages A+B go early as filler; Stages C+D are
genuinely opportunistic and rank low.**

The plan as a whole scores **62 — medium**: no production impact, no data-loss
risk, and after the `duckdb_gauges` transfer it unlocks nothing. By score alone it
sits at order 11, between Plan 137 (72) and Plan 121 (63).

**But scoring the plan as a whole hides that Stages A and B are two lines of YAML
and a dependency, and that their payoff compounds across every plan above them.**
Every one of the eleven plans in the build order will open pull requests that pay
the current 333s CI cost. Doing Stage B first makes all of them cheaper; doing it
at order 11 means eleven plans' worth of PRs paid full price for a fix that takes
an hour.

Concretely:

| Slice | Score | Effort | Placement |
|---|---:|---|---|
| **A + B** (coverage visible, CI path recovered) | 70 | XS | **Order 1** — ahead of Plan 136/140 |
| C (profile the 92s step) | 60 | S | Order 13, after Plan 121 |
| D (markers, gate decision, xdist) | 52 | S | Order 15, or opportunistic filler |

Adopted in [PLANS.md](PLANS.md) 2026-08-17 as three separate rows in the default
build order. That is the point of the split: a slice that is not a row is not
covered by the "do not start a lower row while a higher one has an executable
next step" rule, and A+B spent its first week invisible to exactly that rule.

The adjacency argument that PLANS.md already makes for Plan 140 versus Plan 134
applies here too: **Plan 140 Stage 3 edits CI test configuration, and Stage A+B
edits `.github/workflows/ci.yml`.** Same file region, same mental model, and
Plan 140 Stage 3 will want a passing, fast CI to iterate its coverage assertions
against. Doing A+B in the same sitting costs close to nothing; doing it eleven
plans later pays the context switch twice.

Stages C and D have no such argument and should wait their turn.
