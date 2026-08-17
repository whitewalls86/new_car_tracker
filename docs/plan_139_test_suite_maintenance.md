# Plan 139: Test Suite Construction and Maintenance

## Status

**SKELETON — measured, not yet decided.** Surfaced 2026-08-17 during Plan 135
Stage 4 development, when the unit suite's wall-clock time prompted the question
"are we missing a mock somewhere?" The answer was no, but the measuring turned
up three things worth a deliberate pass rather than a drive-by fix.

This doc records the measurements and frames the decisions. It does not make
them.

## What was actually measured (2026-08-17, local, warm)

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
**cold** run populating `__pycache__`. Warm runs are consistently 10.7–10.9s.
Quote warm numbers.

---

## Question 1 — Should script tests run on every build?

**The number that makes this a real question:**

| Area | Tests | Share |
|---|---:|---:|
| **`tests/scripts/`** | **517** | **23%** |
| `tests/archiver/` | 485 | 22% |
| `tests/ops/` | 244 | 11% |
| `tests/scraper/` | 224 | 10% |
| `tests/processing/` | 197 | 9% |
| `tests/shared/` | 176 | 8% |
| `tests/lakehouse/` | 164 | 7% |

`tests/scripts/` is the **largest single area in the suite** — larger than the
archiver's — and by this repo's own convention (`scripts/` is for one-off
measurement; anything recurring is a processor) it covers the least production-
critical code in the tree.

**For keeping them on every build:** they cost roughly nothing in wall-clock, a
broken script is still broken, and several `scripts/` entries are load-bearing
for CI itself (`seed_lake_snapshot_fixture.py` is a CI step).

**Against:** every PR is gated on code that is by definition not production, and
some of it is thin — e.g. `test_estimate_recompression_savings.py` spends tests
on argparse error paths.

**Open question, not yet decided:** the useful split is probably not
scripts-vs-not but *load-bearing-for-CI* vs *one-off measurement*. The former
belongs in every build; the latter could move behind a marker. Worth an hour of
triage, not a rewrite.

## Question 2 — Can we parallelize?

**Yes, and this suite is close to an ideal candidate** — the time is almost
entirely fixed per-test overhead, which is exactly what spreads across cores.

- `pytest-xdist` is **not currently installed**. `pytest-cov` and `pytest-mock`
  are.
- Expected: ~11s → ~3-4s locally with `-n auto`.

**But the wall-clock prize is not the 11s job.** CI's critical path is a serial
chain:

```
lint  →  unit-tests  →  dbt
                        └─ 3 service containers (postgres, minio, loki)
                           + flyway + dbt deps + dbt build
                           + 6 serialized pytest invocations
                           + a separate isolated Airflow venv install
docker-build ─ (runs in parallel, independent)
```

Shaving 7s off `unit-tests` is noise against the `dbt` job. **The real
questions are whether `unit-tests` needs to block `dbt`, and whether the six
integration invocations inside `dbt` can share setup or run concurrently.**

**Known hazard before enabling xdist:** several suites mutate process-global
state — e.g. `tests/archiver/test_app.py`'s module-scoped autouse fixture
flips `archiver_app._ALLOW_PACK_JOBS`. Under xdist each worker is a separate
process, so this is *probably* fine, but it needs verifying rather than
assuming, along with anything writing to a shared `LOG_PATH`.

## Question 3 — What's coverage like? Any holes?

**88% overall**, which is healthy. Three structural gaps matter more than the
number:

**a) Coverage is not measured in CI at all.** No `--cov` flag, no
`[tool.coverage]` config, no gate, no trend. The 88% above had to be computed by
hand for this doc. A number nobody watches will drift.

**b) Two directories are outside coverage entirely** — `airflow/dags/` and
`dashboard/`. DAG logic is currently only exercised by the pure predicate
functions the DAGs expose (the `try/except ImportError` pattern), which is a
deliberate and good design, but it means DAG wiring is unmeasured.

**c) The worst-covered files, and which of them actually matter:**

| File | Cover | Note |
|---|---:|---|
| `archiver/processors/lake_source_audit.py` | 21% | |
| **`ops/metrics/duckdb_gauges.py`** | **25%** | **see below** |
| `archiver/processors/lake_snapshot_cohort.py` | 44% | largest single gap, 183 statements |
| `archiver/processors/lake_snapshot_selectors.py` | 53% | |
| `scraper/app.py` | 65% | already Plan 103's P2 (recorded there as 69%) |

`ops/metrics/duckdb_gauges.py` at **25%** is the standout. That is the code
behind the `cartracker_*` gauges that are known to hold their last value
silently when dbt holds the DuckDB lock — a failure mode that already cost an
8-hour blind spot. **Plan 136 Stage 1 is about to build its staleness
convention directly on top of it.** Covering it is cheap insurance immediately
before something depends on it.

Note also that Plan 103 already owns part of this territory (P1 `ops/info.py`,
P2 `scraper/app.py`) and Plan 107 folds Plan 103 in. **This plan should not
duplicate them** — its distinct contribution is the CI measurement gap (a),
the uncovered directories (b), and `duckdb_gauges.py` (c).

---

## Candidate next steps, cheapest first

Nothing here is committed to yet.

| # | Step | Effort | Why now |
|---|---|---|---|
| 1 | Add `--cov` + `[tool.coverage]` config to the CI unit job, report only, **no gate** | XS | Makes the number visible before arguing about a threshold |
| 2 | Cover `ops/metrics/duckdb_gauges.py` | S | Plan 136 is about to depend on it |
| 3 | Install `pytest-xdist`, verify no shared-state failures, enable `-n auto` | S | Real local speedup; verify the global-state fixtures first |
| 4 | Triage `tests/scripts/` into load-bearing vs one-off; mark the latter | S | 23% of the suite, least production-critical code |
| 5 | Revisit whether `dbt` must depend on `unit-tests`, and whether its 6 pytest invocations can share setup | M | The actual CI wall-clock, unlike everything above |
| 6 | Decide whether to add `airflow/dags/` and `dashboard/` to coverage | XS | Or record deliberately that they are excluded, and why |

## Explicitly out of scope

- Chasing the 88% upward as a goal in itself. The holes named above are worth
  closing because something depends on them, not to move a number.
- Rewriting the integration layering. Plan 84 established Layers 1-3 and the
  ordering constraint (SQL smoke before dbt); that design is not in question
  here.
- Anything that would make the local suite slower to make CI tidier.
