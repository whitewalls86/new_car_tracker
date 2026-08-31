# Plan 162: The Testing Census and CI Restructure

## Status

**Stub, written 2026-08-30.** Deliberately unscoped: this plan is the
implementation half of [Plan 161](plan_161_testing_contract.md), and its shape
depends on answers Plan 161 has not produced yet. It exists now so the work has
a number, a row and a priority rather than living in a conversation.

**Do not start it before Plan 161 lands.** Its first act — the census — is
measurement against a standard, and Plan 161 is the standard. Running the census
first produces what this planning session already produced once: a list of
differences with no way to say which side is wrong.

Priority and effort are proposed in [`docs/PLANS.md`](../PLANS.md), which owns
both; this document does not choose them. Effort is **XL** on the understanding
that Plan 161 will replace that guess with a real one.

## What it owns

### 1. The census

The full inventory of the test suite against Plan 161's contract, service by
service and layer by layer. Not an audit of style — a measurement of where the
suite and the contract disagree, with each disagreement classified as a
violation to fix, an exemption to record, or a rule to revise.

Preliminary readings taken 2026-08-30, during the conversation that produced
Plan 161. They are the starting point, not the census:

| Question | Reading |
|---|---|
| Mock convention | 157 test files: 52 pytest-mock, 37 `unittest.mock`, 25 `monkeypatch`, **21 mixing two in one file** |
| SQL location | 384 `.sql` files, but **16 service modules still hold inline SQL**; `tests/integration/sql/` is 7 files over 5 areas while its docstring claims "every query the ops service runs" |
| Endpoint coverage | **32 of 35 routes** are referenced somewhere in `tests/`. The three that are not: `ops` `/`, and `container_health` `/oneoff-processes` and `/project-status/{project}` |
| Per-service balance | `container_health` 5 source files / 2 test files; `ops` 23 / 29; `airflow` 19 / 12 |
| Coverage | `--cov --cov-report=term-missing` runs on the unit job and nothing consumes the number |

The endpoint figure uses the weakest possible reading — the path string appears
somewhere under `tests/`. Plan 161 question 5 decides what "exercised" means,
and the real number will be lower.

**The two unreferenced `container_health` routes are the two that silently
vanished from a deployed image on 2026-08-30**, unnoticed for eleven hours.
That coincidence is the census's argument for existing.

### 2. The CI restructure

- **Split the `dbt build + test` job.** It is **267s of a ~4 minute wall clock —
  the critical path** — and it is eight sequential suites sharing one Postgres
  and one MinIO. One of them builds an entire Airflow venv with `pip install
  apache-airflow==3.2.0` on every run. The job's name describes one of the
  eight.
- **Build dbt against the Plan 120 lake snapshot.** The fixture exists and is
  already seeded by `scripts/seed_lake_snapshot_fixture.py`; the very next step
  is *"dbt build (DuckDB — empty data compilation check)"*. The
  production-shaped fixture is paid for and unused for the thing it was built
  for.
- **Run the suites against real Compose services.** Locally before a PR, and in
  CI where it is affordable. CI uses bare `postgres:16` and `minio/minio:latest`
  service containers rather than the Compose definitions, which is the root of
  the CI-schema gap that [Plan 139](plan_139_test_suite_maintenance.md) Stage F
  fixes narrowly and this plan should close generally.

### 3. What it absorbs from Plan 139

Plan 139 was written as test-suite *maintenance*. Most of it is now either
superseded by the restructure or answered by Plan 161's contract:

| Stage | Disposition |
|---|---|
| A — make coverage visible | Effectively shipped; `ci.yml` runs `--cov`. Needs a completion marker, not work |
| B — recover the CI critical path | The critical path is now the 267s dbt job. **This plan** |
| C — understand the 92s step | **This plan.** It is the census's first measurement |
| D — intent markers and the coverage decision | Split: the gate decision is Plan 161's questions 6 and 8; the markers are **this plan** |
| E — advisory CI impact selection | **This plan.** Its own premise is "before any new fast path", and this is the fast path |
| F — CI's database does not model production's schemas | **Stays in 139.** Independent, S, ships before either of these plans |
| G — Promtail contract checker | Already moved to [Plan 160](plan_160_promtail_contract_checker_reliability.md) |
| H — one invariant, two censuses | **Stays in 139.** XS, unrelated to the standard |

So Plan 139 reduces to Stages F and H. Whether it is formally split, superseded
or simply re-scoped is a decision for when this plan is written properly — it is
recorded here so the disposition is not lost, not decided here.

**Consequence, resolved 2026-08-30:** Plans 103 and 107 were triggered by "Plan
139 Stage D settles the coverage gate." Stage D is being taken apart, so that
trigger named something that will not happen. Rather than re-point them, both
were **superseded by Plans 161 and 162** — their premises are a coverage
percentage and a self-scored rubric, both last edited 2026-04-29, and both are
what Plan 161's contract now decides. Parts of each had already shipped under
other plans without them.

Stage E carries one piece of thinking worth preserving verbatim rather than
rediscovering: Plan 142's service graph is *evidence* for a CI selector, not the
selector itself, because "production asks which live work depends on a service,
while CI asks which tests, images and integration environments can detect a
changed path."

## Non-goals

- **Deciding the standard.** That is Plan 161. If this plan finds itself
  arguing about which mock library is correct, Plan 161 did not finish.
- **Being written before Plan 161 lands.** This document is a stub on purpose.

## Success criteria

Deliberately absent. Plan 161's answers determine what this plan must be true
of, and inventing criteria now would pre-empt the decisions that plan exists to
make. They get written when this document does.

## Intersections

### Plan 161 — the testing contract

The blocker, and the reason this is a stub. 161 decides the rules; this plan
measures the gap and closes it.

### Plan 139 — test suite maintenance

Reduced to Stages F and H by the table above. Stage F should ship before either
plan: it closes the CI-schema gap that hung the first production deploy of Plan
142's coordination gate, and Plan 142 Stage 4 runs a maintenance window driven
by exactly those queries.

### Plan 120 — CI lake snapshot

Complete, and supplies the fixture the dbt rebuild needs. Its output is already
seeded in CI without being used for dbt.

### Plans 103 and 107 — coverage

Superseded by Plan 161 and this plan on 2026-08-30. **Their targets are now this
plan's inheritance, not their own**: 103's per-file coverage gaps and 107's
testing-rubric third are exactly what the census must measure once Plan 161 has
said what "enough" means. Read them for the gap list they already assembled, not
for their numbers.
