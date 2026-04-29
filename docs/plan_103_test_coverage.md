# Plan 103: Test Coverage Gaps

**Status:** Planned

---

## Overview

Non-integration test suite sits at 77% overall. This plan closes the meaningful gaps in production code, prioritised by severity. Dashboard (Streamlit, 0%) is explicitly out of scope — no standard unit test path exists.

---

## Priority 1 — `ops/routers/info.py` (25%)

Brand-new Plan 95 code with no unit tests. `_load_stats()` has four independent DB queries each wrapped in `try/except`, making it the most branch-rich untested path.

**File:** `tests/ops/routers/test_info.py` (new)

Test cases:
- All four queries succeed → all stat keys present in template context
- One query fails → that key absent, others present (once per query)
- All queries fail → stats dict is empty, template still renders
- `_load_stats()` itself raises → endpoint catches it, renders with `stats = {}`
- Stats section is rendered when `stats` is non-empty; hidden when empty

Fixtures needed: mock `db_cursor` (same pattern as `test_admin.py`), mock `templates.TemplateResponse`.

---

## Priority 2 — `scraper/app.py` (69%)

Missing lines 42–67 (batch scrape orchestration helpers) and 78–113 (job status tracking). These are reachable via `TestScrapeBatch` and `TestJobStatus` patterns already in the file — missing edge cases.

**File:** `tests/scraper/test_app.py` (extend)

Test cases:
- `POST /scrape_results` with invalid JSON body → 422
- `GET /scrape_results/jobs/{job_id}` for unknown job → 404
- `GET /scrape_results/jobs/{job_id}` for completed job → status fields present
- `POST /scrape_detail/batch` with empty listing list → returns immediately

---

## Priority 3 — 87–90% files (error branch sweep)

Small, targeted additions to cover identified missing lines.

### `processing/routers/batch.py` (90%, lines 39–66)
- DB error on `claim_artifacts` → 503 or error response shape
- Empty candidate set → returns zero counts immediately

### `archiver/processors/flush_staging_events.py` (87%, lines 42–50, 236–256, 289–314)
- `_flush_table` with DB error mid-flush → error captured in result
- `flush_staging_events` with no tables configured → returns `total_flushed: 0`

### `archiver/processors/flush_silver_observations.py` (87%, lines 105–132, 155, 171)
- `_fetch_unflushed` returns empty → write not called
- Parquet write failure → error surfaced in return dict

### `ops/routers/users.py` (89%, lines 59–60, 95–96, 233–234, 243–244)
- `POST /users/request-access` with duplicate email → DB unique violation handled
- `POST /users/{id}/approve` with already-approved request → idempotent or error

---

## Priority 4 — `airflow/dags/results_processing.py` (39%)

DAG integrity (import, dag_id, task set) is already covered by the integration test. The 39% here reflects that task callables aren't exercised in unit tests.

**Acceptable gap** — DAG task logic calls HTTP endpoints tested independently. No new tests required unless task-level branching grows complex.

---

## Out of Scope

- `dashboard/` (0%) — Streamlit pages have no viable unit test path. Covered implicitly by integration smoke and manual verification.
- Integration test files showing 20–30% — by design; they require a live DB.
