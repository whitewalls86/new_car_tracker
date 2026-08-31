# Plan 106: Code Review Cleanup

**Status:** Superseded by [Plan 163](plan_163_documented_code_quality_fixes.md) (2026-08-30)
**Priority:** Medium — no user-visible impact; reduces fragility and maintenance surface
**Source:** Three-axis code review (duplication, unenforced assumptions, coherency), 2026-05-04

---

## Problem

A targeted code review surfaced three categories of issues:

1. **Duplication** — SQL query loading, logging setup, and the `/ready` response contract are each implemented independently per service instead of shared once.
2. **Unenforced assumptions** — Several parsers, Airflow DAGs, and DB cursor call sites assume inputs are well-formed without guarding against None or malformed data. Silent crashes in production are the risk.
3. **Consistency gaps** — `dashboard/db.py` has retry logic that `shared/db.py` does not; scraper's `/ready` returns a 503 while every other service returns a 200.

The codebase is otherwise coherent and deliberately designed — these are targeted fixes, not a refactor.

---

## Track A — Extraction (Duplication)

### A1: Shared SQL query loader

**Problem:** Every service has its own `_load(filename)` or `_q(name)` function that reads `.sql` files from a sibling `sql/` directory. Identical logic, different function names (`_load` in scraper/processing, `_q` in archiver/ops).

**Fix:** Add `shared/query_loader.py` with a single `load_query(sql_dir: Path, name: str) -> str` function. Each service's `queries.py` calls it with `Path(__file__).parent / "sql"`.

**Files to update:**
- Create `shared/query_loader.py`
- `scraper/queries.py`
- `processing/queries.py`
- `archiver/queries.py`
- `ops/queries.py`

---

### A2: Shared logging setup

**Problem:** Every service's `app.py` contains the identical 5-line `RotatingFileHandler` block:
```python
_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
```

**Fix:** Add `shared/logging_setup.py` with `configure_logging()`. Each service calls it once at module level.

**Files to update:**
- Create `shared/logging_setup.py`
- `scraper/app.py`, `processing/app.py`, `archiver/app.py`, `ops/app.py`, `dbt_runner/app.py`

---

## Track B — Hardening (Unenforced Assumptions)

These are the highest-risk findings — any one of them can cause a confusing runtime crash on a real page or skipped Airflow task.

### B1: `json.loads()` in HTML parsers — no try-except

**Problem:** Both scraper and processing parse a `<script>` tag from a live web page and call `json.loads(raw)` without error handling. A single malformed page kills the entire parse call.

**Files:**
- `scraper/processors/parse_detail_page.py` (~line 34)
- `processing/processors/parse_detail_page.py` (~line 40)

**Fix:**
```python
try:
    return json.loads(raw)
except (json.JSONDecodeError, ValueError):
    return None
```

---

### B2: XCom `.get()` called on nullable pull result

**Problem:** Airflow DAGs call `xcom_pull(...)` then immediately call `.get("configs", [])` on the result. If the upstream task was skipped, `xcom_pull` returns `None` → `AttributeError`.

**Files:**
- `airflow/dags/scrape_listings.py` (lines ~47–49)
- `airflow/dags/dbt_build.py` (~line 19)

**Fix:** `rotation = context["ti"].xcom_pull(...) or {}`

---

### B3: Empty string passed to `json.loads()` in card parsers

**Problem:** Both scraper and processing card parsers do `raw = card.get("data-vehicle-details") or ""` then `json.loads(raw)`. An empty string will raise `JSONDecodeError`.

**Files:**
- `scraper/processors/results_page_cards.py` (~line 63)
- `processing/processors/results_page_cards.py` (~line 63)

**Fix:** Add `if not raw.strip(): continue` (or `return None`) before `json.loads(raw)`.

---

### B4: `cur.description` accessed without None guard

**Problem:** `ops/routers/scrape.py` builds column names from `cur.description` without checking whether it's None (which it is when the cursor hasn't executed or returned no result set).

**File:** `ops/routers/scrape.py` (~line 198)

**Fix:** `if not cur.description: raise ValueError("Query returned no result set")`

---

### B5: `PGPORT` coerced to `int` without error handling

**Problem:** `shared/db.py` does `int(os.environ.get("PGPORT", "5432"))` inline. A misconfigured env var (e.g. `"5432 "`) raises a `ValueError` with no useful context.

**File:** `shared/db.py` (~line 33)

**Fix:**
```python
try:
    _port = int(os.environ.get("PGPORT", "5432"))
except ValueError:
    raise ValueError(f"PGPORT must be an integer, got: {os.environ.get('PGPORT')!r}")
```

---

## Track C — Consistency

### C1: Standardize `/ready` response across services

**Problem:** Scraper's `/ready` raises `HTTPException(503)` when not ready. Every other service returns `HTTP 200 {"ready": false, "reason": "..."}`. Any monitoring or Airflow sensor that polls `/ready` across services needs to handle two different contracts.

**Decision needed:** Pick one contract and apply it everywhere.
- **Option A (recommended):** 200 with `{"ready": bool, "reason": str | null}` — easier to parse, consistent with processing/archiver/dbt_runner.
- **Option B:** 503 on not-ready — more REST-idiomatic, but requires scraper to be changed.

**File to update:** `scraper/app.py` if Option A.

---

### C2: Resolve `dashboard/db.py` vs `shared/db.py` divergence

**Problem:** `dashboard/db.py` wraps psycopg2 with `@st.cache_resource` and adds a retry-after-reconnect behavior that doesn't exist in `shared/db.py`. This is intentional for Streamlit but the retry logic is silent and undocumented.

**Fix (low risk):** Add a comment in `dashboard/db.py` explaining why it doesn't use `shared/db.py` and why the retry exists. The divergence is justified (Streamlit's session model), but should be explicitly acknowledged so future readers don't "fix" it.

---

## Track D — Low Priority (Deferred)

These are real but low-urgency. Log them and revisit if the areas see active development.

### D1: Raw tuple indexing in `ops/routers/scrape.py`
Several call sites use `row[0]`, `row[1]` instead of `dict_cursor=True` or named access. Not wrong today, but fragile if queries change.

### D2: `scrape_listings.py` DAG structure outlier
This DAG exposes raw Python functions rather than using `PythonOperator` like every other DAG. Not broken — it works — but inconsistent. Only worth addressing if the DAG needs modification anyway.

---

## Sequencing

| Order | Track | Effort | Risk |
|---|---|---|---|
| 1 | B1, B2, B3 | Small | Low — pure guard additions |
| 2 | B4, B5 | Small | Low |
| 3 | C1 | Small | Low (behavior change in scraper only) |
| 4 | A2 (logging) | Medium | Low — pure extraction |
| 5 | A1 (query loader) | Medium | Low — pure extraction, well-tested |
| 6 | C2 | Tiny | Informational only |
| 7 | D1, D2 | Deferred | — |

Track B items are highest value per unit of effort — they're all single-function changes that prevent confusing crashes.

---

## Out of Scope

- Migrating scraper to sync psycopg2 (or other services to asyncpg). The async/sync split is load-justified — scraper has high concurrency requirements; ops/archiver do not.
- Adding Pydantic `Settings` classes. The inline `os.getenv` pattern works fine at this scale; the only fragile case (PGPORT) is fixed by B5.
- Rewriting `scrape_listings.py` DAG structure (D2) — no active development planned there.
