# Plan 107: Quality Drive to 90

**Status:** Planned
**Goal:** Raise codebase score from 80 → 90 across the agreed rubric
**Integrates:** Plan 103 (test coverage), Plan 106 (code review cleanup)
**Source rubric:** Correctness & Reliability (25), Code Quality (25), Architecture (20), Testing (15), Operational Readiness (15)

---

## Score Map

| Category | Current | Target | Delta | Primary work |
|---|---|---|---|---|
| Correctness & Reliability | 21/25 | 23/25 | +2 | Track B: crash guards |
| Code Quality | 19/25 | 22/25 | +3 | Track A: extraction, Track C: consistency, deal score docs |
| Architecture | 18/20 | 19/20 | +1 | C1: `/ready` contract |
| Testing | 10/15 | 13/15 | +3 | Plan 103 P1–P3 + dbt unit tests |
| Operational Readiness | 12/15 | 13/15 | +1 | Track D: JSON logging |

---

## Track A — Code Quality: Extraction (Plan 106 A1 + A2)

### A1: `shared/query_loader.py`

Every service has a private `_load(filename)` or `_q(name)` function that reads `.sql` files from a sibling `sql/` directory. Identical logic, four different function names.

**Create:** `shared/query_loader.py`
```python
from pathlib import Path

def load_query(sql_dir: Path, name: str) -> str:
    return (sql_dir / f"{name}.sql").read_text()
```

**Update:** `scraper/queries.py`, `processing/queries.py`, `archiver/queries.py`, `ops/queries.py` — replace private loaders with `load_query(Path(__file__).parent / "sql", name)`.

---

### A2: `shared/logging_setup.py`

Every `app.py` duplicates the same 5-line `RotatingFileHandler` block.

**Create:** `shared/logging_setup.py`
```python
def configure_logging() -> None:
    _LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
    os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
    handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)
```

**Update:** `scraper/app.py`, `processing/app.py`, `archiver/app.py`, `ops/app.py`, `dbt_runner/app.py` — replace the duplicated block with `configure_logging()`.

---

## Track B — Correctness: Crash Guards (Plan 106 B1–B5)

These are the highest-risk items — each is a single unguarded assumption that produces a confusing runtime crash on a real input.

### B1: `json.loads()` in HTML parsers — no try-except

A malformed `<script>` tag on any live page kills the entire parse call.

**Files:** `scraper/processors/parse_detail_page.py` (~line 34), `processing/processors/parse_detail_page.py` (~line 40)

**Fix:** Wrap `json.loads(raw)` in `try/except (json.JSONDecodeError, ValueError): return None`.

---

### B2: XCom `.get()` on nullable pull result

`xcom_pull` returns `None` when the upstream task was skipped → `AttributeError` on `.get("configs", [])`.

**Files:** `airflow/dags/scrape_listings.py` (~line 47), `airflow/dags/dbt_build.py` (~line 19)

**Fix:** `rotation = context["ti"].xcom_pull(...) or {}`

---

### B3: Empty string passed to `json.loads()` in card parsers

`card.get("data-vehicle-details") or ""` then `json.loads(raw)` — empty string raises `JSONDecodeError`.

**Files:** `scraper/processors/results_page_cards.py` (~line 63), `processing/processors/results_page_cards.py` (~line 63)

**Fix:** Add `if not raw.strip(): continue` before `json.loads(raw)`.

---

### B4: `cur.description` accessed without None guard

Accessing `cur.description` in `ops/routers/scrape.py` when the cursor hasn't returned a result set → `AttributeError`.

**File:** `ops/routers/scrape.py` (~line 198)

**Fix:** `if not cur.description: raise ValueError("Query returned no result set")`

---

### B5: `PGPORT` coerced to `int` inline

`int(os.environ.get("PGPORT", "5432"))` with a misconfigured env var raises a bare `ValueError` with no useful context.

**File:** `shared/db.py` (~line 33)

**Fix:**
```python
try:
    _port = int(os.environ.get("PGPORT", "5432"))
except ValueError:
    raise ValueError(f"PGPORT must be an integer, got: {os.environ.get('PGPORT')!r}")
```

---

## Track C — Architecture: Consistency (Plan 106 C1 + C2)

### C1: Standardize `/ready` response across services

Scraper raises `HTTPException(503)` on not-ready; every other service returns `HTTP 200 {"ready": false, "reason": "..."}`. Monitoring and Airflow sensors need one contract.

**Decision:** Option A — `200 {"ready": bool, "reason": str | None}` everywhere (consistent with processing/archiver/dbt_runner).

**File:** `scraper/app.py` — change the not-ready path to return `{"ready": False, "reason": "..."}` with status 200.

---

### C2: Document `dashboard/db.py` divergence

`dashboard/db.py` has Streamlit-specific `@st.cache_resource` and silent retry logic that doesn't exist in `shared/db.py`. This is justified but undocumented — future readers will "fix" it.

**Fix:** Add a single comment in `dashboard/db.py` explaining that it intentionally does not use `shared/db.py` because Streamlit's session model requires `@st.cache_resource`, and documenting why the retry exists.

---

## Track D — Operational Readiness: JSON Logging

### D1: Switch to structured JSON log format

Current plaintext logs prevent Loki from extracting structured fields (`artifact_id`, `run_id`, `search_key`) for ad-hoc queries. All useful context is in the message string.

**Update:** `shared/logging_setup.py` (A2 above) — change the formatter to emit JSON:
```python
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        })
```

This is a one-place change once A2 is in place — all services inherit it automatically.

Note: This changes log output format. Verify Promtail pipeline stages still parse correctly after the change (update `promtail/promtail.yml` if needed — likely just remove any regex parse stages).

---

## Track E — Testing (Plan 103 integrated + dbt extension)

### E1: `ops/routers/info.py` — Priority 1 (Plan 103)

**File:** `tests/ops/routers/test_info.py` (new)

Test cases:
- All four queries succeed → all stat keys present in template context
- One query fails → that key absent, others present (one test per query, 4 total)
- All queries fail → stats dict is empty, template still renders
- `_load_stats()` itself raises → endpoint catches, renders with `stats = {}`
- Stats section rendered when non-empty; hidden when empty

Fixtures: mock `db_cursor` (same pattern as `test_admin.py`), mock `templates.TemplateResponse`.

---

### E2: `scraper/app.py` edge cases — Priority 2 (Plan 103)

**File:** `tests/scraper/test_app.py` (extend)

Test cases:
- `POST /scrape_results` with invalid JSON body → 422
- `GET /scrape_results/jobs/{job_id}` for unknown job → 404
- `GET /scrape_results/jobs/{job_id}` for completed job → status fields present
- `POST /scrape_detail/batch` with empty listing list → returns immediately

---

### E3: Error branch sweep — Priority 3 (Plan 103)

#### `processing/routers/batch.py`
- DB error on `claim_artifacts` → 503 response shape
- Empty candidate set → returns zero counts immediately

#### `archiver/processors/flush_staging_events.py`
- `_flush_table` with DB error mid-flush → error captured in result dict
- `flush_staging_events` with no tables configured → returns `total_flushed: 0`

#### `archiver/processors/flush_silver_observations.py`
- `_fetch_unflushed` returns empty → write not called
- Parquet write failure → error surfaced in return dict

#### `ops/routers/users.py`
- `POST /users/request-access` with duplicate email → DB unique violation handled
- `POST /users/{id}/approve` with already-approved request → idempotent or error

---

### E4: dbt unit tests for critical models (new)

The three models that drive the dashboard have no unit tests at all.

**File:** `dbt/models/marts/unit_tests.yml` (new) and extend `dbt/models/staging/unit_tests.yml`

#### `mart_deal_scores` — scoring formula
- Listing at MSRP, no price drops, median percentile → score in expected range
- Listing with 15% MSRP discount, low percentile, 3 price drops → score > 70
- MSRP = 0 (edge case) → MSRP component is 0, score still computed
- price = 0 (edge case) → handled by COALESCE, score still computed

#### `int_price_history` — price trajectory
- Single observation → no drops, `min_price = max_price = price`
- Two observations, price decreases → `price_drop_count = 1`, `min_price = lower`
- Two observations, price increases → `price_drop_count = 0`

#### `int_benchmarks` — national percentile
- Single VIN at given price → percentile = 0.0 (cheapest)
- Three VINs; target is median → percentile ≈ 0.5

---

### E5: Guard tests for Track B fixes

Once B1–B3 are implemented, add unit tests that feed malformed inputs and assert `None` / `continue` behavior rather than exceptions. These are small and quick to write.

- `test_parse_detail_malformed_script` — `json.loads` on garbled `<script>` → returns `None`, no exception
- `test_card_parser_empty_vehicle_details` — empty `data-vehicle-details` → card skipped, no exception
- `test_xcom_null_pull` — `xcom_pull` returns `None` → DAG callable continues with empty config list

---

## Track F — Documentation: Deal Score Formula

### F1: Annotate deal score weights in `mart_deal_scores.sql`

The 35/30/15/10/5/5 weight split is not explained anywhere. A reader cannot distinguish tuning artifact from intentional design.

**File:** `dbt/models/marts/mart_deal_scores.sql`

Add a comment block above the scoring expression explaining the weight rationale:
- 35 pts MSRP discount — primary signal, most reliable price anchor
- 30 pts national percentile — market-relative pricing
- 15 pts days on market — seller motivation proxy
- 10 pts price drop count — seller willingness to negotiate
- 5 pts + 5 pts dealer inventory / national listing count — supply-side scarcity

---

## Sequencing

| Order | Track | Items | Effort | Risk |
|---|---|---|---|---|
| 1 | B | B1, B2, B3, B4, B5 | Small | Low — guard additions |
| 2 | E | E5 (guard tests) | Small | Low — follows directly from B |
| 3 | C | C1, C2 | Small | Low — one behavior change |
| 4 | A | A2 (logging) | Medium | Low — pure extraction |
| 5 | D | D1 (JSON logging) | Small | Low — formatter only, but verify Promtail |
| 6 | A | A1 (query loader) | Medium | Low — pure extraction |
| 7 | E | E1, E2, E3 (Plan 103) | Medium | Low |
| 8 | E | E4 (dbt unit tests) | Medium | Low — YAML only |
| 9 | F | F1 (deal score docs) | Tiny | None |

B items first — they fix real crash paths and enable E5 tests naturally. Logging extraction (A2) before JSON format (D1) so D1 is a one-line formatter change rather than a 5-service edit.

---

## Out of Scope

- End-to-end pipeline tests (full stack: scraper → processing → archiver → dbt → DuckDB) — not enough ROI at current scale; observability covers this in production.
- `mypy` / `pyright` CI enforcement — adds friction to the dev loop; type hints are already comprehensive and serve as documentation.
- Plan 106 Track D (raw tuple indexing in `ops/routers/scrape.py`, DAG structure outlier) — deferred as in Plan 106.
- Dashboard (Streamlit, 0%) — no viable unit test path; unchanged from Plan 103.
