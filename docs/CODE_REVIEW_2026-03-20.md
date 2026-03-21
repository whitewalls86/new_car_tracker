# CarTracker Code Review Report
**Date:** 2026-03-20
**Scope:** Full codebase review — scraper, dashboard, dbt, dbt_runner, DB schema, Docker, n8n workflows
**Commit:** `019308f` (master)

---

## 1. Project Status Summary

CarTracker is a **fully operational** car inventory tracking system scraping Cars.com for new vehicle listings. The system:

- Scrapes search result pages (SRP) and detail pages on a rotating schedule
- Parses HTML into structured observations stored in PostgreSQL
- Transforms data through a 19-model dbt pipeline into deal scoring analytics
- Presents findings via a 4-tab Streamlit dashboard

**Architecture maturity:** The project has evolved through 37+ plans from a simple scraper into a well-instrumented, multi-service pipeline with alerting, error handling, rotation scheduling, and incremental dbt builds. The README is excellent and accurately reflects the current state.

**Current state:** Stable and running. Recent work (commits) focused on fixing logic holes in scrape job tracking, VIN validation, rotation self-blocking, and credential handling.

---

## 2. Architecture Assessment

### Strengths

1. **Clean separation of concerns** — 7 services, each with a single responsibility
2. **Robust scrape scheduling** — Slot-based rotation with dual guards (per-slot idle + global gap) prevents over-scraping and Akamai detection
3. **Discovery mode** is well-designed — VIN breakpoint + consecutive-no-new-pages + 80% overlap detection keeps request volume low
4. **Artifact-first design** — Every HTTP response is persisted to disk before processing, enabling replay and debugging
5. **Incremental dbt builds** — Most heavy models (listing_to_vin, latest_price, latest_tier1, price_events, listing_days_on_market) are incremental
6. **Orphan recovery** — Container restart cleans up stale jobs/runs automatically
7. **Thread-local browser instances** — Prevents state corruption in parallel scrape jobs
8. **Comprehensive error handling** — Pipeline errors table, Telegram alerts, error rate thresholds
9. **n8n workflow orchestration** — Good separation between scheduling logic (n8n) and scraping logic (FastAPI)

### Architecture Risks

1. **In-memory job store (`_jobs` dict)** — All queued/running SRP jobs live only in memory. A container restart loses all in-flight results. The orphan recovery handles DB state but artifacts already written to disk may become orphaned files never linked to DB records.

2. **Two DB connection strategies** — `asyncpg` pool for admin/rotation routes, `psycopg2` for background threads and dbt_runner. This works but creates maintenance burden and inconsistency (different connection parameters, different error handling patterns).

3. **No health checks** — The scraper container has no Docker `HEALTHCHECK`. If the FastAPI process hangs (Playwright deadlock, thread pool exhaustion), Docker won't restart it. Same for dashboard and dbt_runner.

4. **External volumes/network required** — `cartracker_pgdata`, `cartracker_raw`, `n8n_data`, and `cartracker-net` are all `external: true`. If any is missing, `docker compose up` fails with a cryptic error. The setup script handles this but manual setup is fragile.

5. **Single point of failure: n8n** — If n8n goes down, all scheduling stops. No cron-based fallback. n8n workflow state is in a Docker volume with no backup strategy.

---

## 3. Code Quality Review

### 3.1 Scraper (`scraper/`)

**app.py (607 lines)**

| Finding | Severity | Details |
|---------|----------|---------|
| `_fetch_known_vins` connection leak | Medium | `conn.close()` on line 63 is unreachable — it's after `with conn` block exits. The `with conn` manages the transaction, not the connection lifecycle. Connection will eventually be GC'd but may leak under load. |
| `datetime.datetime.utcnow()` usage | Low | Line 80 uses deprecated `utcnow()` (no timezone info). Rest of codebase correctly uses `datetime.now(UTC)`. |
| `Body(...)` parameter in non-route function | Low | `scrape_results()` in `scrape_results.py` line 174 declares `payload: dict = Body(...)` but is called directly from `_run_scrape_job`, not as a route handler. `Body(...)` has no effect here — it's misleading. |
| No job expiration in memory | Medium | If the Job Poller n8n workflow stops running, completed/failed jobs accumulate in `_jobs` dict forever. Only `mark_job_fetched` removes them. No TTL or max-size guard. |
| `ThreadPoolExecutor(4)` is global | Low | All 4 workers could be consumed by long-running scrape jobs, blocking new submissions entirely. No queue depth limit or backpressure mechanism. |

**scrape_results.py (347 lines)**

| Finding | Severity | Details |
|---------|----------|---------|
| Three fallback paging meta parsers | Info | `extract_results_paging_meta` handles 3 different HTML formats (CarsWeb.SearchController, data-site-activity, spark-card). Well-structured with clear priority order. Good defensive coding against Cars.com frontend changes. |
| `max_safety_pages` default 500 | Low | A search could theoretically hit 500 pages before stopping. Discovery mode breakpoints should catch this much sooner, but if VIN matching fails (e.g., no known VINs), you'd scrape 500 pages. Consider lowering to 30 as documented in README. |
| Browser context per page | Good | Fresh browser context per page is the correct approach for Akamai bypass. |
| Error message truncation | Good | `[:500].replace("'", "")` on error strings prevents SQL injection in downstream n8n inserts. |

**scrape_detail.py (230 lines)**

| Finding | Severity | Details |
|---------|----------|---------|
| `curl_cffi` vs Playwright | Info | Detail pages use `curl_cffi` with TLS fingerprinting while SRP uses Playwright. This is intentional — detail pages don't need JS rendering. Good choice for speed. |
| No retry logic | Medium | A single HTTP failure loses that vehicle for this cycle. The n8n workflow handles retries at the batch level, but individual transient failures (network blip) aren't retried. |
| Session not reused | Low | A new `cf_requests.Session()` is created per call. For batch detail scrapes (called per vehicle), this means new TLS negotiation each time. Could reuse session across a batch. |

**parse_detail_page.py (326 lines)**

| Finding | Severity | Details |
|---------|----------|---------|
| `_parse_dealer_card` regex fragility | Medium | `re.search(r'"seller"\s*:\s*\{([^}]+)\}', str(soup))` will break if the seller JSON contains nested objects (the `[^}]+` stops at the first `}`). Currently works because seller objects are flat, but this is fragile. |
| Carousel parser dual-component support | Good | Handles both `fuse-card` and `spark-card` elements. Forward-compatible with Cars.com's component migration. |

**results_page_cards.py (370 lines)**

| Finding | Severity | Details |
|---------|----------|---------|
| Three parser versions maintained | Info | v1 (strict HTML), v2 (data-site-activity), v3 (spark-card/fuse-card). Only v3 is likely used for current Cars.com. v1/v2 are dead code but kept for backward compatibility. Consider removing if no longer used. |
| `_to_int` duplicated | Low | Same helper function defined in both v2 and v3 parsers. Should be module-level. |

**browser.py (42 lines)** — Clean, correct, well-documented.

**cleanup_artifacts.py (48 lines)** — Clean, handles edge cases (FileNotFoundError = success).

**admin.py (274 lines)**

| Finding | Severity | Details |
|---------|----------|---------|
| No CSRF protection | Medium | Form POSTs have no CSRF token. An attacker on the local network could craft a cross-site request to create/delete search configs. Low risk since this is a local-only tool, but worth noting. |
| Soft delete renames key | Good | `delete_search` renames the key rather than deleting. Preserves referential integrity with historical data. |
| `rotation_slot` not in admin UI | Medium | The `search_configs` table has a `rotation_slot` column, but the admin form doesn't expose it. Configs can only be assigned to rotation slots via direct SQL. This is a gap in the UI. |

**search_config.py (72 lines)** — Clean Pydantic model with good validation.

### 3.2 Dashboard (`dashboard/app.py`, 936 lines)

| Finding | Severity | Details |
|---------|----------|---------|
| **SQL injection in Deal Finder** | **High** | Lines 690-694: User-selected makes/tiers from `st.multiselect` are interpolated directly into SQL via f-strings (`f"'{m}'"` for m in selected_makes). While Streamlit multiselect constrains options to the dropdown list, a malicious or corrupted database value for `make` could inject SQL. Should use parameterized queries. |
| Single persistent connection | Medium | `@st.cache_resource` creates one psycopg2 connection shared across all Streamlit sessions. Under concurrent use, this could cause `OperationalError` (connection in use). Should use a connection pool. |
| No query timeouts | Medium | Complex dashboard queries (especially rotation schedule with 3 CTEs + 3 joins, and the deal finder with its f-string SQL) could hang indefinitely. Consider `SET statement_timeout` per query. |
| Hardcoded timezone | Low | All queries use `AT TIME ZONE 'America/Chicago'`. Should be configurable. |
| Hardcoded `localhost` quicklinks | Low | Sidebar links to `http://localhost:5678`, etc. Won't work if accessed remotely. |
| No pagination on large result sets | Medium | Deal Finder returns ALL active deals with `ORDER BY deal_score DESC` — no LIMIT. With thousands of vehicles, this could be slow and memory-intensive in Streamlit. |
| Emoji in code | Low | Uses emoji (checkmark/cross) for dbt status display. Minor style issue. |

### 3.3 dbt Runner (`dbt_runner/app.py`, 143 lines)

| Finding | Severity | Details |
|---------|----------|---------|
| Good input validation | Good | `SAFE_TOKEN` regex prevents command injection in dbt select/exclude tokens. |
| `_record_run` silently swallows errors | Low | Line 44: `except Exception: pass`. If DB logging fails, you lose build history with no indication. |
| No concurrent build protection | Medium | Two simultaneous `/dbt/build` calls could run dbt in parallel, causing lock contention or corrupt incremental state. Should use a lock or queue. |
| Subprocess with no timeout | Medium | `subprocess.run(cmd, ...)` has no `timeout` parameter. A hung dbt build would block the worker thread indefinitely. |

### 3.4 Database Schema

| Finding | Severity | Details |
|---------|----------|---------|
| `search_configs` missing `rotation_slot` | **High** | The schema dump (`schema_new.sql`) doesn't include `rotation_slot` column, but the application code (`advance_rotation`, admin UI) heavily depends on it. The schema file is out of date. New deployments from this schema will break. |
| `detail_observations` missing `seller_customer_id` | **High** | `ops_vehicle_staleness.sql` references `customer_id` on detail_observations, and the detail page parser extracts `customer_id`, but the column isn't in `schema_new.sql`. Same issue — schema dump is stale. |
| No `dbt_runs` table in schema | High | Dashboard queries `dbt_runs` but it's not in `schema_new.sql`. |
| Good indexing | Good | SRP observations have 6 indexes covering common query patterns. Detail observations have 4. Carousel hints have 2. |
| Missing index: `raw_artifacts(run_id, artifact_type, search_key)` | Medium | Dashboard rotation schedule query joins on `(run_id, artifact_type, search_key)` — would benefit from a composite index. |
| No partitioning | Info | Tables like `srp_observations` and `raw_artifacts` will grow unboundedly. Eventually may need time-based partitioning. Not urgent until millions of rows. |

### 3.5 Docker Configuration

| Finding | Severity | Details |
|---------|----------|---------|
| `COPY .. .` in scraper Dockerfile | **High** | Line 6: `COPY .. .` copies the parent directory into the container. This likely includes `.git/`, `.venv/`, `.env`, `docs/`, etc. The Docker build context is `./scraper` per docker-compose, so this actually resolves to `COPY . .` relative to context. But semantically, this should be `COPY . .` to avoid confusion. |
| No `.dockerignore` | Medium | No `.dockerignore` files in any service directory. Docker builds will include unnecessary files (tests, docs, etc.) increasing image size. |
| No pinned base images | Low | `python:3.11-slim` and `dpage/pgadmin4` use floating tags. A future pull could break builds. Should pin digests or version tags. |
| Dashboard volume mount | Good | `./dashboard/app.py:/app/app.py` enables live reload during development. |

### 3.6 n8n Workflows

7 workflow JSON files exported in `n8n/workflows/`. These aren't directly reviewable for logic bugs without loading them, but the architecture documentation suggests they're well-structured with clear responsibilities.

| Finding | Severity | Details |
|---------|----------|---------|
| No version control for n8n state | Medium | Workflow JSONs are exported snapshots. If someone modifies a workflow in the n8n UI without re-exporting, the git repo becomes stale. No CI check for this. |
| Postgres credentials in workflows | Info | Each workflow has Postgres credentials wired in. If the password changes, all 7 workflows need manual updates in n8n UI. |

---

## 4. dbt Pipeline Assessment

### Model Dependency Graph (simplified)

```
Sources: srp_observations, detail_observations, detail_carousel_hints, dealers, search_configs
    ↓
Staging: stg_srp_observations, stg_detail_observations, stg_detail_carousel_hints
    ↓
Intermediate:
  int_listing_to_vin (incremental)
  int_latest_tier1_observation_by_vin (incremental)
  int_latest_price_by_vin (incremental)
  int_price_events (incremental)
  int_listing_days_on_market (incremental)
  int_price_history_by_vin (view)
  int_srp_vehicle_attributes (view)
  int_model_price_benchmarks (view)
  int_dealer_inventory (view)
  int_price_percentiles_by_vin (table, full rebuild)
  int_carousel_price_events_mapped (view)
  int_carousel_price_events_unmapped (table)
  int_latest_dealer_name_by_vin (incremental)
    ↓
Marts:
  mart_vehicle_snapshot (view) — joins tier1 + price
  mart_deal_scores (view) — 13 left joins, composite scoring
    ↓
Ops:
  ops_vehicle_staleness (view) — staleness detection for detail scrape targeting
```

### Key Issues

1. **`mart_deal_scores` is a view with 13 left joins** — Every dashboard query hitting this model triggers the full join cascade. Since multiple intermediate dependencies are also views, this can cascade into expensive query plans. Consider materializing as a table (it's already configured as table in `dbt_project.yml` for marts, but mart_deal_scores may be overriding this).

2. **`int_price_percentiles_by_vin` must be a full table rebuild** — `PERCENT_RANK()` requires all rows in the partition. This means every dbt build re-scans all SRP observations within the staleness window. As data grows, this becomes the bottleneck.

3. **Incremental vs full-refresh behavior differs for `int_price_events`** — Full refresh deduplicates by `(vin, observed_at, price)` with source priority. Incremental mode skips dedup. Over time, this can cause drift between a fresh build and an incremental build.

4. **`ops_vehicle_staleness` has expensive EXISTS subqueries** — Two correlated EXISTS checks per row against large tables. These should be converted to LEFT JOINs with IS NULL checks.

5. **`staleness_window_days` variable (default 3)** — Used by 4+ models. If changed, several table materializations need full refresh. Not documented as a config dependency.

---

## 5. Security Assessment

| Finding | Severity | Details |
|---------|----------|---------|
| **Dashboard SQL injection** | **High** | Deal Finder tab interpolates user selections into SQL. While constrained by Streamlit widget options, the values come from the database itself. A poisoned `make` value like `Honda'; DROP TABLE srp_observations; --` would execute. Fix: use parameterized queries via `psycopg2.sql` module. |
| `.env` in repo root | Medium | `.gitignore` excludes `.env`, but `support_bundle_*.zip` is present in the repo root (107KB). If this contains logs or config, it could leak credentials. |
| No authentication on any service | Medium | Scraper API, admin UI, dashboard, dbt_runner, and pgAdmin are all accessible without authentication on their respective ports. Fine for local development but dangerous if any port is exposed. |
| Error messages expose internals | Low | Error responses include stack traces, file paths, and SQL errors. |

---

## 6. Testing Assessment

**There are no tests.** No unit tests, no integration tests, no test files anywhere in the project. This is the single biggest gap.

Priority test targets:
1. HTML parsers (3 SRP versions + detail page parser) — most fragile, most critical
2. VIN validation logic
3. Rotation scheduling (advance_rotation edge cases)
4. Deal score calculation
5. Incremental dbt model correctness

---

## 7. What Needs Work (Prioritized)

### P0 — Fix Now

1. **Update `schema_new.sql`** — It's missing `rotation_slot`, `seller_customer_id` on detail_observations, `dbt_runs` table, and likely other columns added since the last dump. New deployments will break.

2. **Fix dashboard SQL injection** — Replace f-string SQL interpolation in Deal Finder with parameterized queries.

3. **Fix `_fetch_known_vins` connection leak** — The `conn.close()` is unreachable. Move it outside the `with` block or use a context manager properly.

### P1 — Important

4. **Add `.dockerignore` files** — Prevent `.git`, `.venv`, `.env`, `docs/` from being copied into container images.

5. **Add concurrent build protection to dbt_runner** — A simple file lock or threading lock prevents parallel builds from corrupting incremental state.

6. **Add `rotation_slot` to admin UI** — Currently configs can only be assigned to slots via direct SQL.

7. **Remove dead parser code** — `parse_cars_results_page_html` (v1) and `parse_cars_results_page_html_v2` are likely unused. Verify and remove.

8. **Add Docker health checks** — At minimum for the scraper container (critical path).

### P2 — Should Do

9. **Add tests** — Start with parsers (golden-file HTML samples), then rotation logic.

10. **Parameterize dashboard timezone** — Replace hardcoded `'America/Chicago'`.

11. **Add connection pooling to dashboard** — Replace single `@st.cache_resource` connection with a connection pool.

12. **Add pagination to Deal Finder** — LIMIT results or use Streamlit pagination for large datasets.

13. **Optimize `ops_vehicle_staleness`** — Convert EXISTS subqueries to LEFT JOINs.

14. **Add `max_safety_pages` validation** — Admin UI or search_config model should enforce the 30-page limit documented in README.

15. **Investigate `mart_deal_scores` materialization** — Verify it's actually materializing as a table (dbt_project.yml says marts are tables, but there may be an override).

### P3 — Nice to Have

16. **Consolidate DB connection strategies** — Either use asyncpg everywhere with async background tasks, or psycopg2 everywhere with sync FastAPI routes.

17. **Add n8n workflow export CI check** — Detect drift between exported JSON and live n8n state.

18. **Add backup strategy for n8n volume** — Currently no backup for workflow state.

19. **Pin Docker base image versions** — Use digest-pinned images for reproducible builds.

20. **Add observability** — Structured logging, request tracing, Prometheus metrics.

---

## 8. Code Metrics

| Component | Files | Lines (approx) | Complexity |
|-----------|-------|-----------------|------------|
| scraper/app.py | 1 | 607 | High — routes + async job management |
| scraper/processors/ | 6 | ~1,100 | Medium — HTML parsing + browser management |
| scraper/routers/ | 1 | 274 | Low — CRUD forms |
| scraper/models/ | 1 | 72 | Low — Pydantic validation |
| dashboard/app.py | 1 | 936 | High — 20+ SQL queries, 4 tabs |
| dbt_runner/app.py | 1 | 143 | Low — subprocess wrapper |
| dbt models | 19 SQL | ~1,200 | High — complex incremental logic |
| dbt schemas | 15 YML | ~400 | Medium — validation rules |
| DB schema | 1 SQL | 668 | Medium — 10 tables, good indexing |
| Docker | 4 files | ~50 | Low |
| **Total project code** | **~50 files** | **~5,400 lines** | |

---

## 9. Overall Assessment

**Grade: B**

CarTracker is a well-architected personal project that has grown thoughtfully through iterative development. The scraping strategy is clever (discovery mode, rotation slots, dual anti-detection approaches), the data pipeline is solid (incremental dbt, artifact-first design), and the dashboard provides genuine analytical value.

The main gaps are:
- **No tests** — The biggest risk. Parser changes could silently break data ingestion.
- **Stale schema dump** — New deployments from `schema_new.sql` will fail.
- **Dashboard SQL injection** — The one security issue that matters.
- **No concurrent build protection** — Could corrupt dbt incremental state.

The codebase is clean, well-organized, and shows good engineering judgment throughout. The documentation (README, inline comments) is above average. With tests and the P0 fixes above, this would be a solid B+/A- project.
