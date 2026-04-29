# CarTracker

CarTracker scrapes Cars.com every 15 minutes across 40+ make/model pairs and runs raw HTML through a bronze → silver → mart data pipeline backed by Postgres, MinIO, and DuckDB. Built as a portfolio piece to show how a real data pipeline handles the messy middle — anti-detection, idempotent writes, event sourcing, schema migrations, and observability — not just the happy path.

**Live site:** https://cartracker.info — dashboard, ops admin, and project info page. Access is gated by Google OAuth2 + DB-backed role authorization. Request access at https://cartracker.info/request-access.

---

## How data flows

```
Scraper ──► MinIO (raw HTML)
                │
           artifacts_queue
                │
           Processing ──► Postgres staging events
                                  │
                             Archiver ──► MinIO Parquet (silver)
                                                │
                                           dbt + DuckDB (mart)
                                                │
                                           Dashboard (Streamlit)
```

| Layer | Storage | What lives here |
|-------|---------|-----------------|
| **Bronze / Ingest** | MinIO | Compressed raw HTML artifacts, partitioned by date and type |
| **Operational** | Postgres | Queue state, price observations, VIN mappings, claims, cooldowns |
| **Silver** | MinIO (Parquet) | Bulk-flushed observation events partitioned by source/date |
| **Mart** | DuckDB | dbt-transformed analytics tables — deal scores, price history, benchmarks |

---

## Services

| Service | Description |
|---------|-------------|
| **scraper** | FastAPI — fetches SRP and detail pages with curl_cffi Chrome TLS fingerprinting. FlareSolverr bootstraps `cf_clearance` cookies shared via a process-wide credential cache (25-min TTL). Writes compressed HTML artifacts to MinIO. |
| **processing** | FastAPI — claims artifacts from `artifacts_queue`, parses HTML, writes price observations and VIN mappings via staging event tables. |
| **archiver** | FastAPI — bulk-flushes staging event buffers to MinIO Parquet on a schedule. Sweeps completed queue rows and expired Parquet months. |
| **dbt_runner** | FastAPI — runs dbt transformations against DuckDB on demand or on schedule. Reads Parquet directly from MinIO via the httpfs extension — no separate data warehouse needed. |
| **ops** | FastAPI — admin UI, deploy coordination, claim management, and the `/auth/check` forward-proxy endpoint used by Caddy. |
| **dashboard** | Streamlit — price history, deal scores, inventory coverage, and pipeline health views backed by the DuckDB mart layer. |
| **airflow** | Schedules all DAGs — scrape rotation, artifact processing, staging flush, Parquet cleanup, queue cleanup, and orphan checks. |
| **postgres** | PostgreSQL 16 — operational tables (queue, claims, observations, auth). Airflow metadata lives in its own `airflow` schema. |
| **minio** | S3-compatible object store for raw HTML artifacts and Parquet silver observations. Queryable directly by DuckDB via httpfs. |
| **caddy** | Reverse proxy — TLS termination, OAuth2 Google auth via oauth2-proxy, and DB-backed role enforcement via `/auth/check`. |
| **grafana** | Operational dashboards sourced from Prometheus — Airflow, Postgres, MinIO, and node metrics. |
| **flaresolverr** | Solves Cloudflare JS challenges and provides `cf_clearance` cookies to the scraper. |

---

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `scrape_listings` | Every 30 min | Advances the rotation slot, explodes configs × scopes, triggers SRP scrapes. |
| `scrape_detail_pages` | Every 15 min | Queries the detail scrape queue, atomically claims a batch, fetches and writes detail page artifacts. |
| `results_processing` | Every 5 min | Claims unprocessed artifacts from `artifacts_queue`, POSTs to the processing service for parsing. |
| `flush_staging_events` | Every 15 min | Archiver bulk-flushes staging event tables to HOT tables. |
| `flush_silver_observations` | Every 5 min | Archiver writes accumulated observations to MinIO Parquet. |
| `dbt_build` | Hourly | Runs `dbt build` against DuckDB. Supports selective model runs via `dag_run.conf`. |
| `orphan_checker` | Every 5 min | Expires stale `detail_scrape_claims` rows left by crashed containers. |
| `cleanup_artifacts` | Hourly | Sweeps completed and expired rows from `artifacts_queue`. |
| `cleanup_parquet` | Daily 3 AM | Marks expired Parquet months as deleted per retention policy. |
| `cleanup_queue` | Hourly | Removes fully-processed queue rows. |
| `delete_stale_emails` | Every 2 hours | Nulls `notification_email` on access requests older than 48 hours (opt-in email retention). |

---

## Technical highlights

**Medallion architecture across two storage engines** — Raw HTML lands in MinIO (bronze), gets parsed into Postgres staging events (operational), flushed to Parquet (silver), and transformed by dbt into DuckDB mart tables. Each layer uses a different storage engine chosen for its access pattern.

**Event sourcing via staging flush** — Price observations write to `staging._events` tables first. The archiver bulk-flushes them to HOT tables and Parquet on a schedule. This decouples write throughput from read consistency and enables bulk-insert optimization.

**Atomic claim pattern** — Parallel detail scrapes use `ON CONFLICT DO UPDATE` against `detail_scrape_claims` to prevent duplicate work without a queue service. Time-based expiry via the `orphan_checker` DAG handles crashed containers.

**dbt backoff model** — 403'd listings are tracked in `staging.blocked_cooldown_events`. Exponential backoff logic (`next_eligible_at`, `fully_blocked`) lives entirely in a dbt staging model — queryable, testable, and separated from application code.

**Anti-detection resilience** — curl_cffi with Chrome TLS fingerprinting bypasses passive TLS inspection. FlareSolverr handles active Cloudflare JS challenges. Process-wide credential cache with 25-min TTL and automatic re-bootstrap on 403.

**Versioned schema migrations** — 36 Flyway migrations track every schema change from initial setup through the full medallion evolution — reviewed as code and applied automatically on deploy.

**Role-based auth without an auth service** — Caddy calls `GET /auth/check` on the ops service as a forward-proxy. The endpoint hashes the `X-Auth-Request-Email` header with a fixed salt and checks `authorized_users`. Emails are stored as `SHA-256(salt + email)` — not plaintext.

---

## Auth & access control

Authentication is handled by Google OAuth2 via oauth2-proxy. Every request passes through Caddy → oauth2-proxy before reaching any service.

Authorization is DB-backed. Caddy calls `GET /auth/check` on the ops service (internal only) after authentication. The ops service looks up the hashed email in `authorized_users` and returns the role.

| Role | Access |
|------|--------|
| `admin` | Everything — deploy panel, search config edits, pgAdmin, MinIO, Grafana |
| `power_user` | Search config edits, ops admin read/write |
| `observer` | All ops admin pages read-only — safe for portfolio demos |
| `viewer` | Dashboard only |

Users who authenticate but aren't in `authorized_users` are redirected to `/request-access`. Admins approve or deny from the ops admin UI.

---

## Operational data model

Tables written by the application (post-decommission of the n8n/runs era):

| Table | Description |
|-------|-------------|
| `artifacts_queue` | Queue of MinIO artifact paths pending processing. Status: pending → processing → done. |
| `staging.artifacts_queue_events` | Event log for queue state transitions. |
| `ops.price_observations` | HOT table — current price per listing, upserted by processing. |
| `staging.price_observation_events` | Staging buffer flushed to HOT by the archiver. |
| `ops.vin_to_listing_mapping` | HOT table — maps VINs to listing IDs. |
| `staging.vin_to_listing_events` | Staging buffer for VIN mapping events. |
| `ops.detail_scrape_claims` | Atomic claim table for parallel detail scrapes. |
| `staging.blocked_cooldown_events` | Raw 403 events — backoff logic lives in dbt. |
| `search_configs` | Search definitions — make/model, rotation slot, last queued at. |
| `tracked_models` | Active make/model pairs to track. |
| `deploy_intent` | Single-row deploy coordination flag. |
| `authorized_users` | Email hashes + roles for DB-backed authorization. |
| `access_requests` | Pending/approved/denied access requests. |

---

## dbt models (DuckDB target)

Sources: MinIO Parquet (silver observations, cooldown events) + Postgres views (search configs, price observations).

**Staging**
- `stg_observations` — silver Parquet observations with typed fields
- `stg_price_events` — price event stream from observation events Parquet
- `stg_blocked_cooldown_events` — 403 events with `next_eligible_at` and `fully_blocked` computed via exponential backoff
- `stg_search_configs` — active search config snapshot from Postgres
- `stg_dealers` — dealer name/ID pairs parsed from observations

**Intermediate**
- `int_latest_observation` — most recent observation per VIN
- `int_price_history` — price trajectory per VIN (first seen, drops, min/max)
- `int_benchmarks` — national price percentiles by make/model/trim
- `int_active_make_models` — distinct active make/model pairs from search configs

**Marts**
- `mart_vehicle_snapshot` — current state per VIN filtered to tracked make/models
- `mart_deal_scores` — composite deal score (0–100) with tier (excellent/good/fair/weak)
- `mart_price_freshness_trend` — price observation recency over time
- `mart_inventory_coverage` — VIN count vs staleness by make/model
- `mart_detail_batch_outcomes` — detail scrape success/403/error rates over time
- `mart_cooldown_cohorts` — 403 cooldown distribution by make/model

---

## Testing

971 tests across two suites. Run from repo root:

```bash
# Unit tests — no database or Docker required
pytest tests/ -m "not integration"

# Integration tests — requires Postgres with Flyway migrations applied
TEST_DATABASE_URL=postgresql://cartracker:cartracker@localhost:5432/cartracker \
  pytest tests/integration/ -m integration
```

**Unit tests (705)** cover scraper discovery mode, VIN breakpoint logic, anti-detection session management, processing parsers, archiver flush logic, ops routing, and dbt DAG integrity.

**Integration tests (266)** execute every SQL query across all services against a real Postgres schema — smoke tests for table/constraint existence, write path correctness, and Layer 3 API behavior. CI spins up Postgres + MinIO, applies all 36 Flyway migrations, seeds MinIO with empty Parquet schemas, runs a full `dbt build`, then runs all integration suites.

---

## Project structure

```
cartracker-scraper/
  scraper/                      # SRP + detail page fetcher
    processors/
      scrape_results.py         # SRP fetcher — discovery mode, VIN breakpoint, adaptive backoff
      scrape_detail.py          # Detail fetcher — curl_cffi, adaptive delay, batch concurrency
      cf_session.py             # FlareSolverr bootstrap, process-wide credential cache
      parse_detail_page.py      # Detail HTML parser
      results_page_cards.py     # SRP HTML parser
      fingerprint.py            # ZIP rotation, human-like pacing
  processing/                   # Artifact queue consumer + parser
    writers/
      srp_writer.py             # Writes price observations + VIN mappings via staging events
  archiver/                     # Staging flush + Parquet writer
    processors/
      flush_staging_events.py   # HOT table flush
      flush_silver_observations.py  # MinIO Parquet writer
      cleanup_parquet.py        # Expired Parquet month cleanup
  ops/                          # Admin UI + deploy coordination + auth proxy
    routers/
      admin.py                  # Search config CRUD, access request management
      deploy.py                 # deploy_intent management, running count
      scrape.py                 # Rotation advance, claim-batch, release
      maintenance.py            # Orphan claim expiry
      info.py                   # Portfolio landing page with live stats
      users.py                  # User + access request management
  dbt_runner/                   # dbt build trigger (FastAPI)
  dashboard/                    # Streamlit analytics UI
  dbt/
    models/
      staging/                  # 5 staging models
      intermediate/             # 4 intermediate models
      marts/                    # 6 mart models
    profiles.yml                # duckdb + postgres targets
  airflow/dags/                 # 12 Airflow DAGs
  db/migrations/                # 36 Flyway migrations (V001–V036)
  tests/
    integration/                # 266 integration tests (real Postgres + MinIO)
      sql/                      # SQL smoke tests
      ops/                      # Layer 3 API tests
      archiver/                 # Archiver write-path tests
      airflow/                  # DAG integrity tests
  docker-compose.yml
  docker-compose.override.yml   # Port mappings for local dev
  .env.example
```

---

## Live endpoints

| | URL |
|-|-----|
| Dashboard | https://cartracker.info |
| Project info | https://cartracker.info/info |
| Ops admin | https://cartracker.info/admin |
| Grafana | https://cartracker.info/grafana |
| MinIO | https://cartracker.info/minio |
| Request access | https://cartracker.info/request-access |
