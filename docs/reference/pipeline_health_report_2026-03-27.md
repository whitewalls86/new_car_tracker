# Cartracker Pipeline — Data Engineering Health Report
*Generated: 2026-03-27*

---

## Phase 1: Strengths, Weaknesses, Gaps, and Next Steps

---

### Strengths

**1. dbt architecture is genuinely solid.**
The staging → intermediate → mart → ops layering is textbook, and more importantly it's actually followed. Every source table has a staging model. Intermediate models carry all the business logic. Marts are clean, narrow, and purposeful. The ops layer (staleness, scrape queue) as a separate materialization type is a mature pattern most projects skip. 24 models, all with a clear reason to exist — the audit that removed dead models (Plan 22, 35.4) shows disciplined stewardship.

**2. Incremental strategy is thoughtful, not lazy.**
Most projects slap `incremental` on everything and call it done. Here the unique keys, cutoffs, and merge strategies are appropriate per model. Expensive window functions (`int_model_price_benchmarks`, `int_price_percentiles_by_vin`) are materialized as tables specifically to break the recompute chain — that's the right call. The full-refresh retry in the dbt build sub-workflow shows understanding of when incrementals break.

**3. Concurrency is handled correctly.**
Two independent concurrency problems are solved cleanly and differently:
- `dbt_lock` (single-row mutex with stale timeout) prevents concurrent dbt builds
- `detail_scrape_claims` (atomic `INSERT ... ON CONFLICT`) prevents duplicate detail scrapes across parallel runs

Both use the database as the coordination layer rather than application-level locking, which is correct in a multi-container environment.

**4. Operational observability is better than most side projects.**
- `runs` + `scrape_jobs` give per-job granularity
- `dbt_runs` captures build history with model counts and duration
- `pipeline_errors` table centralizes n8n errors
- Admin UI surfaces run history, dbt ops, and live logs
- Telegram alerts on error rate thresholds and Akamai kills
- Dashboard exposes pipeline health, not just analytics

**5. The scraping strategy is non-trivial.**
Discovery mode (stop at 80% known VINs or <1 new VIN/5-page window) is not something you throw together — it requires a feedback loop between the analytics layer and the scraper. The VIN breakpoint from `int_vehicle_attributes` is architecturally interesting: the scraper consults its own downstream model to decide whether to keep paginating. Slot rotation with dual guards (idle + gap) prevents thundering herd without a dedicated scheduler.

**6. Workflow separation in n8n is clean.**
Sub-workflows for Build DBT, Parse Detail Pages, and Results Processing mean shared logic isn't duplicated across triggers. The Orphan Checker running every 5 minutes as a dedicated concern (rather than startup logic in the scraper) is the right design — startup recovery shouldn't live in the application it's recovering.

**7. dbt test coverage is real.**
Sources have column-level tests (unique, not_null, accepted_values, FK relationships), freshness warnings/errors, and a custom VIN validation macro. This isn't checkbox testing — the custom macro shows domain-specific quality thinking.

---

### Weaknesses

**1. No Python tests at all.**
The dbt layer is tested. The Python layer — which contains scraping logic, parsing, job management, rotation guards, and the admin UI — has zero pytest coverage. This is the single biggest quality gap. The scraper's discovery mode logic, VIN breakpoint calculation, and rotation guard math are exactly the kind of business-critical logic that quietly breaks and is impossible to debug without a test harness.

**2. No CI/CD.**
There is no GitHub Actions, no automated test run on PR, no build validation before merge. The current workflow is: push branch → create PR → manually rebuild containers. A bad import, a syntax error, or a broken dbt model won't be caught until someone manually deploys and hits an error. This makes the worktree + test container approach (which is genuinely good thinking) largely manual.

**3. Schema migrations are unmanaged.**
`schema_new.sql` is a pg_dump snapshot, not a migration history. When a new table or column is needed, the change is applied manually to production. There's no Alembic, no Flyway, no ordered migration files. This is fine right now with one environment, but it means: no rollback capability, no audit trail of schema changes, and no way to safely automate deployments against a fresh database without running the full dump.

**4. Admin UI has no authentication.**
Every endpoint under `/admin/` is open to anyone with network access to port 8000. Right now that's protected only by Docker network isolation (which is reasonable for a home lab), but it's one misconfigured port mapping away from being fully exposed. No auth, no CSRF protection on form POSTs.

**5. `psycopg2` in `dbt_runner` vs `asyncpg` in the scraper.**
Two sync connection patterns in the same codebase. `dbt_runner` creates a new `psycopg2` connection on every function call with no pooling — under load (concurrent lock checks, intent reads, run records) this will exhaust connections. With `max_connections=50` on Postgres and 8 services, this headroom is tight. A connection pool (even `psycopg2.pool.SimpleConnectionPool`) or switching to `psycopg3` with async support would close this gap.

**6. Raw artifact storage is single-node and unbacked.**
HTML artifacts live on a named Docker volume (`cartracker_raw`) on one machine. No S3, no backup, no retention verification beyond the cleanup workflow. If that volume is lost, the raw data is gone — though since the source is live and re-scrapeable, this is lower risk than a transactional system. Still, for a serious deployment, raw storage belongs in object storage (S3/MinIO).

**7. n8n credential setup is a manual step.**
The Postgres connection inside n8n must be wired by hand on first setup. The entrypoint auto-imports workflows, but a workflow that references a credential by name that doesn't exist will fail silently or error on first run. This is the most fragile part of the setup story.

---

### Gaps

| Gap | Impact | Effort to close |
|-----|--------|----------------|
| No Python unit tests | High — logic bugs go undetected | Medium |
| No CI/CD | High — every deploy is manual and unvalidated | Medium |
| Schema migrations (Alembic/Flyway) | Medium — one environment today, painful at two | Low-Medium |
| Connection pooling in dbt_runner | Medium — connection exhaustion under load | Low |
| Admin authentication | Medium — security boundary relies on network only | Low |
| n8n credential automation | Low-Medium — only matters on fresh install | Low |
| No alerting on dbt model failures beyond Telegram | Low — dbt_runs table exists but nothing monitors it | Low |

---

### Biggest Wins (highest impact, reasonable effort)

1. **Add pytest for scraper core logic.** Target: `advance_rotation` guards, discovery mode thresholds, VIN breakpoint logic, rotation slot selection. These are pure functions with complex conditionals — ideal for unit testing. A 2-hour investment here would catch the class of bugs that currently only surface during a live scrape run.

2. **Add GitHub Actions CI.** A basic workflow: `docker compose build`, `dbt build --select tests`, optionally `pytest`. Even without a full staging environment, catching syntax errors and broken dbt tests before they reach production is a major reliability improvement.

3. **Connection pooling in dbt_runner.** Three lines of code — replace `psycopg2.connect()` per call with a module-level `SimpleConnectionPool`. Prevents connection exhaustion under any concurrent load.

4. **Alembic for schema migrations.** Don't replace `schema_new.sql` — add Alembic on top of it for all future changes. Autogenerate migration scripts from model changes. Gives rollback capability and a clear deployment story.

---

## Phase 2: Portfolio Assessment

---

### What It Demonstrates Well

**Web scraping engineering (uncommon skill).**
Cloudflare/Akamai bypass via Patchright and curl_cffi, TLS fingerprint rotation, human-like paging behavior, randomized page order, viewport/UA/ZIP rotation. This is not tutorial-level scraping — this is adversarial crawling with real anti-detection engineering. Most data engineers can't do this at all.

**dbt at production depth.**
Staging/intermediate/mart separation, incremental models with proper unique keys, custom test macros, source freshness monitoring, ops models as a first-class layer. This goes well beyond "I ran dbt init and added some models." The decisions around *which* models to materialize and *why* (breaking recompute chains, enabling incremental cutoffs) show genuine understanding, not cargo-culting.

**Database design for operational systems.**
The schema isn't just analytics tables — it's a set of operational tables that coordinate behavior: `dbt_lock` as a mutex, `detail_scrape_claims` for parallel coordination, `runs`/`scrape_jobs` for job lifecycle tracking, `search_configs` as a live config table. Using the database as the coordination layer across services (rather than Redis or Zookeeper) is a defensible architectural choice that shows SQL depth.

**End-to-end pipeline ownership.**
Scraping → storage → parsing → transformation → serving → monitoring — one person owns the full stack. This is increasingly rare as data teams specialize. The breadth is genuinely impressive for a portfolio piece.

**Operational tooling.**
Most portfolio data projects have a README that says "run `dbt run` to build." This one has an admin UI, a log viewer, a dbt action panel with intent management, run history drill-down, Telegram alerting, and a health dashboard. That's production thinking, not academic thinking.

**Docker Compose multi-service architecture.**
Eight services, custom networks, external volumes, environment-injected secrets, profile-gated services, build-time vs. runtime separation. This shows infrastructure fluency beyond just "I dockerized a script."

**Workflow orchestration with n8n.**
Sub-workflows, conditional routing, webhook-based error handling, scheduled triggers with guard logic — this shows orchestration thinking that translates directly to Airflow, Prefect, or Dagster in an enterprise context.

---

### What Isn't Shown (and Would Be Asked About in a Hire)

**1. Testing discipline.**
This is the first thing a senior data engineer or hiring manager will ask. "Where are your tests?" The dbt tests are there, but there's no pytest. In a professional context, untested data pipeline code is a liability. A candidate who can't show a test suite for their scraper logic will face follow-up questions about quality practices. Fix: add pytest for core business logic functions.

**2. CI/CD and automation.**
No GitHub Actions means no evidence of automated quality gates. This signals that the project works on one machine, manually. Enterprise data engineering is inseparable from CI/CD. Even a simple workflow that runs `dbt build` on PR would close this gap significantly.

**3. Cloud-native storage and compute.**
Everything is local Docker on one machine. No S3, no managed Postgres (RDS/CloudSQL), no cloud scheduler. A hiring manager at a company running on AWS or GCP will wonder "can this person design for cloud?" The architecture *could* be lifted to cloud (swap Docker volume for S3, Postgres for RDS, n8n for MWAA) but it's not demonstrated.

**4. Data warehouse technologies.**
Postgres is solid, but Snowflake, BigQuery, and Redshift dominate enterprise data engineering. There's no evidence of warehouse-specific features: partitioning, clustering, external tables, warehouse-level query optimization. A candidate strong in Postgres who's never touched Snowflake will need to address this.

**5. Streaming / event-driven patterns.**
Everything here is batch: scheduled scrapes, periodic dbt builds, polling-based job management. There's no Kafka, no Kinesis, no event triggers. For roles that involve real-time pipelines, this is a gap — though it's a reasonable scope decision for a scraping project.

**6. Infrastructure as Code.**
No Terraform, no Pulumi, no CDK. The Docker Compose is well-structured, but it's not IaC. A senior data engineer is expected to provision infrastructure declaratively, especially in cloud environments.

**7. Type annotations in Python.**
The Python code largely lacks type hints. In a professional Python codebase, especially one being reviewed for a hire, untyped code raises questions about maintainability. This is a quick win — add return types and parameter annotations to the scraper and dbt_runner functions.

**8. Airflow familiarity.**
n8n is an excellent tool and the workflow design is clean, but it's not Airflow. Airflow is the default orchestrator in most data engineering job descriptions. The orchestration thinking clearly transfers — DAGs, sub-workflows, error handling, retry logic — but the candidate would need to explicitly draw that connection in an interview, or better yet, translate one workflow to an Airflow DAG as a portfolio addition.

---

### Summary Scorecard (Portfolio Lens)

| Dimension | Score | Notes |
|-----------|-------|-------|
| Data modeling (dbt) | ★★★★★ | Exceptional for a solo project |
| Pipeline architecture | ★★★★☆ | Strong; cloud-native gaps |
| Scraping engineering | ★★★★★ | Rare, advanced skill |
| Operational tooling | ★★★★☆ | Above average; auth gap |
| Testing | ★★☆☆☆ | dbt only; no Python tests |
| CI/CD | ★☆☆☆☆ | Absent |
| Cloud/warehouse | ★★☆☆☆ | Local-only; translatable |
| Documentation | ★★★★☆ | README + plans.md is strong |
| Monitoring/alerting | ★★★☆☆ | Functional; not production-grade |
| Code quality | ★★★☆☆ | Clean; lacks types, tests |

**Overall:** This is a strong portfolio project that demonstrates unusual breadth and genuine production thinking. The core data engineering — dbt modeling, incremental strategy, concurrency handling, operational tooling — is well above what most candidates show. The gaps (testing, CI/CD, cloud) are real but addressable, and importantly, they're the kind of gaps that don't indicate lack of skill — they indicate scope decisions made on a solo project. A candidate who can articulate *why* those gaps exist and *what they would do differently* in a professional context will interview well.
