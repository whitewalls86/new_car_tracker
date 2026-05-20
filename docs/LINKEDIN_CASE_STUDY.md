# LinkedIn Case Study — Draft

---

## ARTICLE (Long-form LinkedIn article)

**Title:** I Built a Vehicle Pricing Data Platform From Scratch. Here's What I Actually Learned About Data Engineering.

---

I started this project because I wanted to track new car prices and couldn't find a data source that didn't cost a fortune or update once a week. So I built one.

What I ended up with is not really a scraper. It's a production data platform running on a single VPS: concurrent scraping with anti-detection, a medallion storage architecture across PostgreSQL and MinIO, dbt-powered analytics on top of DuckDB, Airflow orchestration, Grafana observability, role-based access control, and a CI pipeline with 974 tests — SQL smoke tests, dbt logic tests, and API integration tests against real infrastructure.

The scraping was the easy part.

---

### The Architecture (And Why It Looks Like This)

The stack is not exotic. PostgreSQL, MinIO (S3-compatible object store), DuckDB, dbt, Airflow, Grafana. What made it interesting was having to reason about *why* each piece belongs where it does.

**Bronze → Silver → Mart** — the classic medallion pattern, made concrete:

- **Bronze (MinIO):** Every raw HTML page the scraper fetches lands here as zstd-compressed bytes, partitioned by date and artifact type. This is the replayable record for the retention window. If a parser bug surfaces, I can re-run affected pages through the fixed parser without re-scraping, as long as they are still within the window.
- **Silver (MinIO Parquet):** Parsed observations, hive-partitioned by source and date. This is authoritative for analytics — not a backup of Postgres, the primary record. If the Postgres hot tables were lost, they could be reconstructed from silver. The reverse is not true.
- **Operational state (PostgreSQL):** Current state only — one row per entity. Price per VIN right now, claim status right now, cooldown state right now. Fast point lookups, small tables, no historical scan burden.
- **Mart (DuckDB):** dbt transforms silver Parquet into analytics tables via the `httpfs` extension — DuckDB queries MinIO directly. No separate warehouse, no ETL copy step. 17 models, run hourly via Airflow.

The key decision: Postgres is *not* the observation store. I tried making it do both operational lookups and analytical history early on, and the tradeoffs were bad in both directions. Separating concerns by storage engine — Postgres for hot state, MinIO/Parquet for history — made both cleaner.

---

### The Hot + Staging Pattern

This pattern came directly from hitting a real problem, which is the best kind.

The early design derived operational state from dbt. Cooldown decisions, claim eligibility, what to scrape next — these came from dbt models rebuilt on a schedule. That created a hard coupling: the pipeline could only act on information that was at least one dbt cycle old. A blocked VIN's cooldown signal could lag by a full dbt cycle before the scraper would respect it. Operational latency was bounded by analytical batch cadence, which is the wrong dependency direction.

The fix was separating the two concerns at the storage level:

- `ops.<table>` — one row per entity, current state only (O(1) lookups, always current)
- `staging.<table>_events` — append-only buffer of every state transition

Services write both in the same transaction: update the hot table, insert into the event buffer. Operational decisions read from `ops` — no dbt dependency, low-latency point lookups. An Airflow DAG reads up to a snapshot boundary, flushes those rows to MinIO Parquet, then deletes them. Postgres stays lean; the historical record moves to object storage; dbt builds over history at its own cadence without blocking anything.

This pattern runs across artifacts, claims, price observations, cooldown state, and VIN mappings. The snapshot-boundary approach (`max(id)` at flush time, delete `WHERE id <= max_id`) is safe under concurrent inserts — rows added during the flush are left behind and caught in the next cycle.

---

### Failure Modes as First-Class Design

Most of the interesting work in this project is about failure handling, not the happy path.

**Orphan claims:** When a scraper container crashes mid-run, its in-flight claims are never released. Every claim row carries a `claimed_at` timestamp. An `orphan_checker` DAG runs every 5 minutes and expires any claim older than its TTL. No distributed lock service, no heartbeat protocol — just time-bounded state and a cleanup cron.

**Cloudflare 403 blocks:** When a request is blocked, the scraper records the event in `ops.blocked_cooldown` with an exponential backoff formula. That state is flushed to MinIO and modeled in dbt as `stg_blocked_cooldown_events`, which computes `next_eligible_at` and a `fully_blocked` flag. Cooldown logic is queryable, testable, and decoupled from application code.

**Parser bugs against recent data:** Because every raw HTML page is stored in MinIO bronze within the retention window, a parser fix does not require a re-scrape for recent artifacts. Bronze artifacts can be requeued and reprocessed — it is just another write to silver.

**Safe deploys without Kubernetes:** Services expose `/health` (liveness) and `/ready` (drain state). A deploy sets a `deploy_intent` flag in Postgres, which Airflow sensors observe to pause new DAG runs. The deploy script polls `/ready` on each service until they drain, cycles the containers, then clears the flag. Drain-aware deploys on a single VPS — in-flight work completes before containers are cycled.

---

### Why I Replaced the Orchestrator Mid-Project

The pipeline started on n8n. It worked until workflow complexity grew to the point where bugs were hard to find: logic lived in a visual UI, not in code. It couldn't be unit tested, didn't produce meaningful diffs, and a broken workflow could look successful until data stopped arriving.

The switch to Airflow wasn't about features. It was about making orchestration subject to the same standards as the rest of the codebase.

That produced the fat-services, thin-DAGs pattern: DAGs contain scheduling and HTTP calls; business logic lives in Python where it can be tested and reviewed. Airflow ran in parallel with n8n for four days, then n8n was deactivated. The cutover was clean because the service contracts didn't change — only the caller did.

---

### Why Observability Became Non-Negotiable

Before Grafana, debugging meant knowing which of six services to look at, grepping into a Docker container's log file, and hoping the relevant line was still in the buffer. There was no single place to search across the pipeline. Alerts were Telegram nodes wired into n8n workflows — which meant they were decommissioned along with n8n when the orchestration layer changed.

The deeper problem: the primary signal that something was wrong was the *absence of data*. I was checking Streamlit dashboards to verify the pipeline was healthy. The system wasn't telling me when it failed — I was noticing the silence.

At six services, twelve DAGs, and three storage tiers, that stops being a viable strategy.

Grafana became the front door: Loki/Promtail for cross-service logs, Prometheus for metrics, and nine Telegram-backed alert rules for error spikes, stuck runs, stale data, and lock timeouts. The value showed up quickly: an alert fired on a spike in processing errors caused by a type mismatch — a price field written as a float where the staging table expected a different type. The write was erroring, but without centralized log monitoring there was no easy way to know. You would have had to already suspect there was a problem, then go grep for it. The alert made the failure visible without requiring anyone to go looking.

The principle is simple: a pipeline complex enough to be worth building is complex enough to need observability. Adding it after the fact is always more painful than building it in.

---

### Anti-Detection

Modern sites use TLS fingerprinting to detect automated clients. The standard Python `requests` library has a fingerprint that gets flagged quickly.

The fix here is `curl_cffi`, which uses libcurl with configurable TLS fingerprints matching real Chrome versions. FlareSolverr handles JavaScript challenge pages and vends `cf_clearance` cookies with a 25-minute TTL, refreshed automatically on 403. The curl_cffi impersonation target is derived dynamically from FlareSolverr's reported user-agent — the TLS fingerprint always matches the browser that generated the session cookie. For SRP requests, each scrape session picks a randomly combined profile: Chrome version (one of four), viewport dimensions (one of six), and search ZIP code (drawn from a pool of 20 national and local codes).

Human-like pacing — 8–20 second delays between pages, with occasional longer pauses and random page ordering — rounds out the approach.

The motivation is not evasion for its own sake. An unreliable ingestion layer makes everything downstream unreliable. Investing in this early was the right call.

---

### Testing Strategy

974 tests across four layers:

1. **Unit tests (705):** No database required. Parsers run against real saved HTML fixtures, not mocked responses.
2. **SQL smoke tests (71):** Every mission-critical query runs against the real schema in CI under rollback isolation. These catch schema breakage before it compounds.
3. **dbt model logic tests:** Real data seeded via autocommit, full `dbt build` in subprocess, assertions against actual model output rows.
4. **API integration tests (37+):** FastAPI `TestClient` against real Postgres — no mocked connections. Auth middleware is exercised for real using hashed test credentials.

The CI layer order matters: unit → SQL smoke → dbt build → API integration. A broken schema fails before the full dbt and API layers run.

---

### Tradeoffs I'd Defend

No architecture choice is free. Here are the ones I'd expect to be challenged on:

**Single VPS instead of cloud services.** Lower cost and full ownership, but higher operational burden — no managed failover, no auto-scaling. Acceptable for a project at this scale; would be a harder call in a team environment.

**DuckDB instead of a warehouse.** Eliminates warehouse cost and a separate sync step. The tradeoff is that DuckDB has meaningful concurrency limitations with writer/reader overlap on the same files. This required careful layout and scheduling to avoid conflicts.

**In-memory scrape state instead of durable job records.** Simple and sufficient for single-container operation, but state is lost across restarts. Orphan recovery via TTL handles the most common failure, but a crash during a run means that run's work is abandoned and retried next cycle.

**Postgres for operational state, not a message queue.** Claim-based parallelism via `ON CONFLICT DO UPDATE` is simple and removes a dependency, but it means polling rather than push. Works well at this scale; Kafka readiness is designed in for the day the polling becomes a bottleneck.

**30-day bronze retention window.** Raw HTML artifacts are kept for 30 days and then cleaned up. This bounds storage cost on a single VPS, with the accepted tradeoff that reprocessing from raw HTML is only possible within that window. Silver Parquet (parsed observations) is kept indefinitely — so the analytical record is durable even after bronze expires.

---

### The Numbers

- **40+ make/model pairs** scraped every 15–30 minutes
- **50,000+ unique VINs** tracked
- **13.7M+ observations** migrated to MinIO Parquet at launch
- **17 dbt models** from staging through mart (deal scores, price history, inventory coverage, 403 block rate trends)
- **39 Flyway migrations** — every schema change versioned, reviewed, and applied automatically in CI
- **12 Airflow DAGs** orchestrating scrape rotation, processing, archiving, cleanup, and dbt builds
- **974 tests** across unit, SQL smoke, dbt logic, and API integration layers
- **9 Telegram alert rules** for pipeline health (error spikes, stuck runs, stale data, lock timeouts)

---

### The Interview Pitch

"I built a scraper" is the wrong frame for this project.

The better pitch: I built a pricing data platform where the scraper is the ingestion layer. The work that mattered was reasoning about operational state vs. analytical history, designing for replayability, building failure recovery into the structure rather than bolting it on, and running a production observability stack on constrained infrastructure — without a cloud data warehouse, managed orchestration, or a team.

Working within the constraint of a single VPS forced explicit decisions. When you can't throw resources at a problem, you have to think about it.

---

### The Stack

**Storage:** PostgreSQL 16, MinIO (S3-compatible), DuckDB  
**Transformation:** dbt-core 1.11, dbt-duckdb  
**Orchestration:** Apache Airflow 2.x (LocalExecutor)  
**Scraping:** curl_cffi (Chrome TLS fingerprinting), FlareSolverr  
**Services:** FastAPI, Streamlit  
**Auth:** Google OAuth2, DB-backed roles, Caddy forward-proxy  
**Observability:** Grafana, Prometheus, Loki, Promtail, StatsD  
**Schema management:** Flyway  
**CI:** GitHub Actions, Postgres 16 + MinIO in-workflow  
**Infra:** Docker Compose, Caddy, single OCI VPS

Live at: **cartracker.info**

---

---

## COMPANION POST (Short LinkedIn post linking to the article)

---

I spent the last thre months building a vehicle pricing data platform — not a dashboard, not a demo. A real system with failure recovery, schema migrations, observability, and CI.

The pitch I should have used from the start:

"The scraper is the ingestion layer. The work that mattered was separating operational state from analytical history, designing for replayability, and building failure recovery into the structure — on a single VPS, without a cloud warehouse or a team."

What I'm most proud of:

→ Raw HTML stored in MinIO bronze — recent parser bugs can be fixed without re-scraping  
→ Hot + staging pattern — Postgres holds only current state; full history lives in Parquet  
→ Orphan claim recovery via TTL — no distributed lock service, no heartbeat protocol  
→ 974 tests: SQL smoke, dbt model logic, and API integration against real infrastructure  
→ n8n → Airflow mid-project — orchestration logic moved from visual workflows into testable code  

I wrote up the architecture decisions, tradeoffs, and failure modes in detail.

[link]

If you were reviewing this architecture in an interview, what failure mode would you challenge first? I'll answer in the comments.

---

---

## POST SERIES

---

### Post 1 — The Pitch Reframe
*Goal: broad reach, recruiter/hiring-manager readable. Publish first, within 24h of the article going live.*

---

For a while I described this project as "a scraper that tracks new car prices."

That framing undersells it every time.

The scraper is the ingestion layer. It fetches HTML pages, writes compressed artifacts to object storage, and puts a message in a queue. The first version of that part came together quickly.

What took three months was everything else:

— A medallion architecture across PostgreSQL, MinIO, and DuckDB, with clear boundaries between operational state and analytical history

— Failure recovery designed into the structure: orphan claim expiry, exponential cooldown tracking, replayable raw artifacts, drain-aware deploys

— An orchestration migration mid-project when I realized workflow logic you can't unit test isn't code — it's a liability

— Centralized observability after I noticed I was checking dashboards for missing data instead of receiving alerts

— 974 tests across SQL smoke, dbt model logic, and API integration layers, all running against real infrastructure in CI

The better pitch: I built a pricing data platform on a single VPS where the scraper is just the ingestion layer. The interesting work was deciding what needs to be true *right now* versus what needs to be true *historically* — and building a system that keeps those concerns separate.

I wrote up the full architecture, tradeoffs, and failure modes here: [link]

What would you challenge first: the single-VPS constraint, DuckDB instead of a warehouse, or Postgres as the operational state store?

---

### Post 2 — n8n → Airflow Migration
*Goal: engineering-process relatability. Best for comments from engineers who've hit the same wall.*

---

I replaced my orchestration layer mid-project. Not because Airflow had better features.

The pipeline started on n8n. Visual workflows, drag-and-drop nodes, easy to get started. It worked fine until it didn't.

The problem: workflow logic lived in a UI, not in code. SQL queries sat inside Postgres nodes. Conditional branching and retry logic lived in a visual graph exported as JSON. None of it could be unit tested. None of it showed up as a meaningful diff in a PR.

The worst part: a broken workflow could look exactly like a passing one. n8n could report success on a run that had silently skipped a step. The only way to catch those failures was to notice that data had stopped arriving.

I was still patching n8n workflows on April 13th. Two days later I started building Airflow.

The switch wasn't about features. It was about one principle the n8n experience had made concrete: orchestration logic should be subject to the same standards as everything else. If you can't unit test it, you can't trust it.

That produced the fat-services, thin-DAGs pattern I use now. DAGs contain two things: scheduling and HTTP calls. All business logic lives in Python where it can be tested, diffed, and reviewed. Airflow ran in parallel with n8n for four days. The cutover was clean because the service contracts hadn't changed — only the caller did.

Full architecture writeup: [link]

Have you ever inherited — or built — workflow logic that lived somewhere it couldn't be tested? How did you get it out?

---

### Post 3 — Observability Gap
*Goal: operations maturity signal. The "absence of data" line tends to resonate broadly.*

---

For a while, the main signal that my data pipeline had failed was that data stopped showing up.

Not an alert. Not an error notification. Just: I'd open a dashboard, notice the numbers hadn't moved, and then go figure out why.

Debugging meant knowing which of six services to look at, opening a shell into a Docker container, and grepping a log file. There was no single place to search across the stack. Alerts were Telegram notifications wired into n8n workflows — which meant they were decommissioned when n8n was.

At a certain complexity threshold — six services, twelve Airflow DAGs, three storage tiers — "check the logs when something seems wrong" stops being a strategy. You need the system to tell you when something is wrong. And you need one place to look when it does.

Adding Grafana, Loki, Promtail, and Prometheus changed the operating model. Cross-service logs in one place. Prometheus metrics. Nine alert rules pushing to Telegram for error spikes, stuck runs, stale data, and lock timeouts.

The value showed up quickly. An alert fired on a spike in processing errors caused by a type mismatch — a price field written as a float where the staging table expected an integer. The write was erroring. But without centralized monitoring there was no easy way to know. You would have had to already suspect there was a problem, then go grep for it.

The alert made the failure visible without requiring anyone to go looking. That's the job.

Full architecture writeup: [link]

What's your observability baseline for a self-hosted data pipeline? Curious how others approach this at small-to-medium scale.

---

### Post 4 — Hot + Staging Pattern
*Goal: senior/staff-level architecture signal. More technical, narrower audience, higher quality engagement.*

---

One of the better architectural decisions I made was recognizing when operational latency was being bounded by analytical batch cadence.

Early on, the pipeline derived operational state from dbt. Cooldown decisions, claim eligibility, what to scrape next — these came from dbt models rebuilt on a schedule. That meant the pipeline could only act on information that was at least one dbt cycle old. A blocked VIN's cooldown signal could lag by a full build cycle before the scraper would respect it.

Operational latency bounded by analytical batch cadence is the wrong dependency direction.

The fix was separating the two concerns at the storage level. Every operational table now follows the same structure:

ops tables — one row per entity, current state only. Fast point lookups, always current, no dbt dependency.

staging event tables — append-only buffer of every state transition, flushed to MinIO Parquet on a schedule, then deleted up to a snapshot boundary.

Services write both in the same transaction: update the hot table, insert into the event buffer. Operational decisions read from ops — no batch dependency. dbt builds over the historical record in object storage at its own cadence without blocking anything.

The snapshot-boundary delete (read max row ID at flush time, delete everything up to that ID) is safe under concurrent inserts — rows added during the flush are caught in the next cycle.

This pattern runs across artifacts, claims, price observations, cooldown state, and VIN mappings.

Full architecture writeup: [link]

Has anyone else run into operational state coupling to batch cadence? Curious whether this is a common pattern or specific to this kind of pipeline.

---
