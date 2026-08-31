# CarTracker

![CarTracker — bronze to mart data pipeline](docs/reference/cover-image.svg)

CarTracker continuously collects new-car listings from Cars.com and turns them
into an analytical product, moving each listing through a replayable bronze →
silver → mart pipeline built on Airflow, Postgres, MinIO, DuckDB, and dbt. It
exists to show how a real data platform handles the messy middle — resumable
ingestion, idempotent writes, event history, schema evolution, storage
economics, and observability designed around failure — not just the happy path.

[![CI](https://github.com/whitewalls86/new_car_tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/whitewalls86/new_car_tracker/actions/workflows/ci.yml)

**Live site:** <https://cartracker.info> — public project page with live
pipeline statistics. The dashboard and admin surfaces sit behind Google OAuth2
plus DB-backed role authorization. Request access at
<https://cartracker.info/request-access>.

---

## What this demonstrates

- **Replayable ingestion.** Every fetched page is preserved as an immutable,
  compressed object before anything parses it, so a parser change can be re-run
  against history instead of requiring a re-scrape.
- **Current state and event history are separate concerns.** Postgres holds one
  current row per entity and a short-lived append-only buffer beside it; the
  durable history lives in columnar object storage.
- **Orchestration that stays thin.** Airflow schedules and sequences work;
  business logic lives in service endpoints and SQL models that can be tested
  without an orchestrator running.
- **Schema evolution as reviewed code.** Every schema change is a versioned
  migration applied automatically on deploy, including expand/contract
  sequences that let readers and writers change in separate releases.
- **Testing across engines, not just functions.** Contracts are asserted over
  Python, Compose configuration, real Postgres and MinIO, and the dbt/DuckDB
  models themselves.
- **Storage economics treated as an engineering problem.** Compression and
  object packing were adopted against measured byte, object, and inode costs
  rather than as defaults.
- **Observability designed around failure modes.** Signals are chosen so that
  missing success is detectable, not only visible errors.

---

## How data flows

```
                        Cars.com
                            │
                            ▼
                         Scraper
                            │
        ┌───────────────────┴───────────────────┐
        ▼                                       ▼
  Bronze: compressed                    Artifact pointer
  HTML objects (MinIO)                  + state (Postgres ops)
        │                                       │
        └───────────────────┬───────────────────┘
                            ▼
                        Processing
                            │
        ┌───────────────────┴───────────────────┐
        ▼                                       ▼
  HOT state: listing, VIN                Event buffer + typed
  mapping, claim, cooldown               observations
  (Postgres ops)                         (Postgres staging)
        │                                       │
        │                                       ▼
        │                                   Archiver
        │                                       │
        │                                       ▼
        │                       Operational history + silver
        │                       observations (MinIO Parquet)
        │                                       │
        └───────────────────┬───────────────────┘
                            ▼
                 dbt + DuckDB → mart tables
                            │
                            ▼
        Dashboard, metrics, and the public project page
```

| Layer | Physical home | Grain | Why it exists |
|---|---|---|---|
| **Bronze** | MinIO | One compressed HTML object per fetched results or detail page | Preserves exactly what the site returned, so parser changes are replayable |
| **Operational** | Postgres `ops` | One current row per artifact, listing, VIN mapping, claim, or cooldown | Low-latency point lookups, transactions, leases, and conflict handling |
| **Staging buffers** | Postgres `staging` | Append-only mutations and typed observations awaiting bulk export | Decouples small transactional writes from columnar object-store writes |
| **Silver** | MinIO (Parquet) | One typed observation per listing appearance, partitioned by source and date | Durable analytical history, cheap and scan-friendly |
| **Mart** | DuckDB, built by dbt | VIN-, listing-, hour-, cohort-, and benchmark-grain products | Turns history into dashboard-ready business meaning |

The write pattern is the part most often described wrongly: **processing updates
the HOT row and appends its event in the same Postgres transaction.** The
archiver does not populate HOT tables — it later exports the event buffer to
Parquet and deletes the exported buffer rows. Postgres therefore owns live
application decisions, while Parquet and dbt own history and its analytical
interpretation.

The same distinction governs the 403 cooldown. The **executable** backoff that
decides whether a listing can be claimed lives in the Postgres
`ops.ops_detail_scrape_queue` view, where the scrape queue can read it
transactionally. dbt consumes the same event stream for cohort, funnel, and
block-rate analysis — history and interpretation, not the control decision.

### Scheduled work

More than a dozen Airflow DAGs are defined. `hourly_analytics_refresh` owns the
scheduled flush-and-build sequence, which is why several component DAGs are
deliberately manual-only rather than running on their own timers.

| DAG | Schedule | What it does |
|---|---|---|
| `scrape_listings` | Every 30 min | Advances the rotation slot, explodes configs × scopes, triggers results-page scrapes |
| `scrape_detail_pages` | Every 15 min | Reads the detail queue, atomically claims a batch, fetches and stores detail artifacts |
| `results_processing` | Every 5 min | Claims unprocessed artifacts and hands them to the processing service |
| `orphan_checker` | Every 5 min | Expires stale detail claims left behind by crashed containers |
| `hourly_analytics_refresh` | Hourly | Owns the scheduled sequence: flush staging events, flush silver observations, then build the dbt models in order |
| `cleanup_queue` | Hourly | Removes fully processed queue rows |
| `delete_stale_emails` | Every 2 hours | Nulls opt-in notification emails on access requests older than 48 hours |
| `disk_usage` | Daily | Records disk, object, and inode usage as metrics |
| `compact_silver` | Daily | Compacts silver Parquet partitions so readers never double-count |
| `prune_task_logs` | Weekly | Deletes closed Airflow task-log trees past retention |
| `pack_bronze_html` | Monthly | Packs a closed monthly bucket of bronze objects, verifies it, and prunes the sources |
| `dbt_build` | Manual | Full or selective dbt build; the hourly refresh owns the scheduled run |
| `flush_staging_events` | Manual | Operational event flush; the hourly refresh owns the scheduled run |
| `flush_silver_observations` | Manual | Silver observation flush; the hourly refresh owns the scheduled run |
| `export_ci_lake_snapshot` | Manual | Exports a production-shaped lake snapshot for CI |

---

## Production services and access boundaries

The platform runs as more than two dozen long-running services on a single
Compose host, alongside a few profile-gated and one-shot services.

| Service | Role |
|---|---|
| **scraper** | FastAPI — fetches results and detail pages using an HTTP client whose TLS behavior matches a mainstream browser, and stores compressed HTML artifacts. Bounded retries and an adaptive cooldown govern refusals. |
| **solver sidecar** | Browser-assisted session bootstrap for pages that require it, addressed over a FlareSolverr-compatible v1 API. The live container is `trawl`; the older `flaresolverr` container is retained but vestigial, and the environment variable kept its original name, `FLARESOLVERR_URL`. |
| **processing** | FastAPI — claims artifact pointers, parses HTML, and writes current state and its event in one transaction. |
| **archiver** | FastAPI — exports staging event buffers and observations to Parquet, compacts silver partitions, and sweeps completed queue rows and expired months. |
| **pack-worker** | Packs closed monthly buckets of bronze objects, on its own service so a month-scale operation cannot starve the archiver's short requests. |
| **dbt_runner** | FastAPI — runs dbt against DuckDB, which reads Parquet directly from MinIO via `httpfs`. No separate warehouse cluster. |
| **ops** | FastAPI — admin UI, deploy coordination, claim management, the public project page, and the `/auth/check` forward-auth endpoint Caddy calls. |
| **dashboard** | Streamlit — price history, deal scores, inventory coverage, and pipeline health over the DuckDB marts. |
| **airflow** (apiserver, scheduler, dag-processor, triggerer) | Schedules and sequences the DAGs above. |
| **postgres** | Operational tables and staging buffers. Airflow metadata lives in its own schema. |
| **minio** | S3-compatible object store for bronze HTML and Parquet history, queried directly by DuckDB. |
| **caddy** + **oauth2-proxy** | TLS termination, Google authentication, and DB-backed role enforcement. |
| **prometheus**, **grafana**, **loki**, **promtail**, **container-health**, exporters | Metrics, logs, three-state health, and Telegram alerting. |
| **pgadmin** | Admin-only database console. |

**Long-running worker services expose `/ready` and participate in deploy
draining.** `/ready` answers 503 while work is in flight, so a deployment can
wait for admitted work to finish instead of interrupting it.

### How authorization works

Authentication and authorization are deliberately separate, and neither
requires a dedicated identity service.

1. Caddy sends the request to oauth2-proxy for Google authentication.
2. Caddy then calls `GET /auth/check` on the ops service as forward auth.
3. Ops hashes the authenticated email with a secret salt, looks it up in
   `authorized_users`, and returns the role. Emails are stored only as
   `SHA-256(salt + email)`, never as plaintext.
4. A user who authenticates but holds no role is redirected to
   `/request-access`, where an admin can approve or deny.

| Role | Access |
|---|---|
| `admin` | Everything — deploy panel, config edits, user management, pgAdmin, MinIO, Airflow, Grafana |
| `power_user` | Config edits and ops admin read/write |
| `observer` | Ops admin, read-only |
| `viewer` | Dashboard only |

---

## Technical case studies

### Separating operational state from analytical history

An early design kept an append-only price log in Postgres. That was the wrong
home for it: append-only observation history is columnar, immutable, and read
in scans, while operational state is small, mutable, and read by key.

The split is now explicit across three Postgres schemas — configuration,
current operational state, and short-lived event buffers — with the durable
history in Parquet and the analytical products in DuckDB. Postgres keeps only
the current row per entity, so the table the scrape queue reads stays small and
deletable no matter how much history accumulates.

### Replacing n8n with Airflow

The pipeline originally ran on n8n. Its workflows were JSON blobs: not
reviewable in a pull request, not unit-testable, and not portable. Migrating to
Airflow was also the moment to move business logic out of orchestrator nodes
and into service endpoints, under a "fat services, thin DAGs" rule — a DAG task
issues an HTTP call, and the logic it invokes is testable without Airflow
running at all.

That boundary is what makes the orchestrator replaceable. If schedules were
ever replaced by events, a consumer would call the same endpoints the DAG tasks
call today.

### Deployment as a state machine, not a restart

Redeploying is a sequence of claims that must become true: request scoped
coordination, drain until admitted work is gone, recreate only the requested
services, wait for each target to reach its real health state, validate, and
only then release the gate.

The two failure branches differ on purpose. If validation fails before any
container changed, coordination is released — the pipeline should not stay
paused for a change that never happened. If a container was already replaced
and a later step fails, coordination stays **held**, because a mixed-version
fleet is a worse place to resume background work than a visible pause.

The health timeout derives from the slowest healthcheck contract in the Compose
file rather than being guessed, and a test asserts that relationship, which
turns a deployment assumption into a cross-file invariant. The tooling also
keeps *recreate* and *restart* distinct: a `git pull` can replace a
bind-mounted file's inode while the container still holds the old one, so a
config reload that reports success can be reading stale bytes.

### Storage economics: compression, then packing

Bronze HTML is written as an independently decompressible zstd frame using a
trained dictionary whose ID is encoded in the frame header — so the reader
selects the right dictionary from the data itself rather than from a mutable
"current dictionary" setting. Training a new dictionary changes future writes;
it never makes an existing object unreadable.

Compression was not the whole problem. Measuring bytes, objects, and inodes
*separately* showed the real constraint was object **count**, not size: millions
of small objects cost far more on disk than their bytes suggested. Packing
groups artifacts into immutable packs with a columnar sidecar index, so reading
one artifact is a ranged GET and a single frame decompression rather than a
scan. A source object may be deleted only after its packed replacement returns
byte-identical content, verified against a hash in the sidecar. Across the
packed months, inode pressure on the data volume fell by roughly two thirds.

### Health that a green dashboard cannot fake

The lesson that shaped the observability design came from an outage where
nothing was red. The scrape path kept running, its container stayed healthy,
and no error rate spiked — but it had stopped producing successful results, and
it stayed that way for hours.

Two changes followed. Health became three-state against a checked-in expected
service set, so a service that is *absent* reads as failure rather than
vanishing from the query, and "running but no healthcheck configured" reads as
its own unattractive value rather than as healthy. And alerts were rewritten to
fire on **missing success**, not only on visible errors: no successful bootstrap
within a window while non-success outcomes accumulate, no successful fetch while
attempts continue.

Container health, scrape volume, and log-based alerts route to Telegram.
Functional liveness — whether the pipeline is producing correct results at all —
remains a separately measured concern, and the work to bound it is in progress
rather than finished.

---

## Production today vs platform evolution

**Running in production and serving users:**

- Airflow orchestrates scraping, processing, archival, maintenance, and analytics.
- Postgres owns current operational state and short-lived event buffers.
- MinIO holds replayable bronze HTML and permanent Parquet history.
- dbt and DuckDB build and serve every analytical mart used by the public page,
  the metrics, and the Streamlit dashboard.
- Caddy, oauth2-proxy, and the ops authorization check protect application routes.

**Proven but not production-serving.** Each of these is exercised and evidenced,
and none of them is in the path of anything a user sees today:

- Production-shaped CI lake snapshots.
- Iceberg tables registered through Lakekeeper and exercised through Spark.
- dbt-Spark parity work and MLflow experiment provenance.
- Adaptive-refresh feature and backtesting foundations.

The dashboard reads DuckDB. The Iceberg work is a migration track with its own
gates, not a shipped capability.

---

## Test strategy

More than 3,000 tests run in CI. The count is the least interesting fact about
them; the variety of boundaries is the point. Tests cover four practical layers:

1. **Pure behavior** — parser rules, retry and backoff calculations, pack
   formats, metrics, state transitions, failure predicates.
2. **Configuration contracts** — Compose healthcheck coverage, the expected
   service set, Prometheus jobs, Grafana selectors, log-shipping policy,
   deployment timeout relationships, planning-document invariants.
3. **Real service integration** — migrated Postgres, MinIO, Loki, DuckDB
   extensions, HTTP routers, Airflow DAG loading, archiver operations.
4. **Cross-engine equivalence** — the same production-shaped lake fixture is
   evaluated by dbt and by independently written selector SQL, so an
   optimization cannot quietly change business semantics and pass as a speedup.

Many of the hardest bugs here live *between* files or engines. A unit test for
the health collector cannot prove every Compose service has a healthcheck; a dbt
model test cannot prove the archiver's separately maintained selector returns
the same cohort. Those need repository-level assertions, which is why
configuration and equivalence tests are first-class rather than an afterthought.

CI builds a miniature production data plane: it applies the real migrations to a
real Postgres, creates the bucket, installs the same pinned dbt adapters and
DuckDB extensions the runtime uses, seeds the shared lake fixture, runs a real
dbt build, and then runs the integration suites against it. Coverage is reported
with missing lines and no arbitrary `fail-under` gate.

```bash
# Unit and contract tests — no database or Docker required
pytest -m "not integration"

# Integration tests — requires Postgres with migrations applied
TEST_DATABASE_URL=postgresql://cartracker:cartracker@localhost:5432/cartracker \
  pytest tests/integration/ -m integration
```

The full contract, including what a new test owes its reviewer, is in
[docs/TESTING.md](docs/TESTING.md).

---

## Local quick start

Running the platform locally needs Docker and a populated `.env`.

```bash
cp .env.example .env
# Edit .env: POSTGRES_PASSWORD, the scoped role passwords, the Google OAuth
# client, the cookie secret, and AUTH_EMAIL_SALT.

# The network and the Postgres data volume are declared external.
docker network create cartracker-net
docker volume create cartracker_pgdata

docker compose up -d
```

Flyway applies every migration on first start — there are **40+ versioned
Flyway migrations**, applied automatically on deploy.
`docker-compose.override.yml` publishes local ports for development, and
`scripts/setup.ps1` wraps the same steps on Windows.

Deeper operational procedure lives in the runbooks:
[host maintenance](docs/runbooks/runbook_host_maintenance.md),
[storage maintenance](docs/runbooks/runbook_storage_maintenance.md), and
[solver recycling](docs/runbooks/runbook_solver_oom_and_recycle.md).
How work is proposed, gated, and archived is in
[docs/PLANS.md](docs/PLANS.md).

---

## Project structure

```
cartracker-scraper/
  scraper/                  # Results + detail page fetcher (FastAPI)
    processors/             # Fetch strategies, session bootstrap, HTML parsers
  processing/               # Artifact queue consumer; writes HOT state + events
  archiver/                 # Parquet export, silver compaction, sweeps
  ops/                      # Admin UI, deploy coordination, auth check, public page
    routers/                # admin, deploy, scrape, maintenance, info, users
  dbt_runner/               # dbt build trigger (FastAPI)
  dashboard/                # Streamlit analytics UI
  shared/                   # Compression, pack format, object-store helpers
  container_health/         # Three-state health exporter + expected service set
  dbt/models/               # 20+ dbt models across staging, intermediate, and marts
  airflow/dags/             # More than a dozen DAG definitions
  db/migrations/            # 40+ versioned Flyway migrations
  tests/
    integration/            # Real Postgres + MinIO suites (SQL, ops API, dbt, DAGs)
  docs/
    plans/  runbooks/  recaps/  evidence/  planning/
  docker-compose.yml
  docker-compose.override.yml
  .env.example
```

### Public and protected endpoints

| Route | Access |
|---|---|
| `/info` | Public — project page with live pipeline statistics |
| `/static_ops/*` | Public — page assets |
| `/oauth2/*` | Public — authentication flow |
| `/request-access` | Google-authenticated, no role required |
| `/` and `/dashboard*` | Any authorized role (`viewer` and above) |
| `/admin*` | `observer` and above, with the existing mutation rules |
| `/admin/users*`, `/admin/access-requests*` | `admin` |
| `/airflow*`, `/grafana*`, `/minio*`, `/pgadmin*` | `admin` |
| `/admin/snapshots/adaptive-refresh*` | Bearer-token authenticated, for CI and local scripts |

---

## Responsible use, affiliation, and license

**Not affiliated with Cars.com.** CarTracker is an independent portfolio
project. It is not affiliated with, endorsed by, sponsored by, or connected to
Cars.com, or to any dealer, manufacturer, or vehicle marketplace whose data or
name appears in it.

**Trademarks.** "Cars.com" and all vehicle make, model, and manufacturer names
are the trademarks of their respective owners. They are used here only to
describe and identify the data the project collects, and their appearance
implies no association or endorsement.

**Collected data.** Listing pages are collected at modest, rate-limited volume
for analysis and demonstration. The project retains no personal data about
consumers. The only personal data it stores about its own users is a salted
hash of the email address of anyone who signs in or requests access.

**Responsible use.** This repository is published so the engineering can be
read, not so the collection can be reproduced against a third party. Anyone
running this code is responsible for the terms of service, robots directives,
and laws that apply to whatever they point it at. Please do not use it to place
load on a site you do not have permission to collect from.

**License status.** This repository has **no `LICENSE` file, and therefore
grants no license.** Source being publicly readable is not permission to use,
copy, modify, or redistribute it; all rights are reserved by default under
copyright. Choosing a license is a separate decision that has not been made. If
you want to use any part of this work, please ask first.
