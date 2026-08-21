# Plan 117: Open Lakehouse + Adaptive Refresh Roadmap

## Goal

Reset the storage and adaptive-refresh roadmap around a new north star:

> Build a local, low-cost open lakehouse that is portable across Spark,
> Databricks-style platforms, Snowflake/Iceberg integrations, and open-source
> query engines.

The prior roadmap assumed a DuckDB-centered analytics layer with a future
Iceberg or DuckLake substrate decision. The revised direction is:

1. Keep Postgres as the hot operational system.
2. Move analytical history from loose Parquet + DuckDB toward Apache Iceberg
   tables.
3. Introduce Unity Catalog OSS or a compatible governance/catalog layer.
4. Move dbt execution away from DuckDB and toward Spark-compatible semantics.
5. Use MLflow, Iceberg table snapshots, and catalog metadata to make adaptive
   refresh experiments reproducible.
6. Add governance work deliberately, not as an afterthought.

This document is the umbrella roadmap. Detailed implementation belongs in
separate stage plans.

---

## Where We Are Now

CarTracker currently has a strong single-VM data platform:

- Postgres owns hot operational state, auth/RBAC, claim eligibility, deploy
  intent, staging buffers, and current queue state.
- MinIO stores raw bronze HTML and silver/ops Parquet history.
- dbt currently runs through DuckDB for analytics models.
- DuckDB reads MinIO Parquet directly through `httpfs` and can scan selected
  Postgres sources.
- MLflow is not yet part of the system.
- Lakehouse table management is not yet part of the system.

This architecture has worked well for the current project size. Its weakness is
not correctness; it is professional portability. DuckDB is excellent local
infrastructure, but it is not the center of gravity for Spark-based
ML/data-platform work or open lakehouse table management.

The next roadmap should therefore optimize for:

- Spark/PySpark experience.
- Iceberg table management.
- Unity Catalog / REST catalog concepts.
- MLflow experiment tracking.
- dbt against a Spark-compatible backend.
- governance and access-control patterns that resemble modern lakehouse
  platforms.

---

## Revised Architecture Target

```text
                         +----------------------+
                         |  Ops / Admin UI      |
                         |  RBAC / deploy gates |
                         +----------+-----------+
                                    |
                                    v
Postgres HOT tables  <---->  staging/event buffers
   |                                |
   |                                v
   |                       Spark/PySpark writers
   |                                |
   v                                v
production claim path       Iceberg tables on MinIO
                                    |
                                    v
                         Unity Catalog OSS / REST catalog layer
                                    |
                +-------------------+-------------------+
                |                   |                   |
                v                   v                   v
          dbt on Spark        MLflow backtests     dashboard/API reads
```

Principles:

- The hot production scraper path must not call MLflow, Unity Catalog, Spark,
  Iceberg, or a live model server.
- Postgres remains the operational source of truth for mutable current state.
- Iceberg tables become the analytical history and reproducible experiment
  layer.
- Unity Catalog OSS or a compatible REST catalog, if it passes the spike,
  becomes the governance/catalog learning surface.
- DuckDB is treated as a transition tool, not the future analytics endpoint.

---

## Why Iceberg + Unity Catalog OSS-Compatible Governance

The project still wants Spark, MLflow, and Databricks-transferable systems
knowledge. The table-format choice now prioritizes portability.

Iceberg is the stronger vendor-neutral open lakehouse story. It is increasingly
supported across Snowflake, Databricks access paths, Trino, Spark, and
open-source catalogs. If a role requires managed Databricks as a hard stop, a
local Delta clone would not fully satisfy that requirement anyway. The more
durable project value is understanding table formats, catalogs, snapshots,
schema evolution, and reproducible ML workflows.

The revised tradeoff is intentional:

| Path | Best for |
|------|----------|
| Iceberg + Unity Catalog OSS-compatible governance | Portable open lakehouse with Databricks-transferable governance concepts |
| Iceberg + Polaris | Open lakehouse / multi-engine table-format engineering |
| Iceberg + Lakekeeper | Practical self-hosted Iceberg operations |
| Delta + Unity Catalog OSS | Databricks-native semantics if managed Databricks becomes the explicit target |
| DuckLake | Minimal-friction DuckDB-native local lakehouse |

For this project, choose Iceberg first and use Unity Catalog OSS or a compatible
catalog/governance layer to bridge the Databricks-style concepts.

---

## Roadmap Stages

### Stage 0: Documented Current-State Baseline

Capture the current shipped system before changing the substrate.

Deliverables:

- Current data-flow diagram.
- Current dbt/DuckDB source map.
- Current MinIO Parquet prefix inventory.
- Current governance/RBAC inventory:
  - users and roles
  - deploy gates
  - protected routes/actions
  - audit/event tables
  - operational ownership boundaries
- Current dashboard/query dependency map.

This stage prevents the migration from turning into folklore.

### Stage 1: Iceberg + Catalog Research Spike

Prove or reject the new substrate on isolated data before touching production
analytics.

Required checks:

1. Stand up a local Spark/PySpark environment.
2. Write a small Iceberg table to MinIO or an equivalent local object-store
   path.
3. Read the table back through Spark.
4. Create at least two Iceberg snapshots.
5. Time travel to an older snapshot.
6. Capture table snapshot metadata programmatically.
7. Log a smoke-test MLflow run containing the table snapshot and input row
   count.
8. Stand up Unity Catalog OSS or a compatible REST catalog/governance layer.
9. Register or expose the test table through the catalog layer if feasible.
10. Test permissions/governance concepts at the smallest useful scale.
11. Document where Unity Catalog OSS differs from managed Databricks Unity
    Catalog.

Decision output:

- `docs/plan_112_refresh_policy_backtesting.md`
- selected table/catalog path
- rejected alternatives
- exact spike commands
- cleanup proof
- known gaps vs managed Databricks and Snowflake-managed Iceberg

Fallback rule:

If Unity Catalog OSS is too immature or too awkward locally, keep Iceberg and
use a simpler catalog while deferring deeper catalog integration to Plan 119. If
Iceberg itself creates disproportionate friction, revisit the table-format
decision before proceeding.

### Stage 2: Iceberg Table Foundation

Create the first real analytical Iceberg tables from normalized history.

Candidate tables:

- `silver_observations`
- `price_observation_events`
- `vin_to_listing_events`
- `blocked_cooldown_events`
- `detail_scrape_claim_events`
- `artifacts_queue_events`

This stage should use isolated prefixes first. Do not replace production
Parquet readers until row counts, schemas, and time-travel behavior are proven.

Deliverables:

- Iceberg table naming convention.
- Physical storage convention.
- table snapshot capture helper.
- row-count and schema validation report.
- operator commands for create, read, time travel, vacuum/retention inspection,
  and cleanup.

### Stage 3: dbt Migration Away From DuckDB

Move dbt from DuckDB toward Spark-compatible execution.

The first target does not need to be managed Databricks. The point is to stop
making DuckDB the long-term analytical contract.

Questions for the detailed plan:

- Should local dbt use `dbt-spark`, `dbt-databricks`, or another adapter for the
  local spike?
- Which current DuckDB SQL patterns need to become Spark-compatible?
- Which models should remain pure dbt SQL versus move into PySpark feature jobs?
- How should CI test dbt models without requiring managed Databricks?
- Which dashboard reads must be redirected away from DuckDB materializations?

Deliverables:

- adapter decision
- model compatibility audit
- source definition plan
- CI strategy
- dashboard transition plan

### Stage 4: MLflow Foundation

Stand up MLflow as the experiment tracking layer.

Initial deployment should be simple but real:

- backend store: Postgres if practical
- artifact store: MinIO or a mounted Docker volume
- access: internal first

Every serious backtest should log:

- code SHA
- Iceberg table name
- Iceberg snapshot ID
- input window
- row counts
- policy params
- metrics
- output artifacts

MLflow should track experiments. It should not be called in the hot production
claim path.

### Stage 5: Adaptive Refresh Backtesting

Use Iceberg table snapshots and MLflow runs to make policy experiments
reproducible.

Backtesting remains VIN-grained. The goal is to decide which detail pages need
refreshing and which can be safely delayed.

Outputs:

- baseline "fetch everything" run
- rule-based policy grid
- XGBoost experiment
- quality gates for skip rate vs detection delay
- sampled decision artifacts
- approved policy config candidate for production

The production output should be a pinned policy/config artifact, not a live
model dependency.

### Stage 6: Production Adaptive Refresh Integration

Deploy the approved refresh policy conservatively.

Production claim logic should read a materialized priority table in Postgres,
not query Iceberg, MLflow, Spark, or Unity Catalog at claim time.

Required controls:

- feature flag
- shadow mode
- policy version pinning
- escape hatches for new/SRP-recent/forced/never-scraped listings
- counters for fetched, throttled, escaped, and due listings
- rollback to unthrottled claim behavior

### Stage 7: Governance Expansion

Turn the existing app-level governance into a more explicit data-platform
governance story.

Current governance already exists in simple form:

- app users and roles
- admin-only actions
- deploy intent gates
- operational config ownership
- event/audit tables
- protected scrape controls

Future governance work should map these concepts into lakehouse vocabulary:

- catalog/schema/table ownership
- role-based access to analytical datasets
- service principals or service identities
- least-privilege writer/reader separation
- table registration rules
- sensitive column classification where relevant
- audit trail for dataset creation and policy promotion
- lineage from source events to feature tables to MLflow runs

This work should be concrete. Do not add generic "governance" pages unless they
protect or explain a real workflow.

### Stage 8: Raw HTML Retention Research

Plan 114 remains parallel.

The adaptive refresh work reduces future redundant fetches. The sectioned HTML
artifact audit separately tests whether historical raw pages can be stored more
efficiently without losing parser replayability.

Do not introduce blunt raw HTML expiry until this research produces evidence.

---

## Updated Plan Map

| Plan | Revised role |
|------|--------------|
| Plan 110 | Storage hygiene and normalized Parquet baseline |
| Plan 111 | Feature foundation for adaptive refresh; write logic to be portable to Spark/Iceberg |
| Plan 112 | Iceberg + catalog + MLflow research spike and backtest foundation |
| Plan 113 | Production adaptive refresh integration using pinned policy outputs |
| Plan 114 | Sectioned raw HTML retention audit |
| Plan 118 | dbt migration from DuckDB to Spark-compatible execution |
| Plan 119 | Governance/catalog expansion around Unity Catalog concepts |
| Plan 123 | dbt incrementalization, cadence separation, and analytics resource governance after Plan 120 Gate D |

---

## Non-Goals Across The Arc

- No production adaptive refresh before backtesting.
- No ML serving in production.
- No MLflow, Spark, Unity Catalog, or Iceberg calls in the hot ops claim path.
- No destructive cleanup before row counts, schema checks, and query checks pass.
- No automatic raw HTML deletion until Plan 114 produces evidence.
- No forced migration to managed Databricks as part of the local roadmap.
- No governance theater: every governance feature should protect, document, or
  audit a real workflow.

---

## Success Criteria

### Platform Direction

- The project can credibly be described as a portable open lakehouse.
- DuckDB is no longer the long-term analytics endpoint.
- Spark/PySpark is used for at least table writes, feature preparation, or ML
  training.
- Iceberg table snapshots are captured for reproducible experiments.
- Unity Catalog OSS or a documented fallback catalog path is evaluated honestly.

### Experiment Reproducibility

- MLflow records policy params, dataset table snapshots, metrics, and artifacts.
- A backtest run can be reproduced from recorded metadata.
- XGBoost training is tied to explicit input table snapshots.

### Adaptive Refresh

- Candidate policy passes agreed quality gates.
- Production policy is pinned to an approved MLflow run/config.
- Shadow mode metrics match expected backtest behavior closely enough to enable.
- Rollout can be disabled without code changes.

### Governance

- Existing app-level RBAC/audit concepts are inventoried.
- Lakehouse table ownership and access patterns are documented.
- At least one real governance workflow is implemented, such as controlled table
  registration, writer/reader role separation, or policy promotion audit.

---

## Reading Order For Implementers

For any implementation in this arc, read:

1. `docs/ARCHITECTURE.md`
2. this roadmap
3. the specific stage plan being implemented
4. referenced completed plans when touching storage, dbt, ops claims, or
   dashboard dependencies

Use graphify before implementation to orient around the relevant service and
data-flow boundaries.
