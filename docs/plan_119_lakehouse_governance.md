# Plan 119: Lakehouse Governance + Catalog Expansion

## Goal

Turn CarTracker's existing app-level governance into a more explicit modern
data-platform governance story.

This plan should add real controls and auditability around lakehouse data, not
generic governance theater.

---

## Context

CarTracker already has governance foundations:

- app users and roles
- admin-only routes/actions
- deploy intent gates
- protected scraper controls
- operational config ownership
- event/audit-style tables
- service health gates
- policy/version lineage in planned adaptive refresh

Those are currently mostly Postgres/application concepts. Plan 117 introduces a
Databricks-style direction where analytical history, experiment inputs, and
feature outputs move toward Delta tables and a catalog/governance layer.

This plan defines how to grow from "app RBAC plus Postgres tables" toward
"governed lakehouse assets."

---

## Scope

This plan owns:

- governance inventory
- catalog/table ownership model
- service identity model
- reader/writer separation
- table registration rules
- adaptive-policy promotion audit
- lineage conventions from source events to features to MLflow runs

This plan does not own:

- Delta table spike; see Plan 112
- dbt migration; see Plan 118
- production adaptive refresh; see Plan 113
- managed Databricks migration

---

## Governance Layers

### Layer 1: Operational Governance

Protects production actions.

Examples:

- who can create or edit search configs
- who can force a scrape
- who can approve deploy intent
- who can enable adaptive refresh
- who can promote a policy version

This layer remains app/Postgres-centered.

### Layer 2: Lakehouse Governance

Protects analytical datasets.

Examples:

- who can create/register tables
- which service can write Delta tables
- which service can read feature tables
- which tables are production-grade vs experimental
- how table ownership is documented
- how retention/cleanup decisions are authorized

This layer should be catalog-centered where the local stack supports it.

### Layer 3: Experiment and Policy Governance

Protects model/policy promotion.

Examples:

- which MLflow run approved a policy
- which Delta table versions trained/evaluated it
- who promoted it
- when it entered shadow mode
- when it entered enforced mode
- how rollback is performed

This layer bridges MLflow, Postgres, and the ops UI.

---

## Phase 0: Governance Inventory

Document what already exists before adding tools.

Deliverable:

- `docs/governance_inventory.md`

Inventory:

- roles and permissions
- admin-only routes
- protected operational actions
- deploy intent flow
- config mutation flows
- current audit/event tables
- service accounts or implicit service identities
- data-producing services
- data-consuming services
- sensitive tables/columns, if any

The output should identify gaps, not just list assets.

---

## Phase 1: Catalog and Namespace Model

Define how lakehouse assets are named and owned.

Proposed namespace shape:

```text
cartracker_raw
cartracker_silver
cartracker_features
cartracker_marts
cartracker_experiments
```

Suggested ownership:

| Namespace | Owner | Writers | Readers |
|-----------|-------|---------|---------|
| `cartracker_raw` | platform/admin | ingestion services | restricted analytics |
| `cartracker_silver` | platform/admin | Spark/archiver jobs | dbt, ML jobs |
| `cartracker_features` | analytics/ml | dbt/Spark feature jobs | MLflow/backtests |
| `cartracker_marts` | analytics | dbt | dashboard/API |
| `cartracker_experiments` | analytics/ml | backtest jobs | analysts/admins |

This may map to Unity Catalog OSS if the spike supports it. If not, document it
as a governance convention enforced by code/config until catalog enforcement is
available.

---

## Phase 2: Service Identity Model

Separate humans from services.

Proposed service identities:

| Identity | Purpose |
|----------|---------|
| `svc_archiver` | writes historical observation/event tables |
| `svc_spark_writer` | creates/appends Delta tables |
| `svc_dbt` | builds models and marts |
| `svc_mlflow` | reads experiment inputs and writes artifacts |
| `svc_dashboard` | reads approved mart/serving tables |
| `svc_ops` | reads production priority table and operational state |

Rules:

- writers should not have broad delete permissions by default
- dashboard should not write analytical tables
- ops claim path should not require lakehouse credentials
- experimental jobs should not mutate production table prefixes
- cleanup/vacuum commands should require explicit operator intent

---

## Phase 3: Table Registration Rules

Define what makes a table production-grade.

Required metadata:

- owner
- writer service
- source system
- grain
- primary key or natural key expectation
- refresh cadence
- retention expectation
- quality checks
- downstream consumers
- whether table is production, experimental, or deprecated

Required checks before promotion:

- row-count validation
- duplicate key validation
- schema validation
- freshness validation
- lineage link to source tables
- rollback/cleanup plan

Deliverable:

- `docs/table_registration_standard.md`

---

## Phase 4: Adaptive Policy Promotion Governance

Add a controlled promotion path for Plan 113.

Promotion record should include:

| Field | Description |
|-------|-------------|
| `policy_version` | Human-readable version |
| `mlflow_run_id` | Approved Plan 112 run |
| `input_table` | Primary Delta input table |
| `input_table_version` | Version used for evaluation |
| `quality_gate_summary` | Pass/fail metrics |
| `promoted_by` | User/service that promoted it |
| `promoted_at` | Timestamp |
| `mode` | `shadow`, `enforced`, or `disabled` |
| `rollback_policy_version` | Previous safe version |

This can initially live in Postgres and surface in the ops UI.

Do not require Unity Catalog OSS for this phase. The promotion workflow is
valuable even before catalog enforcement is mature.

---

## Phase 5: Access-Control Enforcement

Implement the first concrete lakehouse governance control.

Good first candidates:

- only `svc_spark_writer` can create/append Delta source tables
- only `svc_dbt` can write mart/feature tables
- dashboard uses read-only credentials
- experimental namespaces cannot write to production prefixes
- table promotion requires admin role

Pick one or two controls that are easy to verify end to end.

---

## Phase 6: Lineage and Documentation

Create a lightweight lineage story that connects:

```text
Postgres staging/event tables
    -> Delta source tables
    -> dbt/Spark feature tables
    -> MLflow run
    -> approved policy version
    -> ops.detail_refresh_priority
```

Minimum implementation:

- table metadata docs
- MLflow tags for source table versions
- promotion record with input versions
- dashboard/admin display of active policy lineage

Avoid building a full lineage platform unless it directly helps the project.

---

## Testing

- governance inventory matches actual routes/tables/services.
- table registration records require owner, grain, writer, and quality checks.
- service identities cannot perform disallowed actions in tests or local spike.
- dashboard read credentials are read-only where practical.
- policy promotion cannot proceed without MLflow run ID and Delta table version.
- rollback policy version is recorded.
- ops UI/API displays active policy lineage.

---

## Files Changed

| File | Change |
|------|--------|
| `docs/governance_inventory.md` | New current-state inventory |
| `docs/table_registration_standard.md` | New table promotion/registration standard |
| `db/migrations/` | Optional policy promotion/audit tables |
| `ops/` | Policy promotion UI/API and lineage display |
| `shared/` | Optional service identity/config helpers |
| `tests/ops/` | Governance and policy promotion tests |
| `tests/integration/` | Service identity / access-control checks |

---

## Out Of Scope

- Managed Databricks Unity Catalog migration.
- Enterprise-grade data catalog implementation.
- Full column-level security unless a real sensitive-data use case appears.
- Replacing existing app auth/RBAC.
- Production adaptive refresh enforcement. See Plan 113.
- dbt migration. See Plan 118.
