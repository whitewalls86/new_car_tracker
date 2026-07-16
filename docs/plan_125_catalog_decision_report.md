# Plan 125 Catalog Decision Report: Lakekeeper, Unity Catalog OSS, Polaris, and Gravitino

## Status

**Decision input for Plan 125 Gate 0.5.** This report answers one question
before Plan 125 implementation starts:

> Do we keep Lakekeeper as the Iceberg catalog through the DuckDB -> Iceberg
> migration, or switch now to a broader governed catalog?

It does not add runtime code. It compares the setup cost, switching cost, and
catalog-neutral guardrails needed to keep the choice reversible.

## TL;DR Recommendation

**Recommended: keep Lakekeeper through Plan 125, and adopt the catalog-neutral
guardrails below.** Do not stand up Unity Catalog OSS, Apache Polaris, or Apache
Gravitino as part of Plan 125 unless a small catalog spike first proves a better
option on the exact operations Plan 125 needs.

Revisit the catalog choice when Plan 119 (governance) or Plan 113 (production
policy promotion) needs real RBAC, credential vending, lineage, or a shared
metastore. Until then, Plan 125 should spend its complexity budget on the
DuckDB -> Iceberg migration itself.

Why this call:

- Plan 112 already proved Lakekeeper + Spark + MinIO locally, in CI, and on the
  ARM64 VM.
- The data path uses the standard Iceberg REST catalog protocol and static-key
  `S3FileIO`; most code is not Lakekeeper-specific.
- The Lakekeeper-specific surface is small: warehouse provisioning,
  storage-profile payloads, and compose service definitions.
- Plan 125's hard part is dbt/Spark/Iceberg parity, incremental semantics, and
  reader cutover. Swapping catalogs at the same time would add a second moving
  target.

## Current Uncertainty

1. **Unity Catalog OSS native-Iceberg write support is the deciding unknown.**
   The self-hostable OSS server is Delta-first and exposes Iceberg through
   UniForm/read pathways. The native Iceberg REST create/commit/write path that
   Plan 125 needs is not proven here, and public messaging is noisy because
   Databricks-managed Unity Catalog has newer Iceberg capabilities that are not
   the same thing as the self-hostable container.

2. **ARM64 packaging is not the blocker.** Multi-arch images exist for UC OSS
   community containers. The real risk is architecture-independent: whether the
   catalog accepts the native Iceberg write operations we need.

3. **The governed-catalog choice is broader than Unity Catalog OSS.** If Plan
   119 or Plan 113 pulls RBAC, credential vending, or lineage into near-term
   scope, the spike should compare Lakekeeper, Apache Polaris, Apache Gravitino,
   and Unity Catalog OSS rather than treating UC OSS as the only alternative.

## Off-the-Shelf OSS Catalog Options

| Option | Iceberg write path | RBAC/governance path | Lineage path | Fit for Plan 125 |
|---|---|---|---|---|
| **Lakekeeper** | **Proven in this repo** through Spark/Iceberg REST | Strong-looking path through auth/RBAC integrations, but currently deployed permissively | External lineage needed | Default. Keep unless it blocks migration. |
| **Apache Polaris** | Iceberg-native catalog; needs repo spike | Strong-looking Iceberg catalog governance path | External lineage likely needed | Best alternate if we want an Iceberg-first governed catalog. |
| **Apache Gravitino** | Iceberg REST support; needs repo spike | Broader metadata/governance platform | Has visible lineage surface area | Worth research if lineage/governance becomes first-order, but heavier than Plan 125 needs. |
| **Unity Catalog OSS** | Needs spike; native-Iceberg write in self-hosted OSS is the concern | Databricks-aligned governance model | Strong ecosystem story | Strategic candidate, but risky as Plan 125 execution catalog until write support is proven. |

Lineage should not drive the Plan 125 catalog choice by itself. Treat lineage as
a separate plane unless Plan 119 explicitly pulls it forward: dbt artifacts,
Spark/OpenLineage, Marquez, DataHub, or OpenMetadata can be layered later without
blocking the Iceberg migration.

## Service Identity Model: Design Now, Enforce Later

Plan 125 does not need full catalog RBAC enforcement, but it should define the
service identities now so namespace layout, env var names, and tests do not bake
in a single all-powerful client.

| Identity | Planned role | Plan 125 enforcement |
|---|---|---|
| `lakehouse_writer` | Create/replace/write feature and mart Iceberg tables | Not enforced yet; used in config names and docs. |
| `dashboard_reader` | Read dashboard-serving gold/mart outputs only | Not enforced yet; reader abstraction should assume read-only. |
| `ops_metrics_reader` | Read operational health marts/extracts for `/metrics` and `/info` | Not enforced yet; keep metric names stable. |
| `mlflow_provenance_writer` | Write MLflow metadata/artifacts; read snapshot/table metadata | Enforced by MLflow service boundary, not catalog RBAC. |
| `ci_local_lakehouse` | Create/read/write isolated local or CI namespaces | Isolated by compose project, bucket/prefix, and teardown rules. |

Plan 119 can later map these roles to real catalog principals/grants.

## How Portable Is the Current Lakekeeper Work?

| Surface | Coupling | Portability |
|---|---|---|
| `spark_conf_for_rest_catalog()` | Iceberg `SparkCatalog` with `type=rest`, `uri`, and `S3FileIO` | Portable. Any Iceberg REST catalog should use the same broad shape. |
| Spark write/read/append/time-travel scripts | Standard Iceberg SQL over REST catalog | Portable. |
| MinIO access | Static access keys through `S3FileIO` | Portable; deliberately avoids credential vending. |
| MLflow provenance | Opaque strings for catalog/table/snapshot | Neutral. |
| `warehouse_storage_payload()` | Lakekeeper management API schema | Lakekeeper-specific. |
| `register_lakehouse_warehouse.py` | Lakekeeper bootstrap/warehouse API | Lakekeeper-specific. |
| `docker-compose.lakehouse*.yml` | Lakekeeper service image and Postgres metadata store | Swappable service definitions. |

Net: the real lock-in is a small provisioning slice plus compose wiring, as long
as the guardrails below are followed.

## What Plan 125 Actually Needs

| Need | Plan 125 requirement | Lakekeeper today | Governed-catalog alternatives |
|---|---|---|---|
| Namespaces | Isolated feature/mart namespaces | Proven | Must be proven per catalog |
| Native Iceberg writes | Spark/dbt create, append, replace, time-travel | Proven | Must be proven before switching |
| Service identities | Design now, enforce later | Deferred | Available in some catalogs, but still extra setup |
| RBAC/read-write separation | Not required for enforcement in Plan 125 | Deferred | Plan 119 scope |
| Lineage/provenance | MLflow strings plus future external lineage | Works | Do not block Plan 125 on catalog-native lineage |
| Local + CI + VM | Same flow on Windows, CI, ARM64 VM | Proven | Must be re-proven |

## Switching-Later Pain Map

Assumes Lakekeeper remains through Plan 125, then a future plan switches to a
different catalog.

| Component | Switch pain with guardrails | Switch pain without guardrails | Driver |
|---|---|---|---|
| Spark/dbt catalog config | Low: swap URI/auth in one function | Medium: catalog URIs scattered through scripts | R1, R2 |
| Warehouse/namespace provisioning | Medium: rewrite one provisioning module | Medium/high if spread through jobs | R6 |
| Iceberg table data | Low: re-materialize from source | High if treated as unrebuildable truth | R5 |
| MinIO storage credentials | Low: static `S3FileIO` remains | High if credential vending is adopted early | R4 |
| Dashboard readers | Low: serving layer repointed | High if pages query catalog directly | R3 |
| Ops metrics and `/info` | Low: same serving layer | High if metrics code embeds catalog assumptions | R3 |
| MLflow provenance | None: opaque strings stay historical | None | R7 |
| CI/local/VM harness | Medium: new compose overrides and smoke loop | High if Lakekeeper assumptions leak everywhere | R6 |

## Catalog Spike Checklist

Only run this if the team wants to override the default Lakekeeper-through-Plan
125 recommendation, or if Plan 119/113 pulls governance forward.

Do the candidates in this order: **Polaris**, **Gravitino**, **Unity Catalog
OSS**. Keep Lakekeeper as the control.

| # | Step | Done-when | Blocking? |
|---|---|---|---|
| 1 | Prove native Iceberg REST create + append + replace + time-travel on MinIO | Matches the existing Lakekeeper roundtrip behavior | Yes |
| 2 | Prove Spark can run the same A2/A3 scripts unchanged except catalog config | Same scripts pass against candidate catalog | Yes |
| 3 | Prove local Docker + CI + ARM64 VM startup | Compose profile starts, health check passes, no unsafe volume coupling | Yes |
| 4 | Prove namespace/warehouse provisioning is idempotent | Repeated registration is safe and scriptable | Yes |
| 5 | Sketch service identity mapping | Writer/reader/metrics/provenance roles have a future principal/grant story | No |
| 6 | Verify lineage story | Native lineage or external OpenLineage/DataHub/OpenMetadata path is documented | No |
| 7 | Confirm rollback | Candidate catalog can be torn down without touching production MinIO data | Yes |

Stop at the first hard failure. A catalog that cannot pass step 1 is not a Plan
125 execution catalog.

## Catalog-Neutral Plan 125 Guardrails

These are implementation rules for Plan 125 regardless of catalog choice.

- **R1: One catalog-config chokepoint.** All Spark/dbt catalog wiring flows
  through one function. A switch edits that function, not every script.
- **R2: Neutral env names in consumers.** Consumers should read neutral names
  such as `ICEBERG_CATALOG_URI`, with temporary fallback to existing
  `LAKEKEEPER_CATALOG_URI` during migration. Keep Lakekeeper-specific names only
  in provisioning code.
- **R3: Consumers read a serving layer, never the catalog directly.** Dashboard,
  `/info`, and ops metrics use extracts, a cache, or one reader module. Do not
  embed catalog clients/URIs in page or metric code.
- **R4: Keep static-key `S3FileIO`; defer credential vending.** Credential
  vending is catalog-specific and belongs to Plan 119 unless a hard requirement
  appears earlier.
- **R5: Treat Iceberg tables as rebuildable until cutover.** Normalized Parquet
  and dbt sources remain the recovery point while dual-run/parity work is active.
- **R6: Provisioning is isolated and idempotent.** Warehouse/namespace creation
  stays in one module behind a stable interface, and compose teardown can only
  touch the catalog project's own resources.
- **R7: Provenance stays engine/catalog-agnostic.** MLflow records catalog,
  table, snapshot, and source metadata as opaque strings. It should not import a
  catalog client.

## Gate 0.5 Implementation Checklist

Before Gate A migration code:

- [x] Add or confirm neutral `ICEBERG_CATALOG_*` env names for consumer-facing
  Spark/dbt config, with backward-compatible fallback to the existing
  Lakekeeper env names.
- [x] Keep Lakekeeper-specific env names and payloads inside provisioning scripts.
- [x] Add tests that prove the neutral config path is used by Spark/dbt scripts.
- [ ] Record the service identity model above in the implementation docs.
  Still open: the identities are defined in this report but are not yet reflected
  in the migration plan's implementation sections
  ([docs/plan_125_duckdb_to_iceberg_migration.md](plan_125_duckdb_to_iceberg_migration.md)),
  which is where Gate D's reader abstraction will need them. Gate D scope, not
  Gate 0.5's.
- [x] Defer any Polaris/Gravitino/UC OSS spike unless the team explicitly chooses
  to challenge the default Lakekeeper path. **Deferred; not run.**

The decision is unchanged: **Lakekeeper remains the default through Plan 125**,
and the alternate-catalog spike above is deferred until Plan 119/113 presents a
concrete governance requirement.

Implemented shape (see the Plan 125 plan's "Catalog config contract" section for
the full table): consumers read `ICEBERG_CATALOG_URI` through
`shared/iceberg_catalog.py::catalog_uri()` and fall back to
`LAKEKEEPER_CATALOG_URI`; provisioning
(`scripts/register_lakehouse_warehouse.py`, `warehouse_storage_payload()`) keeps
the Lakekeeper-specific names, management-API layout, and storage-profile schema.
R1, R2, R4, R6, and R7 hold today. R3 (consumers read a serving layer) and R5
(Iceberg tables stay rebuildable) are Gate D/E obligations, not yet exercised.

## Recommendation Summary

Keep Lakekeeper for Plan 125. Spend the effort on the DuckDB -> Iceberg
migration and on the catalog-neutral guardrails, not on a catalog swap. If
governance becomes urgent, run the catalog spike against Polaris, Gravitino, and
Unity Catalog OSS, with Lakekeeper as the control. The uncertainty that could
change the decision is native Iceberg write maturity in the candidate
self-hosted catalog, verified on the ARM64 VM as part of the spike.
