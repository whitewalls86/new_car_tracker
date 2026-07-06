# Plan 113: Production Adaptive Refresh Integration

## Goal

Wire the approved refresh policy from Plans 111-112 into production ops so the
scraper only fetches detail pages that are due.

Production should deploy a concrete, pinned policy/config output. It must not
call MLflow, Spark, Delta, Unity Catalog, or a live model server at claim time.

---

## Context

Plan 110 and Plan 111 are complete:

- Plan 110 normalized storage and tightened the object-store contract.
- Plan 111 built the adaptive-refresh feature layer.

Plan 112 now audits those feature outputs, runs Delta/MLflow-backed replay, and
selects a policy candidate with evidence.

This plan is only the production integration layer. It does not train models or
search policy thresholds.

---

## Ownership Boundary

```text
Plan 112 approves policy/config
        |
        v
materialized priority table in Postgres
        |
        v
ops claim query filters due listings
        |
        v
scraper fetches claimed listings
```

Ops owns claim eligibility. Scraper continues to fetch the listings it is
given. Analytical/lakehouse systems remain upstream of production claims.

---

## Production Materialization

Materialize the approved priority output into Postgres:

```text
ops.detail_refresh_priority
```

Suggested columns:

| Column | Description |
|--------|-------------|
| `vin17` | Primary policy entity |
| `latest_listing_id` | Current/latest listing ID observed for the VIN |
| `next_detail_fetch_after` | Earliest recommended next detail fetch |
| `refresh_tier` | `hot`, `daily`, `cool`, or `cold` |
| `volatility_score` | Interpretable 0-100 score or model score |
| `reason` | Dominant reason for the decision |
| `policy_version` | Human-readable policy/config version |
| `mlflow_run_id` | Approved Plan 112 run ID |
| `input_table` | Primary Delta input table |
| `input_table_version` | Primary Delta version used for approval |
| `computed_at` | When this priority row was generated |

Postgres is the production serving layer for this table. Delta/MLflow metadata
is lineage only.

---

## Ops Claim Query Changes

Add refresh eligibility to detail claim selection.

Conceptual shape:

```sql
LEFT JOIN ops.detail_refresh_priority rp
  ON rp.vin17 = c.vin17
WHERE (
    rp.next_detail_fetch_after IS NULL
    OR rp.next_detail_fetch_after <= now()
    OR escape_hatch_applies
)
```

The real query must preserve:

- existing priority ordering
- existing blocked-cooldown behavior
- forced/manual claim behavior
- never-detail-scraped enrichment behavior
- deploy intent and service-drain protections

If the current claim table does not carry `vin17` directly, the implementation
must join through the existing listing/VIN mapping without changing scraper
payload shape.

---

## Escape Hatches

These conditions bypass throttling regardless of score:

- VIN/listing discovered in the last 48 hours.
- Listing seen in SRP in the last 24 hours.
- Listing manually forced through ops.
- Listing has no prior successful detail fetch.
- Listing has missing dealer/customer enrichment.
- Policy row is missing or stale beyond an agreed freshness window.
- Feature flag disables adaptive refresh.

Escape hatches should be counted separately from normal due listings.

---

## Shadow Mode

Deploy shadow mode before enforcement.

In shadow mode:

- the claim query still returns the unthrottled result set
- each candidate is classified as `would_fetch`, `would_throttle`, or
  `would_escape`
- counters are emitted per batch
- sampled decisions are stored for inspection

Do not enable enforcement until shadow counters are close enough to Plan 112
expectations for several days.

---

## Safety Controls

- Feature flag or env var to disable refresh throttling.
- Conservative first intervals from the approved Plan 112 run.
- Policy version pinning.
- Config refuses to enable enforcement without:
  - `policy_version`
  - `mlflow_run_id`
  - `input_table`
  - `input_table_version`
  - approval timestamp or promotion record
- Manual force path for a VIN/listing or batch.
- Rollback path to the previous policy version or fully unthrottled claims.

---

## Observability

Log the following counters per ops claim batch:

| Metric | Description |
|--------|-------------|
| `claims_considered` | Listings evaluated by the claim query |
| `claims_due` | Listings due for fetch under policy |
| `claims_throttled` | Listings skipped by refresh tier |
| `claims_escaped` | Listings bypassing throttling |
| `claims_missing_policy` | Listings with no priority row |
| `claims_policy_stale` | Listings with stale priority metadata |
| `tier_distribution` | Count per `hot/daily/cool/cold` |

Dashboard after rollout:

- detail fetch count by day
- processing count by day
- 403/block rate
- tier distribution
- estimated skipped fetches
- freshness/detection-delay proxy
- active policy version and MLflow run ID
- shadow-vs-enforced divergence during rollout

---

## Rollout

1. Select an approved Plan 112 MLflow run that passed quality gates.
2. Export/pin the policy config and Delta table-version metadata.
3. Materialize `ops.detail_refresh_priority` without changing claim behavior.
4. Add shadow counters and sampled decisions.
5. Compare shadow counters to Plan 112 expectations for several days.
6. Enable enforcement with conservative thresholds.
7. Watch scrape volume, processing volume, block/403 rate, and freshness.
8. Tighten intervals only after observed behavior matches the backtest and a
   new Plan 112 run is reviewed.

---

## Testing

### Integration Tests

- Claim query excludes listings whose `next_detail_fetch_after` is in the
  future when enforcement is enabled.
- Claim query includes listings whose `next_detail_fetch_after` is due.
- Escape hatches bypass throttling for newly discovered, SRP-recent, forced,
  missing-policy, stale-policy, and never-detail-scraped listings.
- Feature flag disables throttling.
- Shadow mode records would-fetch/would-throttle without changing selected
  claims.
- Priority rows include policy version, MLflow run ID, and Delta table version.
- Production config refuses to enable throttling without an approved policy
  version.

### Regression Tests

- Existing blocked-cooldown behavior is unchanged.
- Existing claim ordering is unchanged except for refresh eligibility.
- Scraper request payload shape is unchanged.
- Service-drain/deploy-intent behavior is unchanged.

---

## Files Changed

| File | Change |
|------|--------|
| `ops/sql/claim_detail_listing.sql` | Add refresh eligibility filter |
| `ops/sql/detail_refresh_priority.sql` | Optional read/materialization helper |
| `ops/config` or env | Policy version and feature flag wiring |
| `ops/metrics/` | Counters/dashboard wiring |
| `tests/ops/test_claim_detail_throttling.py` | Claim query integration tests |
| `tests/ops/test_adaptive_refresh_shadow_mode.py` | Shadow-mode behavior tests |

---

## Out of Scope

- XGBoost training.
- MLflow model serving or registry promotion.
- Delta, Spark, MLflow, or Unity Catalog reads at claim time.
- Online feature serving.
- Full dbt migration. See Plan 118.
- Governance/catalog expansion. See Plan 119.
- Sectioned HTML storage. See Plan 114.
