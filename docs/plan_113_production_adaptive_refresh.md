# Plan 113: Production Adaptive Refresh Integration

## Goal

Wire the approved refresh policy from Plans 111-112 into production ops so the
scraper only fetches listings that are due.

Do not introduce XGBoost or MLflow model serving in this plan. MLflow is used
upstream to identify and audit the selected policy run; production should deploy
a concrete rule/config snapshot, not call MLflow at claim time.

---

## Context

Plan 110 normalizes the storage layer and prepares the Parquet lake for Iceberg.
Plan 111 builds refresh features. Plan 112 uses Iceberg snapshots and MLflow to
backtest candidate thresholds and intervals reproducibly.

This plan deploys the chosen rule-based policy behind guardrails. The production
policy must reference the approved Plan 112 evidence:

- MLflow run ID
- policy config artifact
- Iceberg snapshot IDs or dataset manifest used in backtesting
- quality-gate metrics

---

## Ownership Boundary

```text
dbt computes refresh priority -> ops selects eligible claims -> scraper fetches
```

dbt owns scoring. Ops owns claim eligibility. Scraper continues to fetch the
listings it is given.

---

## Materialization

Materialize `mart_detail_refresh_priority` as a Postgres table in the `ops`
schema on each dbt run so the ops claim query can join against it without
DuckDB overhead at claim time.

Suggested table name:

```text
ops.detail_refresh_priority
```

The materialized table should include policy lineage columns:

| Column | Description |
|--------|-------------|
| `policy_version` | Human-readable policy/config version |
| `mlflow_run_id` | Approved backtest run ID |
| `input_snapshot_id` | Primary Iceberg snapshot/manifest reference |
| `computed_at` | When this priority row was generated |

Ops should not query Iceberg or MLflow at claim time.

---

## Ops Claim Query Changes

Add `next_detail_fetch_after <= now()` to detail claim eligibility:

```sql
LEFT JOIN ops.detail_refresh_priority rp
  ON rp.listing_id = c.listing_id
WHERE (
    rp.next_detail_fetch_after IS NULL
    OR rp.next_detail_fetch_after <= now()
)
```

The real query should preserve existing priority ordering and blocked-cooldown
behavior.

---

## Escape Hatches

These conditions bypass throttling regardless of score:

- Listing discovered in the last 48 hours.
- Listing seen in SRP in the last 24 hours.
- Listing manually forced via ops flag.
- Listing has no prior successful detail fetch.
- Listing has missing dealer/customer enrichment.

---

## Safety Controls

- Feature flag or env var to disable refresh throttling.
- Conservative first intervals from Plan 112.
- Shadow mode before enforcing eligibility.
- Per-run counters for throttled vs fetched claims.
- Manual force path for a listing or batch.
- Policy version pinning: production uses one approved policy config until a
  new Plan 112 run is reviewed and promoted.
- Rollback path to the previous policy version or to unthrottled claims.

---

## Observability

Log the following counters per ops claim batch:

| Metric | Description |
|--------|-------------|
| `claims_considered` | Listings evaluated by the claim query |
| `claims_eligible` | Listings due for a fetch |
| `claims_throttled` | Listings skipped by refresh tier |
| `claims_escaped` | Listings bypassing throttling via escape hatch |
| `tier_distribution` | Count per `hot/daily/cool/cold` |

Dashboard these after rollout:

- Detail fetch count by day.
- Processing count by day.
- 403/block rate.
- Tier distribution.
- Estimated skipped fetches.
- Freshness/detection-delay proxy.
- Active policy version and MLflow run ID.
- Shadow-vs-enforced divergence during rollout.

---

## Rollout

1. Select an approved Plan 112 MLflow run that passed quality gates.
2. Export/pin the policy config and snapshot metadata for production.
3. Materialize `mart_detail_refresh_priority` with lineage columns, without
   changing claim behavior.
4. Add shadow counters: due, throttled, escaped, tier distribution.
5. Compare live shadow counters to Plan 112 expectations for several days.
6. Enable throttling with conservative thresholds.
7. Watch scrape volume, processing volume, block/403 rate, and freshness metrics.
8. Tighten intervals only after observed behavior matches the backtest and a new
   Plan 112 run is reviewed.

---

## Testing

### Integration Tests

- Ops claim query excludes listings whose `next_detail_fetch_after` is in the
  future.
- Ops claim query includes listings whose `next_detail_fetch_after` is due.
- Escape hatches bypass throttling for newly discovered, SRP-recent, forced, and
  never-detail-scraped listings.
- Feature flag disables throttling.
- Observability counters are logged per batch.
- Materialized priority rows include policy version, MLflow run ID, and snapshot
  metadata.
- Production config refuses to enable throttling without an approved policy
  version.

### Regression Tests

- Existing blocked-cooldown behavior is unchanged.
- Existing claim ordering is unchanged except for refresh eligibility.
- Scraper request payload shape is unchanged.

---

## Files Changed

| File | Change |
|------|--------|
| `ops/sql/claim_detail_listing.sql` | Add refresh tier eligibility filter |
| `dbt/models/marts/mart_detail_refresh_priority.sql` | Materialize selected rule-based score |
| `ops/sql/detail_refresh_priority.sql` | Optional query/table sync for priority rows |
| `ops/config` or env | Policy version / feature flag wiring |
| `tests/ops/test_claim_detail_throttling.py` | Claim query integration tests |
| `ops/metrics/` | Optional counters/dashboard wiring |

---

## Out of Scope

- XGBoost or any ML model.
- MLflow model serving or registry promotion.
- Iceberg reads at claim time.
- Online feature serving.
- Replacing Airflow scheduling.
- Sectioned HTML storage. See Plan 114.
