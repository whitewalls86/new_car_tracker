# Plan 113: Production Adaptive Refresh Integration

## Goal

Wire the rule-based refresh priority from Plans 111-112 into production ops so
the scraper only fetches listings that are due.

Do not introduce XGBoost or MLflow model serving in this plan. The first
production rollout should be conservative, observable, and easy to disable.

---

## Context

Plan 111 builds refresh features. Plan 112 backtests candidate thresholds and
intervals. This plan deploys the chosen rule-based policy behind guardrails.

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

---

## Rollout

1. Materialize `mart_detail_refresh_priority` without changing claim behavior.
2. Add shadow counters: due, throttled, escaped, tier distribution.
3. Compare live shadow counters to Plan 112 expectations for several days.
4. Enable throttling with conservative thresholds.
5. Watch scrape volume, processing volume, block/403 rate, and freshness metrics.
6. Tighten intervals only after observed behavior matches the backtest.

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
| `tests/ops/test_claim_detail_throttling.py` | Claim query integration tests |
| `ops/metrics/` | Optional counters/dashboard wiring |

---

## Out of Scope

- XGBoost or any ML model.
- MLflow model registry.
- Online feature serving.
- Replacing Airflow scheduling.
- Sectioned HTML storage. See Plan 114.
