# Plan 115: Detail Unenriched Circuit Breaker

## Goal

Stop listings with `customer_id IS NULL` from being re-queued for detail scraping
every 15 minutes after they have already been successfully detail-scraped.

This is a production bugfix. It should land before Plans 111-114 so later
adaptive-refresh and storage analyses are not distorted by pathological
`dealer_unenriched` retry loops.

---

## Problem

The detail scrape queue currently treats `customer_id IS NULL` as
`dealer_unenriched` and immediately eligible for detail scraping:

```sql
po.customer_id IS NULL AS is_full_details_stale
```

That worked when `customer_id IS NULL` meant "never detail-scraped." It fails
for pages where detail processing succeeds but no `customer_id` can be extracted:

- private sellers
- page formats that do not expose dealer/customer metadata
- parser gaps where vehicle data is valid but dealer metadata is missing

The detail writer updates `last_seen_at`, but `ops_vehicle_staleness` ignores
that for `customer_id IS NULL`. The listing falls back into
`ops_detail_scrape_queue` on the next DAG run and can be scraped every 15
minutes indefinitely.

---

## Root Cause Chain

1. `ops_vehicle_staleness` marks a listing stale if `customer_id IS NULL`.
2. Detail scrape succeeds and updates `last_seen_at`.
3. If parser output has `customer_id = NULL`, the upsert preserves NULL:

   ```sql
   customer_id = COALESCE(EXCLUDED.customer_id, ops.price_observations.customer_id)
   ```

4. Because `customer_id` is still NULL, the listing remains
   `dealer_unenriched`.
5. `ops_detail_scrape_queue` picks it again on the next run.
6. `blocked_cooldown` does not help because these are successful scrape/process
   attempts, not 403s.

The result is many repeated artifacts for a small subset of listings, even
though total daily artifact count can still roughly match active inventory.

---

## Fix

Track successful detail-processing attempts separately from generic
`last_seen_at`.

Add `last_detail_scraped_at` to `ops.price_observations`.

Detail writes set it on every successful detail processing path, including:

- active listings with `customer_id`
- active listings without `customer_id`
- unlisted detail pages if a row remains or is updated before deletion

SRP and carousel writes must not set it.

Then change `dealer_unenriched` eligibility:

```sql
customer_id IS NULL
AND (
    last_detail_scraped_at IS NULL
    OR last_detail_scraped_at < now() - interval '7 days'
)
```

This preserves periodic re-checking for listings that never expose
`customer_id`, but prevents a successful no-customer-id detail scrape from
causing an immediate 15-minute loop.

---

## Schema Migration

Add a nullable column:

```sql
ALTER TABLE ops.price_observations
  ADD COLUMN last_detail_scraped_at timestamptz;
```

Backfill choice:

- Leave NULL for all existing rows if we want current behavior until each
  listing is detail-scraped again.
- Or backfill from `last_seen_at` where `last_artifact_id` points to a
  `detail_page` artifact.

Recommended first rollout: leave NULL. It is simpler and lets the new circuit
breaker activate naturally after the next successful detail scrape.

---

## Writer Changes

### `processing/sql/upsert_price_observation.sql`

Add `last_detail_scraped_at` as an optional upsert parameter:

```sql
INSERT INTO ops.price_observations
    (..., last_seen_at, last_artifact_id, last_detail_scraped_at)
VALUES
    (..., %(last_seen_at)s, %(last_artifact_id)s, %(last_detail_scraped_at)s)
ON CONFLICT (listing_id) DO UPDATE SET
    ...
    last_detail_scraped_at = COALESCE(
        EXCLUDED.last_detail_scraped_at,
        ops.price_observations.last_detail_scraped_at
    )
```

### Detail Writer

In `processing/writers/detail_writer.py`, pass:

```python
"last_detail_scraped_at": fetched_at
```

for the primary detail row.

Do not set `last_detail_scraped_at` for carousel upserts.

### SRP Writer

In `processing/writers/srp_writer.py`, pass:

```python
"last_detail_scraped_at": None
```

or use a query wrapper that defaults the value to NULL. SRP must not refresh the
detail circuit-breaker timestamp.

---

## View Changes

Update `ops.ops_vehicle_staleness`:

```sql
po.customer_id IS NULL
AND (
    po.last_detail_scraped_at IS NULL
    OR po.last_detail_scraped_at < now() - interval '7 days'
) AS is_full_details_stale
```

Update `stale_reason`:

```sql
CASE
    WHEN po.customer_id IS NULL
     AND (
        po.last_detail_scraped_at IS NULL
        OR po.last_detail_scraped_at < now() - interval '7 days'
     )
    THEN 'dealer_unenriched'
    WHEN po.last_seen_at < now() - interval '24 hours'
    THEN 'price_only'
    ELSE 'not_stale'
END
```

Expose `last_detail_scraped_at` from the view for diagnostics.

---

## Operational Verification

Before deploy:

- Count listings currently in `ops_detail_scrape_queue` because
  `customer_id IS NULL`.
- Count listings with multiple detail artifacts in the last day.
- Identify worst offenders by detail artifact count.
- Count successful detail artifacts where the resulting primary observation
  still has `customer_id IS NULL`.

After deploy:

- Verify successful detail scrapes without `customer_id` leave the queue.
- Verify they re-enter only after 7 days.
- Watch total detail artifact count per day.
- Watch worst-offender repeat counts.

---

## Observability And Alerts

Add visibility for the exact failure mode so this does not linger unnoticed
again.

### Metrics

Track these daily and per DAG run where feasible:

| Metric | Purpose |
|--------|---------|
| `detail_artifacts_total` | Baseline detail scrape volume |
| `detail_artifacts_per_listing_p95_24h` | Detect repeated scraping of the same listings |
| `detail_listings_scraped_more_than_3x_24h` | Direct circuit-breaker regression signal |
| `detail_success_customer_id_null_total` | Successful detail parses that still lack enrichment |
| `detail_queue_dealer_unenriched_total` | Size of the unenriched queue pool |
| `detail_queue_dealer_unenriched_recently_scraped_total` | Listings that should be suppressed by `last_detail_scraped_at` |
| `detail_claims_by_stale_reason` | Distribution of `dealer_unenriched`, `price_only`, and future reasons |

### Alert Candidates

- Alert if any listing has more than 3 successful detail artifacts in 24 hours
  without a 403/block reason.
- Alert if `detail_queue_dealer_unenriched_recently_scraped_total > 0` after
  the fix is deployed.
- Alert if `detail_success_customer_id_null_total` spikes materially above its
  trailing 7-day baseline.
- Dashboard a top-N table of repeat-scraped listings with:
  `listing_id`, `artifact_count_24h`, `customer_id`, `last_seen_at`,
  `last_detail_scraped_at`, `stale_reason`, and latest parser metadata.

### RCA Queries

Keep a small set of operational SQL snippets in the plan or runbook:

```sql
-- Listings successfully detail-scraped repeatedly in the last 24h.
SELECT
    listing_id,
    count(*) AS detail_artifacts_24h,
    min(fetched_at) AS first_seen,
    max(fetched_at) AS last_seen
FROM staging.artifacts_queue_events
WHERE artifact_type = 'detail_page'
  AND status = 'complete'
  AND fetched_at >= now() - interval '24 hours'
GROUP BY listing_id
HAVING count(*) > 3
ORDER BY detail_artifacts_24h DESC;
```

```sql
-- Unenriched listings that were detail-scraped recently and should not be queued.
SELECT
    listing_id,
    customer_id,
    last_seen_at,
    last_detail_scraped_at
FROM ops.ops_vehicle_staleness
WHERE customer_id IS NULL
  AND last_detail_scraped_at >= now() - interval '7 days'
  AND is_full_details_stale = true;
```

---

## RCA: Why This Lingered

This bug was allowed to linger because the system had tests for individual queue
eligibility rules but no regression test for a successful detail scrape that
still leaves `customer_id` NULL.

Contributing factors:

- `customer_id IS NULL` encoded two meanings:
  - never detail-scraped
  - detail-scraped but not enrichable
- `last_seen_at` was intentionally source-agnostic, so it could not answer
  "when did detail last try?"
- The queue tests covered fresh enriched rows and stale unenriched rows, but not
  recently detail-scraped unenriched rows.
- `blocked_cooldown` only controls 403 failures, not successful low-value
  repeats.
- Aggregate daily artifact volume looked plausible, hiding a skew where a small
  subset of listings repeated too often.
- No metric highlighted "successful detail scrapes per listing per day."

The preventative control is a combination of:

1. A separate detail-attempt timestamp.
2. Regression tests for queue suppression.
3. Repeat-scrape observability by listing.
4. Alerts on successful high-frequency repeats.

---

## Testing

### Migration Tests

- `ops.price_observations` has nullable `last_detail_scraped_at`.
- Existing rows remain valid after migration.

### SQL View Tests

- `customer_id IS NULL` and `last_detail_scraped_at IS NULL` is queued as
  `dealer_unenriched`.
- `customer_id IS NULL` and `last_detail_scraped_at = now()` is not queued.
- `customer_id IS NULL` and `last_detail_scraped_at > 7 days ago` is queued.
- `customer_id IS NOT NULL` and `last_seen_at < 24h` is not queued.
- `customer_id IS NOT NULL` and `last_seen_at > 24h` is queued as `price_only`.
- `detail_queue_dealer_unenriched_recently_scraped_total` query returns zero
  for healthy seeded data.

### Writer Tests

- Detail active write sets `last_detail_scraped_at = fetched_at`.
- Detail active write sets `last_detail_scraped_at` even when `customer_id` is
  NULL.
- Carousel upserts do not set or refresh `last_detail_scraped_at`.
- SRP upserts do not set or refresh `last_detail_scraped_at`.

### Integration Tests

- Seed `customer_id NULL`, `last_detail_scraped_at now`, and assert the listing
  is absent from `ops_detail_scrape_queue`.
- Seed `customer_id NULL`, `last_detail_scraped_at NULL`, and assert it is
  present.
- Seed `customer_id NULL`, `last_detail_scraped_at 8 days ago`, and assert it is
  present.
- Simulate two successful detail cycles with `customer_id NULL` and assert the
  second cycle cannot immediately reclaim the listing.

### Observability Tests

- Repeat-scrape query identifies a seeded listing with more than 3 detail
  artifacts in 24 hours.
- Recently-scraped-unenriched anomaly query returns seeded bad rows.
- Claim counters include stale reason distribution.

---

## Files Changed

| File | Change |
|------|--------|
| `db/migrations/V040__detail_scrape_circuit_breaker.sql` | Add column and recreate ops views |
| `processing/sql/upsert_price_observation.sql` | Add optional `last_detail_scraped_at` |
| `processing/writers/detail_writer.py` | Set timestamp for primary detail writes |
| `processing/writers/srp_writer.py` | Ensure SRP writes do not set timestamp |
| `tests/integration/sql/test_ops_views.py` | Queue eligibility regression tests |
| `tests/processing/test_detail_writer.py` | Detail writer timestamp tests |
| `tests/processing/test_srp_writer.py` | SRP/carousel non-refresh tests |
| `ops/metrics/duckdb_gauges.py` or Grafana SQL panel | Repeat-scrape and unenriched-loop metrics |
| `docs/runbooks/` or plan appendix | RCA queries for repeat detail artifacts |

---

## Rollout

1. Deploy migration and writer changes together.
2. Run integration tests against the Postgres test container.
3. Deploy to production.
4. Run operational verification queries before and after one detail DAG cycle.
5. Keep Plan 111 backtesting paused until this fix has at least one day of clean
   production behavior.

---

## Out of Scope

- Adaptive refresh scoring.
- ML-based detail refresh.
- Sectioned HTML storage.
- Changing the 24-hour price staleness threshold.
- Changing the 7-day detail retry interval dynamically.
