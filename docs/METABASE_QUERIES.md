# Metabase Dashboard Queries

All timestamps display in US Central time. Paste each query into Metabase as a **Native Query** question.

---

## Section 1: Pipeline Health

### Last Search Scrape *(Scalar)*
```sql
SELECT MAX(started_at) AT TIME ZONE 'America/Chicago'
FROM runs
WHERE status = 'success' AND trigger = 'search scrape'
```

### Last Detail Scrape *(Scalar)*
```sql
SELECT MAX(started_at) AT TIME ZONE 'America/Chicago'
FROM runs
WHERE status = 'success' AND trigger = 'detail scrape'
```

### New Vehicles Added (since last search scrape) *(Scalar)*
```sql
WITH last_run AS (
  SELECT started_at FROM runs
  WHERE status = 'success' AND trigger = 'search scrape'
  ORDER BY started_at DESC LIMIT 1
)
SELECT COUNT(DISTINCT vin)
FROM analytics.mart_deal_scores
WHERE first_seen_at >= (SELECT started_at FROM last_run)
```

### Vehicles Updated (since last search scrape) *(Scalar)*
```sql
WITH last_run AS (
  SELECT started_at FROM runs
  WHERE status = 'success' AND trigger = 'search scrape'
  ORDER BY started_at DESC LIMIT 1
)
SELECT COUNT(DISTINCT vin)
FROM srp_observations
WHERE fetched_at >= (SELECT started_at FROM last_run)
```

### Price Updates Since Last Detail Scrape *(Table — carousel vs direct)*
```sql
WITH last_run AS (
  SELECT started_at FROM runs
  WHERE status = 'success' AND trigger = 'detail scrape'
  ORDER BY started_at DESC LIMIT 1
)
SELECT 'Direct Page Load' AS source, COUNT(*) AS updates
FROM detail_observations
WHERE fetched_at >= (SELECT started_at FROM last_run)
UNION ALL
SELECT 'Carousel Hint' AS source, COUNT(*) AS updates
FROM detail_carousel_hints
WHERE fetched_at >= (SELECT started_at FROM last_run)
```

### Runs Over Time by Type *(Time Series — group by day)*
```sql
SELECT
  date_trunc('day', started_at AT TIME ZONE 'America/Chicago') AS run_date,
  trigger,
  COUNT(*) AS runs,
  COUNT(*) FILTER (WHERE status = 'success') AS successful,
  COUNT(*) FILTER (WHERE status = 'terminated') AS terminated
FROM runs
WHERE started_at > now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

### Recent Pipeline Errors *(Table)*
```sql
SELECT
  occurred_at AT TIME ZONE 'America/Chicago' AS occurred_at_ct,
  workflow_name,
  node_name,
  error_type,
  error_message
FROM pipeline_errors
ORDER BY occurred_at DESC
LIMIT 50
```

### Artifact Processing Backlog *(Table)*
```sql
SELECT
  processor,
  status,
  COUNT(*) AS count
FROM artifact_processing
WHERE status IN ('retry', 'processing')
GROUP BY processor, status
ORDER BY count DESC
```

---

## Section 2: Inventory Overview

### Active Listings by Make/Model *(Bar Chart)*
```sql
SELECT
  make,
  model,
  COUNT(*) AS active_listings,
  ROUND(AVG(current_price)) AS avg_price,
  MIN(current_price) AS min_price
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
GROUP BY make, model
ORDER BY active_listings DESC
```

### New Listings — Last 24h / 7d / 30d *(Scalar cards — use separate queries)*
```sql
-- Last 24 hours
SELECT COUNT(DISTINCT vin)
FROM analytics.mart_deal_scores
WHERE first_seen_at > now() - interval '24 hours'
```
```sql
-- Last 7 days
SELECT COUNT(DISTINCT vin)
FROM analytics.mart_deal_scores
WHERE first_seen_at > now() - interval '7 days'
```
```sql
-- Last 30 days
SELECT COUNT(DISTINCT vin)
FROM analytics.mart_deal_scores
WHERE first_seen_at > now() - interval '30 days'
```

### New Listings Over Time *(Time Series)*
```sql
SELECT
  date_trunc('day', first_seen_at AT TIME ZONE 'America/Chicago') AS date,
  make,
  COUNT(*) AS new_listings
FROM analytics.mart_deal_scores
WHERE first_seen_at > now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 DESC
```

### Total Active Listings *(Scalar)*
```sql
SELECT COUNT(*)
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
```

---

## Section 3: Deal Finder

### All Active Deals *(Table — main deal-finding view)*
```sql
SELECT
  make,
  model,
  vehicle_trim,
  model_year,
  current_price,
  national_median_price,
  msrp,
  ROUND(msrp_discount_pct::numeric, 1) AS msrp_off_pct,
  deal_tier,
  ROUND(deal_score::numeric, 1) AS deal_score,
  ROUND(national_price_percentile::numeric, 1) AS price_percentile,
  days_on_market,
  CASE WHEN is_local THEN 'Local' ELSE 'National' END AS scope,
  canonical_detail_url
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
ORDER BY deal_score DESC
```

### Price Drop Events (Last 30 Days) *(Table)*
```sql
SELECT
  ds.make,
  ds.model,
  ds.vehicle_trim,
  ds.model_year,
  ds.current_price,
  ds.first_price,
  ds.current_price - ds.first_price AS price_change,
  ROUND(ds.total_price_drop_pct::numeric, 1) AS total_drop_pct,
  ds.price_drop_count AS drops,
  ds.days_on_market,
  ds.canonical_detail_url
FROM analytics.mart_deal_scores ds
WHERE ds.listing_state = 'active'
  AND ds.price_drop_count > 0
  AND ds.price_observed_at > now() - interval '30 days'
ORDER BY ds.total_price_drop_pct DESC
```

### Deal Tier Distribution *(Bar Chart)*
```sql
SELECT
  deal_tier,
  COUNT(*) AS listings
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
GROUP BY deal_tier
ORDER BY
  CASE deal_tier
    WHEN 'excellent' THEN 1
    WHEN 'good' THEN 2
    WHEN 'fair' THEN 3
    WHEN 'weak' THEN 4
  END
```

### Days on Market Distribution *(Bar Chart)*
```sql
SELECT
  CASE
    WHEN days_on_market <= 7  THEN '0-7 days'
    WHEN days_on_market <= 14 THEN '8-14 days'
    WHEN days_on_market <= 30 THEN '15-30 days'
    WHEN days_on_market <= 60 THEN '31-60 days'
    ELSE '60+ days'
  END AS bucket,
  COUNT(*) AS listings
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
GROUP BY 1
ORDER BY MIN(days_on_market)
```

---

## Section 4: Market Trends

### Median Price by Model Over Time *(Time Series)*
```sql
SELECT
  date_trunc('week', price_observed_at AT TIME ZONE 'America/Chicago') AS week,
  model,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_price) AS median_price,
  COUNT(*) AS listing_count
FROM analytics.mart_deal_scores
WHERE price_observed_at > now() - interval '90 days'
  AND listing_state = 'active'
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

### Inventory Levels by Model Over Time *(Time Series)*
```sql
SELECT
  date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
  make,
  model,
  COUNT(DISTINCT vin) AS listings_seen
FROM srp_observations
WHERE fetched_at > now() - interval '30 days'
GROUP BY 1, 2, 3
ORDER BY 1 DESC
```

### Price vs MSRP by Model *(Bar Chart)*
```sql
SELECT
  model,
  ROUND(AVG(current_price)) AS avg_price,
  ROUND(AVG(msrp)) AS avg_msrp,
  ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct,
  COUNT(*) AS listings
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
  AND msrp IS NOT NULL
GROUP BY model
ORDER BY avg_msrp_off_pct DESC
```
