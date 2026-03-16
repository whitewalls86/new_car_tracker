# Metabase Dashboard Queries

All timestamps display in US Central time. Paste each query into Metabase as a **Native Query** question.

> **Table notes:**
> - `analytics.mart_deal_scores` — one row per active VIN (seen in SRP last 3 days). `listing_state` is now always populated (`'active'` or `'unlisted'`).
> - `analytics.int_listing_days_on_market` — all 67k VINs ever seen; use for historical new-listing counts.
> - `analytics.int_price_events` — full price time-series (SRP + detail + carousel); use for price trends.
> - `analytics.int_srp_vehicle_attributes` — latest make/model/trim/msrp per VIN.

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

### New Vehicles Added — Since Last Search Scrape *(Scalar)*
Counts VINs whose `first_seen_at` falls within the most recent search run window.
```sql
WITH last_run AS (
  SELECT started_at FROM runs
  WHERE status = 'success' AND trigger = 'search scrape'
  ORDER BY started_at DESC LIMIT 1
)
SELECT COUNT(DISTINCT vin)
FROM analytics.int_listing_days_on_market
WHERE first_seen_at >= (SELECT started_at FROM last_run)
```

### Vehicles Observed — Since Last Search Scrape *(Scalar)*
Counts all VINs seen in SRP during the last search run (new + existing).
```sql
WITH last_run AS (
  SELECT started_at FROM runs
  WHERE status = 'success' AND trigger = 'search scrape'
  ORDER BY started_at DESC LIMIT 1
)
SELECT COUNT(DISTINCT vin)
FROM srp_observations
WHERE fetched_at >= (SELECT started_at FROM last_run)
  AND vin IS NOT NULL
```

### Price Updates — Since Last Detail Scrape *(Table)*
Breaks down price observations by source since the last detail scrape.
```sql
WITH last_run AS (
  SELECT started_at FROM runs
  WHERE status = 'success' AND trigger = 'detail scrape'
  ORDER BY started_at DESC LIMIT 1
)
SELECT 'Direct Detail Page' AS source, COUNT(*) AS updates
FROM detail_observations
WHERE fetched_at >= (SELECT started_at FROM last_run)
UNION ALL
SELECT 'Carousel Hint' AS source, COUNT(*) AS updates
FROM detail_carousel_hints
WHERE fetched_at >= (SELECT started_at FROM last_run)
```

### Detail Scrape Success Rate — Last 30 Days *(Bar Chart grouped by day)*
Shows HTTP 200 vs 403 (Cloudflare block) vs errors per day for detail page fetches.
```sql
SELECT
  date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
  CASE
    WHEN http_status = 200 THEN '200 OK'
    WHEN http_status = 403 THEN '403 Blocked'
    WHEN http_status IS NULL THEN 'Error/Timeout'
    ELSE http_status::text
  END AS result,
  COUNT(*) AS fetches
FROM raw_artifacts
WHERE artifact_type = 'detail_page'
  AND fetched_at > now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

### Runs Over Time by Type *(Time Series — group by day)*
```sql
SELECT
  date_trunc('day', started_at AT TIME ZONE 'America/Chicago') AS day,
  trigger,
  COUNT(*) AS runs,
  COUNT(*) FILTER (WHERE status = 'success') AS successful,
  COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
  COUNT(*) FILTER (WHERE status = 'failed') AS failed
FROM runs
WHERE started_at > now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

### Stale Vehicle Backlog *(Scalar — vehicles needing detail scrape)*
```sql
SELECT COUNT(*) AS vehicles_needing_refresh
FROM ops.ops_vehicle_staleness
WHERE (is_price_stale OR is_full_details_stale)
  AND COALESCE(listing_state, 'active') = 'active'
  AND current_listing_url IS NOT NULL
```

### Stale Vehicles by Reason *(Table)*
```sql
SELECT
  stale_reason,
  COUNT(*) AS vehicle_count,
  ROUND(AVG(tier1_age_hours), 1) AS avg_tier1_age_hours,
  ROUND(AVG(price_age_hours), 1) AS avg_price_age_hours
FROM ops.ops_vehicle_staleness
WHERE COALESCE(listing_state, 'active') = 'active'
GROUP BY stale_reason
ORDER BY vehicle_count DESC
```

### Artifact Processing Backlog *(Table)*
Shows processors with unfinished work.
```sql
SELECT
  processor,
  status,
  COUNT(*) AS count,
  MIN(processed_at) AT TIME ZONE 'America/Chicago' AS oldest
FROM artifact_processing
WHERE status IN ('retry', 'processing')
GROUP BY processor, status
ORDER BY count DESC
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

### Terminated Runs — Last 7 Days *(Table)*
Surfaces runs that were auto-terminated by the cleanup job (indicates stuck workflows).
```sql
SELECT
  trigger,
  COUNT(*) AS terminated_count,
  MAX(started_at) AT TIME ZONE 'America/Chicago' AS most_recent
FROM runs
WHERE status = 'terminated'
  AND started_at > now() - interval '7 days'
GROUP BY trigger
ORDER BY terminated_count DESC
```

---

## Section 2: Inventory Overview

### Total Active Listings *(Scalar)*
```sql
SELECT COUNT(*)
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
```

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

### New Listings — Last 24h *(Scalar)*
```sql
SELECT COUNT(DISTINCT vin)
FROM analytics.int_listing_days_on_market
WHERE first_seen_at > now() - interval '24 hours'
```

### New Listings — Last 7 Days *(Scalar)*
```sql
SELECT COUNT(DISTINCT vin)
FROM analytics.int_listing_days_on_market
WHERE first_seen_at > now() - interval '7 days'
```

### New Listings — Last 30 Days *(Scalar)*
```sql
SELECT COUNT(DISTINCT vin)
FROM analytics.int_listing_days_on_market
WHERE first_seen_at > now() - interval '30 days'
```

### New Listings Over Time *(Time Series — group by day)*
```sql
SELECT
  date_trunc('day', dom.first_seen_at AT TIME ZONE 'America/Chicago') AS day,
  a.make,
  COUNT(DISTINCT dom.vin) AS new_listings
FROM analytics.int_listing_days_on_market dom
JOIN analytics.int_srp_vehicle_attributes a ON a.vin = dom.vin
WHERE dom.first_seen_at > now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

### Listings Going Unlisted Over Time *(Time Series — group by day)*
VINs confirmed sold/removed via detail page scrapes.
```sql
SELECT
  date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
  listing_state,
  COUNT(DISTINCT vin) AS vehicles
FROM detail_observations
WHERE fetched_at > now() - interval '30 days'
  AND listing_state IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

### Active Listings by Dealer *(Table)*
```sql
SELECT
  seller_customer_id AS dealer_id,
  make,
  model,
  COUNT(*) AS active_listings,
  ROUND(AVG(current_price)) AS avg_price,
  MIN(current_price) AS min_price
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
  AND seller_customer_id IS NOT NULL
GROUP BY seller_customer_id, make, model
ORDER BY active_listings DESC
LIMIT 50
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
  ROUND(national_price_percentile::numeric * 100, 0) AS price_pct,
  days_on_market,
  price_drop_count AS drops,
  CASE WHEN is_local THEN 'Local' ELSE 'National' END AS scope,
  canonical_detail_url
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
ORDER BY deal_score DESC
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

### Price Drop Events *(Table — listings that dropped in price)*
```sql
SELECT
  make,
  model,
  vehicle_trim,
  model_year,
  current_price,
  first_price,
  current_price - first_price AS price_change,
  ROUND(total_price_drop_pct::numeric, 1) AS total_drop_pct,
  price_drop_count AS drops,
  days_on_market,
  canonical_detail_url
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
  AND price_drop_count > 0
ORDER BY total_price_drop_pct DESC
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
  AND msrp IS NOT NULL AND msrp > 0
GROUP BY model
ORDER BY avg_msrp_off_pct DESC
```

---

## Section 4: Market Trends

### Median Price by Model Over Time *(Time Series — group by week)*
Uses `int_price_events` (full history) joined to vehicle attributes. Filtered to SRP source only to avoid double-counting detail + carousel.
```sql
SELECT
  date_trunc('week', pe.observed_at AT TIME ZONE 'America/Chicago') AS week,
  a.make,
  a.model,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe.price) AS median_price,
  COUNT(DISTINCT pe.vin) AS listing_count
FROM analytics.int_price_events pe
JOIN analytics.int_srp_vehicle_attributes a ON a.vin = pe.vin
WHERE pe.observed_at > now() - interval '90 days'
  AND pe.price > 0
  AND pe.source = 'srp'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3
```

### Inventory Levels by Model Over Time *(Time Series — group by day)*
Distinct VINs seen per model per day in SRP observations.
```sql
SELECT
  date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
  make,
  model,
  COUNT(DISTINCT vin) AS listings_seen
FROM srp_observations
WHERE fetched_at > now() - interval '30 days'
  AND vin IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
```

### Days on Market by Model *(Bar Chart — current snapshot)*
```sql
SELECT
  make,
  model,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_on_market)) AS median_days,
  ROUND(AVG(days_on_market), 1) AS avg_days,
  MIN(days_on_market) AS min_days,
  MAX(days_on_market) AS max_days,
  COUNT(*) AS listings
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
GROUP BY make, model
ORDER BY median_days DESC
```

### National Supply vs Local Availability *(Table)*
Shows how many units of each model exist nationally vs how many are available locally.
```sql
SELECT
  make,
  model,
  COUNT(*) AS national_listings,
  COUNT(*) FILTER (WHERE is_local) AS local_listings,
  ROUND(AVG(national_listing_count)) AS avg_national_supply,
  ROUND(AVG(current_price)) AS avg_price,
  ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct
FROM analytics.mart_deal_scores
WHERE listing_state = 'active'
GROUP BY make, model
ORDER BY national_listings DESC
```
