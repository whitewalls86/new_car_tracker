"""
Layer 1 — SQL smoke tests for dashboard service queries.

All tests run as the `viewer` role (via the viewer_cur fixture) — the same
role the dashboard uses in production. This catches both SQL breakage and
permission regressions (missing GRANTs after schema changes).

Queries are imported from dashboard.queries (the same module the dashboard
uses), so tests and production are always in sync.
"""
import pytest

from dashboard.queries import (
    ACTIVE_RUNS,
    ARTIFACT_BACKLOG,
    BLOCKED_COOLDOWN_HISTOGRAM,
    COOLDOWN_BACKLOG,
    DBT_BUILD_HISTORY,
    DBT_LOCK_STATUS,
    DETAIL_EXTRACTION_COVERAGE,
    PG_STAT_CONNECTIONS,
    PG_STAT_SLOW_QUERIES,
    PIPELINE_ERRORS,
    PRICE_FRESHNESS,
    PROCESSING_THROUGHPUT,
    PROCESSOR_ACTIVITY,
    RECENT_DETAIL_RUNS,
    ROTATION_SCHEDULE,
    RUNS_OVER_TIME,
    SEARCH_SCRAPE_JOBS,
    STALE_VEHICLE_BACKLOG,
    SUCCESS_RATE,
    TERMINATED_RUNS,
)

pytestmark = pytest.mark.integration


# ============================================================================
# app.py — data freshness
# ============================================================================

class TestAppQueries:

    def test_data_freshness(self, viewer_cur):
        viewer_cur.execute("""
            SELECT MAX(price_observed_at) AT TIME ZONE 'America/Chicago' AS ts
            FROM analytics.mart_vehicle_snapshot
        """)
        viewer_cur.fetchone()


# ============================================================================
# deals.py
# ============================================================================

class TestDealQueries:

    def test_distinct_makes(self, viewer_cur):
        viewer_cur.execute("""
            SELECT DISTINCT make FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted' ORDER BY make
        """)
        viewer_cur.fetchall()

    def test_deals_table(self, viewer_cur):
        viewer_cur.execute("""
            SELECT make, model, vehicle_trim, model_year, dealer_name,
                   current_price, national_median_price, msrp,
                   ROUND(msrp_discount_pct::numeric, 1) AS msrp_off_pct,
                   deal_tier,
                   ROUND(deal_score::numeric, 1) AS deal_score,
                   ROUND(national_price_percentile::numeric * 100, 0) AS price_pct,
                   days_on_market,
                   price_drop_count AS drops,
                   CASE WHEN is_local THEN 'Local' ELSE 'National' END AS scope,
                   canonical_detail_url
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            ORDER BY deal_score DESC
        """)
        viewer_cur.fetchall()

    def test_deal_tier_distribution(self, viewer_cur):
        viewer_cur.execute("""
            SELECT deal_tier, COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY deal_tier
            ORDER BY CASE deal_tier
                WHEN 'excellent' THEN 1 WHEN 'good' THEN 2
                WHEN 'fair' THEN 3 WHEN 'weak' THEN 4 END
        """)
        viewer_cur.fetchall()

    def test_days_on_market_buckets(self, viewer_cur):
        viewer_cur.execute("""
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
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY 1 ORDER BY MIN(days_on_market)
        """)
        viewer_cur.fetchall()

    def test_price_drops(self, viewer_cur):
        viewer_cur.execute("""
            SELECT make, model, vehicle_trim, model_year, dealer_name,
                   current_price, first_price,
                   current_price - first_price AS price_change,
                   ROUND(total_price_drop_pct::numeric, 1) AS total_drop_pct,
                   price_drop_count AS drops, days_on_market,
                   canonical_detail_url
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND price_drop_count > 0
            ORDER BY total_price_drop_pct DESC
        """)
        viewer_cur.fetchall()

    def test_price_vs_msrp_by_model(self, viewer_cur):
        viewer_cur.execute("""
            SELECT model,
                   ROUND(AVG(current_price)) AS avg_price,
                   ROUND(AVG(msrp)) AS avg_msrp,
                   ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct,
                   COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND msrp IS NOT NULL AND msrp > 0
            GROUP BY model ORDER BY avg_msrp_off_pct DESC
        """)
        viewer_cur.fetchall()


# ============================================================================
# inventory.py
# ============================================================================

class TestInventoryQueries:

    def test_active_listings_count(self, viewer_cur):
        viewer_cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
        """)
        viewer_cur.fetchone()

    def test_new_listings_24h(self, viewer_cur):
        viewer_cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '24 hours'
        """)
        viewer_cur.fetchone()

    def test_new_listings_7d(self, viewer_cur):
        viewer_cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '7 days'
        """)
        viewer_cur.fetchone()

    def test_new_listings_30d(self, viewer_cur):
        viewer_cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '30 days'
        """)
        viewer_cur.fetchone()

    def test_listings_by_make_model(self, viewer_cur):
        viewer_cur.execute("""
            SELECT make, model,
                   COUNT(*) AS active_listings,
                   ROUND(AVG(current_price)) AS avg_price,
                   MIN(current_price) AS min_price
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY make, model
            ORDER BY active_listings DESC
        """)
        viewer_cur.fetchall()

    def test_new_listings_over_time(self, viewer_cur):
        viewer_cur.execute("""
            SELECT date_trunc('day', first_seen_at AT TIME ZONE 'America/Chicago') AS day,
                   make, COUNT(*) AS new_listings
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '30 days'
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        viewer_cur.fetchall()

    def test_unlisted_over_time(self, viewer_cur):
        viewer_cur.execute("""
            WITH first_unlisted AS (
                SELECT upper(d.vin) AS vin, MIN(d.fetched_at) AS unlisted_at
                FROM analytics.stg_detail_observations d
                WHERE d.listing_state = 'unlisted'
                  AND d.vin IS NOT NULL AND length(d.vin) = 17
                  AND d.fetched_at > now() - interval '30 days'
                GROUP BY upper(d.vin)
            )
            SELECT date_trunc('day', unlisted_at AT TIME ZONE 'America/Chicago') AS day,
                   COUNT(*) AS vehicles_unlisted
            FROM first_unlisted
            GROUP BY 1 ORDER BY 1
        """)
        viewer_cur.fetchall()

    def test_top_dealers(self, viewer_cur):
        viewer_cur.execute("""
            SELECT COALESCE(dealer_name, seller_customer_id) AS dealer,
                   make, model,
                   COUNT(*) AS active_listings,
                   ROUND(AVG(current_price)) AS avg_price,
                   MIN(current_price) AS min_price
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND seller_customer_id IS NOT NULL
            GROUP BY COALESCE(dealer_name, seller_customer_id), make, model
            ORDER BY active_listings DESC LIMIT 50
        """)
        viewer_cur.fetchall()


# ============================================================================
# market_trends.py
# ============================================================================

class TestMarketTrendsQueries:

    def test_median_price_by_model_weekly(self, viewer_cur):
        viewer_cur.execute("""
            SELECT date_trunc('week', ph.observed_at AT TIME ZONE 'America/Chicago') AS week,
                   va.make, va.model,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mrt.price) AS median_price,
                   COUNT(DISTINCT ph.vin) AS listing_count
            FROM analytics.mart_vehicle_snapshot mrt
            LEFT JOIN analytics.int_price_events ph ON mrt.vin = ph.vin
            LEFT JOIN analytics.int_vehicle_attributes va ON mrt.vin = va.vin
            WHERE ph.observed_at > now() - interval '90 days'
              AND ph.price > 0 AND mrt.vin IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
        """)
        viewer_cur.fetchall()

    def test_inventory_levels_daily(self, viewer_cur):
        viewer_cur.execute("""
            SELECT date_trunc('day', ph.observed_at AT TIME ZONE 'America/Chicago') AS day,
                   va.make, va.model,
                   COUNT(DISTINCT ph.vin) AS listings_seen
            FROM analytics.mart_vehicle_snapshot mrt
            LEFT JOIN analytics.int_price_events ph ON mrt.vin = ph.vin
            LEFT JOIN analytics.int_vehicle_attributes va ON mrt.vin = va.vin
            WHERE ph.observed_at > now() - interval '30 days' AND mrt.vin IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC
        """)
        viewer_cur.fetchall()

    def test_days_on_market_by_model(self, viewer_cur):
        viewer_cur.execute("""
            SELECT make, model,
                   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_on_market))
                       AS median_days,
                   ROUND(AVG(days_on_market)::numeric, 1) AS avg_days,
                   MIN(days_on_market) AS min_days,
                   MAX(days_on_market) AS max_days,
                   COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY make, model ORDER BY median_days DESC
        """)
        viewer_cur.fetchall()

    def test_national_vs_local_supply(self, viewer_cur):
        viewer_cur.execute("""
            SELECT make, model,
                   COUNT(*) AS national_listings,
                   COUNT(*) FILTER (WHERE is_local) AS local_listings,
                   ROUND(AVG(national_listing_count)) AS avg_national_supply,
                   ROUND(AVG(current_price)) AS avg_price,
                   ROUND(AVG(msrp_discount_pct)::numeric, 1) AS avg_msrp_off_pct
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY make, model ORDER BY national_listings DESC
        """)
        viewer_cur.fetchall()


# ============================================================================
# pipeline_health.py
# ============================================================================

class TestPipelineHealthQueries:

    def test_active_runs(self, viewer_cur):
        viewer_cur.execute(ACTIVE_RUNS)
        viewer_cur.fetchall()

    def test_dbt_lock_status(self, viewer_cur):
        viewer_cur.execute(DBT_LOCK_STATUS)
        row = viewer_cur.fetchone()
        assert row is not None

    def test_rotation_schedule(self, viewer_cur):
        viewer_cur.execute(ROTATION_SCHEDULE)
        viewer_cur.fetchall()

    def test_recent_detail_scrape_runs(self, viewer_cur):
        viewer_cur.execute(RECENT_DETAIL_RUNS)
        viewer_cur.fetchall()

    def test_stale_vehicle_backlog(self, viewer_cur):
        viewer_cur.execute(STALE_VEHICLE_BACKLOG)
        viewer_cur.fetchall()

    def test_cooldown_backlog(self, viewer_cur):
        viewer_cur.execute(COOLDOWN_BACKLOG)
        viewer_cur.fetchall()

    def test_price_freshness(self, viewer_cur):
        viewer_cur.execute(PRICE_FRESHNESS)
        viewer_cur.fetchall()

    def test_blocked_cooldown_histogram(self, viewer_cur):
        viewer_cur.execute(BLOCKED_COOLDOWN_HISTOGRAM)
        viewer_cur.fetchall()

    def test_success_rate_detail(self, viewer_cur):
        viewer_cur.execute(SUCCESS_RATE.format(
            artifact_type="detail_page", interval="7 days"
        ))
        viewer_cur.fetchall()

    def test_success_rate_results(self, viewer_cur):
        viewer_cur.execute(SUCCESS_RATE.format(
            artifact_type="results_page", interval="7 days"
        ))
        viewer_cur.fetchall()

    def test_search_scrape_jobs_7d(self, viewer_cur):
        viewer_cur.execute(SEARCH_SCRAPE_JOBS)
        viewer_cur.fetchall()

    def test_runs_over_time(self, viewer_cur):
        viewer_cur.execute(RUNS_OVER_TIME)
        viewer_cur.fetchall()

    def test_artifact_processing_backlog(self, viewer_cur):
        viewer_cur.execute(ARTIFACT_BACKLOG)
        viewer_cur.fetchall()

    def test_terminated_runs(self, viewer_cur):
        viewer_cur.execute(TERMINATED_RUNS)
        viewer_cur.fetchall()

    def test_pipeline_errors(self, viewer_cur):
        viewer_cur.execute(PIPELINE_ERRORS)
        viewer_cur.fetchall()

    def test_dbt_build_history(self, viewer_cur):
        viewer_cur.execute(DBT_BUILD_HISTORY)
        viewer_cur.fetchall()

    def test_processor_activity(self, viewer_cur):
        viewer_cur.execute(PROCESSOR_ACTIVITY)
        viewer_cur.fetchall()

    def test_processing_throughput(self, viewer_cur):
        viewer_cur.execute(PROCESSING_THROUGHPUT)
        viewer_cur.fetchall()

    def test_detail_extraction_coverage(self, viewer_cur):
        viewer_cur.execute(DETAIL_EXTRACTION_COVERAGE)
        viewer_cur.fetchall()

    def test_pg_stat_activity(self, viewer_cur):
        viewer_cur.execute(PG_STAT_CONNECTIONS)
        viewer_cur.fetchone()

    def test_long_running_queries(self, viewer_cur):
        viewer_cur.execute(PG_STAT_SLOW_QUERIES)
        viewer_cur.fetchall()
