"""
Layer 1 — SQL smoke tests for dashboard service queries.

The dashboard runs 39 SELECT queries, many with complex CTEs and JOINs across
analytics.* and public.* tables. These are the highest-risk queries in the
system — they touch the most tables and are the most likely to break on schema
changes. All queries should execute without error against an empty-but-valid
schema (Flyway migrations applied, dbt tables exist but are empty).
"""
import pytest

pytestmark = pytest.mark.integration


# ============================================================================
# app.py — data freshness
# ============================================================================

class TestAppQueries:

    def test_data_freshness(self, cur):
        cur.execute("""
            SELECT MAX(price_observed_at) AT TIME ZONE 'America/Chicago' AS ts
            FROM analytics.mart_vehicle_snapshot
        """)
        cur.fetchone()


# ============================================================================
# deals.py
# ============================================================================

class TestDealQueries:

    def test_distinct_makes(self, cur):
        cur.execute("""
            SELECT DISTINCT make FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted' ORDER BY make
        """)
        cur.fetchall()

    def test_deals_table(self, cur):
        cur.execute("""
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
        cur.fetchall()

    def test_deal_tier_distribution(self, cur):
        cur.execute("""
            SELECT deal_tier, COUNT(*) AS listings
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY deal_tier
            ORDER BY CASE deal_tier
                WHEN 'excellent' THEN 1 WHEN 'good' THEN 2
                WHEN 'fair' THEN 3 WHEN 'weak' THEN 4 END
        """)
        cur.fetchall()

    def test_days_on_market_buckets(self, cur):
        cur.execute("""
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
        cur.fetchall()

    def test_price_drops(self, cur):
        cur.execute("""
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
        cur.fetchall()

    def test_price_vs_msrp_by_model(self, cur):
        cur.execute("""
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
        cur.fetchall()


# ============================================================================
# inventory.py
# ============================================================================

class TestInventoryQueries:

    def test_active_listings_count(self, cur):
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
        """)
        cur.fetchone()

    def test_new_listings_24h(self, cur):
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '24 hours'
        """)
        cur.fetchone()

    def test_new_listings_7d(self, cur):
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '7 days'
        """)
        cur.fetchone()

    def test_new_listings_30d(self, cur):
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '30 days'
        """)
        cur.fetchone()

    def test_listings_by_make_model(self, cur):
        cur.execute("""
            SELECT make, model,
                   COUNT(*) AS active_listings,
                   ROUND(AVG(current_price)) AS avg_price,
                   MIN(current_price) AS min_price
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
            GROUP BY make, model
            ORDER BY active_listings DESC
        """)
        cur.fetchall()

    def test_new_listings_over_time(self, cur):
        cur.execute("""
            SELECT date_trunc('day', first_seen_at AT TIME ZONE 'America/Chicago') AS day,
                   make, COUNT(*) AS new_listings
            FROM analytics.mart_deal_scores
            WHERE COALESCE(listing_state, 'active') != 'unlisted'
              AND first_seen_at > now() - interval '30 days'
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        cur.fetchall()

    def test_unlisted_over_time(self, cur):
        cur.execute("""
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
        cur.fetchall()

    def test_top_dealers(self, cur):
        cur.execute("""
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
        cur.fetchall()


# ============================================================================
# market_trends.py
# ============================================================================

class TestMarketTrendsQueries:

    def test_median_price_by_model_weekly(self, cur):
        cur.execute("""
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
        cur.fetchall()

    def test_inventory_levels_daily(self, cur):
        cur.execute("""
            SELECT date_trunc('day', ph.observed_at AT TIME ZONE 'America/Chicago') AS day,
                   va.make, va.model,
                   COUNT(DISTINCT ph.vin) AS listings_seen
            FROM analytics.mart_vehicle_snapshot mrt
            LEFT JOIN analytics.int_price_events ph ON mrt.vin = ph.vin
            LEFT JOIN analytics.int_vehicle_attributes va ON mrt.vin = va.vin
            WHERE ph.observed_at > now() - interval '30 days' AND mrt.vin IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC
        """)
        cur.fetchall()

    def test_days_on_market_by_model(self, cur):
        cur.execute("""
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
        cur.fetchall()

    def test_national_vs_local_supply(self, cur):
        cur.execute("""
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
        cur.fetchall()


# ============================================================================
# pipeline_health.py
# ============================================================================

class TestPipelineHealthQueries:

    def test_active_runs(self, cur):
        cur.execute("""
            SELECT r.trigger, r.started_at AT TIME ZONE 'America/Chicago' AS started_at,
                   ROUND(EXTRACT(EPOCH FROM now() - r.started_at) / 60) AS elapsed_min,
                   r.progress_count, r.total_count,
                   CASE WHEN r.total_count > 0
                        THEN ROUND(r.progress_count::numeric /
                                   (EXTRACT(EPOCH FROM now() - r.started_at) / 60), 1)
                   END AS vins_per_min,
                   (SELECT COUNT(*) FROM scrape_jobs j
                    WHERE j.run_id = r.run_id AND j.status = 'failed') AS failed_jobs
            FROM runs r WHERE r.status = 'running' ORDER BY r.started_at
        """)
        cur.fetchall()

    def test_dbt_lock_status(self, cur):
        cur.execute("""
            SELECT locked, locked_at AT TIME ZONE 'America/Chicago' AS locked_at, locked_by
            FROM dbt_lock WHERE id = 1
        """)
        row = cur.fetchone()
        assert row is not None

    def test_rotation_schedule(self, cur):
        cur.execute("""
            WITH slot_configs AS (
                SELECT rotation_slot,
                       string_agg(search_key, ', ' ORDER BY search_key) AS search_keys,
                       MAX(last_queued_at) AS last_queued_at
                FROM search_configs
                WHERE enabled = true AND rotation_slot IS NOT NULL
                GROUP BY rotation_slot
            ), slot_last_run AS (
                SELECT DISTINCT ON (sc.rotation_slot)
                    sc.rotation_slot, r.run_id,
                    r.status AS run_status, r.started_at
                FROM search_configs sc
                JOIN scrape_jobs j ON j.search_key = sc.search_key
                JOIN runs r ON r.run_id = j.run_id AND r.trigger = 'search scrape'
                WHERE sc.enabled = true AND sc.rotation_slot IS NOT NULL
                ORDER BY sc.rotation_slot, r.started_at DESC
            ), slot_results AS (
                SELECT slr.rotation_slot,
                       COUNT(DISTINCT a.artifact_id) AS pages,
                       COUNT(DISTINCT a.artifact_id) FILTER (
                           WHERE a.http_status IS NULL OR a.http_status >= 400
                       ) AS errors,
                       COUNT(DISTINCT so.vin) AS vins_observed
                FROM slot_last_run slr
                JOIN scrape_jobs j ON j.run_id = slr.run_id
                    AND j.search_key IN (
                        SELECT search_key FROM search_configs
                        WHERE rotation_slot = slr.rotation_slot
                    )
                JOIN raw_artifacts a ON a.run_id = slr.run_id
                    AND a.artifact_type = 'results_page'
                    AND a.search_key = j.search_key
                    AND a.search_scope = j.scope
                LEFT JOIN srp_observations so ON so.artifact_id = a.artifact_id
                    AND so.vin IS NOT NULL
                GROUP BY slr.rotation_slot
            )
            SELECT c.rotation_slot AS slot, c.search_keys,
                   c.last_queued_at AT TIME ZONE 'America/Chicago' AS last_fired,
                   ROUND(EXTRACT(EPOCH FROM (now() - c.last_queued_at)) / 3600, 1) AS hours_ago,
                   COALESCE(slr.run_status, '-') AS last_status,
                   COALESCE(res.pages, 0) AS pages,
                   COALESCE(res.errors, 0) AS errors,
                   COALESCE(res.vins_observed, 0) AS vins_observed,
                   (c.last_queued_at + interval '1439 minutes')
                       AT TIME ZONE 'America/Chicago' AS next_eligible,
                   CASE
                       WHEN c.last_queued_at IS NULL THEN 'Ready now'
                       WHEN now() > c.last_queued_at + interval '1439 minutes' THEN 'Ready now'
                       ELSE 'In ' || ROUND(EXTRACT(EPOCH FROM (
                           c.last_queued_at + interval '1439 minutes' - now()
                       )) / 3600, 1)::text || 'h'
                   END AS next_status
            FROM slot_configs c
            LEFT JOIN slot_last_run slr ON slr.rotation_slot = c.rotation_slot
            LEFT JOIN slot_results res ON res.rotation_slot = c.rotation_slot
            ORDER BY c.rotation_slot
        """)
        cur.fetchall()

    def test_recent_detail_scrape_runs(self, cur):
        cur.execute("""
            WITH my_runs AS (
                SELECT * FROM runs
                WHERE trigger = 'detail scrape'
                ORDER BY started_at DESC LIMIT 20
            ), price_min AS (
                SELECT vin, MIN(observed_at) as min_observed_at
                FROM analytics.int_price_events GROUP BY vin
            ), filtered_artifacts AS (
                SELECT ra.* FROM raw_artifacts ra JOIN my_runs r USING (run_id)
            )
            SELECT r.started_at AT TIME ZONE 'America/Chicago' AS started,
                   CASE
                       WHEN r.finished_at IS NOT NULL
                       THEN ROUND(EXTRACT(EPOCH FROM (r.finished_at - r.started_at))
                                  / 60)::text || 'm'
                       ELSE ROUND(EXTRACT(EPOCH FROM (now() - r.started_at))
                                  / 60)::text || 'm (running)'
                   END AS duration,
                   r.status, r.total_count AS batch_size, r.error_count as num_errors,
                   COUNT(DISTINCT d.vin) FILTER (WHERE d.price IS NOT NULL) AS prices_refreshed,
                   COUNT(DISTINCT ra.artifact_id) FILTER (
                       WHERE d.listing_state = 'unlisted') AS newly_unlisted,
                   COUNT(DISTINCT ra.artifact_id) FILTER (
                       WHERE ap.message = 'unlisted' AND d.artifact_id IS NULL
                   ) AS unlisted_carousel_hit,
                   COUNT(DISTINCT d.vin17) FILTER (
                       WHERE pe.vin IS NULL) AS newly_mapped_vins
            FROM my_runs r
            LEFT JOIN filtered_artifacts ra ON r.run_id = ra.run_id
            LEFT JOIN artifact_processing ap ON ra.artifact_id = ap.artifact_id
            LEFT JOIN analytics.stg_detail_observations d ON ra.artifact_id = d.artifact_id
            LEFT JOIN price_min pe ON d.vin = pe.vin AND pe.min_observed_at <= r.started_at
            GROUP BY r.run_id, r.started_at, r.finished_at, r.status,
                     r.total_count, r.error_count, r.last_error
            ORDER BY started DESC
        """)
        cur.fetchall()

    def test_stale_vehicle_backlog(self, cur):
        cur.execute("""
            WITH batch_marking AS (
                SELECT q.listing_id, q.stale_reason,
                       ROW_NUMBER() OVER (
                           PARTITION BY 1 ORDER BY q.priority, q.listing_id
                       ) as priority_row
                FROM ops.ops_detail_scrape_queue q
                LEFT JOIN detail_scrape_claims c
                    ON c.listing_id = q.listing_id::uuid AND c.status = 'running'
                WHERE c.listing_id IS NULL
            )
            SELECT
                CASE
                    WHEN priority_row < 601 THEN '00_next_batch'
                    WHEN priority_row < 1201 THEN '01_following_batch'
                    WHEN priority_row < 1801 THEN '02_third_batch'
                    ELSE '03_backlog'
                END as batch_param,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'price_only%%')::varchar as price_only,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'force_stale_36h')::varchar as force_stale,
                COUNT(*) FILTER (WHERE stale_reason LIKE 'full_details')::varchar AS full_details,
                COUNT(*) FILTER (
                    WHERE stale_reason LIKE 'unmapped_carousel'
                )::varchar as unmapped_carousel,
                COUNT(*) FILTER (
                    WHERE stale_reason LIKE 'dealer_unenriched'
                )::varchar as dealer_unenriched,
                COUNT(*)::varchar AS total_count
            FROM batch_marking q
            GROUP BY 1 ORDER BY batch_param ASC
        """)
        cur.fetchall()

    def test_cooldown_backlog(self, cur):
        cur.execute("""
            WITH batch_marking AS (
                SELECT q.listing_id, q.stale_reason,
                       ROW_NUMBER() OVER (
                           PARTITION BY 1 ORDER BY q.priority, q.listing_id
                       ) as priority_row
                FROM ops.ops_detail_scrape_queue q
                LEFT JOIN detail_scrape_claims c
                    ON c.listing_id = q.listing_id::uuid AND c.status = 'running'
                WHERE c.listing_id IS NULL
            )
            SELECT bc.num_of_attempts,
                   MIN(bc.next_eligible_at) FILTER (
                       WHERE bc.next_eligible_at > now()
                   ) AT TIME ZONE 'America/Chicago' as next_attempt_at,
                   COUNT(bc.listing_id) as num_listings,
                   COUNT(bc.listing_id) FILTER (
                       WHERE bc.next_eligible_at < now()
                         AND ovs.stale_reason != 'not_stale'
                   ) as eligible_now,
                   COUNT(bc.listing_id) FILTER (
                       WHERE q.priority_row < 601 AND q.priority_row IS NOT NULL
                   ) as num_in_next_batch
            FROM analytics.stg_blocked_cooldown bc
            LEFT JOIN batch_marking q ON q.listing_id = bc.listing_id
            LEFT JOIN ops.ops_vehicle_staleness ovs ON bc.listing_id = ovs.listing_id
            GROUP BY bc.num_of_attempts
            ORDER BY bc.num_of_attempts
        """)
        cur.fetchall()

    def test_price_freshness(self, cur):
        cur.execute("""
            WITH buckets AS (
                SELECT FLOOR(LEAST(vs.price_age_hours, 24) * 2) / 2 AS age_floor,
                       vs.price_tier, vs.is_full_details_stale
                FROM ops.ops_vehicle_staleness vs
                LEFT JOIN analytics.stg_blocked_cooldown bc
                       ON bc.listing_id = vs.listing_id
                WHERE vs.price_age_hours IS NOT NULL AND bc.listing_id IS NULL
            )
            SELECT (24 - age_floor)::numeric AS hours_until_stale,
                   TO_CHAR((24 - age_floor)::numeric, 'FM90.0') || 'h' AS expiry_bucket,
                   COUNT(*) FILTER (
                       WHERE price_tier = 1 AND NOT is_full_details_stale) AS tier1,
                   COUNT(*) FILTER (
                       WHERE price_tier = 2 AND NOT is_full_details_stale) AS tier2,
                   COUNT(*) FILTER (WHERE is_full_details_stale) AS full_details_stale,
                   COUNT(*) AS total
            FROM buckets GROUP BY age_floor ORDER BY age_floor DESC
        """)
        cur.fetchall()

    def test_blocked_cooldown_histogram(self, cur):
        cur.execute("""
            WITH buckets AS (
                SELECT FLOOR(GREATEST(
                    (EXTRACT(EPOCH FROM (next_eligible_at - now())) / 3600), 0
                ) / 2) * 2 AS age_floor
                FROM analytics.stg_blocked_cooldown
                WHERE next_eligible_at IS NOT NULL
            )
            SELECT age_floor::numeric AS hours_until_eligible,
                   TO_CHAR(age_floor::numeric, 'FM90.0') || 'h' AS eligible_bucket,
                   COUNT(*) AS total
            FROM buckets GROUP BY age_floor ORDER BY age_floor DESC
        """)
        cur.fetchall()

    def test_success_rate_detail(self, cur):
        cur.execute("""
            SELECT date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
                   CASE
                       WHEN http_status = 200 THEN '200 OK'
                       WHEN http_status = 403 THEN '403 Blocked'
                       WHEN http_status IS NULL THEN 'Error/Timeout'
                       ELSE http_status::text
                   END AS result,
                   COUNT(*) AS fetches
            FROM raw_artifacts
            WHERE artifact_type = 'detail_page'
              AND fetched_at > now() - interval '7 days'
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        cur.fetchall()

    def test_success_rate_results(self, cur):
        cur.execute("""
            SELECT date_trunc('day', fetched_at AT TIME ZONE 'America/Chicago') AS day,
                   CASE
                       WHEN http_status = 200 THEN '200 OK'
                       WHEN http_status = 403 THEN '403 Blocked'
                       WHEN http_status IS NULL THEN 'Error/Timeout'
                       ELSE http_status::text
                   END AS result,
                   COUNT(*) AS fetches
            FROM raw_artifacts
            WHERE artifact_type = 'results_page'
              AND fetched_at > now() - interval '7 days'
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        cur.fetchall()

    def test_search_scrape_jobs_7d(self, cur):
        cur.execute("""
            SELECT r.run_id,
                   r.started_at AT TIME ZONE 'America/Chicago' AS run_started,
                   r.status AS run_status,
                   j.search_key, j.scope, j.status AS job_status,
                   j.artifact_count,
                   COUNT(srp.vin) as vins_recorded,
                   COUNT(srp.vin) FILTER (WHERE pe.vin IS NULL) as new_vins_recorded
            FROM runs r
            JOIN scrape_jobs j ON j.run_id = r.run_id
            LEFT JOIN raw_artifacts ra
                ON j.scope = ra.search_scope AND ra.run_id = r.run_id
                   AND ra.search_key = j.search_key
            LEFT JOIN analytics.stg_srp_observations srp
                ON ra.artifact_id = srp.artifact_id
            LEFT JOIN (
                SELECT vin, min(observed_at) as first_seen
                FROM analytics.int_price_events GROUP BY vin
            ) pe ON srp.vin17 = pe.vin AND pe.first_seen < r.started_at
            WHERE r.trigger = 'search scrape'
              AND r.started_at > now() - interval '7 days'
            GROUP BY 1,2,3,4,5,6,7
            ORDER BY r.started_at DESC, j.search_key, j.scope
        """)
        cur.fetchall()

    def test_runs_over_time(self, cur):
        cur.execute("""
            SELECT date_trunc('day', started_at AT TIME ZONE 'America/Chicago') AS day,
                   trigger, COUNT(*) AS runs,
                   COUNT(*) FILTER (WHERE status = 'success') AS successful,
                   COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
                   COUNT(*) FILTER (WHERE status = 'failed') AS failed
            FROM runs
            WHERE started_at > now() - interval '7 days'
              AND status NOT IN ('skipped', 'terminated')
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        cur.fetchall()

    def test_artifact_processing_backlog(self, cur):
        cur.execute("""
            SELECT processor, status, COUNT(*) AS count,
                   MIN(processed_at) AT TIME ZONE 'America/Chicago' AS oldest
            FROM artifact_processing
            WHERE status IN ('retry', 'processing')
            GROUP BY processor, status ORDER BY count DESC
        """)
        cur.fetchall()

    def test_terminated_runs(self, cur):
        cur.execute("""
            SELECT trigger, COUNT(*) AS terminated_count,
                   MAX(started_at) AT TIME ZONE 'America/Chicago' AS most_recent
            FROM runs
            WHERE status = 'terminated' AND started_at > now() - interval '7 days'
            GROUP BY trigger ORDER BY terminated_count DESC
        """)
        cur.fetchall()

    def test_pipeline_errors(self, cur):
        cur.execute("""
            SELECT occurred_at AT TIME ZONE 'America/Chicago' AS occurred_at_ct,
                   workflow_name, node_name, error_type, error_message
            FROM pipeline_errors ORDER BY occurred_at DESC LIMIT 50
        """)
        cur.fetchall()

    def test_dbt_build_history(self, cur):
        cur.execute("SELECT * FROM dbt_runs ORDER BY started_at DESC LIMIT 10")
        cur.fetchall()

    def test_processor_activity(self, cur):
        cur.execute("""
            SELECT processor,
                   COUNT(*) FILTER (WHERE status = 'ok') AS ok,
                   COUNT(*) FILTER (
                       WHERE status IN ('retry', 'processing')) AS pending,
                   COUNT(*) FILTER (
                       WHERE status = 'ok' AND message ILIKE '%%cloudflare%%'
                   ) AS cloudflare_blocked,
                   COUNT(*) FILTER (
                       WHERE status = 'ok'
                         AND meta->>'primary_json_present' = 'true'
                   ) AS has_primary_data,
                   MAX(processed_at) AT TIME ZONE 'America/Chicago' AS last_processed
            FROM artifact_processing GROUP BY processor ORDER BY processor
        """)
        cur.fetchall()

    def test_processing_throughput(self, cur):
        cur.execute("""
            SELECT date_trunc('hour',
                       processed_at AT TIME ZONE 'America/Chicago') AS hour,
                   processor, COUNT(*) AS processed,
                   COUNT(*) FILTER (WHERE status = 'ok') AS ok,
                   COUNT(*) FILTER (WHERE status NOT IN ('ok')) AS errors
            FROM artifact_processing
            WHERE processed_at > now() - interval '24 hours'
            GROUP BY 1, 2 ORDER BY 1 DESC, 2
        """)
        cur.fetchall()

    def test_detail_extraction_coverage(self, cur):
        cur.execute("""
            SELECT date_trunc('day',
                       ap.processed_at AT TIME ZONE 'America/Chicago') AS day,
                   COUNT(*) AS total_processed,
                   COUNT(*) FILTER (
                       WHERE ap.meta->>'primary_json_present' = 'true'
                   ) AS has_vehicle_data,
                   COUNT(*) FILTER (
                       WHERE ap.message LIKE '%%403%%') AS cloudflare_blocked,
                   COUNT(*) FILTER (
                       WHERE ap.meta->>'primary_json_present' = 'false'
                         AND (ap.message IS NULL
                              OR ap.message NOT ILIKE '%%cloudflare%%')
                   ) AS no_data,
                   ROUND(100.0 * COUNT(*) FILTER (
                       WHERE ap.meta->>'primary_json_present' = 'true'
                   ) / NULLIF(COUNT(*), 0), 1) AS extraction_pct
            FROM artifact_processing ap
            WHERE ap.processor LIKE 'cars_detail_page__%%'
              AND ap.processed_at > now() - interval '14 days'
            GROUP BY 1 ORDER BY 1 DESC
        """)
        cur.fetchall()

    def test_pg_stat_activity(self, cur):
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE state = 'active') AS active,
                   COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
                   ROUND(MAX(
                       CASE WHEN state = 'active' AND query_start IS NOT NULL
                            THEN EXTRACT(EPOCH FROM (now() - query_start))
                       END
                   )::numeric, 1) AS longest_query_s
            FROM pg_stat_activity WHERE backend_type = 'client backend'
        """)
        cur.fetchone()

    def test_long_running_queries(self, cur):
        cur.execute("""
            SELECT pid, state,
                   ROUND(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 1) AS duration_s,
                   LEFT(query, 80) AS query
            FROM pg_stat_activity
            WHERE state = 'active'
              AND query_start < now() - interval '5 seconds'
              AND backend_type = 'client backend'
            ORDER BY duration_s DESC
        """)
        cur.fetchall()
