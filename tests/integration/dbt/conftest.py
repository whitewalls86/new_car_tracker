"""
Layer 2 dbt integration test fixtures.

Key differences from Layer 1:
- dbt_conn uses autocommit=True so seeded rows are immediately visible to the
  dbt subprocess (which runs in a separate process and cannot see open transactions).
- All test data is seeded once at session start via seed_and_build, which also
  runs the full dbt DAG. Individual test modules contain only assertions.
- Teardown (TRUNCATE) happens after the full session completes.
- run_dbt shells out `dbt build` and fails the test on non-zero exit.
- analytics_ci_cur reads from the analytics_ci schema where the ci target writes.

ID scheme for seeded rows (prevents primary key conflicts across groups):
  100s — VIN mapping scenarios
  200s — price percentile cohort (Test-Make / Test-Model)
  300s — ops / staleness scenarios (honda / crv)
  400s — deal score target + cohort (honda / crv / Hybrid)
  500s — int_vehicle_attributes (Attr-Make-SRP vs Attr-Make-DET)
  600s — int_price_history_by_vin (PH-Make / PH-Model, 5-event sequence)
  700s — int_listing_days_on_market (DOM-Make / DOM-Model, national + local)
  800s — int_price_events dedup (PE-Make / PE-Model, same-timestamp SRP+detail)
  900s — mart_vehicle_snapshot listing_state='unlisted' (honda / crv, SRP-only > 7 days)
"""
import os
import subprocess
from urllib.parse import urlparse

import psycopg2
import pytest
from psycopg2.extras import RealDictCursor

_DEFAULT_URL = "postgresql://cartracker:cartracker@localhost:5432/cartracker"
_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", _DEFAULT_URL)
_DBT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../dbt"))

_RUN_ID = "aa57b5bc-c909-4fc7-8965-dfe9657c4e7d"


def _parse_dsn(url: str) -> dict:
    p = urlparse(url)
    return {
        "host": p.hostname or "localhost",
        "port": p.port or 5432,
        "dbname": p.path.lstrip("/") or "cartracker",
        "user": p.username or "cartracker",
        "password": p.password or "cartracker",
    }


def _seed_all(cur):
    """
    Seed all source data needed by every Layer 2 dbt test module in one shot.

    Groups:
      VML* listing_ids / 1xx artifact & obs IDs  — int_listing_to_vin
      PL*  listing_ids / 2xx artifact & obs IDs  — int_price_percentiles_by_vin
      OL*  listing_ids / 3xx artifact & obs IDs  — ops_vehicle_staleness, ops_detail_scrape_queue
      DL*  listing_ids / 4xx artifact & obs IDs  — mart_deal_scores
      AL*  listing_ids / 5xx artifact & obs IDs  — int_vehicle_attributes
      PHL* listing_ids / 6xx artifact & obs IDs  — int_price_history_by_vin
      DOM* listing_ids / 7xx artifact & obs IDs  — int_listing_days_on_market
      PE*  listing_ids / 8xx artifact & obs IDs  — int_price_events
      MVS* listing_ids / 9xx artifact & obs IDs  — mart_vehicle_snapshot
    """

    # ------------------------------------------------------------------
    # runs
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.runs (run_id, started_at, status, trigger)
        VALUES (%s, now(), 'running', 'integration_test')
        ON CONFLICT (run_id) DO NOTHING
    """, (_RUN_ID,))

    # ------------------------------------------------------------------
    # raw_artifacts
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.raw_artifacts
            (artifact_id, run_id, source, artifact_type, url, fetched_at, filepath,
             search_key, search_scope)
        VALUES
            -- VIN mapping group (100s): used only for listing→VIN resolution
            (101, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (102, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (103, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 hours', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (104, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '2 hours', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (105, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),

            -- Price percentile group (200s): national scope
            -- 201-205 within the 3-day staleness window; 206-207 outside (4 days)
            (201, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (202, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (203, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (204, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (205, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (206, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '4 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (207, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '4 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (208, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),

            -- Ops group (300s): honda-cr_v_hybrid, national, varied ages for staleness
            (301, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '25 hours',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (302, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '200 hours', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (303, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 hours',   '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (304, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '1 hour',    '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (305, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '25 hours',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (306, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '36 hours',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (307, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '170 hours', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),

            -- Deal scores group (400s): honda-cr_v_hybrid, national
            -- 401-405: current cohort (within percentile window)
            -- 406-407: historical target VIN obs (outside window, for DOM/price history)
            -- 408:     active detail scrape for target VIN
            (401, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',   '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (402, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',   '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (403, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',   '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (404, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',   '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (405, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',   '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (406, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '30 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (407, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '45 days', '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national'),
            (408, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html',
             'honda-cr_v_hybrid', 'national')
    """, (_RUN_ID,) * 28)

    # ------------------------------------------------------------------
    # srp_observations
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.srp_observations
            (id, artifact_id, run_id, listing_id, created_at, fetched_at,
             vin, make, model, "trim", price, canonical_detail_url)
        VALUES
            -- VIN mapping group: no make/model/price — only listing→VIN resolution matters
            (101, 101, %s, 'VML1', now()-interval '1 hour',  now()-interval '1 hour',
             'L1SRP000000000001', NULL, NULL, NULL, NULL, 'https://nowhere.com'),
            (102, 103, %s, 'VML2', now()-interval '2 hours', now()-interval '2 hours',
             'L2SRP000000000001', NULL, NULL, NULL, NULL, 'https://nowhere.com'),
            (103, 105, %s, 'VML3', now()-interval '1 hour',  now()-interval '1 hour',
             'L3SRP000000000001', NULL, NULL, NULL, NULL, 'https://nowhere.com'),

            -- Price percentile group: Test-Make / Test-Model / Test-Trim cohort
            -- 201-205 within staleness window; 206-207 outside (verifies window filter)
            -- 208 uses Test-Make-Two (different make, separate cohort)
            (201, 201, %s, 'PL1', now()-interval '1 day',  now()-interval '1 day',
             'SRPL1000000010000', 'Test-Make', 'Test-Model', 'Test-Trim', 10000, 'https://nowhere.com'),
            (202, 202, %s, 'PL2', now()-interval '1 day',  now()-interval '1 day',
             'SRPL2000000020000', 'Test-Make', 'Test-Model', 'Test-Trim', 20000, 'https://nowhere.com'),
            (203, 203, %s, 'PL3', now()-interval '1 day',  now()-interval '1 day',
             'SRPL3000000030000', 'Test-Make', 'Test-Model', 'Test-Trim', 30000, 'https://nowhere.com'),
            (204, 204, %s, 'PL4', now()-interval '2 days', now()-interval '2 days',
             'SRPL4000000040000', 'Test-Make', 'Test-Model', 'Test-Trim', 40000, 'https://nowhere.com'),
            (205, 205, %s, 'PL5', now()-interval '2 days', now()-interval '2 days',
             'SRPL5000000050000', 'Test-Make', 'Test-Model', 'Test-Trim', 50000, 'https://nowhere.com'),
            (206, 206, %s, 'PL6', now()-interval '4 days', now()-interval '4 days',
             'SRPL6000000015000', 'Test-Make', 'Test-Model', 'Test-Trim', 15000, 'https://nowhere.com'),
            (207, 207, %s, 'PL7', now()-interval '4 days', now()-interval '4 days',
             'SRPL7000000025000', 'Test-Make', 'Test-Model', 'Test-Trim', 25000, 'https://nowhere.com'),
            (208, 208, %s, 'PL8', now()-interval '2 days', now()-interval '2 days',
             'SRPL8000000035000', 'Test-Make-Two', 'Test-Model', 'Test-Trim', 35000, 'https://nowhere.com'),

            -- Ops group: honda / crv, no price (price comes from detail_observations)
            (301, 301, %s, 'OL1', now()-interval '25 hours',  now()-interval '25 hours',
             'L100000PRICESTALE', 'honda', 'crv', NULL, NULL, 'https://nowhere.com'),
            (302, 302, %s, 'OL2', now()-interval '200 hours', now()-interval '200 hours',
             'L20000FULLDETAILS', 'honda', 'crv', NULL, NULL, 'https://nowhere.com'),
            (303, 303, %s, 'OL3', now()-interval '2 hours',   now()-interval '2 hours',
             'L30000000NOTSTALE', 'honda', 'crv', NULL, NULL, 'https://nowhere.com'),

            -- Deal scores group: honda / crv / Hybrid with price
            -- Target VIN (DL1) has three SRP observations forming a price drop sequence:
            --   45 days ago @40k → 30 days ago @38k → 1 day ago @35k (2 drops)
            -- Cohort VINs (DL2-DL5) within percentile window; target is cheapest -> percentile=0
            (401, 401, %s, 'DL1', now()-interval '1 day',   now()-interval '1 day',
             'DS0TARGET00000001', 'honda', 'crv', 'Hybrid', 35000, 'https://nowhere.com'),
            (402, 402, %s, 'DL2', now()-interval '1 day',   now()-interval '1 day',
             'DS0COHORT00000002', 'honda', 'crv', 'Hybrid', 45000, 'https://nowhere.com'),
            (403, 403, %s, 'DL3', now()-interval '1 day',   now()-interval '1 day',
             'DS0COHORT00000003', 'honda', 'crv', 'Hybrid', 50000, 'https://nowhere.com'),
            (404, 404, %s, 'DL4', now()-interval '1 day',   now()-interval '1 day',
             'DS0COHORT00000004', 'honda', 'crv', 'Hybrid', 55000, 'https://nowhere.com'),
            (405, 405, %s, 'DL5', now()-interval '1 day',   now()-interval '1 day',
             'DS0COHORT00000005', 'honda', 'crv', 'Hybrid', 60000, 'https://nowhere.com'),
            (406, 406, %s, 'DL1', now()-interval '30 days', now()-interval '30 days',
             'DS0TARGET00000001', 'honda', 'crv', 'Hybrid', 38000, 'https://nowhere.com'),
            (407, 407, %s, 'DL1', now()-interval '45 days', now()-interval '45 days',
             'DS0TARGET00000001', 'honda', 'crv', 'Hybrid', 40000, 'https://nowhere.com')
    """, (_RUN_ID,) * 21)

    # ------------------------------------------------------------------
    # detail_observations
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.detail_observations
            (id, artifact_id, listing_id, fetched_at, listing_state,
             vin, make, model, "trim", price, msrp)
        VALUES
            -- VIN mapping group: listing_id determines VIN priority vs SRP
            -- VML2: detail (1h ago) fresher than SRP (2h ago) → detail VIN wins
            -- VML3: detail (2h ago) older than SRP (1h ago) → SRP VIN wins
            (101, 102, 'VML2', now()-interval '1 hour',  'active',
             'L2DET000000000001', NULL, NULL, NULL, NULL, NULL),
            (102, 104, 'VML3', now()-interval '2 hours', 'active',
             'L3DET000000000001', NULL, NULL, NULL, NULL, NULL),

            -- Ops group: price here is what gives the non-stale VIN a valid price_observed_at.
            -- Without price in detail_observations, price_observed_at would be NULL and
            -- every VIN would be flagged is_price_stale regardless of observation age.
            (301, 304, 'OL3', now()-interval '1 hour',    'active',
             'L30000000NOTSTALE', 'honda', 'crv', NULL, 10000, NULL),
            (302, 305, 'OL4', now()-interval '25 hours',  'active',
             'L4STALEONCOOLDOWN', 'honda', 'crv', NULL, 10000, NULL),
            (303, 306, 'OL5', now()-interval '36 hours',  'active',
             'L50STALEFULLBLOCK', 'honda', 'crv', NULL, 10000, NULL),
            (304, 307, 'OL2', now()-interval '170 hours', 'active',
             'L20000FULLDETAILS', 'honda', 'crv', NULL, 10000, NULL),

            -- Deal scores group: target VIN with msrp and trim for score calculation
            -- msrp=50000, price=35000 → 30% discount → full 35-pt MSRP component
            (401, 408, 'DL1', now()-interval '1 hour', 'active',
             'DS0TARGET00000001', 'honda', 'crv', 'Hybrid', 35000, 50000)
    """)

    # ------------------------------------------------------------------
    # blocked_cooldown (ops scenarios only)
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.blocked_cooldown
            (listing_id, first_attempt_at, last_attempted_at, num_of_attempts)
        VALUES
            -- OL5 / L50STALEFULLBLOCK: fully blocked (5 attempts)
            ('OL5', now()-interval '5 days',  now()-interval '1 hour',   5),
            -- OL4 / L4STALEONCOOLDOWN: on cooldown, not yet eligible
            --   next_eligible_at = last_attempted + 12h * 2^(2-1) = 1h ago + 24h = 23h from now
            ('OL4', now()-interval '2 days',  now()-interval '1 hour',   2),
            -- OL2 / L20000FULLDETAILS: cooldown elapsed, eligible for re-scrape
            --   next_eligible_at = last_attempted + 12h * 2^(1-1) = 13h ago + 12h = 1h ago (past)
            ('OL2', now()-interval '3 days',  now()-interval '13 hours', 1)
    """)

    # ==================================================================
    # Phase 3 seed data — intermediate model coverage (groups 500–900)
    # ==================================================================

    # ------------------------------------------------------------------
    # raw_artifacts (500–900 groups)
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.raw_artifacts
            (artifact_id, run_id, source, artifact_type, url, fetched_at, filepath,
             search_key, search_scope)
        VALUES
            -- 500s: int_vehicle_attributes (AL1=detail-wins, AL2=SRP-only, AL3=SRP-fresher)
            (501, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 hours', '/data/raw/fakefile.html', 'attr-test', 'national'),
            (502, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html', 'attr-test', 'national'),
            (503, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html', 'attr-test', 'national'),
            (504, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 hour',  '/data/raw/fakefile.html', 'attr-test', 'national'),
            (505, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '2 hours', '/data/raw/fakefile.html', 'attr-test', 'national'),

            -- 600s: int_price_history_by_vin (5 events: 100→120→90→90→110)
            (601, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '10 days', '/data/raw/fakefile.html', 'ph-test', 'national'),
            (602, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '8 days',  '/data/raw/fakefile.html', 'ph-test', 'national'),
            (603, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '6 days',  '/data/raw/fakefile.html', 'ph-test', 'national'),
            (604, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '4 days',  '/data/raw/fakefile.html', 'ph-test', 'national'),
            (605, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 days',  '/data/raw/fakefile.html', 'ph-test', 'national'),

            -- 700s: int_listing_days_on_market
            -- 701-703: DOM1 national SRP at t-10d / t-5d / t-1d
            -- 704:     DOM2 national SRP at t-10d
            -- 705:     DOM2 local SRP at t-2d  (triggers first_seen_local_at)
            -- 706:     DOM1 detail at t-3d (later than t-10d SRP → first_seen unchanged)
            (701, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '10 days', '/data/raw/fakefile.html', 'dom-test', 'national'),
            (702, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '5 days',  '/data/raw/fakefile.html', 'dom-test', 'national'),
            (703, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '1 day',   '/data/raw/fakefile.html', 'dom-test', 'national'),
            (704, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '10 days', '/data/raw/fakefile.html', 'dom-test', 'national'),
            (705, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '2 days',  '/data/raw/fakefile.html', 'dom-test', 'local'),
            (706, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             now() - interval '3 days',  '/data/raw/fakefile.html', 'dom-test', 'national'),

            -- 800s: int_price_events dedup
            -- Pinned timestamp ensures SRP and detail share an IDENTICAL fetched_at so that
            -- (vin, observed_at, price) matches exactly and DISTINCT ON collapses to 1 row.
            -- now()-interval evaluated across separate cur.execute() calls would differ by
            -- microseconds, preventing dedup.
            (801, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             '2026-01-10 12:00:00+00'::timestamptz, '/data/raw/fakefile.html', 'pe-test', 'national'),
            (802, %s, 'cars.com', 'detail_page',  'https://www.dummy.com',
             '2026-01-10 12:00:00+00'::timestamptz, '/data/raw/fakefile.html', 'pe-test', 'national'),

            -- 900s: mart_vehicle_snapshot listing_state='unlisted'
            -- SRP-only honda/crv VIN last seen 10 days ago → inferred 'unlisted'(> 7-day threshold)
            (901, %s, 'cars.com', 'results_page', 'https://www.dummy.com',
             now() - interval '10 days', '/data/raw/fakefile.html', 'honda-cr_v_hybrid', 'national')
    """, (_RUN_ID,) * 19)

    # ------------------------------------------------------------------
    # srp_observations (500–900 groups)
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.srp_observations
            (id, artifact_id, run_id, listing_id, created_at, fetched_at,
             vin, make, model, "trim", price, canonical_detail_url)
        VALUES
            -- 500s: int_vehicle_attributes
            -- AL1: SRP (2h ago) — detail will override (detail is 1h ago, priority 0)
            (501, 501, %s, 'AL1', now()-interval '2 hours', now()-interval '2 hours',
             'VA0ATTRDETAIL0001', 'Attr-Make-SRP', 'Attr-Model', 'SRP-Trim', 30000,
             'https://nowhere.com'),
            -- AL2: SRP-only — no detail obs seeded for this VIN
            (502, 503, %s, 'AL2', now()-interval '1 hour',  now()-interval '1 hour',
             'VB0ATTRSRPONLY001', 'Attr-Make-SRP', 'Attr-Model', 'SRP-Trim', 30000,
             'https://nowhere.com'),
            -- AL3: SRP (1h ago) — detail (2h ago) is older, but detail source_priority always wins
            (503, 504, %s, 'AL3', now()-interval '1 hour',  now()-interval '1 hour',
             'VC0ATTRSRPFRESHER', 'Attr-Make-SRP', 'Attr-Model', 'SRP-Trim', 30000,
             'https://nowhere.com'),

            -- 600s: int_price_history_by_vin — 5 events for PH0PRICEHISTORY01
            -- prices in ascending time order: 100, 120, 90, 90, 110
            -- drops: 120→90 (1); increases: 100→120, 90→110 (2); flat: 90→90
            (601, 601, %s, 'PHL1', now()-interval '10 days', now()-interval '10 days',
             'PH0PRICEHISTORY01', 'PH-Make', 'PH-Model', NULL, 100,  'https://nowhere.com'),
            (602, 602, %s, 'PHL1', now()-interval '8 days',  now()-interval '8 days',
             'PH0PRICEHISTORY01', 'PH-Make', 'PH-Model', NULL, 120,  'https://nowhere.com'),
            (603, 603, %s, 'PHL1', now()-interval '6 days',  now()-interval '6 days',
             'PH0PRICEHISTORY01', 'PH-Make', 'PH-Model', NULL, 90,   'https://nowhere.com'),
            (604, 604, %s, 'PHL1', now()-interval '4 days',  now()-interval '4 days',
             'PH0PRICEHISTORY01', 'PH-Make', 'PH-Model', NULL, 90,   'https://nowhere.com'),
            (605, 605, %s, 'PHL1', now()-interval '2 days',  now()-interval '2 days',
             'PH0PRICEHISTORY01', 'PH-Make', 'PH-Model', NULL, 110,  'https://nowhere.com'),

            -- 700s: int_listing_days_on_market
            (701, 701, %s, 'DOM1', now()-interval '10 days', now()-interval '10 days',
             'DOM0SRPONLYMULT01', 'DOM-Make', 'DOM-Model', NULL, NULL, 'https://nowhere.com'),
            (702, 702, %s, 'DOM1', now()-interval '5 days',  now()-interval '5 days',
             'DOM0SRPONLYMULT01', 'DOM-Make', 'DOM-Model', NULL, NULL, 'https://nowhere.com'),
            (703, 703, %s, 'DOM1', now()-interval '1 day',   now()-interval '1 day',
             'DOM0SRPONLYMULT01', 'DOM-Make', 'DOM-Model', NULL, NULL, 'https://nowhere.com'),
            -- DOM2: national at t-10d and local at t-2d to exercise scope split
            (704, 704, %s, 'DOM2', now()-interval '10 days', now()-interval '10 days',
             'DOM0NATLOCALSPLIT', 'DOM-Make', 'DOM-Model', NULL, NULL, 'https://nowhere.com'),
            (705, 705, %s, 'DOM2', now()-interval '2 days',  now()-interval '2 days',
             'DOM0NATLOCALSPLIT', 'DOM-Make', 'DOM-Model', NULL, NULL, 'https://nowhere.com'),

            -- 800s: int_price_events dedup — SRP side; pinned timestamp matches detail obs
            (801, 801, %s, 'PE1', '2026-01-10 12:00:00+00'::timestamptz, '2026-01-10 12:00:00+00'::timestamptz,
             'PE0PRICEEVTDEDUP1', 'PE-Make', 'PE-Model', NULL, 22000, 'https://nowhere.com'),

            -- 900s: mart_vehicle_snapshot — honda/crv SRP-only, last seen 10 days ago
            (901, 901, %s, 'MVS1', now()-interval '10 days', now()-interval '10 days',
             'MVS0SRPONLYOLD001', 'honda', 'crv', NULL, NULL, 'https://nowhere.com')
    """, (_RUN_ID,) * 15)

    # ------------------------------------------------------------------
    # detail_observations (500–800 groups; 900s is SRP-only by design)
    # ------------------------------------------------------------------
    cur.execute("""
        INSERT INTO public.detail_observations
            (id, artifact_id, listing_id, fetched_at, listing_state,
             vin, make, model, "trim", price, msrp)
        VALUES
            -- 500s: int_vehicle_attributes
            -- AL1: detail (1h ago) fresher than SRP (2h ago) → detail wins
            (501, 502, 'AL1', now()-interval '1 hour',  'active',
             'VA0ATTRDETAIL0001', 'Attr-Make-DET', 'Attr-Model', 'DET-Trim', 28000, 45000),
            -- AL3: detail (2h ago) older than SRP (1h ago), detail source_priority=0 always wins
            (502, 505, 'AL3', now()-interval '2 hours', 'active',
             'VC0ATTRSRPFRESHER', 'Attr-Make-DET', 'Attr-Model', 'DET-Trim', 28000, 45000),

            -- 700s: int_listing_days_on_market
            -- DOM1 detail at t-3d: later than earliest SRP (t-10d) → first_seen_at unchanged
            (701, 706, 'DOM1', now()-interval '3 days', 'active',
             'DOM0SRPONLYMULT01', NULL, NULL, NULL, NULL, NULL),

            -- 800s: int_price_events dedup — detail side; pinned timestamp matches SRP obs
            -- Same (vin, observed_at, price) as SRP obs 801 → expect 1 output row, source='detail'
            (801, 802, 'PE1', '2026-01-10 12:00:00+00'::timestamptz, 'active',
             'PE0PRICEEVTDEDUP1', 'PE-Make', 'PE-Model', NULL, 22000, NULL)
    """)


@pytest.fixture(scope="session")
def dbt_conn():
    """
    Session-scoped autocommit connection.

    autocommit=True means every INSERT is committed immediately, making seeded
    rows visible to the dbt subprocess without any explicit COMMIT call.
    """
    conn = psycopg2.connect(**_parse_dsn(_DATABASE_URL))
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture()
def dbt_cur(dbt_conn):
    """Function-scoped RealDictCursor on the autocommit connection (for seeding)."""
    with dbt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur


@pytest.fixture(scope="session")
def run_dbt():
    """
    Returns a callable that shells out `dbt build --select <selector>`.

    Usage inside a test or module-scoped fixture:
        run_dbt("stg_srp_observations stg_raw_artifacts int_listing_to_vin")

    Fails the test immediately if dbt exits non-zero.
    """
    def _run(select: str):
        result = subprocess.run(
            [
                "dbt", "build",
                "--select", select,
                "--target", "ci",
                "--profiles-dir", ".",
                "--full-refresh",
                "--fail-fast",
            ],
            cwd=_DBT_DIR,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(
                f"dbt build failed for selector '{select}':\n"
                f"{result.stdout}\n{result.stderr}"
            )
        return result

    return _run


@pytest.fixture(scope="session", autouse=True)
def seed_and_build(dbt_conn, run_dbt):
    """
    Seed all source data and run the full dbt DAG once for the entire test session.

    '+mart_deal_scores +ops_detail_scrape_queue' covers every model under test:
      - All staging models (stg_raw_artifacts, stg_srp_observations, stg_detail_observations,
        stg_blocked_cooldown, stg_search_configs, stg_dealers, stg_detail_carousel_hints)
      - All intermediate models including int_listing_to_vin, int_price_percentiles_by_vin,
        int_price_history_by_vin, int_listing_days_on_market, int_vehicle_attributes, etc.
      - mart_vehicle_snapshot, mart_deal_scores
      - ops_vehicle_staleness, ops_detail_scrape_queue
    """
    with dbt_conn.cursor() as cur:
        _seed_all(cur)

    run_dbt("+mart_deal_scores +ops_detail_scrape_queue")

    yield

    with dbt_conn.cursor() as cur:
        cur.execute("""
            TRUNCATE public.runs, public.raw_artifacts, public.srp_observations,
                     public.detail_observations, public.blocked_cooldown CASCADE
        """)


@pytest.fixture()
def analytics_ci_cur(dbt_conn):
    """
    Function-scoped RealDictCursor pre-set to the analytics_ci schema.

    Use this to read dbt model output after run_dbt completes.
    """
    with dbt_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SET search_path TO analytics_ci")
        yield cur
