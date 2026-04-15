import pytest

pytestmark = pytest.mark.integration


def _seed_ops(dbt_cur):
    dbt_cur.execute(
        """
            INSERT INTO public.runs (run_id, started_at, status, trigger)
            VALUES ('aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', now(), 'running', 'integration_test')
            ON CONFLICT (run_id) DO NOTHING
        """
    )

    dbt_cur.execute(
        """
            INSERT INTO public.raw_artifacts (
                artifact_id, run_id, source, artifact_type, url, fetched_at, filepath,
                search_key, search_scope
            )
            VALUES
                (1, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'results_page',
                'https://www.dummy.com', now() - interval '25 hours', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national'),
                (2, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'results_page',
                'https://www.dummy.com', now() - interval '200 hours', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national'),
                (3, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'results_page',
                'https://www.dummy.com', now() - interval '2 hours', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national'),
                (4, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'detail_page',
                'https://www.dummy.com', now() - interval '1 hour', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national'),
                (5, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'detail_page',
                'https://www.dummy.com', now() - interval '25 hours', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national'),
                (6, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'detail_page',
                'https://www.dummy.com', now() - interval '36 hours', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national'),
                (7, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 'detail_page',
                'https://www.dummy.com', now() - interval '170 hours', '/data/raw/fakefile.html',
                'honda-cr_v_hybrid', 'national')

        """
    )

    dbt_cur.execute(
        """
            INSERT INTO public.srp_observations (
                id, artifact_id, run_id, listing_id, created_at, fetched_at, vin, make, model,
                canonical_detail_url
            )
            VALUES
                (1, 1, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'L1',
                 now() - interval '25 hours', now() - interval '25 hours',
                 'L100000PRICESTALE', 'honda', 'crv', 'https://nowhere.com'),
                (2, 2, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'L2',
                 now() - interval '200 hours', now() - interval '200 hours',
                 'L20000FULLDETAILS', 'honda', 'crv', 'https://nowhere.com'),
                (3, 3, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'L3',
                 now() - interval '2 hours', now() - interval '2 hours',
                 'L30000000NOTSTALE', 'honda', 'crv', 'https://nowhere.com')
        """
    )

    dbt_cur.execute(
        """
            INSERT INTO public.detail_observations (
                id, artifact_id, listing_id, fetched_at, listing_state, vin, make, model, price
            )
            VALUES
                (1, 4, 'L3', now() - interval '1 hour', 'active', 'L30000000NOTSTALE', 
                'honda', 'crv', 10000),
                (2, 5, 'L4', now() - interval '25 hours', 'active', 'L4STALEONCOOLDOWN', 
                'honda', 'crv', 10000),
                (3, 6, 'L5', now() - interval '36 hours', 'active', 'L50STALEFULLBLOCK', 
                'honda', 'crv', 10000),
                (4, 7, 'L2', now() - interval '170 hours', 'active', 'L20000FULLDETAILS', 
                'honda', 'crv', 10000)
                
        """
    )

    dbt_cur.execute(
        """
            INSERT INTO public.blocked_cooldown (
                listing_id, first_attempt_at, last_attempted_at, num_of_attempts
            )
            VALUES
                ('L5', now() - interval '5 days', now() - interval '1 hour', 5),
                ('L4', now() - interval '2 days', now() - interval '1 hour', 2),
                ('L2', now() - interval '3 days', now() - interval '13 hours', 1)
        """
    )


@pytest.fixture(scope="module", autouse=True)
def seed_and_build(dbt_conn, run_dbt):
    with dbt_conn.cursor() as cur:
        _seed_ops(cur)
    run_dbt("+ops_detail_scrape_queue")
    yield
    with dbt_conn.cursor() as cur:
        cur.execute("""
            TRUNCATE public.runs, public.raw_artifacts, public.srp_observations,
                     public.detail_observations, public.blocked_cooldown CASCADE
        """)


# --- ops_vehicle_staleness ---

def test_price_stale_vin_is_stale(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT is_price_stale, stale_reason
        FROM analytics_ci.ops_vehicle_staleness
        WHERE vin = 'L100000PRICESTALE'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is True


def test_full_details_stale_vin(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT is_full_details_stale, stale_reason
        FROM analytics_ci.ops_vehicle_staleness
        WHERE vin = 'L20000FULLDETAILS'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_full_details_stale"] is True
    assert row["stale_reason"] == "full_details"


def test_not_stale_vin(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT is_price_stale, is_full_details_stale
        FROM analytics_ci.ops_vehicle_staleness
        WHERE vin = 'L30000000NOTSTALE'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is False
    assert row["is_full_details_stale"] is False


def test_price_only_stale_reason(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT is_price_stale, stale_reason
        FROM analytics_ci.ops_vehicle_staleness
        WHERE vin = 'L4STALEONCOOLDOWN'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is True
    assert row["stale_reason"] == "price_only"


def test_fully_blocked_vin_is_stale(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT is_price_stale, stale_reason
        FROM analytics_ci.ops_vehicle_staleness
        WHERE vin = 'L50STALEFULLBLOCK'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is True
    assert row["stale_reason"] == "price_only"


# --- ops_detail_scrape_queue ---

def test_stale_no_cooldown_in_queue(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT listing_id, priority
        FROM analytics_ci.ops_detail_scrape_queue
        WHERE listing_id = 'L1'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["priority"] == 1


def test_eligible_cooldown_in_queue(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM analytics_ci.ops_detail_scrape_queue
        WHERE listing_id = 'L2'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None


def test_not_stale_not_in_queue(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM analytics_ci.ops_detail_scrape_queue
        WHERE listing_id = 'L3'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is None


def test_cooldown_ineligible_not_in_queue(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM analytics_ci.ops_detail_scrape_queue
        WHERE listing_id = 'L4'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is None


def test_fully_blocked_not_in_queue(analytics_ci_cur):
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM analytics_ci.ops_detail_scrape_queue
        WHERE listing_id = 'L5'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is None
