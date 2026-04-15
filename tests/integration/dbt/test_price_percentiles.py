import pytest

pytestmark = pytest.mark.integration


def _seed_price_data(dbt_cur):
    dbt_cur.execute("""
                    INSERT INTO public.runs (
                        run_id, started_at, status, trigger
                    )
                    VALUES
                        ('aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', now(), 'running',
                         'integration_test')
                    ON CONFLICT (run_id) DO NOTHING
                    """)

    dbt_cur.execute("""
                    INSERT INTO public.raw_artifacts 
                        (artifact_id, run_id, source, artifact_type, url, fetched_at, filepath,
                         search_scope)
                    VALUES
                        (1, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '1 day', 
                            '/data/raw/fakefile.html', 'national'),
                        (2, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '1 day', 
                            '/data/raw/fakefile.html', 'national'),
                        (3, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '1 day', 
                            '/data/raw/fakefile.html', 'national'),
                        (4, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '2 days', 
                            '/data/raw/fakefile.html', 'national'),
                        (5, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '2 days', 
                            '/data/raw/fakefile.html', 'national'),
                        (6, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '4 days', 
                            '/data/raw/fakefile.html', 'national'),
                        (7, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '4 days', 
                            '/data/raw/fakefile.html', 'national'),
                        (8, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '2 days', 
                            '/data/raw/fakefile.html', 'national')
                    """)
    
    dbt_cur.execute("""
                    INSERT INTO public.srp_observations
                        (id, artifact_id, run_id, listing_id, created_at, fetched_at,
                         make, model, trim, price, vin)
                    VALUES
                        (1, 1, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L1', now() - interval '1 day', now() - interval '1 day',
                            'Test-Make', 'Test-Model', 'Test-Trim', 10000,
                            'SRPL1000000010000'),
                        (2, 2, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L2', now() - interval '1 day', now() - interval '1 day',
                            'Test-Make', 'Test-Model', 'Test-Trim', 20000,
                            'SRPL2000000020000'),
                        (3, 3, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L3', now() - interval '1 day', now() - interval '1 day',
                            'Test-Make', 'Test-Model', 'Test-Trim', 30000,
                            'SRPL3000000030000'),
                        (4, 4, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L4', now() - interval '2 days', now() - interval '2 days',
                            'Test-Make', 'Test-Model', 'Test-Trim', 40000,
                            'SRPL4000000040000'),
                        (5, 5, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L5', now() - interval '2 days', now() - interval '2 days',
                            'Test-Make', 'Test-Model', 'Test-Trim', 50000,
                            'SRPL5000000050000'),
                        (6, 6, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L6', now() - interval '4 days', now() - interval '4 days',
                            'Test-Make', 'Test-Model', 'Test-Trim', 15000,
                            'SRPL6000000015000'),
                        (7, 7, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d',
                            'L7', now() - interval '4 days', now() - interval '4 days',
                            'Test-Make', 'Test-Model', 'Test-Trim', 25000,
                            'SRPL7000000025000'),
                        (8, 8, 'L8', now() - interval '2 days', now() - interval '2 days',
                            'Test-Make-Two', 'Test-Model', 'Test-Trim', 35000,
                            'SRPL8000000035000')
                    """)
    

@pytest.fixture(scope="module", autouse=True)
def seed_and_build(dbt_conn, run_dbt):
    with dbt_conn.cursor() as cur:
        _seed_price_data(cur)
    
    run_dbt("stg_raw_artifacts stg_srp_observations int_price_percentiles_by_vin")
    yield
    with dbt_conn.cursor() as cur:
        cur.execute("""
            TRUNCATE public.runs, public.raw_artifacts, public.srp_observations CASCADE
        """)


def test_cheapest(analytics_ci_cur):
    analytics_ci_cur.execute(
        """
            SELECT vin, national_price_percentile 
            FROM analytics_ci.int_price_percentiles_by_vin 
            WHERE vin = 'SRPL1000000010000'
        """
    )
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["national_price_percentile"] <= 0.01


def test_middle(analytics_ci_cur):
    analytics_ci_cur.execute(
        """
            SELECT vin, national_price_percentile 
            FROM analytics_ci.int_price_percentiles_by_vin 
            WHERE vin = 'SRPL3000000030000'
        """
    )
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert 0.49 <= row["national_price_percentile"] <= 0.51


def test_high(analytics_ci_cur):
    analytics_ci_cur.execute(
        """
            SELECT vin, national_price_percentile 
            FROM analytics_ci.int_price_percentiles_by_vin 
            WHERE vin = 'SRPL5000000050000'
        """
    )
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["national_price_percentile"] >= 0.99
