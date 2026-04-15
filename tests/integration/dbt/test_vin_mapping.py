import pytest

pytestmark = pytest.mark.integration


def _seed_int_listing_to_vin(dbt_cur):
    dbt_cur.execute("""
                    INSERT INTO public.raw_artifacts
                        (artifact_id, run_id, source, artifact_type, url, fetched_at, filepath)
                    VALUES
                        (1, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '1 hour', 
                            '/data/raw/fakefile.html'),
                        (2, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'detail_page', 'https://www.dummy.com', now() - interval '1 hour', 
                            '/data/raw/fakefile.html'),
                        (3, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '2 hours', 
                            '/data/raw/fakefile.html'),
                        (4, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'detail_page', 'https://www.dummy.com', now() - interval '2 hours', 
                            '/data/raw/fakefile.html'),
                        (5, 'aa57b5bc-c909-4fc7-8965-dfe9657c4e7d', 'cars.com', 
                            'results_page', 'https://www.dummy.com', now() - interval '1 hour', 
                            '/data/raw/fakefile.html')
                    """)
    
    dbt_cur.execute("""
                    INSERT INTO public.srp_observations
                        (id, artifact_id, listing_id, created_at, fetched_at, vin)
                    VALUES
                        (1, 1, 'L1', now() - interval '1 hour', now() - interval '1 hour', 
                            'L1SRP000000000001'),
                        (2, 3, 'L2', now() - interval '2 hours', now() - interval '2 hours', 
                            'L2SRP000000000001'),
                        (3, 5, 'L3', now() - interval '1 hour', now() - interval '1 hour', 
                            'L3SRP000000000001')
                    """)
    
    dbt_cur.execute("""
                    INSERT INTO public.detail_observations
                        (id, artifact_id, listing_id, fetched_at, listing_state, vin)
                    VALUES
                        (1, 2, 'L2', now() - interval '1 hour', 'active', 'L2DET000000000001'),
                        (2, 4, 'L3', now() - interval '2 hours', 'active', 'L3DET000000000001')
                    """)
    

@pytest.fixture(scope="module", autouse=True)
def seed_and_build(dbt_conn, run_dbt):
    with dbt_conn.cursor() as cur:
        _seed_int_listing_to_vin(cur)
    
    run_dbt("stg_raw_artifacts stg_srp_observations stg_detail_observations int_listing_to_vin")
    yield
    with dbt_conn.cursor() as cur:
        cur.execute("""
            TRUNCATE public.raw_artifacts, public.srp_observations, public.detail_observations
        """)


def test_srp_only(analytics_ci_cur):
    analytics_ci_cur.execute("""
                             SELECT vin FROM analytics_ci.int_listing_to_vin WHERE listing_id = 'L1'
                             """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["vin"] == "L1SRP000000000001"


def test_detail_fresh(analytics_ci_cur):
    analytics_ci_cur.execute("""
                             SELECT vin FROM analytics_ci.int_listing_to_vin WHERE listing_id = 'L2'
                             """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["vin"] == "L2DET000000000001"


def test_srp_fresh(analytics_ci_cur):
    analytics_ci_cur.execute("""
                             SELECT vin FROM analytics_ci.int_listing_to_vin WHERE listing_id = 'L3'
                             """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["vin"] == "L3SRP000000000001"
