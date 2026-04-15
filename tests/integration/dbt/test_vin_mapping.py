import pytest

pytestmark = pytest.mark.integration


def test_srp_only(analytics_ci_cur):
    """VML1 has only an SRP observation — SRP VIN must be returned."""
    analytics_ci_cur.execute("""
        SELECT vin FROM int_listing_to_vin WHERE listing_id = 'VML1'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["vin"] == "L1SRP000000000001"


def test_detail_fresh(analytics_ci_cur):
    """VML2: detail observation (1h ago) is fresher than SRP (2h ago) — detail VIN wins."""
    analytics_ci_cur.execute("""
        SELECT vin FROM int_listing_to_vin WHERE listing_id = 'VML2'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["vin"] == "L2DET000000000001"


def test_srp_fresh(analytics_ci_cur):
    """VML3: SRP observation (1h ago) is fresher than detail (2h ago) — SRP VIN wins."""
    analytics_ci_cur.execute("""
        SELECT vin FROM int_listing_to_vin WHERE listing_id = 'VML3'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["vin"] == "L3SRP000000000001"
