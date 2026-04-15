import pytest

pytestmark = pytest.mark.integration

# Cohort: 5 Test-Make/Test-Model/Test-Trim VINs within the 3-day staleness window
# at prices 10k, 20k, 30k, 40k, 50k.  Two additional VINs at 4 days old are
# excluded by the window, so they do not affect the ranks below.


def test_cheapest(analytics_ci_cur):
    """Cheapest VIN in the cohort should have percentile at or near 0."""
    analytics_ci_cur.execute("""
        SELECT vin, national_price_percentile
        FROM int_price_percentiles_by_vin
        WHERE vin = 'SRPL1000000010000'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["national_price_percentile"] <= 0.01


def test_middle(analytics_ci_cur):
    """Middle VIN (30k of 5) should have percentile near 0.5."""
    analytics_ci_cur.execute("""
        SELECT vin, national_price_percentile
        FROM int_price_percentiles_by_vin
        WHERE vin = 'SRPL3000000030000'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert 0.49 <= row["national_price_percentile"] <= 0.51


def test_most_expensive(analytics_ci_cur):
    """Most expensive VIN in the cohort should have percentile at or near 1."""
    analytics_ci_cur.execute("""
        SELECT vin, national_price_percentile
        FROM int_price_percentiles_by_vin
        WHERE vin = 'SRPL5000000050000'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["national_price_percentile"] >= 0.99
