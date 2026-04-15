import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Target VIN: DS0TARGET00000001
#
# Seeded scenario (see conftest._seed_all for full data):
#   SRP observations:  @40k (45d ago), @38k (30d ago), @35k (1d ago)
#   Detail observation: listing_state='active', price=35000, msrp=50000, 1h ago
#   Cohort (same make/model/trim, within 3-day window): 35k, 45k, 50k, 55k, 60k
#
# Expected score breakdown:
#   MSRP discount    (35 pts max): (50000-35000)/50000 * 350 = 105 → capped at 35
#   Price percentile (30 pts max): percentile=0.0 (cheapest of 5) → (1-0.0)*30 = 30
#   Days on market   (15 pts max): 45 days → 45/90*15 = 7.5
#   Price drops      (10 pts max): 2 drops (40k→38k, 38k→35k) → 2/3*10 ≈ 6.67
#   Dealer inventory  (5 pts max): no dealer data → 0
#   National supply   (5 pts max): 5 listings → 5/500*5 = 0.05
#   Total ≈ 79.2  →  deal_tier = 'excellent'
# ---------------------------------------------------------------------------

_TARGET_VIN = "DS0TARGET00000001"
_TOLERANCE = 3.0


def test_deal_score_value(analytics_ci_cur):
    """Target VIN deal_score should be within ±3 of the manually calculated 79.2."""
    analytics_ci_cur.execute("""
        SELECT deal_score
        FROM mart_deal_scores
        WHERE vin = %s
    """, (_TARGET_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, "Target VIN not found in mart_deal_scores"
    expected = 79.2
    assert abs(float(row["deal_score"]) - expected) <= _TOLERANCE, (
        f"deal_score {row['deal_score']} not within {_TOLERANCE} of {expected}"
    )


def test_deal_tier_excellent(analytics_ci_cur):
    """Target VIN score ≥70 should produce deal_tier = 'excellent'."""
    analytics_ci_cur.execute("""
        SELECT deal_tier
        FROM mart_deal_scores
        WHERE vin = %s
    """, (_TARGET_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["deal_tier"] == "excellent"


def test_deal_score_in_bounds(analytics_ci_cur):
    """Every row in mart_deal_scores must have a score between 0 and 100."""
    analytics_ci_cur.execute("""
        SELECT vin, deal_score
        FROM mart_deal_scores
        WHERE deal_score < 0 OR deal_score > 100
    """)
    bad_rows = analytics_ci_cur.fetchall()
    assert bad_rows == [], (
        f"Found {len(bad_rows)} row(s) with deal_score outside [0, 100]: "
        + ", ".join(f"{r['vin']}={r['deal_score']}" for r in bad_rows)
    )


def test_msrp_discount_component(analytics_ci_cur):
    """Target VIN msrp_discount_pct should reflect the 30% seeded discount."""
    analytics_ci_cur.execute("""
        SELECT msrp_discount_pct
        FROM mart_deal_scores
        WHERE vin = %s
    """, (_TARGET_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert abs(float(row["msrp_discount_pct"]) - 30.0) <= 0.1, (
        f"Expected msrp_discount_pct ≈ 30.0, got {row['msrp_discount_pct']}"
    )


def test_price_drop_count(analytics_ci_cur):
    """Target VIN should have exactly 2 recorded price drops (40k→38k→35k)."""
    analytics_ci_cur.execute("""
        SELECT price_drop_count
        FROM mart_deal_scores
        WHERE vin = %s
    """, (_TARGET_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["price_drop_count"] == 2, (
        f"Expected 2 price drops, got {row['price_drop_count']}"
    )


def test_days_on_market(analytics_ci_cur):
    """Target VIN first seen 45 days ago — days_on_market should be ~45."""
    analytics_ci_cur.execute("""
        SELECT days_on_market
        FROM mart_deal_scores
        WHERE vin = %s
    """, (_TARGET_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert 44 <= row["days_on_market"] <= 46, (
        f"Expected days_on_market ≈ 45, got {row['days_on_market']}"
    )
