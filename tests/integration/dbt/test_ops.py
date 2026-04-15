import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# ops_vehicle_staleness
# ---------------------------------------------------------------------------

def test_price_stale_vin_is_stale(analytics_ci_cur):
    """OL1/L100000PRICESTALE: SRP-only, 25h old, no price event → is_price_stale."""
    analytics_ci_cur.execute("""
        SELECT is_price_stale, stale_reason
        FROM ops.ops_vehicle_staleness
        WHERE vin = 'L100000PRICESTALE'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is True


def test_full_details_stale_vin(analytics_ci_cur):
    """OL2/L20000FULLDETAILS: tier-1 observation 170h old → full_details stale."""
    analytics_ci_cur.execute("""
        SELECT is_full_details_stale, stale_reason
        FROM ops.ops_vehicle_staleness
        WHERE vin = 'L20000FULLDETAILS'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_full_details_stale"] is True
    assert row["stale_reason"] == "full_details"


def test_not_stale_vin(analytics_ci_cur):
    """OL3/L30000000NOTSTALE: fresh SRP (2h) + fresh detail (1h) → neither flag set."""
    analytics_ci_cur.execute("""
        SELECT is_price_stale, is_full_details_stale
        FROM ops.ops_vehicle_staleness
        WHERE vin = 'L30000000NOTSTALE'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is False
    assert row["is_full_details_stale"] is False


def test_price_only_stale_reason(analytics_ci_cur):
    """OL4/L4STALEONCOOLDOWN: detail price 25h old → price_only stale_reason."""
    analytics_ci_cur.execute("""
        SELECT is_price_stale, stale_reason
        FROM ops.ops_vehicle_staleness
        WHERE vin = 'L4STALEONCOOLDOWN'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is True
    assert row["stale_reason"] == "price_only"


def test_fully_blocked_vin_is_stale(analytics_ci_cur):
    """OL5/L50STALEFULLBLOCK: detail price 36h old → price_only stale_reason."""
    analytics_ci_cur.execute("""
        SELECT is_price_stale, stale_reason
        FROM ops.ops_vehicle_staleness
        WHERE vin = 'L50STALEFULLBLOCK'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["is_price_stale"] is True
    assert row["stale_reason"] == "price_only"


# ---------------------------------------------------------------------------
# ops_detail_scrape_queue
# ---------------------------------------------------------------------------

def test_stale_no_cooldown_in_queue(analytics_ci_cur):
    """OL1: stale, no cooldown record → must appear in queue at priority 1."""
    analytics_ci_cur.execute("""
        SELECT listing_id, priority
        FROM ops.ops_detail_scrape_queue
        WHERE listing_id = 'OL1'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["priority"] == 1


def test_eligible_cooldown_in_queue(analytics_ci_cur):
    """OL2: stale, cooldown elapsed (next_eligible_at in the past) → must appear in queue."""
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM ops.ops_detail_scrape_queue
        WHERE listing_id = 'OL2'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is not None


def test_not_stale_not_in_queue(analytics_ci_cur):
    """OL3: fresh, not stale → must not appear in queue."""
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM ops.ops_detail_scrape_queue
        WHERE listing_id = 'OL3'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is None


def test_cooldown_ineligible_not_in_queue(analytics_ci_cur):
    """OL4: stale but cooldown not yet elapsed → must not appear in queue."""
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM ops.ops_detail_scrape_queue
        WHERE listing_id = 'OL4'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is None


def test_fully_blocked_not_in_queue(analytics_ci_cur):
    """OL5: fully blocked (5 attempts) → must not appear in queue."""
    analytics_ci_cur.execute("""
        SELECT listing_id
        FROM ops.ops_detail_scrape_queue
        WHERE listing_id = 'OL5'
    """)
    row = analytics_ci_cur.fetchone()
    assert row is None
