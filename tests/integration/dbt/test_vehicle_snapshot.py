import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# mart_vehicle_snapshot infers listing_state for SRP-only VINs:
#   - If a detail observation provides an explicit listing_state → use it.
#   - Elif most recent tier-1 observed_at >= now() - 7 days → 'active'  (inferred)
#   - Else → 'unlisted'  (inferred)
#
# Note: ops_vehicle_staleness filters out 'unlisted' VINs, so unlisted listings
# are invisible to the scrape queue. A wrong inference here silently drops targets.
#
# VINs under test:
#
#   L30000000NOTSTALE  (300s group) — detail obs with listing_state='active'.
#                      Explicit state from detail must flow through unchanged.
#
#   L100000PRICESTALE  (300s group) — SRP-only, last seen 25h ago (< 7 days).
#                      listing_state must be inferred as 'active'.
#
#   MVS0SRPONLYOLD001  (900s group) — SRP-only, last seen 10 days ago (> 7 days).
#                      listing_state must be inferred as 'unlisted'.
# ---------------------------------------------------------------------------

_VIN_EXPLICIT_ACTIVE = "L30000000NOTSTALE"
_VIN_SRP_RECENT      = "L100000PRICESTALE"
_VIN_SRP_OLD         = "MVS0SRPONLYOLD001"


def test_detail_explicit_active_state(analytics_ci_cur):
    """L30000000NOTSTALE: detail obs has listing_state='active' — must pass through as 'active'."""
    analytics_ci_cur.execute("""
        SELECT listing_state
        FROM mart_vehicle_snapshot
        WHERE vin = %s
    """, (_VIN_EXPLICIT_ACTIVE,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_EXPLICIT_ACTIVE} not found in mart_vehicle_snapshot"
    assert row["listing_state"] == "active"


def test_srp_only_recent_inferred_active(analytics_ci_cur):
    """L100000PRICESTALE: SRP-only, last seen 25h ago (< 7-day threshold) — inferred 'active'."""
    analytics_ci_cur.execute("""
        SELECT listing_state
        FROM mart_vehicle_snapshot
        WHERE vin = %s
    """, (_VIN_SRP_RECENT,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_SRP_RECENT} not found in mart_vehicle_snapshot"
    assert row["listing_state"] == "active"


def test_srp_only_old_inferred_unlisted(analytics_ci_cur):
    """
    MVS0SRPONLYOLD001: SRP-only honda/crv, last seen 10 days ago (> 7-day threshold).
    listing_state must be inferred as 'unlisted'.
    This VIN will NOT appear in ops_vehicle_staleness (which filters out unlisted).
    """
    analytics_ci_cur.execute("""
        SELECT listing_state
        FROM mart_vehicle_snapshot
        WHERE vin = %s
    """, (_VIN_SRP_OLD,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_SRP_OLD} not found in mart_vehicle_snapshot"
    assert row["listing_state"] == "unlisted"
