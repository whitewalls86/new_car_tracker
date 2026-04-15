import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Target VINs (700s group — see conftest._seed_all for full data):
#
#   DOM0SRPONLYMULT01 — Three national SRP observations: t-10d, t-5d, t-1d.
#                       One detail observation at t-3d (later than the earliest SRP).
#                       first_seen_at = min(t-10d, t-5d, t-1d, t-3d) = t-10d.
#                       days_on_market ≈ 10. Tests that:
#                         (a) first_seen_at is the overall minimum across all sources, and
#                         (b) a later detail observation does not displace the SRP-established
#                             first_seen_at.
#
#   DOM0NATLOCALSPLIT — National SRP at t-10d, local SRP at t-2d.
#                       first_seen_national_at ≈ t-10d, first_seen_local_at ≈ t-2d.
#                       first_seen_national_at must be strictly before first_seen_local_at.
# ---------------------------------------------------------------------------

_VIN_MULTI_SRP = "DOM0SRPONLYMULT01"
_VIN_NAT_LOCAL = "DOM0NATLOCALSPLIT"


def test_first_seen_uses_earliest_observation(analytics_ci_cur):
    """
    DOM0SRPONLYMULT01: three national SRP obs (t-10d/t-5d/t-1d) plus a detail at t-3d.
    first_seen_at = t-10d regardless of the later detail; days_on_market must be ≈ 10.
    """
    analytics_ci_cur.execute("""
        SELECT days_on_market
        FROM int_listing_days_on_market
        WHERE vin = %s
    """, (_VIN_MULTI_SRP,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_MULTI_SRP} not found in int_listing_days_on_market"
    assert 9 <= row["days_on_market"] <= 11, (
        f"Expected days_on_market ≈ 10, got {row['days_on_market']}"
    )


def test_national_first_seen_before_local(analytics_ci_cur):
    """
    DOM0NATLOCALSPLIT: national SRP at t-10d, local SRP at t-2d.
    first_seen_national_at must be earlier than first_seen_local_at.
    """
    analytics_ci_cur.execute("""
        SELECT first_seen_national_at, first_seen_local_at
        FROM int_listing_days_on_market
        WHERE vin = %s
    """, (_VIN_NAT_LOCAL,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN_NAT_LOCAL} not found in int_listing_days_on_market"
    assert row["first_seen_national_at"] is not None, "first_seen_national_at is NULL"
    assert row["first_seen_local_at"] is not None, "first_seen_local_at is NULL"
    assert row["first_seen_national_at"] < row["first_seen_local_at"], (
        f"Expected first_seen_national_at < first_seen_local_at, "
        f"got {row['first_seen_national_at']} vs {row['first_seen_local_at']}"
    )


def test_national_first_seen_is_approximately_ten_days_ago(analytics_ci_cur):
    """DOM0NATLOCALSPLIT: the national first_seen is ≈ t-10d (not t-2d from the local obs)."""
    analytics_ci_cur.execute("""
        SELECT
            abs(extract(epoch from (first_seen_national_at - (now() - interval '10 days')))) < 3600
            AS within_one_hour
        FROM int_listing_days_on_market
        WHERE vin = %s
    """, (_VIN_NAT_LOCAL,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["within_one_hour"] is True, (
        "first_seen_national_at is not close to t-10d"
    )
