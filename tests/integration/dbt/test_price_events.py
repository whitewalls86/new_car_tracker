import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# int_price_events unions SRP, detail, and carousel sources, then deduplicates
# on (vin, observed_at, price) preferring detail > srp > carousel.
#
# Scenarios covered:
#
#   SRPL1000000010000  (200s group) — SRP-only with price=10000.
#                      Verifies a plain SRP price event passes through unchanged.
#
#   L4STALEONCOOLDOWN  (300s group) — detail-only VIN (no SRP obs seeded for OL4).
#                      Verifies a plain detail price event passes through unchanged.
#
#   PE0PRICEEVTDEDUP1  (800s group) — SRP (artifact 801) and detail (artifact 802)
#                      both have fetched_at = now()-5d and price=22000.
#                      The dedup DISTINCT ON (vin, observed_at, price) must collapse
#                      these to one row and the surviving row must have source='detail'.
#
#   DS0TARGET00000001  (400s group) — three SRP obs at distinct (time, price) pairs.
#                      Verifies that multiple distinct events are not collapsed.
# ---------------------------------------------------------------------------

_VIN_SRP_ONLY  = "SRPL1000000010000"
_VIN_DET_ONLY  = "L4STALEONCOOLDOWN"
_VIN_DEDUP     = "PE0PRICEEVTDEDUP1"
_VIN_MULTI     = "DS0TARGET00000001"


def test_srp_only_price_produces_one_row(analytics_ci_cur):
    """SRP-only VIN with a single price should produce exactly one row."""
    analytics_ci_cur.execute("""
        SELECT count(*) AS cnt
        FROM int_price_events
        WHERE vin = %s
    """, (_VIN_SRP_ONLY,))
    row = analytics_ci_cur.fetchone()
    assert row["cnt"] == 1, f"Expected 1 price event for {_VIN_SRP_ONLY}, got {row['cnt']}"


def test_detail_only_price_produces_one_row(analytics_ci_cur):
    """Detail-only VIN (no SRP obs) with a single price should produce exactly one row."""
    analytics_ci_cur.execute("""
        SELECT count(*) AS cnt
        FROM int_price_events
        WHERE vin = %s
    """, (_VIN_DET_ONLY,))
    row = analytics_ci_cur.fetchone()
    assert row["cnt"] == 1, f"Expected 1 price event for {_VIN_DET_ONLY}, got {row['cnt']}"


def test_same_timestamp_and_price_deduped_to_one_row(analytics_ci_cur):
    """
    PE0PRICEEVTDEDUP1: SRP and detail share the same fetched_at and price=22000.
    DISTINCT ON (vin, observed_at, price) must collapse them to one row,
    and the surviving row must have source='detail' (detail beats SRP in priority order).
    """
    analytics_ci_cur.execute("""
        SELECT count(*) AS cnt, min(source) AS source
        FROM int_price_events
        WHERE vin = %s
    """, (_VIN_DEDUP,))
    row = analytics_ci_cur.fetchone()
    assert row["cnt"] == 1, (
        f"Expected 1 deduped row for {_VIN_DEDUP}, got {row['cnt']}"
    )
    assert row["source"] == "detail", (
        f"Expected source='detail' after dedup, got '{row['source']}'"
    )


def test_multiple_distinct_prices_produce_multiple_rows(analytics_ci_cur):
    """
    DS0TARGET00000001 has SRP obs at three distinct (time, price) pairs (40k/38k/35k).
    int_price_events must contain at least 3 rows — distinct events are not collapsed.
    """
    analytics_ci_cur.execute("""
        SELECT count(*) AS cnt
        FROM int_price_events
        WHERE vin = %s
    """, (_VIN_MULTI,))
    row = analytics_ci_cur.fetchone()
    assert row["cnt"] >= 3, (
        f"Expected ≥3 price events for {_VIN_MULTI}, got {row['cnt']}"
    )
