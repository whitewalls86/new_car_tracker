import pytest

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Target VIN: PH0PRICEHISTORY01 (600s group — see conftest._seed_all)
#
# Five SRP observations in chronological order:
#   t-10d: price=100   (first observation)
#   t-8d:  price=120   → increase (100→120)
#   t-6d:  price=90    → drop     (120→90)
#   t-4d:  price=90    → flat     (90→90,  not counted as drop or increase)
#   t-2d:  price=110   → increase (90→110)
#
# Expected int_price_history_by_vin output:
#   first_price          = 100
#   price_drop_count     = 1
#   price_increase_count = 2
#   min_price            = 90
#   max_price            = 120
# ---------------------------------------------------------------------------

_VIN = "PH0PRICEHISTORY01"


def test_price_drop_count(analytics_ci_cur):
    """Only one drop in the sequence: 120→90."""
    analytics_ci_cur.execute("""
        SELECT price_drop_count
        FROM int_price_history_by_vin
        WHERE vin = %s
    """, (_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None, f"{_VIN} not found in int_price_history_by_vin"
    assert row["price_drop_count"] == 1, (
        f"Expected price_drop_count=1, got {row['price_drop_count']}"
    )


def test_price_increase_count(analytics_ci_cur):
    """Two increases in the sequence: 100→120 and 90→110."""
    analytics_ci_cur.execute("""
        SELECT price_increase_count
        FROM int_price_history_by_vin
        WHERE vin = %s
    """, (_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["price_increase_count"] == 2, (
        f"Expected price_increase_count=2, got {row['price_increase_count']}"
    )


def test_first_price(analytics_ci_cur):
    """first_price must be 100 — the observation at t-10d."""
    analytics_ci_cur.execute("""
        SELECT first_price
        FROM int_price_history_by_vin
        WHERE vin = %s
    """, (_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["first_price"] == 100, (
        f"Expected first_price=100, got {row['first_price']}"
    )


def test_min_and_max_price(analytics_ci_cur):
    """min_price=90 (two observations at that level), max_price=120."""
    analytics_ci_cur.execute("""
        SELECT min_price, max_price
        FROM int_price_history_by_vin
        WHERE vin = %s
    """, (_VIN,))
    row = analytics_ci_cur.fetchone()
    assert row is not None
    assert row["min_price"] == 90,  f"Expected min_price=90, got {row['min_price']}"
    assert row["max_price"] == 120, f"Expected max_price=120, got {row['max_price']}"
