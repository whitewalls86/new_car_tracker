"""
Layer 1 — SQL smoke tests for dashboard service queries.

Imports the exact SQL constants from dashboard.queries (the same module the
dashboard uses) and executes them against the DuckDB file produced by
`dbt build --target duckdb`. Any column rename, dropped model, or type error
in the DuckDB schema will surface here before it reaches production.

Tests use the `duckdb_con` fixture which skips automatically if DUCKDB_PATH
is not set.

Pipeline Health queries were removed in Plan 101.
"""
import pytest

from dashboard.queries import (
    DEALS_DAYS_ON_MARKET,
    DEALS_MAKES,
    DEALS_PRICE_DROPS,
    DEALS_PRICE_VS_MSRP,
    DEALS_TABLE,
    DEALS_TIER_DISTRIBUTION,
    INVENTORY_ACTIVE_COUNT,
    INVENTORY_BY_MAKE_MODEL,
    INVENTORY_NEW_7D,
    INVENTORY_NEW_24H,
    INVENTORY_NEW_30D,
    INVENTORY_NEW_OVER_TIME,
    INVENTORY_TOP_DEALERS,
    INVENTORY_UNLISTED_OVER_TIME,
    MARKET_TRENDS_DAYS_ON_MARKET,
    MARKET_TRENDS_NATIONAL_SUPPLY,
    MARKET_TRENDS_PRICE_DISTRIBUTION,
    MART_FRESHNESS,
)

pytestmark = pytest.mark.integration


def q(con, sql, params=None):
    """Execute sql and return all rows. Mirrors run_duckdb_query()."""
    if params:
        return con.execute(sql, params).fetchall()
    return con.execute(sql).fetchall()


# ============================================================================
# app.py — data freshness
# ============================================================================

class TestAppQueries:

    def test_data_freshness(self, duckdb_con):
        q(duckdb_con, MART_FRESHNESS)


# ============================================================================
# deals.py
# ============================================================================

class TestDealQueries:

    def test_distinct_makes(self, duckdb_con):
        q(duckdb_con, DEALS_MAKES)

    def test_deals_table_no_filter(self, duckdb_con):
        q(duckdb_con, DEALS_TABLE.format(filter_clause=""))

    def test_deals_table_filtered(self, duckdb_con):
        q(duckdb_con, DEALS_TABLE.format(filter_clause="AND make IN (?)"), params=["Honda"])

    def test_deal_tier_distribution_no_filter(self, duckdb_con):
        q(duckdb_con, DEALS_TIER_DISTRIBUTION.format(filter_clause=""))

    def test_days_on_market_no_filter(self, duckdb_con):
        q(duckdb_con, DEALS_DAYS_ON_MARKET.format(filter_clause=""))

    def test_price_drops_no_filter(self, duckdb_con):
        q(duckdb_con, DEALS_PRICE_DROPS.format(filter_clause=""))

    def test_price_vs_msrp_no_filter(self, duckdb_con):
        q(duckdb_con, DEALS_PRICE_VS_MSRP.format(filter_clause=""))


# ============================================================================
# inventory.py
# ============================================================================

class TestInventoryQueries:

    def test_active_count(self, duckdb_con):
        q(duckdb_con, INVENTORY_ACTIVE_COUNT)

    def test_new_24h(self, duckdb_con):
        q(duckdb_con, INVENTORY_NEW_24H)

    def test_new_7d(self, duckdb_con):
        q(duckdb_con, INVENTORY_NEW_7D)

    def test_new_30d(self, duckdb_con):
        q(duckdb_con, INVENTORY_NEW_30D)

    def test_by_make_model(self, duckdb_con):
        q(duckdb_con, INVENTORY_BY_MAKE_MODEL)

    def test_new_over_time(self, duckdb_con):
        q(duckdb_con, INVENTORY_NEW_OVER_TIME)

    def test_unlisted_over_time(self, duckdb_con):
        q(duckdb_con, INVENTORY_UNLISTED_OVER_TIME)

    def test_top_dealers(self, duckdb_con):
        q(duckdb_con, INVENTORY_TOP_DEALERS)


# ============================================================================
# market_trends.py
# ============================================================================

class TestMarketTrendsQueries:

    def test_days_on_market(self, duckdb_con):
        q(duckdb_con, MARKET_TRENDS_DAYS_ON_MARKET)

    def test_national_supply(self, duckdb_con):
        q(duckdb_con, MARKET_TRENDS_NATIONAL_SUPPLY)

    def test_price_distribution(self, duckdb_con):
        q(duckdb_con, MARKET_TRENDS_PRICE_DISTRIBUTION)
