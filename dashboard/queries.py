"""
DuckDB analytics query loader for the dashboard service.

Loads all .sql files from dashboard/sql/ at import time and exposes them
as module-level constants. File name (without extension) becomes the constant
name in UPPER_CASE.

Usage:
    from dashboard.queries import DEALS_TABLE, INVENTORY_ACTIVE_COUNT
    # Template queries — call .format(filter_clause=...) before executing:
    from dashboard.queries import DEALS_TABLE  # .format(filter_clause="AND make IN (?)")
"""
from pathlib import Path

_SQL_DIR = Path(__file__).parent / "sql"


def _load(filename: str) -> str:
    return (_SQL_DIR / filename).read_text()


# app.py
MART_FRESHNESS = _load("mart_freshness.sql")

# deals.py — templates: call .format(filter_clause="AND ..." or "") before executing
DEALS_MAKES = _load("deals_makes.sql")
DEALS_TABLE = _load("deals_table.sql")
DEALS_TIER_DISTRIBUTION = _load("deals_tier_distribution.sql")
DEALS_DAYS_ON_MARKET = _load("deals_days_on_market.sql")
DEALS_PRICE_DROPS = _load("deals_price_drops.sql")
DEALS_PRICE_VS_MSRP = _load("deals_price_vs_msrp.sql")

# inventory.py
INVENTORY_ACTIVE_COUNT = _load("inventory_active_count.sql")
INVENTORY_NEW_24H = _load("inventory_new_24h.sql")
INVENTORY_NEW_7D = _load("inventory_new_7d.sql")
INVENTORY_NEW_30D = _load("inventory_new_30d.sql")
INVENTORY_BY_MAKE_MODEL = _load("inventory_by_make_model.sql")
INVENTORY_NEW_OVER_TIME = _load("inventory_new_over_time.sql")
INVENTORY_UNLISTED_OVER_TIME = _load("inventory_unlisted_over_time.sql")
INVENTORY_TOP_DEALERS = _load("inventory_top_dealers.sql")

# market_trends.py
MARKET_TRENDS_DAYS_ON_MARKET = _load("market_trends_days_on_market.sql")
MARKET_TRENDS_NATIONAL_SUPPLY = _load("market_trends_national_supply.sql")
MARKET_TRENDS_PRICE_DISTRIBUTION = _load("market_trends_price_distribution.sql")
