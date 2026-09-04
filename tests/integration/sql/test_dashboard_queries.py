"""
Layer 2 — SQL smoke tests for dashboard service queries.

Executes the exact SQL constants from ``dashboard.queries`` — the same module
the dashboard imports — against the DuckDB file ``dbt build --target duckdb``
produced earlier in the same job, and asserts each statement returns the
columns the page reads.

**Until Plan 162 Stage M this file was 25 tests and no assertions**, the only
Layer 2 suite with none. Every test executed a statement and discarded the
result, which satisfies Layer 2's first clause and not its second: the
statements ran, and nothing checked that they *"return the columns the caller
expects"*. Every page indexes by name — ``df['cnt']``, ``df["p75"] -
df["median"]``, ``df["hour"].dt.floor(...)`` — so a renamed mart column passed
this suite green and ``KeyError``d in production.

**The contract is one block, on purpose.** ``EXPECTED_COLUMNS`` is the whole of
what this suite asserts, readable in one screen, rather than 24 tuples spread
through 24 test bodies where no one reviews them together. That shape is what
stops the repair becoming the failure it repairs: a contract nobody can read at
once is a contract that gets bulk-updated unread the first time a mart changes.

**It cannot silently go stale, and the mechanism matters more than the
promise.** These tuples are not a denominator — nothing consults them to decide
what to check. They are the assertions themselves, compared for equality on
every run, so a statement that gains, loses or renames a column fails here in
both directions. What a per-statement assertion still cannot notice is a
*query* added to ``dashboard/queries.py`` with no contract at all, so
``test_every_dashboard_query_has_a_column_contract`` derives that denominator
from the module rather than from this file.

What this suite deliberately does not assert is that the queries return
*correct values*. That is Layer 3's shape — known inputs, known outputs — and
``duckdb_con`` is a read-only connection to whatever CI's seed produced, so
there are no known inputs to assert against. The reasoning, and the one query
it does not hold, are recorded in Plan 162 §"Stage M narrowed".

Pipeline Health queries were removed in Plan 101.
"""
import pytest

from dashboard import queries

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# The column contract: every statement, and the columns its page reads.
#
# Keyed by constant name rather than by the SQL itself so a failure names the
# query a maintainer can find, and so
# ``test_every_production_sql_file_is_touched_by_a_layer_2_test`` still credits
# each ``.sql`` file -- that rule matches a file's stem in either case, and
# these keys are the upper-case form of the stem.
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    # app.py
    "MART_FRESHNESS": ("ts",),

    # deals.py
    "DEALS_MAKES": ("make",),
    "DEALS_TABLE": (
        "make", "model", "vehicle_trim", "model_year", "dealer_name",
        "current_price", "national_median_price", "msrp", "msrp_off_pct",
        "deal_tier", "deal_score", "price_pct", "days_on_market", "drops",
        "canonical_detail_url",
    ),
    "DEALS_TIER_DISTRIBUTION": ("deal_tier", "listings"),
    "DEALS_DAYS_ON_MARKET": ("bucket", "listings"),
    "DEALS_PRICE_DROPS": (
        "make", "model", "vehicle_trim", "model_year", "dealer_name",
        "current_price", "first_price", "price_change", "total_drop_pct",
        "drops", "days_on_market", "canonical_detail_url",
    ),
    "DEALS_PRICE_VS_MSRP": (
        "model", "avg_price", "avg_msrp", "avg_msrp_off_pct", "listings",
    ),

    # inventory.py
    "INVENTORY_ACTIVE_COUNT": ("cnt",),
    "INVENTORY_NEW_24H": ("cnt",),
    "INVENTORY_NEW_7D": ("cnt",),
    "INVENTORY_NEW_30D": ("cnt",),
    "INVENTORY_BY_MAKE_MODEL": (
        "make", "model", "active_listings", "avg_price", "min_price",
    ),
    "INVENTORY_NEW_OVER_TIME": ("day", "make", "new_listings"),
    "INVENTORY_UNLISTED_OVER_TIME": ("day", "vehicles_unlisted"),
    "INVENTORY_TOP_DEALERS": (
        "dealer", "make", "model", "active_listings", "avg_price", "min_price",
    ),

    # market_trends.py
    "MARKET_TRENDS_DAYS_ON_MARKET": (
        "make", "model", "median_days", "avg_days", "min_days", "max_days",
        "listings",
    ),
    "MARKET_TRENDS_NATIONAL_SUPPLY": (
        "make", "model", "tracked_listings", "avg_national_supply",
        "avg_price", "avg_msrp_off_pct",
    ),
    "MARKET_TRENDS_PRICE_DISTRIBUTION": (
        "make", "model", "p10", "p25", "median", "p75", "p90", "listings",
    ),

    # data_health.py
    "DATA_HEALTH_SCRAPE_VOLUME": (
        "hour", "source", "artifact_count", "observation_count",
        # unique_listings and vin_extraction_pct are returned and read by
        # nothing; see the dead-column note at the foot of this block.
        "unique_listings", "vin_extraction_pct",
    ),
    "DATA_HEALTH_BLOCK_RATE": (
        "hour", "new_blocks", "block_increments",
        # total_block_events and max_attempts_seen are read by nothing, and
        # block_rate_pct is recomputed in pandas rather than read.
        "total_block_events", "unique_listings_blocked", "max_attempts_seen",
        "total_observations", "block_rate_pct",
    ),
    "DATA_HEALTH_INVENTORY_COVERAGE": (
        "make", "model", "total_vins", "detail_enriched", "srp_only",
        "coverage_pct",
    ),
    "DATA_HEALTH_PRICE_FRESHNESS": (
        "make", "model", "total_vins", "fresh_lt_1d", "fresh_1_3d",
        "fresh_4_7d", "fresh_8_14d", "stale_gt_14d", "fresh_lt_7d_pct",
    ),
    "DATA_HEALTH_BATCH_OUTCOMES": (
        "obs_date", "detail_observations", "detail_artifacts",
        "valid_vin_count", "unique_vins_enriched", "extraction_yield",
    ),
    "DATA_HEALTH_COOLDOWN_COHORTS": (
        "attempt_bucket", "listing_count", "min_attempts", "max_attempts",
    ),
}

# Five columns above are returned by production SQL and consumed by no page,
# and writing the contract down is what surfaced them -- the dividend Plan 162
# §"Stage 8 narrowed" predicted, at five rather than the three it predicted.
# All five are on Data Health, which is the one page that renders no frame
# wholesale: everywhere else ``st.dataframe(df)`` displays every column it is
# handed, so a column that is never named is still shown. They are recorded
# rather than deleted here, because dropping a column from a mart's consumer is
# a modeling question and Plan 150 Stage 0c owns it.

# ``deals.py`` appends "AND col IN (?...)" fragments to these before executing,
# so the constant is a template and ``.format`` has to run before DuckDB sees
# it. Empty is the unfiltered page load; the filtered shape is asserted below.
TEMPLATED = frozenset({
    "DEALS_TABLE",
    "DEALS_TIER_DISTRIBUTION",
    "DEALS_DAYS_ON_MARKET",
    "DEALS_PRICE_DROPS",
    "DEALS_PRICE_VS_MSRP",
})


def _statement(name: str, filter_clause: str = "") -> str:
    sql = getattr(queries, name)
    return sql.format(filter_clause=filter_clause) if name in TEMPLATED else sql


def _columns(con, sql: str, params=None) -> tuple[str, ...]:
    """Execute *sql* and return its result column names, in order.

    Mirrors ``run_duckdb_query()``: the dashboard hands DuckDB the same text
    and the same parameter list, then indexes the frame by column name.
    """
    result = con.execute(sql, params) if params else con.execute(sql)
    return tuple(column[0] for column in result.description)


@pytest.mark.parametrize("name", sorted(EXPECTED_COLUMNS), ids=sorted(EXPECTED_COLUMNS))
def test_query_returns_the_columns_the_page_reads(duckdb_con, name):
    """The statement executes, and its result columns are what the page indexes.

    Both halves matter and only the first was ever checked here. A statement
    that no longer parses fails on ``execute``; a statement that parses and
    returns different columns is the failure this assertion exists for, and it
    is the one that reaches production.
    """
    assert _columns(duckdb_con, _statement(name)) == EXPECTED_COLUMNS[name]


def test_every_dashboard_query_has_a_column_contract():
    """A new query with no entry above fails here rather than going uncovered.

    This is the denominator, and it is derived from ``dashboard.queries``
    rather than restated -- the rule every other census in this repository
    learned the hard way. Without it ``EXPECTED_COLUMNS`` would be exactly the
    kind of list that is complete on the day it is written and quietly partial
    afterwards. It needs no database, so it holds even where the DuckDB
    fixture skips.
    """
    declared = {
        name for name in dir(queries)
        if name.isupper() and not name.startswith("_")
    }
    assert declared == set(EXPECTED_COLUMNS), (
        "dashboard.queries and this file's column contract disagree.\n"
        f"  no contract here: {sorted(declared - set(EXPECTED_COLUMNS))}\n"
        f"  no longer a query: {sorted(set(EXPECTED_COLUMNS) - declared)}"
    )


def test_a_deals_filter_clause_does_not_change_the_result_shape(duckdb_con):
    """``deals.py`` builds this fragment at runtime; the page reads one shape.

    Filtered and unfiltered are the same statement as far as the page is
    concerned -- it renders the frame the same way either way -- so a fragment
    that changed the projection would break the table without changing the
    ``.sql`` file that the rest of this suite watches.
    """
    filtered = _statement("DEALS_TABLE", filter_clause="AND make IN (?)")
    assert _columns(duckdb_con, filtered, params=["Honda"]) == (
        EXPECTED_COLUMNS["DEALS_TABLE"]
    )
