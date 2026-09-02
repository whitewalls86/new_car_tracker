"""Execute the exact production serving-snapshot SQL against a dbt artifact."""

import pytest

from dbt_runner.queries import (
    ANALYTICS_METRIC_COLUMNS,
    ANALYTICS_METRICS_SNAPSHOT,
    PUBLIC_STATS_COLUMNS,
    PUBLIC_STATS_SNAPSHOT,
)

pytestmark = pytest.mark.integration


def test_metrics_snapshot_query_matches_production_contract(duckdb_con):
    result = duckdb_con.execute(ANALYTICS_METRICS_SNAPSHOT)
    row = result.fetchone()

    assert row is not None
    assert tuple(column[0] for column in result.description) == ANALYTICS_METRIC_COLUMNS


def test_public_stats_snapshot_query_matches_production_contract(duckdb_con):
    result = duckdb_con.execute(PUBLIC_STATS_SNAPSHOT)
    row = result.fetchone()

    assert row is not None
    assert tuple(column[0] for column in result.description) == PUBLIC_STATS_COLUMNS
