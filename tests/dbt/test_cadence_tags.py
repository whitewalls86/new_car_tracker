"""
Plan 123 Phase 1: validate the dbt build-cadence tag/selector configuration.

Pure YAML/filesystem checks — no dbt invocation or database required, so
these run in the default (non-integration) test job.
"""
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parents[2]
MODELS_DIR = REPO_ROOT / "dbt" / "models"
SELECTORS_PATH = REPO_ROOT / "dbt" / "selectors.yml"

HOURLY_CORE = "hourly_core"
FEATURE_DAILY = "feature_daily"
BACKTEST = "backtest"

# Every dashboard/API-facing model that Plan 123 Phase 1 requires to stay on
# the hourly cadence (see dashboard/queries.py, ops/metrics/duckdb_gauges.py,
# ops/routers/info.py, and dashboard/sql/deals_table.sql for the underlying
# dependency evidence).
EXPECTED_HOURLY_CORE = {
    "stg_observations",
    "stg_price_events",
    "stg_blocked_cooldown_events",
    "stg_search_configs",
    "stg_dealers",
    "int_latest_observation",
    "int_active_make_models",
    "int_price_history",
    "int_benchmarks",
    "mart_vehicle_snapshot",
    "mart_deal_scores",
    "mart_scrape_volume",
    "mart_block_rate",
    "mart_detail_batch_outcomes",
    "mart_inventory_coverage",
    "mart_cooldown_cohorts",
    "mart_price_freshness_trend",
}

# Feature-store/backtest-only models with no dashboard/API dependency.
EXPECTED_FEATURE_DAILY = {
    "int_listing_state_fingerprints",
    "int_listing_state_runs",
    "int_listing_volatility_features",
}


def _model_tags() -> dict:
    """Map model name -> set of config.tags, read from every *.schema.yml."""
    tags_by_model = {}
    for schema_path in MODELS_DIR.glob("*/*.schema.yml"):
        doc = yaml.safe_load(schema_path.read_text())
        for model in doc.get("models", []):
            tags = set(model.get("config", {}).get("tags", []))
            tags_by_model[model["name"]] = tags
    return tags_by_model


def test_selectors_yml_parses_and_defines_expected_names():
    doc = yaml.safe_load(SELECTORS_PATH.read_text())
    names = {s["name"] for s in doc["selectors"]}
    assert names == {HOURLY_CORE, FEATURE_DAILY, BACKTEST, "full_validation"}


def test_every_model_has_at_least_one_cadence_tag():
    tags_by_model = _model_tags()
    sql_model_names = {p.stem for p in MODELS_DIR.glob("*/*.sql")}

    untagged = {
        name for name in sql_model_names
        if name in tags_by_model and not tags_by_model[name]
    }
    assert not untagged, f"Models with a schema.yml but no cadence tag: {untagged}"

    missing_schema = sql_model_names - tags_by_model.keys()
    assert not missing_schema, (
        f"Models with no schema.yml (and therefore no cadence tag): {missing_schema}"
    )


def test_dashboard_and_api_dependencies_are_tagged_hourly_core():
    tags_by_model = _model_tags()
    for name in EXPECTED_HOURLY_CORE:
        assert HOURLY_CORE in tags_by_model.get(name, set()), (
            f"{name} is used by a dashboard/API path but is not tagged {HOURLY_CORE!r}"
        )


def test_feature_daily_models_are_not_pulled_into_hourly_core():
    tags_by_model = _model_tags()
    for name in EXPECTED_FEATURE_DAILY:
        tags = tags_by_model.get(name, set())
        assert FEATURE_DAILY in tags, f"{name} should be tagged {FEATURE_DAILY!r}"
        assert HOURLY_CORE not in tags, (
            f"{name} has no dashboard/API dependency and must not run on the "
            f"hourly cadence"
        )


def test_hourly_core_and_feature_daily_tag_sets_are_disjoint():
    tags_by_model = _model_tags()
    hourly = {name for name, tags in tags_by_model.items() if HOURLY_CORE in tags}
    daily = {name for name, tags in tags_by_model.items() if FEATURE_DAILY in tags}
    assert hourly.isdisjoint(daily), (
        f"Models tagged both hourly_core and feature_daily: {hourly & daily}"
    )
