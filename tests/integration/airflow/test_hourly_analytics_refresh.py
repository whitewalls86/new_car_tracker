"""
Behaviour tests for hourly_analytics_refresh's _run_dbt_build callable.

Plan 123 Phase 1: the scheduled hourly DAG must default to the hourly_core
dbt cadence instead of rebuilding the complete model graph every hour, while
still honoring an explicit dag_run.conf["select"] override for manual runs.

hourly_analytics_refresh.py imports `airflow.exceptions` unconditionally
(unlike scrape_listings.py, which guards its DAG construction behind
try/except ImportError), so importing it always requires a real Airflow
install. The import is deferred into a fixture — not done at module level —
so this file collects cleanly in the main "not integration" test job (which
has no Airflow installed); the tests themselves stay marked `integration`
and only run in the isolated Airflow venv CI job.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration

DAGS_DIR = Path(__file__).parents[3] / "airflow" / "dags"


@pytest.fixture
def dbt_build_module():
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))
    import hourly_analytics_refresh
    return hourly_analytics_refresh


def _mock_context(conf=None):
    dag_run = MagicMock()
    dag_run.conf = conf or {}
    ti = MagicMock()
    return {"dag_run": dag_run, "ti": ti}


class TestHourlyDbtBuildPayload:
    def test_default_payload_selects_hourly_core(self, dbt_build_module):
        """No dag_run.conf → payload must select the hourly_core tag, not the full graph."""
        with patch.object(dbt_build_module, "post_json") as mock_post_json:
            mock_post_json.return_value = {"ok": True}
            dbt_build_module._run_dbt_build(**_mock_context())

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": ["tag:hourly_core"]}
        assert dbt_build_module.DEFAULT_DBT_SELECT == ["tag:hourly_core"]

    def test_explicit_select_override_is_honored(self, dbt_build_module):
        """dag_run.conf={"select": [...]} must override the hourly_core default."""
        with patch.object(dbt_build_module, "post_json") as mock_post_json:
            mock_post_json.return_value = {"ok": True}
            dbt_build_module._run_dbt_build(**_mock_context({"select": ["tag:feature_daily"]}))

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": ["tag:feature_daily"]}

    def test_full_refresh_conf_is_still_honored_alongside_default_select(self, dbt_build_module):
        """full_refresh from conf must pass through even when select falls back to the default."""
        with patch.object(dbt_build_module, "post_json") as mock_post_json:
            mock_post_json.return_value = {"ok": True}
            dbt_build_module._run_dbt_build(**_mock_context({"full_refresh": True}))

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": ["tag:hourly_core"], "full_refresh": True}

    def test_explicit_empty_select_list_is_still_honored(self, dbt_build_module):
        """
        An explicit empty list is a deliberate 'build everything' override, not
        a missing key. This is the documented way to force a full-graph build
        through this DAG: dbt_runner only forwards raw --select/--exclude
        tokens (never --selector), so neither "tag:full_validation" nor
        "fqn:*" reaches dbt as a full-graph selection — an empty list is what
        makes dbt_runner omit --select and build everything.
        """
        with patch.object(dbt_build_module, "post_json") as mock_post_json:
            mock_post_json.return_value = {"ok": True}
            dbt_build_module._run_dbt_build(**_mock_context({"select": []}))

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": []}
