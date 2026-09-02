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
from unittest.mock import MagicMock

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
    def test_default_payload_selects_hourly_core(self, dbt_build_module, mocker):
        """No dag_run.conf → payload must select the hourly_core tag, not the full graph."""
        mock_post_json = mocker.patch.object(dbt_build_module, "post_json")
        mock_post_json.return_value = {"ok": True}

        dbt_build_module._run_dbt_build(**_mock_context())

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": ["tag:hourly_core"]}
        assert dbt_build_module.DEFAULT_DBT_SELECT == ["tag:hourly_core"]

    def test_explicit_select_override_is_honored(self, dbt_build_module, mocker):
        """dag_run.conf={"select": [...]} must override the hourly_core default."""
        mock_post_json = mocker.patch.object(dbt_build_module, "post_json")
        mock_post_json.return_value = {"ok": True}

        dbt_build_module._run_dbt_build(**_mock_context({"select": ["tag:feature_daily"]}))

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": ["tag:feature_daily"]}

    def test_full_refresh_conf_is_still_honored_alongside_default_select(
        self, dbt_build_module, mocker
    ):
        """full_refresh from conf must pass through even when select falls back to the default."""
        mock_post_json = mocker.patch.object(dbt_build_module, "post_json")
        mock_post_json.return_value = {"ok": True}

        dbt_build_module._run_dbt_build(**_mock_context({"full_refresh": True}))

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": ["tag:hourly_core"], "full_refresh": True}

    def test_explicit_empty_select_list_is_still_honored(self, dbt_build_module, mocker):
        """
        An explicit empty list is a deliberate 'build everything' override, not
        a missing key. This is the documented way to force a full-graph build
        through this DAG: dbt_runner only forwards raw --select/--exclude
        tokens (never --selector), so neither "tag:full_validation" nor
        "fqn:*" reaches dbt as a full-graph selection — an empty list is what
        makes dbt_runner omit --select and build everything.
        """
        mock_post_json = mocker.patch.object(dbt_build_module, "post_json")
        mock_post_json.return_value = {"ok": True}

        dbt_build_module._run_dbt_build(**_mock_context({"select": []}))

        _, kwargs = mock_post_json.call_args
        assert kwargs["payload"] == {"select": []}


# ---------------------------------------------------------------------------
# Plan 134 Stage 1 — what this DAG owes the shared notifier
#
# The notification behaviour itself lives in airflow/dags/notifications.py and
# is tested in tests/airflow/test_notifications.py, which needs no Airflow.
# What is DAG-specific is here: the headline, the set of tasks worth quoting,
# and the XCom push without which the page has nothing to quote at all.
# ---------------------------------------------------------------------------

class TestNotifyDelegatesToTheSharedNotifier:
    def test_it_pages_with_this_dags_headline_and_work_tasks(self, dbt_build_module, mocker):
        context = {"ti": MagicMock(), "dag_run": MagicMock()}
        send = mocker.patch.object(dbt_build_module, "send_failure_alert")

        dbt_build_module._notify(**context)

        send.assert_called_once()
        args, kwargs = send.call_args
        assert args[1] == "hourly analytics refresh FAILED"
        assert kwargs["task_ids"] == dbt_build_module._WORK_TASKS

    def test_the_work_tasks_are_the_tasks_that_can_fail(self, dbt_build_module):
        """A task missing from this tuple is a task whose failure detail the
        page silently drops."""
        assert set(dbt_build_module._WORK_TASKS) == {
            "flush_silver_observations",
            "flush_staging_events",
            "dbt_build",
            "reconcile_cooldown_cohorts",
        }


class TestPostResultPreservesTheFailureForNotify:
    """Without this push, a failed flush leaves the page nothing to quote.

    The old callables returned post_json(...) directly, so a JsonPostError
    propagated with no XCom behind it — and _notify pulled only dbt_build's,
    which on a flush failure does not exist either.
    """

    def test_a_successful_post_is_pushed(self, dbt_build_module, mocker):
        context = {"ti": MagicMock(), "dag_run": MagicMock()}
        mocker.patch.object(dbt_build_module, "post_json", return_value={"flushed": 7})

        result = dbt_build_module._run_flush_silver(**context)

        assert result == {"flushed": 7}
        context["ti"].xcom_push.assert_called_once_with(
            key="result", value={"flushed": 7}
        )

    def test_a_500_body_is_pushed_and_the_error_still_raises(self, dbt_build_module, mocker):
        """JsonPostError.result is the endpoint's summary plus its
        failure_reason. The task must still fail — the push is so the page can
        say why, not so the DAG can go green."""
        body = {"flushed": 0, "error": "boom", "failure_reason": "flush aborted: boom"}
        context = {"ti": MagicMock(), "dag_run": MagicMock()}
        error = dbt_build_module.JsonPostError("500 Error", result=body)

        mocker.patch.object(dbt_build_module, "post_json", side_effect=error)

        with pytest.raises(dbt_build_module.JsonPostError):
            dbt_build_module._run_flush_staging(**context)

        context["ti"].xcom_push.assert_called_once_with(key="result", value=body)

    def test_the_cooldown_reconcile_is_covered_too(self, dbt_build_module, mocker):
        body = {"error": "ops unreachable"}
        context = {"ti": MagicMock(), "dag_run": MagicMock()}
        error = dbt_build_module.JsonPostError("500 Error", result=body)

        mocker.patch.object(dbt_build_module, "post_json", side_effect=error)

        with pytest.raises(dbt_build_module.JsonPostError):
            dbt_build_module._run_reconcile_cooldowns(**context)

        context["ti"].xcom_push.assert_called_once_with(key="result", value=body)
