"""The shared Telegram notifier — airflow/dags/notifications.py.

Plan 134 Stage 1. This pager has never worked. `dbt_build`'s notify task last
succeeded on 2026-05-08 at 12:03 UTC; commit `c92bd97` added
`ti.dag_run.run_id` and `ti.execution_date` to it at 13:10 UTC that day, and it
has failed 268 times and succeeded zero times since. Airflow 3's task SDK
`RuntimeTaskInstance` has neither attribute, so the task raised AttributeError
on its second line before building any message. The block was then copied into
`hourly_analytics_refresh` (12 failures, 0 successes) and `pack_bronze_html`,
where it has never fired and is still latent.

These tests run in the fast suite deliberately: notifications.py imports only
logging, os and requests, so the one piece of this repo that is supposed to
speak up when everything else breaks does not need an Airflow install to be
covered.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parents[2] / "airflow" / "dags"))

from notifications import (  # noqa: E402
    MAX_MESSAGE_CHARS,
    failed_task_ids,
    send_failure_alert,
    task_detail,
)

_RUN_ID = "scheduled__2026-08-27T13:00:00+00:00"

# Exactly the Airflow 3 RuntimeTaskInstance surface the notifier is allowed to
# touch. ti.dag_run and ti.execution_date raise AttributeError off this spec,
# which is precisely what production did.
_TASK_INSTANCE_ATTRS = ["dag_id", "xcom_pull", "xcom_push", "get_task_states"]


def _context(*, task_states=None, xcoms=None, states_error=None, logical_date=None):
    ti = MagicMock(spec=_TASK_INSTANCE_ATTRS)
    ti.dag_id = "hourly_analytics_refresh"
    if states_error is not None:
        ti.get_task_states.side_effect = states_error
    else:
        ti.get_task_states.return_value = {_RUN_ID: task_states or {}}
    ti.xcom_pull.side_effect = lambda task_ids=None, key=None: (xcoms or {}).get(task_ids)

    dag_run = MagicMock()
    dag_run.run_id = _RUN_ID
    dag_run.run_after = "2026-08-27T13:05:00+00:00"
    context = {"ti": ti, "dag_run": dag_run}
    if logical_date is not None:
        context["logical_date"] = logical_date
    return context


@pytest.fixture
def telegram(mocker, monkeypatch):
    """Configured credentials and a captured sendMessage. Returns the mock post."""
    monkeypatch.setenv("TELEGRAM_API", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100")
    post = mocker.patch("notifications.requests.post")
    post.return_value = MagicMock(ok=True, status_code=200, text="")
    return post


def _sent_text(post):
    assert post.call_count == 1, "the page must be sent exactly once"
    return post.call_args.kwargs["json"]["text"]


class TestItNoLongerTouchesAttributesAirflow3Removed:
    """The regression that produced 280 failed notifications and zero pages."""

    def test_the_run_id_and_date_come_from_the_dag_run(self, telegram):
        context = _context(
            task_states={"dbt_build": "failed"},
            logical_date="2026-08-27T13:00:00+00:00",
        )

        assert send_failure_alert(context, "dbt build FAILED", task_ids=("dbt_build",))

        text = _sent_text(telegram)
        assert _RUN_ID in text
        assert "2026-08-27T13:00:00+00:00" in text

    def test_a_run_with_no_logical_date_still_gets_a_date(self, telegram):
        """logical_date is None for an asset- or manually-triggered run.
        run_after is always set."""
        context = _context(task_states={"dbt_build": "failed"})

        send_failure_alert(context, "dbt build FAILED", task_ids=("dbt_build",))

        assert "2026-08-27T13:05:00+00:00" in _sent_text(telegram)


class TestItNamesTheTaskThatBroke:
    def test_a_flush_failure_is_reported_with_no_dbt_build_xcom(self, telegram):
        """The case the repair exists for.

        A 500 on flush_silver skips the dbt build, so there is no dbt_build
        XCom at all — and the old code pulled task_ids="dbt_build" and nothing
        else. Even unbroken it would have paged "FAILED" with a run id and no
        indication of which task died.
        """
        context = _context(
            task_states={
                "flush_silver_observations": "failed",
                "flush_staging_events": "upstream_failed",
                "notify": "running",
            },
            xcoms={
                "flush_silver_observations": {
                    "flushed": 0,
                    "error": "XMinioStorageFull",
                    "failure_reason": "flush aborted: XMinioStorageFull",
                },
            },
        )

        send_failure_alert(
            context, "hourly analytics refresh FAILED",
            task_ids=("flush_silver_observations", "flush_staging_events", "dbt_build"),
        )

        text = _sent_text(telegram)
        assert "Failed:  flush_silver_observations" in text
        assert "flush_silver_observations: flush aborted: XMinioStorageFull" in text

    def test_a_skipped_downstream_task_is_not_blamed(self, telegram):
        context = _context(
            task_states={
                "flush_silver_observations": "failed",
                "flush_staging_events": "upstream_failed",
            },
        )

        send_failure_alert(
            context, "hourly analytics refresh FAILED",
            task_ids=("flush_silver_observations", "flush_staging_events"),
        )

        failed_line = [
            line for line in _sent_text(telegram).splitlines()
            if line.startswith("Failed:")
        ][0]
        assert "flush_staging_events" not in failed_line

    def test_several_failed_tasks_are_all_named(self, telegram):
        context = _context(
            task_states={
                "flush_silver_observations": "failed",
                "reconcile_cooldown_cohorts": "failed",
            },
            xcoms={
                "flush_silver_observations": {"error": "DB connection failed"},
                "reconcile_cooldown_cohorts": {"error": "ops unreachable"},
            },
        )

        send_failure_alert(
            context, "hourly analytics refresh FAILED",
            task_ids=("flush_silver_observations", "reconcile_cooldown_cohorts"),
        )

        text = _sent_text(telegram)
        assert "DB connection failed" in text
        assert "ops unreachable" in text

    def test_a_sensor_failure_is_named_even_with_no_xcom(self, telegram):
        """`ready` is in the fan-in and leaves no result behind. Naming it still
        beats naming the DAG, which is the defect Plan 140 Stage 4 fixed for
        the health sensors."""
        context = _context(task_states={"ready": "failed"})

        send_failure_alert(
            context, "hourly analytics refresh FAILED", task_ids=("dbt_build",)
        )

        assert "Failed:  ready" in _sent_text(telegram)

    def test_a_task_that_succeeded_still_has_its_result_quoted(self, telegram):
        """pack_bronze_html's detail can sit on a task that did not itself fail
        — a pack that stopped for a deploy, say. Quoting only failed tasks
        would drop it."""
        context = _context(
            task_states={"verify_pack_read_path": "failed"},
            xcoms={"pack_bronze_html": {"error": "orphan packs found"}},
        )

        send_failure_alert(
            context, "bronze pack lifecycle FAILED",
            task_ids=("pack_bronze_html", "verify_pack_read_path"),
        )

        assert "orphan packs found" in _sent_text(telegram)


class TestTheDetailItQuotes:
    def test_the_dbt_build_triple_survives(self):
        """The one case the old code did handle. It must keep working."""
        lines = task_detail("dbt_build", {
            "cmd": "dbt build --select tag:hourly_core",
            "returncode": 2,
            "stderr": "Compilation Error in model fct_listing_price",
        })

        assert "Command: dbt build --select tag:hourly_core" in lines
        assert "Exit:    2" in lines
        assert "Compilation Error in model fct_listing_price" in lines

    def test_failure_reason_wins_over_error(self):
        lines = task_detail(
            "flush_staging_events",
            {"error": "one or more tables failed",
             "failure_reason": "2 staging table(s) failed to flush: a, b"},
        )

        assert lines[0] == "flush_staging_events: 2 staging table(s) failed to flush: a, b"

    def test_a_task_with_no_result_says_nothing(self):
        assert task_detail("dbt_build", None) == []
        assert task_detail("dbt_build", {}) == []

    def test_a_clean_result_says_nothing(self):
        assert task_detail("flush_silver_observations", {"flushed": 0, "error": None}) == []

    def test_the_fallback_reason_is_only_consulted_when_needed(self):
        """pack/verify report failure in their own vocabulary. That must not
        override a service's own failure_reason when there is one."""
        def fallback(result):
            return "deploy intent remained pending through all retries"

        with_reason = task_detail(
            "pack_bronze_html",
            {"failure_reason": "run aborted: no free space", "stopped_for_deploy": True},
            fallback,
        )
        assert with_reason[0].endswith("run aborted: no free space")

        without = task_detail("pack_bronze_html", {"stopped_for_deploy": True}, fallback)
        assert without[0].endswith("deploy intent remained pending through all retries")

    def test_a_failures_list_is_quoted(self):
        lines = task_detail(
            "prune_packed_source_html",
            {
                "error": "refused",
                "failures": [{"source_key": "html/x", "error": "sha256 mismatch"}],
            },
        )

        assert any("sha256 mismatch" in line for line in lines)


class TestItStillPagesWhenThingsGoWrongInsideIt:
    def test_the_page_survives_a_state_lookup_failure(self, telegram):
        """get_task_states is a round trip to the execution API. If it fails,
        the page still has to go out — a notifier that dies on its own
        diagnostics is the failure mode being repaired here."""
        context = _context(
            states_error=RuntimeError("execution API unreachable"),
            xcoms={"flush_silver_observations": {"error": "XMinioStorageFull"}},
        )

        assert send_failure_alert(
            context, "hourly analytics refresh FAILED",
            task_ids=("flush_silver_observations",),
        )

        text = _sent_text(telegram)
        assert "hourly analytics refresh FAILED" in text
        assert "XMinioStorageFull" in text
        # It cannot say which task failed, so it does not guess.
        assert "Failed:" not in text

    def test_a_rejected_send_is_logged_rather_than_passing_as_delivered(
        self, telegram, caplog
    ):
        """A 400 from Telegram looks exactly like a success unless it is read.
        These pagers spent months looking delivered."""
        telegram.return_value = MagicMock(
            ok=False, status_code=400, text="Bad Request: message is too long"
        )
        context = _context(task_states={"dbt_build": "failed"})

        with caplog.at_level("WARNING"):
            sent = send_failure_alert(context, "dbt build FAILED", task_ids=("dbt_build",))

        assert sent is False
        assert "Telegram rejected" in caplog.text

    def test_a_connection_error_is_swallowed_not_raised(self, telegram, caplog):
        """notify must not fail the DAG run a second time on its own account."""
        telegram.side_effect = requests.ConnectionError("boom")
        context = _context(task_states={"dbt_build": "failed"})

        with caplog.at_level("WARNING"):
            assert send_failure_alert(
                context, "dbt build FAILED", task_ids=("dbt_build",)
            ) is False

        assert "Failed to send Telegram notification" in caplog.text

    def test_the_message_stays_under_telegrams_limit(self, telegram):
        """Telegram rejects a body over 4096 characters outright, and four
        tasks quoting stderr clears it easily."""
        tasks = ("a", "b", "c", "d")
        context = _context(
            task_states={task: "failed" for task in tasks},
            xcoms={task: {"error": "x" * 900, "stderr": "y" * 4000} for task in tasks},
        )

        send_failure_alert(context, "FAILED", task_ids=tasks)

        assert len(_sent_text(telegram)) <= MAX_MESSAGE_CHARS

    def test_no_credentials_means_no_send(self, mocker, monkeypatch):
        monkeypatch.setenv("TELEGRAM_API", "")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "")
        post = mocker.patch("notifications.requests.post")

        sent = send_failure_alert(
            _context(task_states={"dbt_build": "failed"}),
            "dbt build FAILED",
            task_ids=("dbt_build",),
        )

        assert sent is False
        post.assert_not_called()


class TestFailedTaskIds:
    def test_it_ignores_the_notify_task_itself(self):
        context = _context(task_states={"notify": "failed", "dbt_build": "failed"})

        assert failed_task_ids(context) == ["dbt_build"]

    def test_it_accepts_an_enum_state_as_well_as_a_string(self):
        """The execution API hands back TaskInstanceState, not str."""
        state = MagicMock()
        state.value = "failed"
        context = _context(task_states={"dbt_build": state})

        assert failed_task_ids(context) == ["dbt_build"]

    def test_an_empty_run_yields_nothing(self):
        assert failed_task_ids(_context(task_states={})) == []
