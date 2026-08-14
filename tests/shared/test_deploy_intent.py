"""Unit tests for shared/deploy_intent.py (Plan 131 Stage 5 D3b).

Almost all of the risk here is in one direction. `long_jobs_paused()` gates
whether a ten-hour job keeps running, so a false True throws the run away and a
false False lets a deploy race it. The plan chose which way to be wrong on
purpose — **fail open** — and that decision is what these tests hold in place.
"""
from unittest.mock import MagicMock

import pytest

from shared import deploy_intent


@pytest.fixture
def cursor(mocker):
    """Patch db_cursor to yield a MagicMock cursor."""
    cur = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cur)
    ctx.__exit__ = MagicMock(return_value=False)
    mocker.patch.object(deploy_intent, "db_cursor", return_value=ctx)
    return cur


class TestPaused:
    def test_pending_deploy_with_the_flag_set_pauses(self, cursor):
        cursor.fetchone.return_value = (True,)

        assert deploy_intent.long_jobs_paused() is True

    def test_no_deploy_does_not_pause(self, cursor):
        # intent = 'none', so the predicate is False in SQL.
        cursor.fetchone.return_value = (False,)

        assert deploy_intent.long_jobs_paused() is False

    def test_a_deploy_that_opted_out_does_not_pause(self, cursor):
        cursor.fetchone.return_value = (False,)

        assert deploy_intent.long_jobs_paused() is False

    def test_no_row_does_not_pause(self, cursor):
        cursor.fetchone.return_value = None

        assert deploy_intent.long_jobs_paused() is False

    def test_null_predicate_does_not_pause(self, cursor):
        # A NULL from SQL must not be truthy by accident.
        cursor.fetchone.return_value = (None,)

        assert deploy_intent.long_jobs_paused() is False


class TestFailsOpen:
    """A Postgres blip must not stop a ten-hour job. This is the decision."""

    def test_a_db_error_does_not_pause(self, mocker):
        mocker.patch.object(
            deploy_intent, "db_cursor", side_effect=RuntimeError("connection refused")
        )

        assert deploy_intent.long_jobs_paused() is False

    def test_a_query_error_does_not_pause(self, cursor):
        cursor.execute.side_effect = RuntimeError("relation does not exist")

        assert deploy_intent.long_jobs_paused() is False

    def test_failing_open_is_logged_at_warning(self, mocker, caplog):
        mocker.patch.object(
            deploy_intent, "db_cursor", side_effect=RuntimeError("connection refused")
        )

        with caplog.at_level("WARNING"):
            deploy_intent.long_jobs_paused()

        # Silent fail-open is how a stuck deploy pause becomes unexplainable.
        assert "could not read the pause flag" in caplog.text
        assert "connection refused" in caplog.text


def test_the_query_does_not_self_heal_a_stale_intent(cursor):
    """A forgotten intent keeps jobs paused until someone clears it.

    Deliberate (D3b): the DAG exhausts its retries and pages, because an intent
    nobody released is a real problem and not one a long job should paper over
    by starting anyway. A staleness clause here would hide it.
    """
    deploy_intent.long_jobs_paused()

    sql = cursor.execute.call_args[0][0]
    assert "interval" not in sql.lower()
    assert "requested_at" not in sql.lower()
