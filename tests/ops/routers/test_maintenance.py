"""Unit tests for ops/routers/maintenance.py — orphan expiry + cleanup endpoints."""
from unittest.mock import MagicMock

from ops.queries import (
    INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT,
    MARK_ARTIFACT_STATUS,
)
from ops.routers.maintenance import (
    _evict_delisted_cooldowns,
    _reap_stuck_processing,
    _reconcile_cooldown_cohorts,
)


def _executed(cursor, sql):
    """Params passed to cursor.execute(SQL, params) for a given SQL constant."""
    return [
        c.args[1] for c in cursor.execute.call_args_list
        if c.args and c.args[0] == sql
    ]


# ---------------------------------------------------------------------------
# POST /maintenance/expire-orphan-detail-claims
# ---------------------------------------------------------------------------

class TestExpireOrphanDetailClaims:
    def test_returns_affected_count(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [("listing-abc",), ("listing-def",)]
        resp = mock_client.post("/maintenance/expire-orphan-detail-claims")
        assert resp.status_code == 200
        assert resp.json() == {"affected": 2}

    def test_no_orphans_returns_zero(self, mock_client, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        resp = mock_client.post("/maintenance/expire-orphan-detail-claims")
        assert resp.json() == {"affected": 0}


# ---------------------------------------------------------------------------
# reap-stuck-processing
# ---------------------------------------------------------------------------

def _stuck_row(aid, path):
    return {
        "artifact_id": aid, "minio_path": path, "artifact_type": "detail_page",
        "fetched_at": None, "listing_id": "L1", "run_id": "R1",
    }


class TestReapStuckProcessing:
    def test_retry_when_object_exists(self, mock_cursor_context, mocker):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [_stuck_row(1, "s3://b/a.zst")]
        mocker.patch("ops.routers.maintenance.object_exists", return_value=True)

        res = _reap_stuck_processing()

        assert res == {"stuck": 1, "retried": 1, "skipped": 0}
        assert _executed(cursor, MARK_ARTIFACT_STATUS)[0]["status"] == "retry"

    def test_skip_when_object_missing(self, mock_cursor_context, mocker):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [_stuck_row(1, "s3://b/a.zst")]
        mocker.patch("ops.routers.maintenance.object_exists", return_value=False)

        res = _reap_stuck_processing()

        assert res == {"stuck": 1, "retried": 0, "skipped": 1}
        assert _executed(cursor, MARK_ARTIFACT_STATUS)[0]["status"] == "skip"

    def test_mixed(self, mock_cursor_context, mocker):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [_stuck_row(1, "s3://b/a.zst"), _stuck_row(2, "s3://b/b.zst")]
        mocker.patch("ops.routers.maintenance.object_exists", side_effect=[True, False])

        res = _reap_stuck_processing()

        assert res == {"stuck": 2, "retried": 1, "skipped": 1}
        assert [p["status"] for p in _executed(cursor, MARK_ARTIFACT_STATUS)] == ["retry", "skip"]

    def test_nothing_stuck(self, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        assert _reap_stuck_processing() == {"stuck": 0, "retried": 0, "skipped": 0}

    def test_route_registered(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.maintenance._reap_stuck_processing",
            return_value={"stuck": 0, "retried": 0, "skipped": 0},
        )
        resp = mock_client.post("/maintenance/reap-stuck-processing")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# evict-delisted-cooldowns
# ---------------------------------------------------------------------------

class TestEvictDelistedCooldowns:
    def test_evicts_and_emits_cleared(self, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = [("lid1", 2), ("lid2", 3)]

        res = _evict_delisted_cooldowns()

        assert res == {"evicted": 2}
        cleared = _executed(cursor, INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT)
        assert {"listing_id": "lid1", "num_of_attempts": 2} in cleared
        assert {"listing_id": "lid2", "num_of_attempts": 3} in cleared

    def test_nothing_delisted(self, mock_cursor_context):
        _, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        assert _evict_delisted_cooldowns() == {"evicted": 0}
        assert _executed(cursor, INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT) == []


# ---------------------------------------------------------------------------
# reconcile-cooldown-cohorts
# ---------------------------------------------------------------------------

def _mock_duckdb(mocker, counted_rows):
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = counted_rows
    mocker.patch("ops.routers.maintenance.get_duckdb_s3_connection", return_value=con)


class TestReconcileCooldownCohorts:
    def test_emits_cleared_for_orphans_only(self, mock_cursor_context, mocker):
        _, cursor = mock_cursor_context
        # counted in analytics: lid1, lid2, lid3
        _mock_duckdb(mocker, [("lid1", 2), ("lid2", 3), ("lid3", 1)])
        # live table = {lid1}; pending cleared = {lid2}  -> orphan = lid3
        cursor.fetchall.side_effect = [[("lid1",)], [("lid2",)]]
        ev = mocker.patch("psycopg2.extras.execute_values")

        res = _reconcile_cooldown_cohorts()

        assert res == {"counted": 3, "live": 1, "pending_cleared": 1, "cleared": 1}
        assert ev.call_args.args[2] == [("lid3", "cleared", 1)]

    def test_no_orphans_no_insert(self, mock_cursor_context, mocker):
        _, cursor = mock_cursor_context
        _mock_duckdb(mocker, [("lid1", 2)])
        cursor.fetchall.side_effect = [[("lid1",)], []]  # live has it
        ev = mocker.patch("psycopg2.extras.execute_values")

        res = _reconcile_cooldown_cohorts()

        assert res["cleared"] == 0
        ev.assert_not_called()

    def test_no_parquet_returns_zeros(self, mocker):
        con = MagicMock()
        con.execute.side_effect = Exception("IO Error: No files found that match the pattern")
        mocker.patch("ops.routers.maintenance.get_duckdb_s3_connection", return_value=con)
        assert _reconcile_cooldown_cohorts() == {
            "counted": 0, "live": 0, "pending_cleared": 0, "cleared": 0,
        }
