"""Plan 145 Stage 5 slice 3 -- the V040 live-state verifier, against real Postgres.

The unit tests use a fake connection with no snapshot semantics, so they cannot
show the one property that matters: that the verifier's transaction actually
*sees* a write another connection commits between the two snapshots. Under
``REPEATABLE READ`` it would not, and the equality check could never fail. This
file proves it can, against a real database with V001-V047 applied.
"""
import json
import uuid

import pytest

from scripts.oneoff.verify_recovery_live_state import run
from shared.db import get_conn

pytestmark = pytest.mark.integration

# The session-scoped _patch_shared_db_kwargs fixture in conftest.py points
# shared.db at the throwaway test database, so get_conn is the test DSN.
_connect = get_conn


def test_clean_window_run_passes_against_real_relations(tmp_path):
    report_path = tmp_path / "clean.json"
    rc = run(["--window", "itest-clean", "--report", str(report_path)],
             connect=_connect, canary=lambda: None)
    assert rc == 0
    report = json.loads(report_path.read_text())
    assert report["passed"] is True
    assert report["txid"]["single_transaction"] is True
    assert report["changed_relations"] == {}
    # every protected relation and V040 view resolved without erroring
    assert set(report["snapshot_before"]) == set(report["relations"])


def test_a_committed_write_between_the_snapshots_is_seen_and_fails(pg_conn, tmp_path):
    """READ COMMITTED, not REPEATABLE READ: the second snapshot must see the row
    the canary commits on another connection."""
    marker = uuid.uuid4()

    def _canary():
        with pg_conn.cursor() as cur:            # pg_conn is autocommit
            cur.execute(
                "INSERT INTO ops.blocked_cooldown (listing_id) VALUES (%s)",
                (str(marker),),
            )

    report_path = tmp_path / "dirty.json"
    try:
        rc = run(["--window", "itest-dirty", "--report", str(report_path)],
                 connect=_connect, canary=_canary)
        assert rc == 1
        report = json.loads(report_path.read_text())
        assert report["passed"] is False
        assert report["txid"]["single_transaction"] is True
        changed = report["changed_relations"]
        assert "ops.blocked_cooldown" in changed
        b = changed["ops.blocked_cooldown"]
        assert b["after"]["rows"] == b["before"]["rows"] + 1
    finally:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM ops.blocked_cooldown WHERE listing_id = %s",
                        (str(marker),))
