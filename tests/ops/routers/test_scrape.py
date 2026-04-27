"""
Unit tests for ops.routers.scrape.

DB interactions are mocked via mock_cursor_context (patches psycopg2.connect).
Tests validate logic branches — SQL correctness is covered by Layer 3 integration tests.
"""
import datetime
import json
import uuid
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from ops.app import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# POST /scrape/rotation/advance
# ---------------------------------------------------------------------------

class TestAdvanceRotation:
    # Mock shape: MAX(last_queued_at) always returns a row — either (None,) or (timestamp,).
    # fetchone.side_effect entries follow the query order:
    #   1. MAX(last_queued_at) gap check
    #   2. slot_row (rotation_slot query)
    #   3. legacy_row (fallback single-config query)

    def test_too_soon_returns_null_slot(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        recent = datetime.datetime.now(datetime.timezone.utc)
        cursor.fetchone.side_effect = [(recent,)]  # gap check: recently queued

        resp = client.post(
            "/scrape/rotation/advance",
            params={"min_idle_minutes": 1439, "min_gap_minutes": 9999},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["slot"] is None
        assert data["run_id"] is None
        assert data["reason"] == "too_soon"
        assert "last_run_minutes_ago" in data

    def test_no_slot_due_returns_empty(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        # gap check: no recent queued, slot_row: None, legacy_row: None
        cursor.fetchone.side_effect = [(None,), None, None]

        resp = client.post("/scrape/rotation/advance")

        assert resp.status_code == 200
        data = resp.json()
        assert data["slot"] is None
        assert data["run_id"] is None
        assert data["configs"] == []

    def test_legacy_fallback_returns_single_config(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        params = json.dumps({"makes": ["Honda"], "scopes": ["local"]})
        # gap check: no recent queued, slot_row: None, legacy_row: found
        cursor.fetchone.side_effect = [(None,), None, ("legacy-key", params)]

        resp = client.post("/scrape/rotation/advance")

        assert resp.status_code == 200
        data = resp.json()
        assert data["slot"] is None
        assert data["run_id"] is not None
        uuid.UUID(data["run_id"])  # valid UUID
        assert len(data["configs"]) == 1
        assert data["configs"][0]["search_key"] == "legacy-key"
        assert data["configs"][0]["scopes"] == ["local"]

    def test_slot_path_returns_slot_and_configs(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        params_a = json.dumps({"makes": ["Honda"], "scopes": ["national"]})
        params_b = json.dumps({"makes": ["Toyota"], "scopes": ["local"]})
        # gap check: no recent queued, slot_row: slot 3 found
        cursor.fetchone.side_effect = [(None,), (3,)]
        cursor.fetchall.return_value = [
            ("slot3-a", params_a),
            ("slot3-b", params_b),
        ]

        resp = client.post("/scrape/rotation/advance")

        assert resp.status_code == 200
        data = resp.json()
        assert data["slot"] == 3
        assert data["run_id"] is not None
        uuid.UUID(data["run_id"])  # valid UUID
        assert len(data["configs"]) == 2
        assert data["configs"][0]["search_key"] == "slot3-a"
        assert data["configs"][1]["search_key"] == "slot3-b"

    def test_response_always_has_slot_configs_and_run_id_keys(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        cursor.fetchone.side_effect = [(None,), None, None]

        resp = client.post("/scrape/rotation/advance")

        data = resp.json()
        assert "slot" in data
        assert "configs" in data
        assert "run_id" in data

    def test_no_last_queued_does_not_trigger_too_soon(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        # MAX returns (None,) — no configs ever queued — must not be blocked
        cursor.fetchone.side_effect = [(None,), None, None]

        resp = client.post(
            "/scrape/rotation/advance",
            params={"min_gap_minutes": 9999},
        )

        assert resp.status_code == 200
        assert resp.json().get("reason") != "too_soon"


# ---------------------------------------------------------------------------
# POST /scrape/claims/claim-batch
# ---------------------------------------------------------------------------

class TestClaimBatch:
    def test_returns_run_id_and_listings(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        cursor.description = []

        resp = client.post("/scrape/claims/claim-batch")

        assert resp.status_code == 200
        data = resp.json()
        assert "run_id" in data
        assert "listings" in data
        uuid.UUID(data["run_id"])  # run_id is a valid UUID

    def test_empty_queue_returns_empty_listings(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        cursor.description = []

        resp = client.post("/scrape/claims/claim-batch")

        assert resp.json()["listings"] == []

    def test_inserts_run_row(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        cursor.fetchall.return_value = []
        cursor.description = []

        client.post("/scrape/claims/claim-batch")

        # First execute call should be the INSERT INTO runs
        first_call_sql = cursor.execute.call_args_list[0][0][0]
        assert "INSERT INTO runs" in first_call_sql

    def test_listings_returned_from_queue(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        cursor.description = [
            MagicMock(name="col0"), MagicMock(name="col1"),
        ]
        cursor.description[0].__getitem__ = lambda s, i: "listing_id" if i == 0 else "vin"
        # Simulate two rows returned from the claim query
        cursor.fetchall.return_value = [
            ("listing-aaa", "VIN001"),
            ("listing-bbb", "VIN002"),
        ]
        # Make description[n][0] return column names
        cursor.description[0] = ("listing_id",)
        cursor.description[1] = ("vin",)

        resp = client.post("/scrape/claims/claim-batch")

        data = resp.json()
        assert len(data["listings"]) == 2
        assert data["listings"][0]["listing_id"] == "listing-aaa"
        assert data["listings"][1]["listing_id"] == "listing-bbb"


# ---------------------------------------------------------------------------
# POST /scrape/claims/release
# ---------------------------------------------------------------------------

class TestReleaseClaims:
    def test_returns_run_id_and_status(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [{"listing_id": "listing-aaa", "status": "ok"}],
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["run_id"] == run_id
        assert data["status"] == "completed"

    def test_all_failed_sets_status_failed(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [
                {"listing_id": "listing-aaa", "status": "failed"},
                {"listing_id": "listing-bbb", "status": "failed"},
            ],
        })

        assert resp.json()["status"] == "failed"

    def test_mixed_results_sets_status_completed(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [
                {"listing_id": "listing-aaa", "status": "ok"},
                {"listing_id": "listing-bbb", "status": "failed"},
            ],
        })

        data = resp.json()
        assert data["status"] == "completed"
        assert data["errors"] == 1
        assert data["total"] == 2

    def test_empty_results_returns_completed(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [],
        })

        data = resp.json()
        assert data["status"] == "completed"
        assert data["total"] == 0
        assert data["errors"] == 0

    def test_deletes_claims_for_listing_ids(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        run_id = str(uuid.uuid4())

        client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [
                {"listing_id": "listing-aaa", "status": "ok"},
                {"listing_id": "listing-bbb", "status": "ok"},
            ],
        })

        sql_calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("DELETE FROM detail_scrape_claims" in sql for sql in sql_calls)

    def test_updates_run_status_in_db(self, mock_cursor_context):
        conn, cursor = mock_cursor_context
        run_id = str(uuid.uuid4())

        client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [{"listing_id": "listing-aaa", "status": "ok"}],
        })

        sql_calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("UPDATE runs" in sql for sql in sql_calls)
