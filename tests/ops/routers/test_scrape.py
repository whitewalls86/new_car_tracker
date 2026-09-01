"""
Unit tests for ops.routers.scrape.

DB interactions are mocked via mock_cursor_context (patches psycopg2.connect).
Tests validate logic branches — SQL correctness is covered by Layer 4 integration tests.
"""
import datetime
import json
import uuid
from unittest.mock import MagicMock

import pytest
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

    def test_none_description_raises_value_error(self, mock_cursor_context):
        """cur.description is None when no result set returned → ValueError, not AttributeError."""
        conn, cursor = mock_cursor_context
        cursor.fetchall.return_value = [("listing-aaa",)]
        cursor.description = None  # simulates cursor that executed no result-set query

        with pytest.raises(ValueError, match="no result set"):
            client.post("/scrape/claims/claim-batch")

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
    def test_returns_run_id_and_counts(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [{"listing_id": "listing-aaa", "status": "ok"}],
        })

        assert resp.status_code == 200
        data = resp.json()
        assert data["run_id"] == run_id
        assert data["total"] == 1
        assert data["errors"] == 0

    def test_all_failed_reflected_in_error_count(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [
                {"listing_id": "listing-aaa", "status": "failed"},
                {"listing_id": "listing-bbb", "status": "failed"},
            ],
        })

        data = resp.json()
        assert data["errors"] == 2
        assert data["total"] == 2

    def test_mixed_results_counts_errors_correctly(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [
                {"listing_id": "listing-aaa", "status": "ok"},
                {"listing_id": "listing-bbb", "status": "failed"},
            ],
        })

        data = resp.json()
        assert data["errors"] == 1
        assert data["total"] == 2

    def test_empty_results_returns_zero_counts(self, mock_cursor_context):
        run_id = str(uuid.uuid4())

        resp = client.post("/scrape/claims/release", json={
            "run_id": run_id,
            "results": [],
        })

        data = resp.json()
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


class TestReleaseRecordsFetch:
    """POST /scrape/claims/release stamps last_detail_fetched_at (Plan 147).

    These are endpoint-contract tests, not evidence about production. The only
    live caller, scrape_detail_pages, reports 'ok' for every claimed listing,
    so the failed/skipped branches below are a rule no caller exercises today.
    They are asserted so the endpoint stays honest for any future caller.
    """

    @staticmethod
    def _fetch_update(cursor):
        """The UPDATE that records the spent request, with its bound ids."""
        for call in cursor.execute.call_args_list:
            sql = call[0][0]
            if "last_detail_fetched_at" in sql:
                return sql, call[0][1]
        return None, None

    def test_ok_records_the_fetch(self, mock_cursor_context):
        _, cursor = mock_cursor_context

        client.post("/scrape/claims/release", json={
            "run_id": str(uuid.uuid4()),
            "results": [{"listing_id": "listing-aaa", "status": "ok"}],
        })

        sql, params = self._fetch_update(cursor)
        assert sql is not None, "an ok result must record the spent request"
        assert "UPDATE ops.price_observations" in sql
        assert params[0] == ["listing-aaa"]

    def test_failed_records_the_fetch(self, mock_cursor_context):
        """A failed fetch spent a request, so the backoff applies to it."""
        _, cursor = mock_cursor_context

        client.post("/scrape/claims/release", json={
            "run_id": str(uuid.uuid4()),
            "results": [{"listing_id": "listing-bbb", "status": "failed"}],
        })

        sql, params = self._fetch_update(cursor)
        assert sql is not None, "a failed result spent a request too"
        assert params[0] == ["listing-bbb"]

    def test_skipped_does_not_record_the_fetch(self, mock_cursor_context):
        """A skipped listing was never attempted, so no request was spent."""
        _, cursor = mock_cursor_context

        client.post("/scrape/claims/release", json={
            "run_id": str(uuid.uuid4()),
            "results": [{"listing_id": "listing-ccc", "status": "skipped"}],
        })

        sql, _ = self._fetch_update(cursor)
        assert sql is None

    def test_mixed_batch_records_only_the_attempted(self, mock_cursor_context):
        _, cursor = mock_cursor_context

        resp = client.post("/scrape/claims/release", json={
            "run_id": str(uuid.uuid4()),
            "results": [
                {"listing_id": "listing-ok", "status": "ok"},
                {"listing_id": "listing-failed", "status": "failed"},
                {"listing_id": "listing-skipped", "status": "skipped"},
            ],
        })

        _, params = self._fetch_update(cursor)
        assert params[0] == ["listing-ok", "listing-failed"]
        assert resp.json()["fetches_recorded"] == 2

    def test_claims_are_still_released_for_skipped_listings(self, mock_cursor_context):
        """The backoff is not a claim leak: a skipped listing keeps no claim."""
        _, cursor = mock_cursor_context

        client.post("/scrape/claims/release", json={
            "run_id": str(uuid.uuid4()),
            "results": [{"listing_id": "listing-ccc", "status": "skipped"}],
        })

        delete = [
            c for c in cursor.execute.call_args_list
            if "DELETE FROM detail_scrape_claims" in c[0][0]
        ]
        assert len(delete) == 1
        assert delete[0][0][1][0] == ["listing-ccc"]

    def test_empty_results_records_nothing(self, mock_cursor_context):
        _, cursor = mock_cursor_context

        resp = client.post("/scrape/claims/release", json={
            "run_id": str(uuid.uuid4()),
            "results": [],
        })

        sql, _ = self._fetch_update(cursor)
        assert sql is None
        assert resp.json()["fetches_recorded"] == 0

