"""
Layer 1 — SQL smoke tests for flush_silver_observations.

Validates the SELECT / DELETE SQL patterns used by flush_silver_observations
against a real DB with Flyway migrations applied. Tests catch schema breakage
(column renames, type changes, dropped tables) — not business logic.

All tests run inside a rolled-back transaction; no data persists.
"""
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)

# Minimal set of NOT NULL columns required by staging.silver_observations
_REQUIRED_COLS = ("artifact_id", "listing_id", "source", "listing_state", "fetched_at")


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------

def _insert_observation(cur, source="detail") -> int:
    """Insert a minimal staging.silver_observations row. Returns id."""
    cur.execute(
        """INSERT INTO staging.silver_observations
               (artifact_id, listing_id, source, listing_state, fetched_at)
           VALUES (999999, 'listing-smoke-test', %s, 'active', %s)
           RETURNING id""",
        (source, _NOW),
    )
    return cur.fetchone()["id"]


# ---------------------------------------------------------------------------
# SELECT MAX(id) — snapshot boundary query
# ---------------------------------------------------------------------------

class TestSelectMaxId:
    def test_returns_none_when_empty(self, cur):
        cur.execute("SELECT MAX(id) FROM staging.silver_observations")
        assert cur.fetchone()["max"] is None

    def test_returns_inserted_id(self, cur):
        row_id = _insert_observation(cur)
        cur.execute("SELECT MAX(id) FROM staging.silver_observations")
        assert cur.fetchone()["max"] == row_id

    def test_returns_highest_id_when_multiple_rows(self, cur):
        _insert_observation(cur)
        id2 = _insert_observation(cur)
        cur.execute("SELECT MAX(id) FROM staging.silver_observations")
        assert cur.fetchone()["max"] == id2


# ---------------------------------------------------------------------------
# SELECT cols WHERE id <= max — fetch rows query
# ---------------------------------------------------------------------------

class TestSelectRowsUpToMax:
    def test_all_processor_columns_present(self, cur):
        _insert_observation(cur)
        cur.execute(
            """SELECT id,
                      artifact_id, listing_id, vin, canonical_detail_url,
                      source, listing_state, fetched_at,
                      price, make, model, trim, year, mileage, msrp,
                      stock_type, fuel_type, body_style,
                      dealer_name, dealer_zip, customer_id, seller_id,
                      dealer_street, dealer_city, dealer_state, dealer_phone,
                      dealer_website, dealer_cars_com_url, dealer_rating,
                      financing_type, seller_zip, seller_customer_id,
                      page_number, position_on_page, trid, isa_context,
                      body, condition
               FROM staging.silver_observations
               WHERE id <= (SELECT MAX(id) FROM staging.silver_observations)"""
        )
        row = cur.fetchone()
        assert row is not None

    def test_source_value_round_trips(self, cur):
        _insert_observation(cur, source="srp")
        cur.execute(
            "SELECT source FROM staging.silver_observations"
            " WHERE id <= (SELECT MAX(id) FROM staging.silver_observations)"
        )
        row = cur.fetchone()
        assert row["source"] == "srp"

    def test_snapshot_boundary_excludes_later_rows(self, cur):
        id1 = _insert_observation(cur)
        id2 = _insert_observation(cur)
        cur.execute(
            "SELECT id FROM staging.silver_observations WHERE id <= %s", (id1,)
        )
        returned = {r["id"] for r in cur.fetchall()}
        assert id1 in returned
        assert id2 not in returned

    def test_nullable_columns_accept_null(self, cur):
        """Columns like vin, price, make, model must be nullable (no NOT NULL)."""
        row_id = _insert_observation(cur)
        cur.execute(
            "SELECT vin, price, make, model FROM staging.silver_observations WHERE id = %s",
            (row_id,),
        )
        row = cur.fetchone()
        assert row["vin"] is None
        assert row["price"] is None
        assert row["make"] is None
        assert row["model"] is None


# ---------------------------------------------------------------------------
# DELETE WHERE id <= max — flush delete query
# ---------------------------------------------------------------------------

class TestDeleteUpToMax:
    def test_row_deleted_after_flush(self, cur):
        row_id = _insert_observation(cur)
        cur.execute(
            "DELETE FROM staging.silver_observations WHERE id <= %s", (row_id,)
        )
        cur.execute(
            "SELECT id FROM staging.silver_observations WHERE id = %s", (row_id,)
        )
        assert cur.fetchone() is None

    def test_only_rows_up_to_boundary_deleted(self, cur):
        id1 = _insert_observation(cur)
        id2 = _insert_observation(cur)
        cur.execute(
            "DELETE FROM staging.silver_observations WHERE id <= %s", (id1,)
        )
        cur.execute(
            "SELECT id FROM staging.silver_observations WHERE id = %s", (id2,)
        )
        assert cur.fetchone() is not None

    def test_delete_returns_correct_rowcount(self, cur):
        _insert_observation(cur)
        id2 = _insert_observation(cur)
        cur.execute(
            "DELETE FROM staging.silver_observations WHERE id <= %s", (id2,)
        )
        assert cur.rowcount == 2
