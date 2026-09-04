"""
Layer 2 — SQL smoke tests for flush_silver_observations.

Validates the SELECT / DELETE SQL patterns used by flush_silver_observations
against a real DB with Flyway migrations applied. Tests catch schema breakage
(column renames, type changes, dropped tables) — not business logic.

All tests run inside a rolled-back transaction; no data persists.
"""
from datetime import datetime, timezone

import pytest

from archiver.processors.flush_silver_observations import _DB_COLUMNS
from archiver.queries import (
    DELETE_SILVER_OBSERVATIONS_UP_TO_ID,
    SELECT_MAX_SILVER_OBSERVATION_ID,
    SELECT_SILVER_OBSERVATIONS_UP_TO_ID,
)

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


# ===========================================================================
# Statements imported from archiver.queries — Plan 162 Stage L
# ===========================================================================

class TestExtractedSilverFlushStatements:
    """The three statements of the flush, as the flush itself holds them.

    Everything above retypes SQL that resembles what the processor runs. Until
    Plan 162 Stage L there was no alternative: the statements were written at
    their ``cur.execute()`` call sites, so no test could import them, and a
    retyped statement passes forever while the original rots. These execute
    ``archiver.queries``' constants — the same objects
    ``flush_silver_observations`` executes.

    Postgres (``cur``), not DuckDB: the flush reads and deletes
    ``staging.silver_observations`` over psycopg2, and only the Parquet write
    in between touches MinIO — that half is pyarrow, not SQL.
    """

    def _select_rows(self) -> str:
        """The projection statement, filled from production's own column list."""
        return SELECT_SILVER_OBSERVATIONS_UP_TO_ID.format(
            columns=", ".join(_DB_COLUMNS)
        )

    def test_select_max_id_on_an_empty_table(self, cur):
        cur.execute(SELECT_MAX_SILVER_OBSERVATION_ID)
        assert cur.fetchone()["max"] is None

    def test_select_max_id_sees_an_inserted_row(self, cur):
        row_id = _insert_observation(cur)
        cur.execute(SELECT_MAX_SILVER_OBSERVATION_ID)
        assert cur.fetchone()["max"] == row_id

    def test_every_column_the_processor_projects_exists(self, cur):
        """_DB_COLUMNS against the real table — the drift this rule exists for.

        A column dropped or renamed by a migration fails here, in the exact
        projection the flush issues, rather than in production.
        """
        row_id = _insert_observation(cur)
        cur.execute(self._select_rows(), (row_id,))
        row = cur.fetchone()
        assert row is not None
        assert set(_DB_COLUMNS) <= set(row)

    def test_select_rows_honours_the_snapshot_boundary(self, cur):
        id1 = _insert_observation(cur)
        id2 = _insert_observation(cur)
        cur.execute(self._select_rows(), (id1,))
        returned = {r["id"] for r in cur.fetchall()}
        assert id1 in returned
        assert id2 not in returned

    def test_delete_up_to_id_removes_only_the_flushed_rows(self, cur):
        id1 = _insert_observation(cur)
        id2 = _insert_observation(cur)
        cur.execute(DELETE_SILVER_OBSERVATIONS_UP_TO_ID, (id1,))
        assert cur.rowcount == 1
        cur.execute(SELECT_MAX_SILVER_OBSERVATION_ID)
        assert cur.fetchone()["max"] == id2
