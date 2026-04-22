"""
Layer 1 — SQL smoke tests for flush_staging_events.

Validates the SELECT / DELETE SQL patterns used by flush_staging_events against
a real DB with Flyway migrations applied. The goal is to catch schema breakage
(column renames, type changes, dropped tables) — not to test business logic.

All tests run inside a rolled-back transaction; no data persists.
"""
import uuid
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _insert_aq_event(cur, artifact_id=None) -> int:
    """Insert a row into staging.artifacts_queue_events. Returns event_id."""
    minio = f"s3://bronze/html/year=2026/month=4/results_page/{uuid.uuid4()}.html.zst"
    cur.execute(
        """INSERT INTO staging.artifacts_queue_events
               (artifact_id, status, event_at, minio_path, 
                artifact_type, fetched_at, listing_id, run_id)
           VALUES (%s, 'pending', %s, %s, 'results_page', %s, 'listing-test', 'run-test')
           RETURNING event_id""",
        (artifact_id or 999999, _NOW, minio, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_claim_event(cur) -> int:
    """Insert a row into staging.detail_scrape_claim_events. Returns event_id."""
    listing_id = uuid.uuid4()
    cur.execute(
        """INSERT INTO staging.detail_scrape_claim_events
               (listing_id, status, event_at)
           VALUES (%s, 'claimed', %s)
           RETURNING event_id""",
        (listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_blocked_event(cur) -> int:
    """Insert a row into staging.blocked_cooldown_events. Returns event_id."""
    listing_id = uuid.uuid4()
    cur.execute(
        """INSERT INTO staging.blocked_cooldown_events
               (listing_id, event_type, num_of_attempts, event_at)
           VALUES (%s, 'blocked', 1, %s)
           RETURNING event_id""",
        (listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_price_event(cur) -> int:
    """Insert a row into staging.price_observation_events. Returns event_id."""
    listing_id = uuid.uuid4()
    cur.execute(
        """INSERT INTO staging.price_observation_events
               (listing_id, artifact_id, event_type, source, event_at)
           VALUES (%s, 999999, 'upserted', 'srp', %s)
           RETURNING event_id""",
        (listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


def _insert_vin_event(cur) -> int:
    """Insert a row into staging.vin_to_listing_events. Returns event_id."""
    vin = str(uuid.uuid4())
    listing_id = uuid.uuid4()
    cur.execute(
        """INSERT INTO staging.vin_to_listing_events
               (vin, listing_id, artifact_id, event_type, event_at)
           VALUES (%s, %s, 999999, 'mapped', %s)
           RETURNING event_id""",
        (vin, listing_id, _NOW),
    )
    return cur.fetchone()["event_id"]


# ---------------------------------------------------------------------------
# SELECT MAX(event_id) — snapshot boundary query
# ---------------------------------------------------------------------------

class TestSelectMaxEventId:
    def test_aq_events_max_returns_none_when_empty(self, cur):
        cur.execute("SELECT MAX(event_id) FROM staging.artifacts_queue_events")
        assert cur.fetchone()["max"] is None

    def test_aq_events_max_returns_inserted_id(self, cur):
        event_id = _insert_aq_event(cur)
        cur.execute("SELECT MAX(event_id) FROM staging.artifacts_queue_events")
        assert cur.fetchone()["max"] == event_id

    def test_claim_events_max_returns_none_when_empty(self, cur):
        cur.execute("SELECT MAX(event_id) FROM staging.detail_scrape_claim_events")
        assert cur.fetchone()["max"] is None

    def test_blocked_events_max_returns_none_when_empty(self, cur):
        cur.execute("SELECT MAX(event_id) FROM staging.blocked_cooldown_events")
        assert cur.fetchone()["max"] is None

    def test_price_events_max_returns_none_when_empty(self, cur):
        cur.execute("SELECT MAX(event_id) FROM staging.price_observation_events")
        assert cur.fetchone()["max"] is None

    def test_vin_events_max_returns_none_when_empty(self, cur):
        cur.execute("SELECT MAX(event_id) FROM staging.vin_to_listing_events")
        assert cur.fetchone()["max"] is None


# ---------------------------------------------------------------------------
# SELECT cols WHERE event_id <= max — fetch rows query
# ---------------------------------------------------------------------------

class TestSelectRowsUpToMax:
    def test_aq_events_select_columns_present(self, cur):
        _insert_aq_event(cur)
        cur.execute(
            """SELECT event_id, artifact_id, status, event_at,
                      minio_path, artifact_type, fetched_at, listing_id, run_id
               FROM staging.artifacts_queue_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.artifacts_queue_events)"""
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "artifact_id", "status", "event_at",
                    "minio_path", "artifact_type", "fetched_at", "listing_id", "run_id"):
            assert col in row

    def test_claim_events_select_columns_present(self, cur):
        _insert_claim_event(cur)
        cur.execute(
            """SELECT event_id, listing_id, run_id, status, stale_reason, vin, event_at
               FROM staging.detail_scrape_claim_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.detail_scrape_claim_events)"""
        )
        row = cur.fetchone()
        assert row is not None
        for col in (
            "event_id", "listing_id", "run_id", "status", "stale_reason", "vin", "event_at"
        ):
            assert col in row

    def test_blocked_events_select_columns_present(self, cur):
        _insert_blocked_event(cur)
        cur.execute(
            """SELECT event_id, listing_id, event_type, num_of_attempts, event_at
               FROM staging.blocked_cooldown_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.blocked_cooldown_events)"""
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "listing_id", "event_type", "num_of_attempts", "event_at"):
            assert col in row

    def test_price_events_select_columns_present(self, cur):
        _insert_price_event(cur)
        cur.execute(
            """SELECT event_id, listing_id, vin, price, make, model,
                      artifact_id, event_type, source, event_at
               FROM staging.price_observation_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.price_observation_events)"""
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "listing_id", "vin", "price", "make", "model",
                    "artifact_id", "event_type", "source", "event_at"):
            assert col in row

    def test_vin_events_select_columns_present(self, cur):
        _insert_vin_event(cur)
        cur.execute(
            """SELECT event_id, vin, listing_id, artifact_id,
                      event_type, previous_listing_id, event_at
               FROM staging.vin_to_listing_events
               WHERE event_id <= (SELECT MAX(event_id) FROM staging.vin_to_listing_events)"""
        )
        row = cur.fetchone()
        assert row is not None
        for col in ("event_id", "vin", "listing_id", "artifact_id",
                    "event_type", "previous_listing_id", "event_at"):
            assert col in row

    def test_snapshot_boundary_excludes_later_rows(self, cur):
        """Rows with event_id > max_pk at snapshot time must not be selected."""
        id1 = _insert_aq_event(cur)
        id2 = _insert_aq_event(cur)
        # Snapshot was taken before id2 was inserted — simulate by using id1 as boundary
        cur.execute(
            "SELECT event_id FROM staging.artifacts_queue_events WHERE event_id <= %s",
            (id1,),
        )
        returned = {r["event_id"] for r in cur.fetchall()}
        assert id1 in returned
        assert id2 not in returned


# ---------------------------------------------------------------------------
# DELETE WHERE event_id <= max — flush delete query
# ---------------------------------------------------------------------------

class TestDeleteUpToMax:
    def test_aq_events_delete_by_max(self, cur):
        eid = _insert_aq_event(cur)
        cur.execute(
            "DELETE FROM staging.artifacts_queue_events WHERE event_id <= %s", (eid,)
        )
        cur.execute(
            "SELECT event_id FROM staging.artifacts_queue_events WHERE event_id = %s", (eid,)
        )
        assert cur.fetchone() is None

    def test_claim_events_delete_by_max(self, cur):
        eid = _insert_claim_event(cur)
        cur.execute(
            "DELETE FROM staging.detail_scrape_claim_events WHERE event_id <= %s", (eid,)
        )
        cur.execute(
            "SELECT event_id FROM staging.detail_scrape_claim_events WHERE event_id = %s", (eid,)
        )
        assert cur.fetchone() is None

    def test_blocked_events_delete_by_max(self, cur):
        eid = _insert_blocked_event(cur)
        cur.execute(
            "DELETE FROM staging.blocked_cooldown_events WHERE event_id <= %s", (eid,)
        )
        cur.execute(
            "SELECT event_id FROM staging.blocked_cooldown_events WHERE event_id = %s", (eid,)
        )
        assert cur.fetchone() is None

    def test_price_events_delete_by_max(self, cur):
        eid = _insert_price_event(cur)
        cur.execute(
            "DELETE FROM staging.price_observation_events WHERE event_id <= %s", (eid,)
        )
        cur.execute(
            "SELECT event_id FROM staging.price_observation_events WHERE event_id = %s", (eid,)
        )
        assert cur.fetchone() is None

    def test_vin_events_delete_by_max(self, cur):
        eid = _insert_vin_event(cur)
        cur.execute(
            "DELETE FROM staging.vin_to_listing_events WHERE event_id <= %s", (eid,)
        )
        cur.execute(
            "SELECT event_id FROM staging.vin_to_listing_events WHERE event_id = %s", (eid,)
        )
        assert cur.fetchone() is None

    def test_delete_only_affects_rows_up_to_boundary(self, cur):
        """Rows inserted after the snapshot boundary must survive the delete."""
        id1 = _insert_aq_event(cur)
        id2 = _insert_aq_event(cur)
        cur.execute(
            "DELETE FROM staging.artifacts_queue_events WHERE event_id <= %s", (id1,)
        )
        cur.execute(
            "SELECT event_id FROM staging.artifacts_queue_events WHERE event_id = %s", (id2,)
        )
        assert cur.fetchone() is not None
