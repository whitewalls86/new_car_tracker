"""
Integration tests: 403 blocked cooldown tracking.

Tests run against real Postgres (rollback on teardown).
Covers the upsert+event path that fires at scrape time when a 403 is received.
"""
import uuid

import pytest

from processing.queries import CLEAR_BLOCKED_COOLDOWN
from scraper.queries import (
    GET_BLOCKED_COOLDOWN_ATTEMPTS,
    INSERT_BLOCKED_COOLDOWN_EVENT,
    UPSERT_BLOCKED_COOLDOWN,
)

pytestmark = pytest.mark.integration


class TestBlockedCooldown:
    """
    Given: scraper receives a 403 for a listing
    When:  blocked cooldown path runs
    Then:  blocked_cooldown upserted with num_of_attempts incremented
           blocked_cooldown_events row inserted
    """

    def test_blocked_upserts_cooldown_and_event(self, cur):
        listing_id = str(uuid.uuid4())

        # First block
        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
        row = cur.fetchone()
        assert row["num_of_attempts"] == 1

        cur.execute(INSERT_BLOCKED_COOLDOWN_EVENT, {
            "listing_id": listing_id,
            "event_type": "blocked",
            "num_of_attempts": 1,
        })

        # Second block (increment)
        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
        row = cur.fetchone()
        assert row["num_of_attempts"] == 2

    def test_clear_blocked_cooldown_on_success(self, cur):
        listing_id = str(uuid.uuid4())

        # Seed a blocked entry
        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.blocked_cooldown WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 1

        # Clear on successful scrape (processor-side cleanup)
        cur.execute(CLEAR_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.blocked_cooldown WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0
