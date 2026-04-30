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

    def test_first_block_event_type_is_blocked(self, cur):
        listing_id = str(uuid.uuid4())

        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
        num_attempts = cur.fetchone()["num_of_attempts"]
        cur.execute(INSERT_BLOCKED_COOLDOWN_EVENT, {
            "listing_id": listing_id,
            "event_type": "blocked" if num_attempts == 1 else "incremented",
            "num_of_attempts": num_attempts,
        })

        cur.execute(
            "SELECT event_type FROM staging.blocked_cooldown_events"
            " WHERE listing_id = %s::uuid ORDER BY event_id",
            (listing_id,),
        )
        assert cur.fetchone()["event_type"] == "blocked"

    def test_second_block_event_type_is_incremented(self, cur):
        listing_id = str(uuid.uuid4())

        for _ in range(2):
            cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
            cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
            num_attempts = cur.fetchone()["num_of_attempts"]
            cur.execute(INSERT_BLOCKED_COOLDOWN_EVENT, {
                "listing_id": listing_id,
                "event_type": "blocked" if num_attempts == 1 else "incremented",
                "num_of_attempts": num_attempts,
            })

        cur.execute(
            "SELECT event_type FROM staging.blocked_cooldown_events"
            " WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1",
            (listing_id,),
        )
        assert cur.fetchone()["event_type"] == "incremented"

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
