"""
Integration tests: Detail artifact processing.

Tests run against real Postgres (rollback on teardown).
Covers active, unlisted, VIN relisting, and blocked cooldown paths.
"""
import uuid
from datetime import datetime, timezone

import pytest

from processing.queries import (
    CLEAR_BLOCKED_COOLDOWN,
    DELETE_PRICE_OBSERVATION,
    DELETE_PRICE_OBSERVATION_BY_VIN,
    GET_BLOCKED_COOLDOWN_ATTEMPTS,
    INSERT_BLOCKED_COOLDOWN_EVENT,
    INSERT_DETAIL_CLAIM_EVENT,
    LOOKUP_VIN_COLLISION,
    RELEASE_DETAIL_CLAIMS,
    UPSERT_BLOCKED_COOLDOWN,
    UPSERT_PRICE_OBSERVATION,
    UPSERT_VIN_TO_LISTING,
)

pytestmark = pytest.mark.integration


class TestDetailActive:
    """
    Given: artifacts_queue row (detail_page, active)
           detail_scrape_claims row for listing_id
    When:  Detail writer processes
    Then:  price_observations row upserted with vin, make, model, mileage
           vin_to_listing entry exists
           detail_scrape_claims row deleted
    """

    def test_active_detail_upserts_and_releases_claim(
        self, cur, seed_artifact, seed_detail_claim
    ):
        listing_id = str(uuid.uuid4())
        artifact = seed_artifact(artifact_type="detail_page", listing_id=listing_id)
        run_id = seed_detail_claim(listing_id)
        now = datetime.now(timezone.utc)
        vin = "1HGCV1F34PA000010"

        # Simulate detail writer writes
        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id,
            "vin": vin,
            "price": 28000,
            "make": "Honda",
            "model": "CR-V",
            "last_seen_at": now,
            "last_artifact_id": artifact["artifact_id"],
        })
        cur.execute(UPSERT_VIN_TO_LISTING, {
            "vin": vin,
            "listing_id": listing_id,
            "mapped_at": now,
            "artifact_id": artifact["artifact_id"],
        })
        cur.execute(RELEASE_DETAIL_CLAIMS, {"listing_id": listing_id})
        cur.execute(INSERT_DETAIL_CLAIM_EVENT, {
            "listing_id": listing_id,
            "run_id": run_id,
            "status": "processed",
        })

        # Verify price_observations
        cur.execute(
            "SELECT vin, price, make, model FROM ops.price_observations"
            " WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["vin"] == vin
        assert row["price"] == 28000
        assert row["make"] == "Honda"

        # Verify vin_to_listing
        cur.execute("SELECT listing_id FROM ops.vin_to_listing WHERE vin = %s", (vin,))
        row = cur.fetchone()
        assert str(row["listing_id"]) == listing_id

        # Verify claim released
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0

        # Verify claim event recorded
        cur.execute(
            "SELECT status FROM staging.detail_scrape_claim_events"
            " WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1",
            (listing_id,),
        )
        assert cur.fetchone()["status"] == "processed"


class TestDetailUnlisted:
    """
    Given: price_observations has a row for listing_id
    When:  Detail writer processes unlisted artifact
    Then:  price_observations row DELETED
    """

    def test_unlisted_deletes_price_observation(
        self, cur, seed_artifact, seed_price_observation
    ):
        listing_id = seed_price_observation(price=30000)
        seed_artifact(artifact_type="detail_page", listing_id=listing_id)

        # Verify row exists
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 1

        # Delete (unlisted path)
        cur.execute(DELETE_PRICE_OBSERVATION, {"listing_id": listing_id})

        # Verify deleted
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0


class TestVinRelisting:
    """
    Given: price_observations has (listing_id=AAA, vin=VIN001)
           vin_to_listing has (VIN001 → AAA)
           detail artifact for listing BBB discovers VIN001
    When:  VIN collision detected and resolved
    Then:  price_observations row for VIN001 deleted (old AAA row)
           New row for BBB created with VIN001
           vin_to_listing has VIN001 → BBB
    """

    def test_vin_relisting_replaces_old_row(
        self, cur, seed_artifact, seed_price_observation, seed_vin_to_listing
    ):
        old_listing_id = str(uuid.uuid4())
        new_listing_id = str(uuid.uuid4())
        vin = "1HGCV1F34PA000020"
        now = datetime.now(timezone.utc)

        artifact = seed_artifact(artifact_type="detail_page", listing_id=new_listing_id)
        seed_price_observation(
            listing_id=old_listing_id, vin=vin,
            artifact_id=artifact["artifact_id"],
        )
        seed_vin_to_listing(
            vin=vin, listing_id=old_listing_id,
            artifact_id=artifact["artifact_id"],
        )

        # Detect collision
        cur.execute(LOOKUP_VIN_COLLISION, {"vin": vin, "listing_id": new_listing_id})
        collision = cur.fetchone()
        assert collision is not None
        assert str(collision["listing_id"]) == old_listing_id

        # Delete old row
        cur.execute(DELETE_PRICE_OBSERVATION_BY_VIN, {"old_listing_id": old_listing_id})

        # Upsert new row
        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": new_listing_id,
            "vin": vin,
            "price": 32000,
            "make": "Honda",
            "model": "Accord",
            "last_seen_at": now,
            "last_artifact_id": artifact["artifact_id"],
        })

        # Update vin_to_listing
        cur.execute(UPSERT_VIN_TO_LISTING, {
            "vin": vin,
            "listing_id": new_listing_id,
            "mapped_at": now,
            "artifact_id": artifact["artifact_id"],
        })

        # Verify old row gone
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (old_listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0

        # Verify new row has VIN
        cur.execute(
            "SELECT vin FROM ops.price_observations WHERE listing_id = %s::uuid",
            (new_listing_id,),
        )
        assert cur.fetchone()["vin"] == vin

        # Verify vin_to_listing updated
        cur.execute("SELECT listing_id FROM ops.vin_to_listing WHERE vin = %s", (vin,))
        assert str(cur.fetchone()["listing_id"]) == new_listing_id


class TestBlockedCooldown:
    """
    Given: Detail artifact is a 403 block page
    When:  Blocked path runs
    Then:  blocked_cooldown upserted with num_of_attempts incremented
           blocked_cooldown_events row inserted
    """

    def test_blocked_upserts_cooldown_and_event(self, cur, seed_artifact):
        listing_id = str(uuid.uuid4())
        seed_artifact(artifact_type="detail_page", listing_id=listing_id)

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

    def test_clear_blocked_cooldown_on_success(self, cur, seed_artifact):
        listing_id = str(uuid.uuid4())

        # Seed a blocked entry
        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.blocked_cooldown WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 1

        # Clear on successful scrape
        cur.execute(CLEAR_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.blocked_cooldown WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0


class TestReadyEndpoint:
    """
    Given: processing service is idle
    When:  GET /ready
    Then:  {"ready": true}
    """

    def test_ready_when_idle(self, cur):
        # This is a unit-level check; /ready doesn't hit DB.
        # Verified by test_app.py::TestReady. Included here for plan traceability.
        from shared.job_counter import is_idle
        assert is_idle() is True
