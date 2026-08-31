"""
Integration tests: Detail artifact processing.

Tests run against real Postgres (rollback on teardown).
Covers active, unlisted, VIN relisting, and blocked cooldown paths.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from processing.queries import (
    DELETE_PRICE_OBSERVATION,
    DELETE_PRICE_OBSERVATION_BY_VIN,
    INSERT_DETAIL_CLAIM_EVENT,
    LOOKUP_VIN_COLLISION,
    RELEASE_DETAIL_CLAIMS,
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
            "customer_id": None,
            "last_seen_at": now,
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": now,
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
        # The prior mapping must predate the artifact that supersedes it:
        # upsert_vin_to_listing.sql only remaps on a strictly newer mapped_at,
        # and production passes the new artifact's fetched_at. Defaulting this
        # to now() made the seed no older than `now` and the remap a no-op.
        seed_vin_to_listing(
            vin=vin, listing_id=old_listing_id,
            artifact_id=artifact["artifact_id"],
            mapped_at=now - timedelta(hours=1),
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
            "customer_id": None,
            "last_seen_at": now,
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": now,
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


class TestScrapeStateOwnership:
    """Plan 147: the upsert owns enrichment and must never touch the fetch fact.

    last_detail_fetched_at belongs to the scraper, written by
    POST /scrape/claims/release. A processor that could advance it would
    reintroduce the very coupling Plan 147 removes, so these run the real
    statement against real Postgres rather than asserting on a params dict.
    """

    def _observation(self, cur, listing_id: str) -> dict:
        cur.execute(
            "SELECT price, last_seen_at, customer_id, last_detail_fetched_at,"
            "       last_detail_enriched_at"
            " FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        return cur.fetchone()

    def test_detail_write_advances_enrichment_but_not_fetch(
        self, cur, seed_artifact,
    ):
        listing_id = str(uuid.uuid4())
        artifact = seed_artifact(artifact_type="detail_page", listing_id=listing_id)
        now = datetime.now(timezone.utc)

        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 28000,
            "make": "Honda", "model": "CR-V", "customer_id": "cust-1",
            "last_seen_at": now, "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": now,
        })

        row = self._observation(cur, listing_id)
        assert row["last_detail_enriched_at"] is not None
        assert row["last_detail_fetched_at"] is None, (
            "the processor must never advance the scraper's fetch fact"
        )

    def test_carousel_write_refreshes_price_and_advances_neither(
        self, cur, seed_artifact,
    ):
        """The load-bearing non-goal: a carousel write against an already
        enriched listing keeps its price fresh and leaves it suppressed."""
        listing_id = str(uuid.uuid4())
        artifact = seed_artifact(artifact_type="detail_page", listing_id=listing_id)
        enriched_at = datetime.now(timezone.utc)

        # Enriched by a detail write.
        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 28000,
            "make": "Honda", "model": "CR-V", "customer_id": "cust-1",
            "last_seen_at": enriched_at,
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": enriched_at,
        })
        # Then seen again in a carousel: price only, no enrichment claimed.
        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 26500,
            "make": None, "model": None, "customer_id": None,
            "last_seen_at": datetime.now(timezone.utc),
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": None,
        })

        row = self._observation(cur, listing_id)
        assert row["price"] == 26500, "carousel must refresh the price"
        assert row["customer_id"] == "cust-1", "carousel must not clear enrichment"
        assert row["last_detail_enriched_at"] == enriched_at, (
            "carousel must not advance the 7-day enrichment window"
        )
        assert row["last_detail_fetched_at"] is None, (
            "carousel must not advance the fetch backoff"
        )

    def test_carousel_discovered_listing_still_enters_the_queue(
        self, cur, seed_artifact,
    ):
        """The other non-goal, asserted: a price without the full detail suite
        is not enrichment, so a never-scraped carousel listing is still work."""
        listing_id = str(uuid.uuid4())
        artifact = seed_artifact(artifact_type="detail_page", listing_id=listing_id)

        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 22000,
            "make": None, "model": None, "customer_id": None,
            "last_seen_at": datetime.now(timezone.utc),
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": None,
        })

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue"
            " WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone() is not None
