"""
Integration tests: SRP artifact processing.

Tests run against real Postgres (rollback on teardown).
MinIO/silver writes are not asserted — validated by Plan 96.
"""
import uuid
from datetime import datetime, timezone

import pytest

from processing.queries import (
    UPSERT_PRICE_OBSERVATION,
    UPSERT_VIN_TO_LISTING,
)

pytestmark = pytest.mark.integration


class TestSrpArtifact:
    """
    Given: artifacts_queue row (results_page, 3 listings, 2 with VINs)
    When:  SRP writer upserts
    Then:  price_observations has 3 rows (2 with vin, 1 null)
           vin_to_listing has 2 entries
    """

    def test_srp_upserts_price_observations(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="results_page")
        artifact_id = artifact["artifact_id"]
        now = datetime.now(timezone.utc)

        listings = [
            {
                "listing_id": str(uuid.uuid4()),
                "vin": "1HGCV1F34PA000001", "price": 25000,
                "make": "Honda", "model": "CR-V",
            },
            {
                "listing_id": str(uuid.uuid4()),
                "vin": "2T1BURHE0KC000002", "price": 30000,
                "make": "Toyota", "model": "RAV4",
            },
            {
                "listing_id": str(uuid.uuid4()),
                "vin": None, "price": 18000,
                "make": "Ford", "model": "Escape",
            },
        ]

        for listing in listings:
            cur.execute(UPSERT_PRICE_OBSERVATION, {
                "listing_id": listing["listing_id"],
                "vin": listing["vin"],
                "price": listing["price"],
                "make": listing["make"],
                "model": listing["model"],
                "customer_id": None,
                "last_seen_at": now,
                "last_artifact_id": artifact_id,
                "last_detail_enriched_at": None,
            })

        # Upsert vin_to_listing for those with VINs
        for listing in listings:
            if listing["vin"]:
                cur.execute(UPSERT_VIN_TO_LISTING, {
                    "vin": listing["vin"],
                    "listing_id": listing["listing_id"],
                    "mapped_at": now,
                    "artifact_id": artifact_id,
                })

        # Verify price_observations
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations"
            " WHERE last_artifact_id = %s",
            (artifact_id,),
        )
        assert cur.fetchone()["cnt"] == 3

        # Verify vin populated
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations"
            " WHERE last_artifact_id = %s AND vin IS NOT NULL",
            (artifact_id,),
        )
        assert cur.fetchone()["cnt"] == 2

        # Verify vin_to_listing
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.vin_to_listing"
            " WHERE artifact_id = %s",
            (artifact_id,),
        )
        assert cur.fetchone()["cnt"] == 2


class TestSrpVinRecencyGuard:
    """
    Given: vin_to_listing has (VIN001 → AAA, mapped_at=T+10)
           SRP artifact from T+5 also sees listing AAA with VIN001
    When:  SRP VIN upsert runs
    Then:  vin_to_listing.mapped_at for VIN001 is still T+10 (not downgraded)
    """

    def test_older_srp_does_not_downgrade_vin_mapping(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="results_page")
        listing_id = str(uuid.uuid4())
        vin = "1HGCV1F34PA999999"

        t_plus_10 = datetime(2026, 4, 20, 12, 10, 0, tzinfo=timezone.utc)
        t_plus_5 = datetime(2026, 4, 20, 12, 5, 0, tzinfo=timezone.utc)

        # Seed existing mapping at T+10
        cur.execute(UPSERT_VIN_TO_LISTING, {
            "vin": vin,
            "listing_id": listing_id,
            "mapped_at": t_plus_10,
            "artifact_id": artifact["artifact_id"],
        })

        # Attempt upsert from older artifact (T+5) — should NOT update
        cur.execute(UPSERT_VIN_TO_LISTING, {
            "vin": vin,
            "listing_id": listing_id,
            "mapped_at": t_plus_5,
            "artifact_id": artifact["artifact_id"] + 1000,  # different artifact
        })

        # Verify mapped_at is still T+10
        cur.execute("SELECT mapped_at FROM ops.vin_to_listing WHERE vin = %s", (vin,))
        row = cur.fetchone()
        assert row["mapped_at"] == t_plus_10


class TestSrpScrapeStateOwnership:
    """Plan 147: an SRP write is a price sighting, not a detail fetch.

    It must advance neither the enrichment window nor the fetch backoff, or an
    SRP sweep would silently suppress detail scraping across the whole result
    set.
    """

    def test_srp_write_advances_neither_fetch_nor_enrichment(
        self, cur, seed_artifact,
    ):
        listing_id = str(uuid.uuid4())
        artifact = seed_artifact(artifact_type="results_page")

        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 24000,
            "make": "Honda", "model": "CR-V", "customer_id": None,
            "last_seen_at": datetime.now(timezone.utc),
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": None,
        })

        cur.execute(
            "SELECT price, last_detail_fetched_at, last_detail_enriched_at"
            " FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["price"] == 24000, "the price sighting is still recorded"
        assert row["last_detail_fetched_at"] is None
        assert row["last_detail_enriched_at"] is None

    def test_srp_write_does_not_clear_an_existing_enrichment(
        self, cur, seed_artifact,
    ):
        """COALESCE semantics: a null incoming value preserves the previous
        one, so an SRP sighting never clears an enrichment."""
        listing_id = str(uuid.uuid4())
        artifact = seed_artifact(artifact_type="results_page")
        enriched_at = datetime.now(timezone.utc)

        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 28000,
            "make": "Honda", "model": "CR-V", "customer_id": "cust-1",
            "last_seen_at": enriched_at,
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": enriched_at,
        })
        cur.execute(UPSERT_PRICE_OBSERVATION, {
            "listing_id": listing_id, "vin": None, "price": 23500,
            "make": "Honda", "model": "CR-V", "customer_id": None,
            "last_seen_at": datetime.now(timezone.utc),
            "last_artifact_id": artifact["artifact_id"],
            "last_detail_enriched_at": None,
        })

        cur.execute(
            "SELECT price, customer_id, last_detail_enriched_at"
            " FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["price"] == 23500
        assert row["customer_id"] == "cust-1"
        assert row["last_detail_enriched_at"] == enriched_at
