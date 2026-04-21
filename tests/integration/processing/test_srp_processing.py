"""
Integration tests: SRP artifact processing.

Tests run against real Postgres (rollback on teardown).
MinIO/silver writes are not asserted — validated by Plan 96.
"""
import uuid
from datetime import datetime, timezone

from processing.queries import (
    UPSERT_PRICE_OBSERVATION,
    UPSERT_VIN_TO_LISTING,
)


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
                "last_seen_at": now,
                "last_artifact_id": artifact_id,
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
