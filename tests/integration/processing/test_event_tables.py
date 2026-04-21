"""
Integration tests: staging event tables for price_observations and vin_to_listing.

Validates V021 migration tables exist and accept the event insert patterns
used by the processing writers.
"""
import uuid

from processing.queries import (
    INSERT_PRICE_OBSERVATION_EVENT,
    INSERT_VIN_TO_LISTING_EVENT,
)


class TestPriceObservationEvents:
    def test_upsert_event_written(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="results_page")
        listing_id = str(uuid.uuid4())

        cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
            "listing_id": listing_id,
            "vin": "1HGCV1F34PA000001",
            "price": 25000,
            "make": "Honda",
            "model": "CR-V",
            "artifact_id": artifact["artifact_id"],
            "event_type": "upserted",
            "source": "srp",
        })

        cur.execute(
            "SELECT * FROM staging.price_observation_events WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["event_type"] == "upserted"
        assert row["source"] == "srp"
        assert row["price"] == 25000
        assert row["vin"] == "1HGCV1F34PA000001"

    def test_delete_event_written(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="detail_page")
        listing_id = str(uuid.uuid4())

        cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
            "listing_id": listing_id,
            "vin": None,
            "price": None,
            "make": "Toyota",
            "model": "Camry",
            "artifact_id": artifact["artifact_id"],
            "event_type": "deleted",
            "source": "detail",
        })

        cur.execute(
            "SELECT * FROM staging.price_observation_events WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["event_type"] == "deleted"
        assert row["price"] is None

    def test_carousel_source_accepted(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="detail_page")
        listing_id = str(uuid.uuid4())

        cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
            "listing_id": listing_id,
            "vin": None,
            "price": 18000,
            "make": None,
            "model": None,
            "artifact_id": artifact["artifact_id"],
            "event_type": "upserted",
            "source": "carousel",
        })

        cur.execute(
            "SELECT source FROM staging.price_observation_events WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["source"] == "carousel"


class TestVinToListingEvents:
    def test_mapped_event_written(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="detail_page")
        listing_id = str(uuid.uuid4())
        vin = "1HGCV1F34PA000010"

        cur.execute(INSERT_VIN_TO_LISTING_EVENT, {
            "vin": vin,
            "listing_id": listing_id,
            "artifact_id": artifact["artifact_id"],
            "event_type": "mapped",
            "previous_listing_id": None,
        })

        cur.execute(
            "SELECT * FROM staging.vin_to_listing_events WHERE vin = %s",
            (vin,),
        )
        row = cur.fetchone()
        assert row["event_type"] == "mapped"
        assert row["previous_listing_id"] is None

    def test_remapped_event_with_previous_listing(self, cur, seed_artifact):
        artifact = seed_artifact(artifact_type="detail_page")
        old_listing_id = str(uuid.uuid4())
        new_listing_id = str(uuid.uuid4())
        vin = "1HGCV1F34PA000020"

        cur.execute(INSERT_VIN_TO_LISTING_EVENT, {
            "vin": vin,
            "listing_id": new_listing_id,
            "artifact_id": artifact["artifact_id"],
            "event_type": "remapped",
            "previous_listing_id": old_listing_id,
        })

        cur.execute(
            "SELECT * FROM staging.vin_to_listing_events WHERE vin = %s",
            (vin,),
        )
        row = cur.fetchone()
        assert row["event_type"] == "remapped"
        assert str(row["previous_listing_id"]) == old_listing_id
        assert str(row["listing_id"]) == new_listing_id
