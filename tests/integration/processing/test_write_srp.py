"""
Integration tests: write_srp_observations() end-to-end.

Calls the real Python function against a real Postgres instance.
Covers: price_observations upserts, vin_to_listing mapping, tracked_models
seeding, silver write to staging, VIN recency guard, VIN fallback lookup.
"""
import uuid
from datetime import datetime, timezone

import pytest

from processing.writers.srp_writer import write_srp_observations

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)


def _listing(listing_id=None, vin=None, price=28000, make="Honda", model="CR-V"):
    return {
        "listing_id": listing_id or str(uuid.uuid4()),
        "vin": vin,
        "price": price,
        "make": make,
        "model": model,
        "trim": "EX",
        "year": 2025,
        "mileage": 0,
        "msrp": 30000,
        "stockType": "new",
        "fuelType": "Gasoline",
        "bodyStyle": "SUV",
        "financingType": None,
        "seller_zip": "77002",
        "seller_customerId": "cust-1",
        "page_number": 1,
        "position_on_page": 1,
        "trid": None,
        "isaContext": None,
        "last_seen_price": price,
        "canonical_detail_url": f"https://www.cars.com/vehicledetail/{listing_id or 'x'}/",
    }


# ---------------------------------------------------------------------------
# Helpers: read back state from DB
# ---------------------------------------------------------------------------

def _get_price_obs(vc, listing_id):
    vc.execute(
        "SELECT * FROM ops.price_observations WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    return vc.fetchone()


def _get_vin_mapping(vc, vin):
    vc.execute("SELECT * FROM ops.vin_to_listing WHERE vin = %s", (vin,))
    return vc.fetchone()


def _count_silver(vc, artifact_id):
    vc.execute(
        "SELECT COUNT(*) AS cnt FROM staging.silver_observations WHERE artifact_id = %s",
        (artifact_id,),
    )
    return vc.fetchone()["cnt"]


def _count_price_obs_events(vc, listing_id):
    vc.execute(
        "SELECT COUNT(*) AS cnt FROM staging.price_observation_events"
        " WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    return vc.fetchone()["cnt"]


def _cleanup(vc, listing_ids=None, vins=None, artifact_id=None):
    """Delete all rows written by write_srp_observations for the given identifiers."""
    if listing_ids:
        vc.execute(
            "DELETE FROM ops.price_observations WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
        vc.execute(
            "DELETE FROM staging.price_observation_events"
            " WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
    if vins:
        vc.execute("DELETE FROM ops.vin_to_listing WHERE vin = ANY(%s)", (vins,))
        vc.execute(
            "DELETE FROM staging.vin_to_listing_events WHERE vin = ANY(%s)", (vins,),
        )
    if artifact_id is not None:
        vc.execute(
            "DELETE FROM staging.silver_observations WHERE artifact_id = %s",
            (artifact_id,),
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWriteSrpBasic:
    def test_upserts_price_observation_for_each_listing(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid, vin="1HGCV1F34PA000101")]

        write_srp_observations(listings, artifact["artifact_id"], _NOW)

        row = _get_price_obs(vc, lid)
        assert row is not None
        assert row["price"] == 28000
        assert row["make"] == "Honda"

        _cleanup(vc, [lid], ["1HGCV1F34PA000101"], artifact["artifact_id"])

    def test_maps_vin_to_listing(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        vin = f"VINMAP{uuid.uuid4().hex[:11].upper()}"
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid, vin=vin)]

        result = write_srp_observations(listings, artifact["artifact_id"], _NOW)

        assert result["vin_mapped"] == 1
        row = _get_vin_mapping(vc, vin)
        assert row is not None
        assert str(row["listing_id"]) == lid

        _cleanup(vc, [lid], [vin], artifact["artifact_id"])

    def test_listing_without_vin_still_upserted(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid, vin=None)]

        result = write_srp_observations(listings, artifact["artifact_id"], _NOW)

        assert result["upserted"] == 1
        assert result["vin_mapped"] == 0
        row = _get_price_obs(vc, lid)
        assert row is not None
        assert row["vin"] is None

        _cleanup(vc, [lid], artifact_id=artifact["artifact_id"])

    def test_empty_listings_returns_zeros_no_db_writes(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        result = write_srp_observations([], artifact["artifact_id"], _NOW)
        assert result == {"upserted": 0, "vin_mapped": 0, "silver_written": 0}

    def test_returns_upserted_count(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lids = [str(uuid.uuid4()) for _ in range(3)]
        listings = [_listing(listing_id=lid) for lid in lids]

        result = write_srp_observations(listings, artifact["artifact_id"], _NOW)

        assert result["upserted"] == 3

        _cleanup(vc, lids, artifact_id=artifact["artifact_id"])


class TestWriteSrpSilverWrite:
    def test_silver_rows_written_to_staging(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid, vin="1HGCV1F34PA000201")]

        result = write_srp_observations(listings, artifact["artifact_id"], _NOW)

        assert result["silver_written"] == 1
        assert _count_silver(vc, artifact["artifact_id"]) == 1

        _cleanup(vc, [lid], ["1HGCV1F34PA000201"], artifact["artifact_id"])

    def test_silver_row_has_srp_source(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid)]

        write_srp_observations(listings, artifact["artifact_id"], _NOW)

        vc.execute(
            "SELECT source FROM staging.silver_observations WHERE artifact_id = %s",
            (artifact["artifact_id"],),
        )
        row = vc.fetchone()
        assert row["source"] == "srp"

        _cleanup(vc, [lid], artifact_id=artifact["artifact_id"])

    def test_multiple_listings_all_in_silver(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lids = [str(uuid.uuid4()) for _ in range(3)]
        listings = [_listing(listing_id=lid) for lid in lids]

        result = write_srp_observations(listings, artifact["artifact_id"], _NOW)

        assert _count_silver(vc, artifact["artifact_id"]) == 3
        assert result["silver_written"] == 3

        _cleanup(vc, lids, artifact_id=artifact["artifact_id"])


class TestWriteSrpEvents:
    def test_price_observation_event_written(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid)]

        write_srp_observations(listings, artifact["artifact_id"], _NOW)

        assert _count_price_obs_events(vc, lid) == 1

        _cleanup(vc, [lid], artifact_id=artifact["artifact_id"])

    def test_vin_to_listing_event_written_on_new_mapping(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="results_page")
        vin = f"VINEV{uuid.uuid4().hex[:12].upper()}"
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid, vin=vin)]

        write_srp_observations(listings, artifact["artifact_id"], _NOW)

        vc.execute("SELECT event_type FROM staging.vin_to_listing_events WHERE vin = %s", (vin,))
        row = vc.fetchone()
        assert row is not None
        assert row["event_type"] == "mapped"

        _cleanup(vc, [lid], [vin], artifact["artifact_id"])


class TestWriteSrpVinRecencyGuard:
    def test_older_artifact_does_not_downgrade_vin_mapping(self, vc, seed_artifact_c):
        """
        Given: vin_to_listing already has VIN → listing at T+10
        When:  SRP upsert at T+5 (older) runs
        Then:  mapped_at stays at T+10
        """
        t_plus_10 = datetime(2026, 4, 20, 12, 10, 0, tzinfo=timezone.utc)
        t_plus_5 = datetime(2026, 4, 20, 12, 5, 0, tzinfo=timezone.utc)

        artifact = seed_artifact_c(artifact_type="results_page")
        vin = f"VINRG{uuid.uuid4().hex[:12].upper()}"
        lid = str(uuid.uuid4())

        # Seed an existing mapping at T+10
        vc.execute(
            "INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)"
            " VALUES (%s, %s::uuid, %s, %s)",
            (vin, lid, t_plus_10, artifact["artifact_id"]),
        )

        # SRP at T+5 tries to upsert
        listings = [_listing(listing_id=lid, vin=vin)]
        write_srp_observations(listings, artifact["artifact_id"], t_plus_5)

        vc.execute("SELECT mapped_at FROM ops.vin_to_listing WHERE vin = %s", (vin,))
        row = vc.fetchone()
        assert row["mapped_at"] == t_plus_10, "Older SRP should not downgrade vin_to_listing"

        _cleanup(vc, [lid], [vin], artifact["artifact_id"])


class TestWriteSrpVinFallback:
    def test_vin_looked_up_from_existing_mapping_when_not_in_listing(
        self, vc, seed_artifact_c
    ):
        """
        Given: vin_to_listing already has VIN for the listing
        When:  SRP listing arrives without a VIN field
        Then:  price_observations row gets the VIN from the lookup
        """
        artifact = seed_artifact_c(artifact_type="results_page")
        vin = f"VINFL{uuid.uuid4().hex[:12].upper()}"
        lid = str(uuid.uuid4())

        # Pre-seed a vin_to_listing mapping
        vc.execute(
            "INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)"
            " VALUES (%s, %s::uuid, now(), %s)",
            (vin, lid, artifact["artifact_id"]),
        )

        # SRP listing has no vin
        listings = [_listing(listing_id=lid, vin=None)]
        write_srp_observations(listings, artifact["artifact_id"], _NOW)

        row = _get_price_obs(vc, lid)
        assert row["vin"] == vin, "VIN should be resolved from existing vin_to_listing"

        _cleanup(vc, [lid], [vin], artifact["artifact_id"])


class TestWriteSrpVinCollision:
    def test_relisted_vin_replaces_old_price_observation(self, vc, seed_artifact_c):
        """
        Given: price_observations has VIN → old_listing
        When:  SRP batch contains new_listing with the same VIN
        Then:  old row deleted, new row upserted — no UniqueViolation
        """
        artifact = seed_artifact_c(artifact_type="results_page")
        old_lid = str(uuid.uuid4())
        new_lid = str(uuid.uuid4())
        vin = f"VINSRP{uuid.uuid4().hex[:10].upper()}"

        # Seed the stale price_observation under the old listing
        vc.execute(
            "INSERT INTO ops.price_observations"
            " (listing_id, vin, price, make, model, last_seen_at, last_artifact_id)"
            " VALUES (%s::uuid, %s, 25000, 'Honda', 'CR-V', now(), %s)",
            (old_lid, vin, artifact["artifact_id"]),
        )

        listings = [_listing(listing_id=new_lid, vin=vin)]
        write_srp_observations(listings, artifact["artifact_id"], _NOW)

        # Old row removed
        vc.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (old_lid,),
        )
        assert vc.fetchone()["cnt"] == 0, "Old price_observation should be deleted on relisting"

        # New row present with correct VIN
        row = _get_price_obs(vc, new_lid)
        assert row is not None
        assert row["vin"] == vin

        _cleanup(vc, [old_lid, new_lid], [vin], artifact["artifact_id"])


class TestWriteSrpTrackedModels:
    def test_search_key_seeds_tracked_models(self, vc, seed_artifact_c):
        """
        Given: SRP artifact with search_key
        When:  write_srp_observations runs with listings
        Then:  ops.tracked_models has a row for each unique make/model
        """
        key = f"test-srp-tm-{uuid.uuid4().hex[:8]}"
        vc.execute(
            """
            INSERT INTO search_configs
                (search_key, enabled, params, rotation_order, created_at, updated_at)
            VALUES (%s, true, '{}'::jsonb, 99, now(), now())
            """,
            (key,),
        )
        artifact = seed_artifact_c(artifact_type="results_page", search_key=key)
        lid = str(uuid.uuid4())
        listings = [_listing(listing_id=lid, make="Toyota", model="RAV4")]

        write_srp_observations(listings, artifact["artifact_id"], _NOW, search_key=key)

        vc.execute(
            "SELECT make, model FROM ops.tracked_models WHERE search_key = %s",
            (key,),
        )
        row = vc.fetchone()
        assert row is not None
        assert row["make"] == "toyota"
        assert row["model"] == "rav4"

        _cleanup(vc, [lid], artifact_id=artifact["artifact_id"])
        vc.execute("DELETE FROM ops.tracked_models WHERE search_key = %s", (key,))
        vc.execute("DELETE FROM staging.tracked_model_events WHERE search_key = %s", (key,))
        vc.execute("DELETE FROM search_configs WHERE search_key = %s", (key,))
