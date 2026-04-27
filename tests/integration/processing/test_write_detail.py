"""
Integration tests: write_detail_active(), write_detail_unlisted(),
write_detail_blocked() end-to-end.

Calls the real Python functions against a real Postgres instance.
Covers: VIN mapping, collision/relisting, carousel filtering, silver write,
claim release, blocked cooldown upsert/increment, and event tables.
"""
import uuid
from datetime import datetime, timezone

import pytest

from processing.writers.detail_writer import (
    write_detail_active,
    write_detail_blocked,
    write_detail_unlisted,
)

pytestmark = pytest.mark.integration

_NOW = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _primary(
    listing_id,
    vin="1HGCV1F34PA111001",
    make="Honda",
    model="CR-V",
    price=28000,
    listing_state="active",
):
    return {
        "listing_id": listing_id,
        "listing_state": listing_state,
        "vin": vin,
        "make": make,
        "model": model,
        "trim": "EX",
        "year": 2025,
        "price": price,
        "mileage": 0,
        "msrp": 30000,
        "stock_type": "new",
        "fuel_type": "Gasoline",
        "body_style": "SUV",
        "dealer_name": "Best Auto",
        "dealer_zip": "77002",
        "dealer_street": "123 Main St",
        "dealer_city": "Houston",
        "dealer_state": "TX",
        "dealer_phone": "555-1234",
        "dealer_website": "https://bestauto.com",
        "dealer_cars_com_url": "/dealers/best-auto-1/",
        "dealer_rating": 4.5,
        "seller_id": "seller-99",
        "customer_id": "cust-77",
        "unlisted_title": None,
        "unlisted_message": None,
    }


def _carousel_hint(listing_id, price=20000, body="New 2026 Honda CR-V EX"):
    return {
        "listing_id": listing_id,
        "price": price,
        "body": body,
        "condition": "New",
        "year": 2026,
        "mileage": 0,
        "canonical_detail_url": None,
    }


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


def _claim_exists(vc, listing_id):
    vc.execute(
        "SELECT COUNT(*) AS cnt FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    return vc.fetchone()["cnt"] > 0


def _blocked_cooldown_row(vc, listing_id):
    vc.execute(
        "SELECT * FROM ops.blocked_cooldown WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    return vc.fetchone()


def _cleanup(vc, listing_ids=None, vins=None, artifact_id=None):
    """Helper to delete all rows created by writer functions for given identifiers."""
    if listing_ids:
        vc.execute(
            "DELETE FROM ops.price_observations WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
        vc.execute(
            "DELETE FROM ops.detail_scrape_claims WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
        vc.execute(
            "DELETE FROM ops.blocked_cooldown WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
        vc.execute(
            "DELETE FROM staging.price_observation_events WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
        vc.execute(
            "DELETE FROM staging.detail_scrape_claim_events WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
        vc.execute(
            "DELETE FROM staging.blocked_cooldown_events WHERE listing_id = ANY(%s::uuid[])",
            (listing_ids,),
        )
    if vins:
        vc.execute("DELETE FROM ops.vin_to_listing WHERE vin = ANY(%s)", (vins,))
        vc.execute(
            "DELETE FROM staging.vin_to_listing_events WHERE vin = ANY(%s)", (vins,),
        )
    if artifact_id:
        vc.execute(
            "DELETE FROM staging.silver_observations WHERE artifact_id = %s",
            (artifact_id,),
        )


# ---------------------------------------------------------------------------
# write_detail_active
# ---------------------------------------------------------------------------

class TestWriteDetailActive:
    def test_upserts_price_observation(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        vin = f"VINACT{uuid.uuid4().hex[:11].upper()}"

        write_detail_active(
            _primary(lid, vin=vin), [], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        row = _get_price_obs(vc, lid)
        assert row is not None
        assert row["price"] == 28000
        assert row["make"] == "Honda"
        assert row["vin"] == vin

        _cleanup(vc, listing_ids=[lid], vins=[vin], artifact_id=artifact["artifact_id"])

    def test_maps_vin_to_listing(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        vin = f"VINMP{uuid.uuid4().hex[:12].upper()}"

        result = write_detail_active(
            _primary(lid, vin=vin), [], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert result["vin"] == vin
        row = _get_vin_mapping(vc, vin)
        assert row is not None
        assert str(row["listing_id"]) == lid

        _cleanup(vc, listing_ids=[lid], vins=[vin], artifact_id=artifact["artifact_id"])

    def test_releases_detail_scrape_claim(self, vc, seed_artifact_c, seed_detail_claim_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        seed_detail_claim_c(lid)

        assert _claim_exists(vc, lid)

        write_detail_active(
            _primary(lid), [], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert not _claim_exists(vc, lid), "Claim should be released after active write"

        _cleanup(vc, listing_ids=[lid], vins=["1HGCV1F34PA111001"],
                 artifact_id=artifact["artifact_id"])

    def test_claim_event_written_after_release(self, vc, seed_artifact_c, seed_detail_claim_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        run_id = seed_detail_claim_c(lid)

        write_detail_active(
            _primary(lid), [], artifact["artifact_id"], _NOW, lid, run_id
        )

        vc.execute(
            "SELECT status FROM staging.detail_scrape_claim_events"
            " WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1",
            (lid,),
        )
        row = vc.fetchone()
        assert row is not None
        assert row["status"] == "processed"

        _cleanup(vc, listing_ids=[lid], vins=["1HGCV1F34PA111001"],
                 artifact_id=artifact["artifact_id"])

    def test_clears_blocked_cooldown(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        # Seed a blocked_cooldown entry
        vc.execute(
            "INSERT INTO ops.blocked_cooldown (listing_id, num_of_attempts)"
            " VALUES (%s::uuid, 1)",
            (lid,),
        )

        write_detail_active(
            _primary(lid), [], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert _blocked_cooldown_row(vc, lid) is None, "blocked_cooldown should be cleared"

        _cleanup(vc, listing_ids=[lid], vins=["1HGCV1F34PA111001"],
                 artifact_id=artifact["artifact_id"])

    def test_silver_written_for_primary(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        vin = f"VINSV{uuid.uuid4().hex[:12].upper()}"

        result = write_detail_active(
            _primary(lid, vin=vin), [], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert result["silver_written"] >= 1
        assert _count_silver(vc, artifact["artifact_id"]) >= 1

        _cleanup(vc, listing_ids=[lid], vins=[vin], artifact_id=artifact["artifact_id"])

    def test_vin_to_listing_event_written(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        vin = f"VINEV{uuid.uuid4().hex[:12].upper()}"

        write_detail_active(
            _primary(lid, vin=vin), [], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        vc.execute(
            "SELECT event_type FROM staging.vin_to_listing_events WHERE vin = %s",
            (vin,),
        )
        row = vc.fetchone()
        assert row is not None
        assert row["event_type"] == "mapped"

        _cleanup(vc, listing_ids=[lid], vins=[vin], artifact_id=artifact["artifact_id"])

    def test_vin_fallback_from_existing_vin_to_listing(self, vc, seed_artifact_c):
        """
        Primary has no VIN — should resolve from an existing vin_to_listing entry.
        """
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        vin = f"VINVF{uuid.uuid4().hex[:12].upper()}"

        # Pre-seed vin_to_listing so the batch lookup finds it
        vc.execute(
            "INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)"
            " VALUES (%s, %s::uuid, now(), %s)",
            (vin, lid, artifact["artifact_id"]),
        )

        primary = _primary(lid, vin=None)  # No VIN in parsed data
        write_detail_active(primary, [], artifact["artifact_id"], _NOW, lid, "run-1")

        row = _get_price_obs(vc, lid)
        assert row["vin"] == vin, "VIN should be resolved from vin_to_listing lookup"

        _cleanup(vc, listing_ids=[lid], vins=[vin], artifact_id=artifact["artifact_id"])


class TestWriteDetailVinCollision:
    def test_vin_relisting_replaces_old_price_observation(self, vc, seed_artifact_c):
        """
        Given: price_observations has VIN→old_listing
        When:  detail active processes new listing with same VIN
        Then:  old listing deleted, new listing upserted
        """
        artifact = seed_artifact_c(artifact_type="detail_page")
        old_lid = str(uuid.uuid4())
        new_lid = str(uuid.uuid4())
        vin = f"VINCL{uuid.uuid4().hex[:12].upper()}"

        # Seed old price_observation with the VIN
        vc.execute(
            "INSERT INTO ops.price_observations"
            " (listing_id, vin, price, make, model, last_seen_at, last_artifact_id)"
            " VALUES (%s::uuid, %s, 25000, 'Honda', 'CR-V', now(), %s)",
            (old_lid, vin, artifact["artifact_id"]),
        )
        vc.execute(
            "INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)"
            " VALUES (%s, %s::uuid, now(), %s)",
            (vin, old_lid, artifact["artifact_id"]),
        )

        result = write_detail_active(
            _primary(new_lid, vin=vin), [], artifact["artifact_id"], _NOW, new_lid, "run-1"
        )

        assert result["vin_collision_deleted"] is True

        # Old row gone
        vc.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (old_lid,),
        )
        assert vc.fetchone()["cnt"] == 0

        # New row present
        row = _get_price_obs(vc, new_lid)
        assert row is not None
        assert row["vin"] == vin

        _cleanup(
            vc,
            listing_ids=[old_lid, new_lid],
            vins=[vin],
            artifact_id=artifact["artifact_id"],
        )


class TestWriteDetailCarousel:
    def test_carousel_matching_tracked_model_upserted_to_price_obs(
        self, vc, seed_artifact_c, seed_tracked_model_c, clear_tracked_models_cache
    ):
        """
        Given: tracked_models has (honda, cr-v)
        When:  carousel hint body = "New 2026 Honda CR-V EX"
        Then:  price_observations row created for carousel listing
        """
        seed_tracked_model_c("honda", "cr-v")
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        carousel_lid = str(uuid.uuid4())

        hint = _carousel_hint(carousel_lid, body="New 2026 Honda CR-V EX")

        result = write_detail_active(
            _primary(lid), [hint], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert result["carousel_upserted"] == 1

        row = _get_price_obs(vc, carousel_lid)
        assert row is not None
        assert row["price"] == 20000

        _cleanup(
            vc,
            listing_ids=[lid, carousel_lid],
            vins=["1HGCV1F34PA111001"],
            artifact_id=artifact["artifact_id"],
        )

    def test_carousel_not_matching_tracked_model_filtered_out(
        self, vc, seed_artifact_c, seed_tracked_model_c, clear_tracked_models_cache
    ):
        """
        Given: tracked_models has (honda, cr-v) only
        When:  carousel hint body = "New 2026 Toyota RAV4 XLE"
        Then:  carousel listing NOT upserted to price_observations
        """
        seed_tracked_model_c("honda", "cr-v")
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        carousel_lid = str(uuid.uuid4())

        hint = _carousel_hint(carousel_lid, body="New 2026 Toyota RAV4 XLE")

        result = write_detail_active(
            _primary(lid), [hint], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert result["carousel_filtered"] == 1
        assert _get_price_obs(vc, carousel_lid) is None

        _cleanup(
            vc,
            listing_ids=[lid, carousel_lid],
            vins=["1HGCV1F34PA111001"],
            artifact_id=artifact["artifact_id"],
        )

    def test_carousel_always_goes_to_silver_regardless_of_filter(
        self, vc, seed_artifact_c, seed_tracked_model_c, clear_tracked_models_cache
    ):
        """
        Carousel hints go to silver_observations even when not matching tracked_models.
        """
        seed_tracked_model_c("honda", "cr-v")
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        carousel_lid = str(uuid.uuid4())

        # Non-matching hint (Toyota) — should NOT go to price_obs but SHOULD go to silver
        hint = _carousel_hint(carousel_lid, body="New 2026 Toyota RAV4 XLE")

        result = write_detail_active(
            _primary(lid), [hint], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        # silver_written covers primary (1) + carousel (1) = 2
        assert result["silver_written"] == 2
        assert _count_silver(vc, artifact["artifact_id"]) == 2

        _cleanup(
            vc,
            listing_ids=[lid, carousel_lid],
            vins=["1HGCV1F34PA111001"],
            artifact_id=artifact["artifact_id"],
        )

    def test_carousel_hint_without_price_skipped(
        self, vc, seed_artifact_c, seed_tracked_model_c, clear_tracked_models_cache
    ):
        """Carousel hints with null price are not written anywhere."""
        seed_tracked_model_c("honda", "cr-v")
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        carousel_lid = str(uuid.uuid4())

        hint = _carousel_hint(carousel_lid, price=None, body="New 2026 Honda CR-V EX")

        result = write_detail_active(
            _primary(lid), [hint], artifact["artifact_id"], _NOW, lid, "run-1"
        )

        assert result["carousel_upserted"] == 0
        # Silver: 1 (primary only — hint without price is also excluded from silver)
        assert _count_silver(vc, artifact["artifact_id"]) == 1

        _cleanup(
            vc,
            listing_ids=[lid],
            vins=["1HGCV1F34PA111001"],
            artifact_id=artifact["artifact_id"],
        )


# ---------------------------------------------------------------------------
# write_detail_unlisted
# ---------------------------------------------------------------------------

class TestWriteDetailUnlisted:
    def test_deletes_price_observation(self, vc, seed_artifact_c, seed_price_observation_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = seed_price_observation_c(price=30000, artifact_id=artifact["artifact_id"])

        vc.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert vc.fetchone()["cnt"] == 1

        write_detail_unlisted(
            _primary(lid, listing_state="unlisted"),
            artifact["artifact_id"], _NOW, lid, "run-1",
        )

        vc.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert vc.fetchone()["cnt"] == 0, "price_observations should be deleted for unlisted"

        _cleanup(vc, listing_ids=[lid], artifact_id=artifact["artifact_id"])

    def test_releases_claim(self, vc, seed_artifact_c, seed_detail_claim_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        seed_detail_claim_c(lid)

        write_detail_unlisted(
            _primary(lid, listing_state="unlisted"),
            artifact["artifact_id"], _NOW, lid, "run-1",
        )

        assert not _claim_exists(vc, lid), "Claim should be released after unlisted write"

        _cleanup(vc, listing_ids=[lid], artifact_id=artifact["artifact_id"])

    def test_silver_written_with_unlisted_state(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        result = write_detail_unlisted(
            _primary(lid, listing_state="unlisted"),
            artifact["artifact_id"], _NOW, lid, "run-1",
        )

        assert result["silver_written"] == 1

        vc.execute(
            "SELECT listing_state FROM staging.silver_observations WHERE artifact_id = %s",
            (artifact["artifact_id"],),
        )
        row = vc.fetchone()
        assert row is not None
        assert row["listing_state"] == "unlisted"

        _cleanup(vc, listing_ids=[lid], artifact_id=artifact["artifact_id"])

    def test_price_observation_delete_event_written(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        write_detail_unlisted(
            _primary(lid, listing_state="unlisted"),
            artifact["artifact_id"], _NOW, lid, "run-1",
        )

        vc.execute(
            "SELECT event_type FROM staging.price_observation_events"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = vc.fetchone()
        assert row is not None
        assert row["event_type"] == "deleted"

        _cleanup(vc, listing_ids=[lid], artifact_id=artifact["artifact_id"])

    def test_clears_blocked_cooldown(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        # Seed blocked_cooldown entry
        vc.execute(
            "INSERT INTO ops.blocked_cooldown (listing_id, num_of_attempts)"
            " VALUES (%s::uuid, 2)",
            (lid,),
        )

        write_detail_unlisted(
            _primary(lid, listing_state="unlisted"),
            artifact["artifact_id"], _NOW, lid, "run-1",
        )

        assert _blocked_cooldown_row(vc, lid) is None, (
            "blocked_cooldown should be cleared on unlisted"
        )

        _cleanup(vc, listing_ids=[lid], artifact_id=artifact["artifact_id"])


# ---------------------------------------------------------------------------
# write_detail_blocked
# ---------------------------------------------------------------------------

class TestWriteDetailBlocked:
    def test_first_block_creates_cooldown_with_attempts_1(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        result = write_detail_blocked(artifact["artifact_id"], lid, "run-1")

        assert result["num_attempts"] == 1
        row = _blocked_cooldown_row(vc, lid)
        assert row is not None
        assert row["num_of_attempts"] == 1

        _cleanup(vc, listing_ids=[lid])

    def test_second_block_increments_attempts(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        write_detail_blocked(artifact["artifact_id"], lid, "run-1")
        result = write_detail_blocked(artifact["artifact_id"], lid, "run-2")

        assert result["num_attempts"] == 2
        row = _blocked_cooldown_row(vc, lid)
        assert row["num_of_attempts"] == 2

        _cleanup(vc, listing_ids=[lid])

    def test_first_block_event_type_is_blocked(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        write_detail_blocked(artifact["artifact_id"], lid, "run-1")

        vc.execute(
            "SELECT event_type FROM staging.blocked_cooldown_events"
            " WHERE listing_id = %s::uuid ORDER BY event_id",
            (lid,),
        )
        row = vc.fetchone()
        assert row["event_type"] == "blocked"

        _cleanup(vc, listing_ids=[lid])

    def test_second_block_event_type_is_incremented(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        write_detail_blocked(artifact["artifact_id"], lid, "run-1")
        write_detail_blocked(artifact["artifact_id"], lid, "run-2")

        vc.execute(
            "SELECT event_type FROM staging.blocked_cooldown_events"
            " WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1",
            (lid,),
        )
        row = vc.fetchone()
        assert row["event_type"] == "incremented"

        _cleanup(vc, listing_ids=[lid])

    def test_releases_detail_claim(self, vc, seed_artifact_c, seed_detail_claim_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())
        seed_detail_claim_c(lid)

        assert _claim_exists(vc, lid)

        write_detail_blocked(artifact["artifact_id"], lid, "run-1")

        assert not _claim_exists(vc, lid), "Claim should be released even on blocked page"

        _cleanup(vc, listing_ids=[lid])

    def test_blocked_result_shape(self, vc, seed_artifact_c):
        artifact = seed_artifact_c(artifact_type="detail_page")
        lid = str(uuid.uuid4())

        result = write_detail_blocked(artifact["artifact_id"], lid, "run-1")

        assert result["blocked"] is True
        assert isinstance(result["num_attempts"], int)

        _cleanup(vc, listing_ids=[lid])
