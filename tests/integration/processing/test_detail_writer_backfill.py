"""Stage 0d: backfill rows append history without changing live state."""
import uuid
from datetime import datetime, timezone

import pytest

from processing.writers.detail_writer import write_detail_active, write_detail_unlisted

pytestmark = pytest.mark.integration

APRIL = datetime(2026, 4, 21, 12, tzinfo=timezone.utc)


def _primary(listing_id, vin, *, state="active"):
    return {"listing_id": listing_id, "listing_state": state, "vin": vin,
            "price": 21000, "make": "Honda", "model": "Civic", "year": 2025}


def _hot_snapshot(vc, listing_id, vin):
    vc.execute("SELECT listing_id, vin, price, make, model, last_seen_at, last_artifact_id FROM ops.price_observations WHERE listing_id = %s::uuid", (listing_id,))
    price = dict(vc.fetchone())
    vc.execute("SELECT vin, listing_id, mapped_at, artifact_id FROM ops.vin_to_listing WHERE vin = %s", (vin,))
    mapping = dict(vc.fetchone())
    vc.execute("SELECT listing_id, num_of_attempts FROM ops.blocked_cooldown WHERE listing_id = %s::uuid", (listing_id,))
    cooldown = dict(vc.fetchone())
    vc.execute("SELECT listing_id, status FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,))
    claim = dict(vc.fetchone())
    vc.execute("SELECT * FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid", (listing_id,))
    staleness = dict(vc.fetchone())
    vc.execute("SELECT * FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid", (listing_id,))
    queue = vc.fetchone()
    return price, mapping, cooldown, claim, staleness, dict(queue) if queue else None


@pytest.mark.parametrize("state,event_type", [("active", "upserted"), ("unlisted", "deleted")])
def test_backfill_leaves_hot_rows_unchanged_and_dates_event_historically(
    vc, seed_artifact_c, seed_price_observation_c, seed_vin_to_listing_c, seed_detail_claim_c,
    state, event_type,
):
    artifact = seed_artifact_c(artifact_type="detail_page")
    listing_id = str(uuid.uuid4())
    vin = f"BACKFILL{uuid.uuid4().hex[:9].upper()}"
    seed_price_observation_c(listing_id, vin, price=45000, artifact_id=artifact["artifact_id"])
    seed_vin_to_listing_c(vin, listing_id, artifact_id=artifact["artifact_id"])
    seed_detail_claim_c(listing_id)
    vc.execute("INSERT INTO ops.blocked_cooldown (listing_id, num_of_attempts) VALUES (%s::uuid, 3)", (listing_id,))
    before = _hot_snapshot(vc, listing_id, vin)

    if state == "active":
        result = write_detail_active(_primary(listing_id, vin), [], artifact["artifact_id"], APRIL, listing_id, None, backfill=True)
    else:
        result = write_detail_unlisted(_primary(listing_id, vin, state=state), artifact["artifact_id"], APRIL, listing_id, None, backfill=True)

    assert result["silver_written"] == 1
    assert _hot_snapshot(vc, listing_id, vin) == before
    vc.execute("SELECT event_type, event_at FROM staging.price_observation_events WHERE listing_id = %s::uuid ORDER BY event_id DESC LIMIT 1", (listing_id,))
    event = vc.fetchone()
    assert event["event_type"] == event_type
    assert event["event_at"] == APRIL

    vc.execute("DELETE FROM staging.silver_observations WHERE artifact_id = %s", (artifact["artifact_id"],))
    vc.execute("DELETE FROM staging.price_observation_events WHERE listing_id = %s::uuid", (listing_id,))
    vc.execute("DELETE FROM ops.price_observations WHERE listing_id = %s::uuid", (listing_id,))
    vc.execute("DELETE FROM ops.vin_to_listing WHERE vin = %s", (vin,))
    vc.execute("DELETE FROM ops.blocked_cooldown WHERE listing_id = %s::uuid", (listing_id,))
    vc.execute("DELETE FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,))
