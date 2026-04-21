"""
Kafka event emission stubs — log-only today.

Plan 87 swaps these with real Kafka producer implementations.
Each function is called after the Postgres commit succeeds,
providing the clean seam for Kafka integration.
"""
import logging

logger = logging.getLogger(__name__)


def emit_price_updated(vin: str, price: int, listing_id: str, source: str) -> None:
    """Emitted when a price observation is upserted with both VIN and price."""
    logger.info(
        "event:price_updated vin=%s price=%s listing_id=%s source=%s",
        vin, price, listing_id, source,
    )


def emit_listing_removed(vin: str | None, listing_id: str) -> None:
    """Emitted when a listing is confirmed unlisted and deleted from price_observations."""
    logger.info(
        "event:listing_removed vin=%s listing_id=%s",
        vin, listing_id,
    )


def emit_vin_mapped(listing_id: str, vin: str) -> None:
    """Emitted when a new VIN → listing mapping is established or updated."""
    logger.info(
        "event:vin_mapped listing_id=%s vin=%s",
        listing_id, vin,
    )
