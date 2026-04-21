-- Pre-upsert VIN resolution: look up known VINs for a batch of listing_ids.
-- Returns listing_id → vin mapping for all known entries.
SELECT listing_id, vin
FROM ops.vin_to_listing
WHERE listing_id = ANY(%(listing_ids)s)
