-- Delete a price observation row for a VIN that has moved to a new listing.
-- Only deletes the old row keyed by listing_id when VIN relisting is detected.
DELETE FROM ops.price_observations
WHERE listing_id = %(old_listing_id)s
