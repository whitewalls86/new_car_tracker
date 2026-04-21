-- Check if a VIN already exists at a different listing_id in price_observations.
-- Used during detail processing to detect VIN relisting.
SELECT listing_id, vin
FROM ops.price_observations
WHERE vin = %(vin)s AND listing_id != %(listing_id)s
