-- Batch insert into staging.silver_observations, the queue the archiver later
-- flushes to Parquet in MinIO (Plan 98).
--
-- The VALUES clause below carries psycopg2.extras.execute_values' placeholder,
-- not an f-string: the driver expands it into one tuple per row and binds every
-- value. The column list is spelled out rather than omitted so a column a
-- migration adds cannot silently shift what the positional tuples mean -- this
-- is the same order as _POSTGRES_COLS, which the rows are built against.
INSERT INTO staging.silver_observations (
    artifact_id, listing_id, vin, canonical_detail_url,
    source, listing_state, fetched_at,
    price, make, model, trim, year, mileage, msrp,
    stock_type, fuel_type, body_style,
    dealer_name, dealer_zip, customer_id, seller_id,
    dealer_street, dealer_city, dealer_state, dealer_phone,
    dealer_website, dealer_cars_com_url, dealer_rating,
    financing_type, seller_zip, seller_customer_id,
    page_number, position_on_page, trid, isa_context,
    body, condition
)
VALUES %s
