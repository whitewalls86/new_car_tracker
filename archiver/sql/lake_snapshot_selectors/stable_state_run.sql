WITH obs AS (
    SELECT
        artifact_id, listing_id, fetched_at, vin,
        price, mileage, msrp, make, model, trim AS vehicle_trim, year AS model_year,
        stock_type, fuel_type, body_style, listing_state,
        dealer_name, dealer_zip, dealer_city, dealer_state, customer_id
    FROM read_parquet('{path}', union_by_name=true)
    {where}
),
vinned AS (
    SELECT *,
        CASE WHEN vin IS NOT NULL AND length(vin) = 17
                  AND regexp_matches(upper(vin), '^[A-Z0-9]{{17}}$')
             THEN upper(vin) ELSE NULL END AS vin17
    FROM obs
),
filtered AS (
    SELECT *,
        md5(concat_ws('|',
            coalesce(listing_id,                   ''),
            coalesce(vin17,                        ''),
            coalesce(CAST(price       AS VARCHAR), ''),
            coalesce(CAST(mileage     AS VARCHAR), ''),
            coalesce(CAST(msrp        AS VARCHAR), ''),
            coalesce(make,                         ''),
            coalesce(model,                        ''),
            coalesce(vehicle_trim,                 ''),
            coalesce(CAST(model_year  AS VARCHAR), ''),
            coalesce(stock_type,                   ''),
            coalesce(fuel_type,                    ''),
            coalesce(body_style,                   ''),
            coalesce(listing_state,                ''),
            coalesce(dealer_name,                  ''),
            coalesce(dealer_zip,                   ''),
            coalesce(dealer_city,                  ''),
            coalesce(dealer_state,                 ''),
            coalesce(customer_id,                  '')
        )) AS fingerprint
    FROM vinned
    WHERE vin17 IS NOT NULL
),
fp AS (
    SELECT *,
        LAG(fingerprint) OVER (
            PARTITION BY vin17 ORDER BY fetched_at, artifact_id
        ) AS prev_fingerprint
    FROM filtered
)
SELECT vin17 AS vin, listing_id, artifact_id, fetched_at, fingerprint
FROM fp
WHERE prev_fingerprint IS NOT NULL AND fingerprint = prev_fingerprint
ORDER BY vin17, fetched_at
