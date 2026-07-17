{{
  config(
    materialized='table',
    file_format='iceberg' if target.type == 'spark' else none
  )
}}

-- National market benchmarks per make/model.
-- Computed from current prices across all VINs we track.
-- Used by mart_deal_scores for deal scoring and national comparison.
--
-- Replaces: int_model_price_benchmarks + int_price_percentiles_by_vin
--
-- Plan 125 Gate B. Three separate cast questions live in this model, and the
-- audit (F10/F12) got the ranking backwards on all three:
--
--  * `percentile_cont(p) within group (order by ...)` — flagged F10 as the
--    rounding risk. It is not: the syntax works unchanged on Spark 3.5.3 and
--    the raw values agree exactly (p10 over 1..10 = 1.9 on both engines).
--  * `::int` — the ACTUAL source of the audit's predicted "one-dollar
--    difference on every benchmark row". DuckDB's ::int ROUNDS; Spark's
--    cast(x as int) TRUNCATES, so 1.9 becomes 2 on DuckDB and 1 on Spark. It
--    is an F12 bug, not an F10 one. cast_to_int uses bround (half-to-even),
--    because that is what DuckDB does to a DOUBLE — round() would be half-up
--    and wrong at 2.5. This is exactly why the parity script asserts exact
--    equality: a ±1 numeric tolerance would hide this bug rather than catch it.
--  * bare `::numeric` — NOT the same item as `::numeric(5,2)` below, which is
--    genuinely identical across engines and stays a literal cast. Bare
--    `numeric` is DECIMAL(18,3) on DuckDB but DECIMAL(10,0) on Spark, and
--    DuckDB's division then promotes the whole expression to DOUBLE where
--    Spark's stays decimal. cast_to_numeric reproduces the DOUBLE. Here the
--    trailing ::numeric(5,2) happens to round all spellings to the same answer,
--    but that is a coincidence of magnitude, not a guarantee — the same
--    expression in int_listing_volatility_features is exposed raw and does
--    diverge visibly.

select
    obs.make,
    obs.model,
    count(*)                                                                             as national_listing_count,
    {{ cast_to_int('avg(ph.current_price)') }}                                           as national_avg_price,
    {{ cast_to_int('percentile_cont(0.5)  within group (order by ph.current_price)') }}  as national_median_price,
    {{ cast_to_int('percentile_cont(0.10) within group (order by ph.current_price)') }}  as national_p10_price,
    {{ cast_to_int('percentile_cont(0.25) within group (order by ph.current_price)') }}  as national_p25_price,
    {{ cast_to_int('percentile_cont(0.75) within group (order by ph.current_price)') }}  as national_p75_price,
    {{ cast_to_int('percentile_cont(0.90) within group (order by ph.current_price)') }}  as national_p90_price,
    cast(avg(case when obs.msrp > 0
             then {{ cast_to_numeric('(obs.msrp - ph.current_price)') }} / obs.msrp * 100
        end) as decimal(5,2))                                                            as national_avg_discount_pct
from {{ ref('int_latest_observation') }} obs
join {{ ref('int_price_history') }} ph on ph.vin = obs.vin17
where ph.current_price > 0
  and obs.make is not null
  and obs.model is not null
group by obs.make, obs.model
