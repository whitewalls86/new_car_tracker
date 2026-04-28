{{
  config(materialized='table')
}}

-- Dealer reference data derived from silver observations.
-- One row per customer_id — most recent known attributes per dealer.

select
    customer_id,
    arg_max(dealer_name, fetched_at)  as name,
    arg_max(dealer_zip, fetched_at)   as zip
from {{ ref('int_latest_observation') }}
where customer_id is not null
group by customer_id
