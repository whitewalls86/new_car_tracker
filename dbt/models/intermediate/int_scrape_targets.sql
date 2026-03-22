-- Active scrape targets: the canonical make/model pairs we track.
-- Joins the seed lookup (slug → display name) to enabled search_configs.

select
    st.search_key,
    st.make,
    st.model,
    lower(st.make) as make_lower,
    lower(st.model) as model_lower,
    sc.enabled,
    sc.params
from {{ ref('scrape_targets') }} st
inner join {{ source('public', 'search_configs') }} sc
    on sc.search_key = st.search_key
where sc.enabled = true
