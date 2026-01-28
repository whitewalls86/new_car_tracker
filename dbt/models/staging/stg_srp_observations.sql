select
  *
from {{ source('public', 'srp_observations') }}
