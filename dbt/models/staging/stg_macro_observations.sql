select
    source,
    series_id,
    series_name,
    unit,
    frequency,
    cast(obs_date as date)            as obs_date,
    value,
    _processed_at
from {{ source('silver', 'macro_observations') }}
