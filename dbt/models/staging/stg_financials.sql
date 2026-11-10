-- SEC facts in long form: one row per ticker / metric / period.
select
    ticker,
    cik,
    entity_name,
    metric,
    concept                           as xbrl_concept,
    unit,
    period_type,                      -- quarter | annual | instant
    cast(period_start as date)        as period_start,
    cast(period_end as date)          as period_end,
    cast(fiscal_year as int)          as fiscal_year,
    cast(fiscal_quarter as int)       as fiscal_quarter,
    value,
    form,
    cast(filed_date as date)          as filed_date,
    accession_number,
    is_restated,
    _processed_at
from {{ source('silver', 'financials') }}
