-- Grain: sector x macro factor. How each sector's monthly return co-moves with rate/FX moves.
--   correlation   : direction + strength of co-movement (-1..1)
--   beta_per_100bp: regression slope scaled to a 1 percentage-point (100 bp) move in the factor
{% set factors = {
    'boc_rate_change': 'Bank of Canada policy rate',
    'fed_rate_change': 'US Fed funds rate',
    'us_10y_change':   'US 10Y Treasury yield',
    'cad_10y_change':  'Canada 10Y bond yield',
    'usd_cad_pct_change': 'USD/CAD exchange rate (%)'
} %}

{% for col, label in factors.items() %}
select
    sector,
    '{{ col }}'                                        as factor,
    '{{ label }}'                                      as factor_label,
    count({{ col }})                                   as n_months,
    corr(sector_return, {{ col }})                     as correlation,
    regr_slope(sector_return, {{ col }})               as beta_per_100bp,
    regr_r2(sector_return, {{ col }})                  as r_squared
from {{ ref('mart_macro_monthly') }}
where {{ col }} is not null
group by sector
{% if not loop.last %}union all{% endif %}
{% endfor %}
