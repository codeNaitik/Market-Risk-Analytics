-- Singular test: fails (returns a row) if the portfolio weights don't add up to 100%.
select portfolio_name, sum(weight) as total_weight
from {{ ref('portfolio_weights') }}
group by portfolio_name
having abs(sum(weight) - 1) > 0.0001
