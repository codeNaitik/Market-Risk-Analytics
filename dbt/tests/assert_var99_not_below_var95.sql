-- A 99% VaR must always be at least as large a loss as the 95% VaR.
select * from {{ ref('mart_portfolio_risk_summary') }}
where var99_cad < var95_cad
