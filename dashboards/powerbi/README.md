# Power BI dashboard build guide

The `.pbix` file is built by you in Power BI Desktop (Windows, free). This folder gives you
everything else: the data model, DAX measures (`measures.dax`), a theme (`theme.json`) and
the page-by-page spec below. Commit your finished `market_risk.pbix` here, plus screenshots
in `screenshots/` for the main README.

## 1. Connect to the data

| Mode | How |
|---|---|
| **Local (synthetic or real data)** | `make demo` or `make pipeline`, then *Get Data → Parquet* for each file in `lake/gold_export/` (or *Get Data → Folder* and combine). Import mode. |
| **Cloud (Azure)** | *Get Data → Azure Databricks*, paste the SQL Warehouse *Server hostname* and *HTTP path*, sign in with Microsoft Entra ID, pick catalog `market_risk`, schema `gold`. Use **Import** for marts and **DirectQuery** only for `mart_crypto_live`. |

Then *View → Themes → Browse for themes →* `theme.json`.

## 2. Data model (star schema)

Mark `dim_date` as the date table (*Table tools → Mark as date table → calendar_date*).

| From (many) | To (one) | Notes |
|---|---|---|
| `fct_daily_prices[ticker]` | `dim_company[ticker]` | single direction |
| `fct_daily_prices[date_key]` | `dim_date[date_key]` | |
| `mart_returns_risk[ticker]` | `dim_company[ticker]` | |
| `mart_returns_risk[trade_date]` | `dim_date[calendar_date]` | |
| `mart_portfolio_daily[trade_date]` | `dim_date[calendar_date]` | |
| `risk_var_backtest[trade_date]` | `dim_date[calendar_date]` | |
| `fct_macro_daily[date_key]` | `dim_date[date_key]` | |
| `mart_fundamentals[ticker]` | `dim_company[ticker]` | 1:1, single direction |
| `mart_sector_performance[month_start_date]` | `dim_date[calendar_date]` | |

Hide the key columns (`date_key`, `price_id`) from report view. Create an empty `_Measures`
table and paste the measures from `measures.dax` into it, using display folders
(Portfolio, VaR, Security, Macro).

## 3. Pages

**Page 1: Executive overview**
- Cards: `Portfolio Value (CAD)`, `Portfolio Return`, `Excess Return vs Benchmark`, `Sharpe Ratio`, `Max Drawdown`, `VaR 99% (CAD)`
- Line chart: `mart_portfolio_daily[portfolio_cumulative_return]` vs `benchmark_cumulative_return` by date
- Area chart: `drawdown` by date (shows the underwater periods)
- Slicer: date range on `dim_date[calendar_date]`; text box with `Data Freshness`

**Page 2: Market risk & VaR**
- Column chart: `risk_var_backtest[portfolio_return]` by date, with a line for `-var_forecast`; conditional colour red when `is_breach`
- Cards: `VaR Breaches`, `Expected Breaches (99%)`, `Backtest Verdict`
- Clustered bar: `risk_var_comparison` var99_cad by `method`, small multiples by `horizon_days`
- Histogram (bins on `portfolio_return`) with the 5% percentile marked as a constant line

**Page 3: Securities**
- Matrix: `dim_company[sector]` → `ticker` with `Stock Return`, `Stock Volatility (Ann.)`, `Latest Beta (1Y)`, `Latest Close`; data bars on return
- Heatmap: matrix of `risk_correlation_matrix` (`ticker_a` rows, `ticker_b` columns, `correlation` values, diverging background colours)
- Line: `fct_daily_prices[close_price]` and `Close 30D MA` for the selected ticker
- Scatter: `risk_efficient_frontier` volatility (X) vs expected_return (Y), coloured by `sharpe_ratio`; highlight `is_max_sharpe`

**Page 4: Fundamentals screener**
- Table from `mart_fundamentals`: revenue_ttm, net_margin_ttm, revenue_growth_yoy, debt_to_equity, pe_ratio_ttm
- Slicers for sector and P/E range; scatter of `revenue_growth_yoy` vs `pe_ratio_ttm` (growth vs valuation)

**Page 5: Macro impact**
- Cards: `BoC Policy Rate (Latest)`, `Yield Curve Status`
- Line: `fct_macro_daily` boc_policy_rate, fed_funds_rate, us_10y_yield, cad_10y_yield
- Heatmap matrix: `mart_rate_sensitivity` sector × factor_label, values = correlation
- Scatter: `mart_macro_monthly` boc_rate_change (X) vs sector_return (Y), play axis on month

**Page 6 (optional): Live crypto**: DirectQuery on `mart_crypto_live`, auto page refresh every 1 minute (Premium/Fabric capacity or Power BI Desktop only), candlestick-style line of close and `sma_15m`.

## 4. Publish

*Publish → My workspace.* For a public portfolio link, "Publish to web" needs your tenant
admin to allow it. If you can't publish, a 2-3 minute screen recording walking through the
pages works just as well on GitHub and LinkedIn.
