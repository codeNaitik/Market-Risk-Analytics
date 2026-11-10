"""Gold-layer risk job: reads dbt marts, runs the Python risk models, writes results back.

Outputs (schema `gold`):
  risk_var_backtest          daily 99% VaR forecast vs realised return, breach flag
  risk_var_backtest_summary  Kupiec POF test result
  risk_var_comparison        historical vs parametric vs Monte Carlo VaR (1d and 10d)
  risk_efficient_frontier    5,000 sampled portfolios + max-Sharpe / min-vol markers
  risk_correlation_matrix    pairwise correlations (long format, for a Power BI heatmap)

Local target = the DuckDB warehouse built by dbt. On Databricks the same code reads
and writes Unity Catalog tables through Spark.

Run:  python -m mra.analytics.run_risk
"""

from __future__ import annotations

import pandas as pd

from mra.analytics import risk
from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.processing.spark_session import on_databricks

log = get_logger(__name__)


class Warehouse:
    def __init__(self):
        s = get_settings()
        if on_databricks():
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()
            self.con = None
        else:
            import duckdb
            self.spark = None
            self.con = duckdb.connect(s.path("warehouse.duckdb"))

    def read(self, sql: str) -> pd.DataFrame:
        return self.spark.sql(sql).toPandas() if self.spark else self.con.sql(sql).df()

    def write(self, df: pd.DataFrame, table: str) -> None:
        if self.spark:
            self.spark.createDataFrame(df).write.mode("overwrite").saveAsTable(f"gold.{table}")
        else:
            self.con.register("_tmp", df)
            self.con.execute(f"create or replace table gold.{table} as select * from _tmp")
            self.con.unregister("_tmp")
        log.info("wrote gold.%s (%d rows)", table, len(df))


def run() -> None:
    s = get_settings()
    wh = Warehouse()
    weights = pd.Series(s.portfolio["weights"], dtype=float)

    port = wh.read("select trade_date, portfolio_return, portfolio_value_cad from gold.mart_portfolio_daily order by trade_date")
    port["trade_date"] = pd.to_datetime(port["trade_date"])
    port_ret = port.set_index("trade_date")["portfolio_return"]
    value_now = float(port["portfolio_value_cad"].iloc[-1])

    asset = wh.read(f"""
        select trade_date, ticker, daily_return from gold.fct_daily_prices
        where daily_return is not null and ticker in ({",".join(f"'{t}'" for t in weights.index)})""")
    asset_ret = asset.pivot(index="trade_date", columns="ticker", values="daily_return").fillna(0.0)

    # 1. VaR backtest + Kupiec test
    bt, summary = risk.var_backtest(port_ret, window=min(252, len(port_ret) // 2), confidence=0.99)
    wh.write(bt.reset_index().rename(columns={"index": "trade_date"}), "risk_var_backtest")
    wh.write(pd.DataFrame([summary]), "risk_var_backtest_summary")
    log.info("backtest: %d breaches vs %.1f expected, Kupiec p=%.3f (%s)", summary["breaches"],
             summary["expected_breaches"], summary["p_value"], "OK" if summary["model_ok_at_5pct"] else "REJECT")

    # 2. VaR methods side by side, last year of data, 1-day and 10-day (Basel horizon)
    recent = asset_ret.tail(252)
    recent_port = port_ret.tail(252)
    rows = []
    for horizon in (1, 10):
        mc = risk.monte_carlo_var(recent, weights, horizon_days=horizon)
        scale = horizon ** 0.5
        rows += [
            {"method": "historical", "horizon_days": horizon,
             "var95": risk.historical_var(recent_port, 0.95) * scale, "var99": risk.historical_var(recent_port, 0.99) * scale,
             "cvar95": risk.historical_cvar(recent_port, 0.95) * scale},
            {"method": "parametric", "horizon_days": horizon,
             "var95": risk.parametric_var(recent_port, 0.95, horizon), "var99": risk.parametric_var(recent_port, 0.99, horizon),
             "cvar95": None},
            {"method": "monte_carlo", "horizon_days": horizon, "var95": mc.var95, "var99": mc.var99, "cvar95": mc.cvar95},
        ]
    comp = pd.DataFrame(rows)
    for col in ("var95", "var99", "cvar95"):
        comp[f"{col}_cad"] = comp[col] * value_now
    comp["note"] = "historical 10d uses sqrt-of-time scaling"
    wh.write(comp, "risk_var_comparison")

    # 3. efficient frontier over the holdings
    frontier = risk.efficient_frontier(asset_ret, risk_free=s.portfolio.get("risk_free_rate", 0.0))
    wh.write(frontier, "risk_efficient_frontier")

    # 4. correlation matrix (long) for a heatmap visual
    c = asset_ret.corr()
    c.index.name, c.columns.name = "ticker_a", "ticker_b"
    corr = c.stack().rename("correlation").reset_index()
    wh.write(corr, "risk_correlation_matrix")


if __name__ == "__main__":
    run()
