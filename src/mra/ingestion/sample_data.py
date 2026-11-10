"""Generate SYNTHETIC bronze data in exactly the same shapes as the real APIs.

Why: lets you (and CI, and a recruiter cloning the repo) run the whole pipeline in
minutes with no API keys or network. The data is clearly labelled `source=synthetic`
and must never be presented as real market data.

It deliberately injects the dirty-data cases silver has to handle:
  * duplicate price rows and a negative/zero price (-> quarantine)
  * a 4:1 stock split on one ticker (close jumps, adj_close doesn't)
  * FRED "." missing values, restated SEC filings (same period, later `filed`)

Usage:
    python -m mra.ingestion.sample_data --years 3
"""

from __future__ import annotations

import argparse
from datetime import date

import numpy as np
import pandas as pd

from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.storage import bronze_path, write_json, write_parquet

log = get_logger(__name__)

SECTOR_DRIFT = {  # annualised drift / vol, loosely realistic per sector
    "Information Technology": (0.16, 0.32), "Communication Services": (0.12, 0.30),
    "Consumer Discretionary": (0.11, 0.30), "Financials": (0.09, 0.24),
    "Energy": (0.06, 0.30), "Health Care": (0.08, 0.18), "Consumer Staples": (0.06, 0.15),
    "Industrials": (0.09, 0.22), "Utilities": (0.05, 0.16), "Real Estate": (0.05, 0.22),
    "Materials": (0.07, 0.22),
}


def _prices(rng: np.random.Generator, tickers: list[dict], benchmark: str, start: str,
            end: str) -> dict[str, pd.DataFrame]:
    days = pd.bdate_range(start, end)
    n = len(days)
    market = rng.normal(0.08 / 252, 0.17 / np.sqrt(252), n)        # common market factor
    out = {}
    universe = [*tickers, {"ticker": benchmark, "sector": "Index"}]
    for i, t in enumerate(universe):
        if t["sector"] == "Index":
            rets = market
        else:
            mu, vol = SECTOR_DRIFT.get(t["sector"], (0.07, 0.25))
            beta = 0.6 + 0.9 * rng.random()
            idio = rng.normal(0, vol / np.sqrt(252) * 0.8, n)
            rets = mu / 252 - beta * 0.08 / 252 + beta * market + idio
        adj = 50 * (1 + i % 7) * np.exp(np.cumsum(rets))
        close = adj.copy()
        dividend_factor = np.linspace(0.97, 1.0, n)                  # adj_close < close historically
        adj = close * dividend_factor
        if t["ticker"] == "NVDA":                                    # 4:1 split two-thirds through
            split = int(n * 0.66)
            close[:split] = close[:split] * 4
        high = close * (1 + np.abs(rng.normal(0, 0.008, n)))
        low = close * (1 - np.abs(rng.normal(0, 0.008, n)))
        open_ = low + (high - low) * rng.random(n)
        df = pd.DataFrame({
            "ticker": t["ticker"], "date": days, "open": open_, "high": high, "low": low,
            "close": close, "adj_close": adj,
            "volume": rng.integers(1_000_000, 60_000_000, n).astype("int64"),
            "source": "synthetic",
        })
        if t["ticker"] == "AAPL":                                    # dirty rows for quarantine
            df = pd.concat([df, df.iloc[[10, 11]]], ignore_index=True)   # duplicates
            df.loc[5, ["close", "adj_close"]] = [-1.0, -1.0]             # impossible price
        out[t["ticker"]] = df
    return out


def _companyfacts(rng: np.random.Generator, ticker: str, cik: int, years: list[int]) -> dict:
    """Mimic data.sec.gov companyfacts: facts.us-gaap.<Concept>.units.<unit>[...]"""
    base_rev = rng.uniform(20e9, 400e9)
    margin = rng.uniform(0.05, 0.3)
    concepts: dict[str, list[dict]] = {k: [] for k in [
        "Revenues", "NetIncomeLoss", "Assets", "Liabilities", "StockholdersEquity",
        "LongTermDebt", "NetCashProvidedByUsedInOperatingActivities", "EarningsPerShareDiluted"]}
    growth_rate = rng.normal(0.07, 0.05)                              # company-level annual growth
    for fy in years:
        growth = (1 + growth_rate) ** (fy - years[0])
        q_rev = [base_rev / 4 * growth * rng.uniform(0.94, 1.06) for _ in range(4)]
        q_ni = [r * margin * rng.uniform(0.85, 1.15) for r in q_rev]
        assets = base_rev * 1.6 * growth
        liab = assets * rng.uniform(0.45, 0.75)
        # 10-Qs for Q1-Q3, the 10-K carries the full year (= sum of the 4 quarters).
        # The Q2 10-Q also reports a 6-month year-to-date figure, like real filings do.
        periods = [("Q1", 1, 3, q_rev[0], q_ni[0]), ("Q2", 4, 6, q_rev[1], q_ni[1]),
                   ("Q2", 1, 6, q_rev[0] + q_rev[1], q_ni[0] + q_ni[1]),
                   ("Q3", 7, 9, q_rev[2], q_ni[2]), ("FY", 1, 12, sum(q_rev), sum(q_ni))]
        for q, (fp, s_m, e_m, rev, ni) in enumerate(periods):
            filed = pd.Timestamp(fy, e_m, 28) + pd.Timedelta(days=40 if fp != "FY" else 60)
            common = {
                "start": f"{fy}-{s_m:02d}-01", "end": str((pd.Timestamp(fy, e_m, 1) + pd.offsets.MonthEnd(0)).date()),
                "fy": fy, "fp": fp, "form": "10-K" if fp == "FY" else "10-Q",
                "filed": str(filed.date()), "accn": f"0000{cik}-{fy % 100:02d}-{q:06d}",
                "frame": f"CY{fy}" if fp == "FY" else None,
            }
            concepts["Revenues"].append({**common, "val": round(rev)})
            concepts["NetIncomeLoss"].append({**common, "val": round(ni)})
            concepts["NetCashProvidedByUsedInOperatingActivities"].append({**common, "val": round(ni * 1.25)})
            concepts["EarningsPerShareDiluted"].append({**common, "val": round(ni / 5e9, 2)})
            if s_m == 1 and e_m == 6:
                continue                                                  # balance sheet only once per period end
            instant = {k: v for k, v in common.items() if k != "start"}  # balance sheet = point in time
            concepts["Assets"].append({**instant, "val": round(assets)})
            concepts["Liabilities"].append({**instant, "val": round(liab)})
            concepts["StockholdersEquity"].append({**instant, "val": round(assets - liab)})
            concepts["LongTermDebt"].append({**instant, "val": round(liab * 0.4)})
        # a restatement: same FY revenue re-filed later with a slightly different value
        last = dict(concepts["Revenues"][-1])
        last.update(val=round(last["val"] * 0.99), filed=str((pd.Timestamp(last["filed"]) + pd.Timedelta(days=200)).date()),
                    accn=last["accn"] + "R")
        concepts["Revenues"].append(last)

    usgaap = {}
    for name, rows in concepts.items():
        unit = "USD/shares" if name == "EarningsPerShareDiluted" else "USD"
        usgaap[name] = {"label": name, "description": f"Synthetic {name}", "units": {unit: rows}}
    return {"cik": cik, "entityName": f"{ticker} (synthetic)", "facts": {"us-gaap": usgaap}, "_ticker": ticker}


def _macro_series(rng, start, end, freq, level, vol, missing_rate=0.01, decimals=2):
    idx = pd.date_range(start, end, freq="MS" if freq == "monthly" else "B")
    vals = level + np.cumsum(rng.normal(0, vol, len(idx)))
    vals = np.clip(vals, 0.05, None)
    return [(d.date().isoformat(), "." if rng.random() < missing_rate else f"{v:.{decimals}f}")
            for d, v in zip(idx, vals, strict=False)]


MACRO_LEVELS = {  # starting level, step volatility
    "FEDFUNDS": (1.5, 0.15), "DGS10": (2.5, 0.04), "DGS2": (1.8, 0.04), "CPIAUCSL": (250, 0.6),
    "UNRATE": (4.5, 0.12), "VIXCLS": (18, 0.9), "V39079": (1.5, 0.01), "FXUSDCAD": (1.32, 0.004),
    "BD.CDN.10YR.DQ.YLD": (2.2, 0.03),
}


def run(years: int = 3, seed: int = 42, ingest_date: date | None = None) -> None:
    s = get_settings()
    rng = np.random.default_rng(seed)
    end = pd.Timestamp.today().normalize() - pd.offsets.BDay(1)
    start = (end - pd.DateOffset(years=years)).normalize()

    for ticker, df in _prices(rng, s.tickers, s.benchmark, start, end).items():
        write_parquet(bronze_path("market", "daily_prices", f"{ticker.replace('^', 'IDX_')}.parquet", ingest_date), df)
    log.info("prices: %d tickers, %s..%s", len(s.tickers) + 1, start.date(), end.date())

    fy = list(range(start.year, end.year))
    for i, t in enumerate(x for x in s.tickers if x["exchange"] != "TSX"):
        payload = _companyfacts(rng, t["ticker"], 100000 + i, fy)
        write_json(bronze_path("sec_edgar", "companyfacts", f"{t['ticker']}.json", ingest_date), payload,
                   source_url="synthetic")
    log.info("sec_edgar: companyfacts for fiscal years %s", fy)

    for series_id, meta in s.raw["fred"]["series"].items():
        level, vol = MACRO_LEVELS[series_id]
        obs = [{"date": d, "value": v} for d, v in _macro_series(rng, start, end, meta["frequency"], level, vol)]
        write_json(bronze_path("fred", "observations", f"{series_id}.json", ingest_date),
                   {"observations": obs, "_series_id": series_id, "_series_meta": meta}, source_url="synthetic")

    for series_id, meta in s.raw["boc_valet"]["series"].items():
        level, vol = MACRO_LEVELS[series_id]
        decimals = 4 if series_id.startswith("FX") else 2
        obs = [{"d": d, series_id: {"v": v}} for d, v in
               _macro_series(rng, start, end, meta["frequency"], level, vol, missing_rate=0, decimals=decimals)]
        write_json(bronze_path("boc_valet", "observations", f"{series_id}.json", ingest_date),
                   {"observations": obs, "_series_id": series_id, "_series_meta": meta}, source_url="synthetic")
    log.info("macro: FRED + Bank of Canada series written. Synthetic bronze is ready at %s", s.path("bronze"))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--years", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    run(a.years, a.seed)
