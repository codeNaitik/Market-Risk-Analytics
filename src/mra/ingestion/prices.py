"""Ingest daily OHLCV prices for the equity universe + benchmark index.

Uses `yfinance`, an unofficial Yahoo Finance client. It is free and convenient for a
portfolio project, but it can rate-limit or break when Yahoo changes its site, so:
  * we request one ticker at a time with a pause, and
  * every run is cached in bronze, so a failure never loses history.
For production you'd swap this module for a licensed vendor (Refinitiv, Bloomberg,
Polygon, Twelve Data) without touching anything downstream.

Both raw `close` and `adj_close` (split/dividend adjusted) are kept: returns must be
computed on adj_close, and silver checks the two agree outside corporate actions.

Usage:
    python -m mra.ingestion.prices                 # full history from settings
    python -m mra.ingestion.prices --days 7        # incremental top-up
"""

from __future__ import annotations

import argparse
import time
from datetime import date, timedelta

import pandas as pd

from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.storage import bronze_path, write_parquet

log = get_logger(__name__)

COLUMNS = ["date", "open", "high", "low", "close", "adj_close", "volume"]


def _download(symbol: str, start: str) -> pd.DataFrame:
    import yfinance as yf  # imported lazily so tests don't need it

    df = yf.download(symbol, start=start, auto_adjust=False, progress=False, threads=False)
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):        # newer yfinance returns (field, ticker)
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index().rename(columns=lambda c: str(c).lower().replace(" ", "_"))
    return df[COLUMNS]


def run(days: int | None = None, ingest_date: date | None = None) -> list[str]:
    s = get_settings()
    start = (date.today() - timedelta(days=days)).isoformat() if days else s.start_date
    written = []
    for symbol in [*s.ticker_symbols, s.benchmark]:
        try:
            df = _download(symbol, start)
        except Exception as exc:  # noqa: BLE001 - one bad ticker must not kill the run
            log.error("download failed for %s: %s", symbol, exc)
            continue
        if df.empty:
            log.warning("no rows for %s", symbol)
            continue
        df.insert(0, "ticker", symbol)
        df["source"] = "yfinance"
        safe = symbol.replace("^", "IDX_")
        out = bronze_path("market", "daily_prices", f"{safe}.parquet", ingest_date)
        written.append(write_parquet(out, df))
        log.info("%s: %d rows %s..%s", symbol, len(df), df["date"].min().date(), df["date"].max().date())
        time.sleep(0.5)
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=None, help="only fetch the last N days")
    parser.add_argument("--ingest-date", type=date.fromisoformat, default=None)
    a = parser.parse_args()
    run(a.days, a.ingest_date)
