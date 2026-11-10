"""Ingest company financial statements from SEC EDGAR's XBRL "companyfacts" API.

API docs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
  - Ticker -> CIK map:  https://www.sec.gov/files/company_tickers.json
  - Company facts:      https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json

Rules SEC enforces: a descriptive User-Agent ("Name email") and <= 10 requests/sec.
Raw JSON lands in bronze untouched; flattening happens in the Spark silver job,
because companyfacts is deeply nested (concept -> unit -> list of filings).

Usage:
    python -m mra.ingestion.sec_edgar
"""

from __future__ import annotations

import argparse
from datetime import date

from mra.config import get_settings
from mra.ingestion.http import RateLimiter, build_session
from mra.logging_utils import get_logger
from mra.storage import bronze_path, write_json

log = get_logger(__name__)

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"


def fetch_ticker_map(session) -> dict[str, int]:
    resp = session.get(TICKER_MAP_URL, timeout=30)
    resp.raise_for_status()
    # Shape: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    return {row["ticker"].upper(): int(row["cik_str"]) for row in resp.json().values()}


def run(ingest_date: date | None = None) -> list[str]:
    s = get_settings()
    session = build_session(s.sec_user_agent)
    limiter = RateLimiter(rate=8)  # stay safely under SEC's 10 req/s

    limiter.wait()
    cik_map = fetch_ticker_map(session)
    written: list[str] = []

    for t in s.tickers:
        symbol = t["ticker"]
        if t["exchange"] == "TSX":
            log.info("skip %s: Canadian issuers file on SEDAR+, not EDGAR", symbol)
            continue
        cik = cik_map.get(symbol.replace(".", "-"))  # BRK.B is BRK-B in SEC's map
        if cik is None:
            log.warning("no CIK found for %s; skipping", symbol)
            continue

        url = COMPANY_FACTS_URL.format(cik=cik)
        limiter.wait()
        resp = session.get(url, timeout=60)
        if resp.status_code == 404:
            log.warning("%s (CIK %s) has no XBRL facts", symbol, cik)
            continue
        resp.raise_for_status()

        payload = resp.json()
        payload["_ticker"] = symbol
        out = bronze_path("sec_edgar", "companyfacts", f"{symbol}.json", ingest_date)
        written.append(write_json(out, payload, source_url=url))
        log.info("wrote %s facts for %s -> %s", len(payload.get("facts", {}).get("us-gaap", {})), symbol, out)

    log.info("SEC EDGAR ingestion done: %d companies", len(written))
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", type=date.fromisoformat, default=None)
    run(parser.parse_args().ingest_date)
