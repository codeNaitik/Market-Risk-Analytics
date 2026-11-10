"""Ingest US macroeconomic series from the St. Louis Fed (FRED) API.

API docs: https://fred.stlouisfed.org/docs/api/fred/series_observations.html
Needs a free API key in FRED_API_KEY. FRED encodes missing values as "." which
we keep as-is in bronze and null out in silver.

Usage:
    python -m mra.ingestion.fred
"""

from __future__ import annotations

import argparse
from datetime import date

from mra.config import get_settings
from mra.ingestion.http import build_session
from mra.logging_utils import get_logger
from mra.storage import bronze_path, write_json

log = get_logger(__name__)

OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


def run(ingest_date: date | None = None) -> list[str]:
    s = get_settings()
    if not s.fred_api_key:
        raise RuntimeError("FRED_API_KEY is not set. Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")

    session = build_session()
    written = []
    for series_id, meta in s.raw["fred"]["series"].items():
        params = {
            "series_id": series_id,
            "api_key": s.fred_api_key,
            "file_type": "json",
            "observation_start": s.start_date,
        }
        resp = session.get(OBSERVATIONS_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        payload["_series_id"] = series_id
        payload["_series_meta"] = meta

        out = bronze_path("fred", "observations", f"{series_id}.json", ingest_date)
        # Never persist the API key: log the URL without query params.
        written.append(write_json(out, payload, source_url=f"{OBSERVATIONS_URL}?series_id={series_id}"))
        log.info("FRED %s: %d observations", series_id, len(payload.get("observations", [])))
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", type=date.fromisoformat, default=None)
    run(parser.parse_args().ingest_date)
