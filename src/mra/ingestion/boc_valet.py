"""Ingest Canadian rates and FX from the Bank of Canada Valet API (no key needed).

API docs: https://www.bankofcanada.ca/valet/docs
Response shape:
    {"seriesDetail": {...},
     "observations": [{"d": "2024-01-02", "FXUSDCAD": {"v": "1.3316"}}, ...]}

Usage:
    python -m mra.ingestion.boc_valet
"""

from __future__ import annotations

import argparse
from datetime import date

from mra.config import get_settings
from mra.ingestion.http import build_session
from mra.logging_utils import get_logger
from mra.storage import bronze_path, write_json

log = get_logger(__name__)

VALET_URL = "https://www.bankofcanada.ca/valet/observations/{series}/json"


def run(ingest_date: date | None = None) -> list[str]:
    s = get_settings()
    session = build_session()
    written = []
    for series_id, meta in s.raw["boc_valet"]["series"].items():
        url = VALET_URL.format(series=series_id)
        resp = session.get(url, params={"start_date": s.start_date}, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        payload["_series_id"] = series_id
        payload["_series_meta"] = meta

        out = bronze_path("boc_valet", "observations", f"{series_id}.json", ingest_date)
        written.append(write_json(out, payload, source_url=url))
        log.info("BoC %s: %d observations", series_id, len(payload.get("observations", [])))
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-date", type=date.fromisoformat, default=None)
    run(parser.parse_args().ingest_date)
