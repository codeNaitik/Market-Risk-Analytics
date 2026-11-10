"""Write dbt seed CSVs from config/settings.yaml (single source of truth).

    python -m mra.export_seeds
"""

from __future__ import annotations

import csv

from mra.config import REPO_ROOT, get_settings

SEEDS = REPO_ROOT / "dbt" / "seeds"


def _write(name: str, header: list[str], rows: list[list]) -> None:
    SEEDS.mkdir(parents=True, exist_ok=True)
    with open(SEEDS / name, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def run() -> None:
    s = get_settings()
    _write("company_universe.csv", ["ticker", "sector", "exchange", "country", "currency", "is_benchmark"],
           [[t["ticker"], t["sector"], t["exchange"],
             "CA" if t["exchange"] == "TSX" else "US",
             "CAD" if t["exchange"] == "TSX" else "USD", False] for t in s.tickers]
           + [[s.benchmark, "Index", "INDEX", "US", "USD", True]])

    p = s.portfolio
    _write("portfolio_weights.csv", ["portfolio_name", "ticker", "weight", "notional_cad"],
           [[p["name"], t, w, p["notional_cad"]] for t, w in p["weights"].items()])

    rows = [[sid, "fred", m["name"], m["unit"], m["frequency"]] for sid, m in s.raw["fred"]["series"].items()]
    rows += [[sid, "boc_valet", m["name"], m["unit"], m["frequency"]] for sid, m in s.raw["boc_valet"]["series"].items()]
    _write("macro_series.csv", ["series_id", "source", "series_name", "unit", "frequency"], rows)
    print(f"seeds written to {SEEDS}")


if __name__ == "__main__":
    run()
