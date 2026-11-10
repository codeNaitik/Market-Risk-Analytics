"""Run bronze -> silver jobs in one Spark session (used by Airflow and `make silver`).

    python -m mra.processing.run_silver                 # all jobs
    python -m mra.processing.run_silver prices macro    # a subset
"""

from __future__ import annotations

import argparse

from mra.processing import silver_financials, silver_macro, silver_prices

JOBS = {"prices": silver_prices.run, "financials": silver_financials.run, "macro": silver_macro.run}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("jobs", nargs="*", help=f"subset of {list(JOBS)}; default all")
    selected = p.parse_args().jobs or list(JOBS)
    unknown = set(selected) - set(JOBS)
    if unknown:
        p.error(f"unknown jobs: {sorted(unknown)}")
    for name in selected:
        JOBS[name]()


if __name__ == "__main__":
    main()
