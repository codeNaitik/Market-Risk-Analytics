"""Export gold tables to Parquet for Power BI Desktop (local mode).

Power BI connects natively to Parquet (Get Data -> Parquet) or to a folder of files.
In the cloud you skip this: Power BI connects straight to the Databricks SQL Warehouse.

    python -m mra.export_gold          -> lake/gold_export/<table>.parquet
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from mra.config import get_settings
from mra.logging_utils import get_logger

log = get_logger(__name__)


def run() -> None:
    s = get_settings()
    out = Path(s.path("gold_export"))
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(s.path("warehouse.duckdb"), read_only=True)
    tables = [r[0] for r in con.sql(
        "select table_name from information_schema.tables where table_schema = 'gold' order by 1").fetchall()]
    for t in tables:
        con.sql(f"copy gold.{t} to '{out / t}.parquet' (format parquet)")
    log.info("exported %d gold tables to %s", len(tables), out)


if __name__ == "__main__":
    run()
