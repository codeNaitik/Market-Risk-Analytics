"""SparkSession factory that works locally and on Azure Databricks unchanged.

On Databricks a session already exists and tables are written as Delta.
Locally we start a small session and write Parquet (no extra jars needed),
so `make silver` runs on a laptop with only `pip install pyspark` + Java 17+.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "market-risk-analytics", extra_packages: list[str] | None = None) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")   # idempotent partition reruns
        .config("spark.ui.showConsoleProgress", "false")
    )
    if extra_packages:
        builder = builder.config("spark.jars.packages", ",".join(extra_packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "ERROR"))
    return spark


def table_format() -> str:
    return "delta" if on_databricks() else "parquet"


def write_table(df: DataFrame, path: str, partition_by: list[str] | None = None, mode: str = "overwrite",
                table_name: str | None = None) -> None:
    """Write a lake table. On Databricks it is also registered in Unity Catalog as an
    external Delta table (e.g. `silver.daily_prices`) so dbt and Power BI can query it."""
    writer = df.write.format(table_format()).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if on_databricks() and table_name:
        writer.option("path", path).saveAsTable(table_name)
    else:
        writer.save(path)
