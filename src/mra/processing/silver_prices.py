"""Bronze -> Silver: daily equity prices.

Steps
  1. Read every bronze parquet file, keeping lineage (source file + ingest_date).
  2. Cast types and standardise names.
  3. Deduplicate on (ticker, trade_date), keeping the most recent ingestion.
  4. Validate rows; failures go to silver/_quarantine/daily_prices with a reason.
  5. Flag corporate actions (splits): raw close jumps while adj_close does not.
  6. Write silver/daily_prices partitioned by trade_year (dynamic overwrite).

Run:  python -m mra.processing.silver_prices
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.processing.spark_session import get_spark, write_table

log = get_logger(__name__)


def rules() -> dict:
    """Rule name -> condition that must be TRUE for a valid row (built lazily, after Spark starts)."""
    return {
        "null_key": F.col("ticker").isNotNull() & F.col("trade_date").isNotNull(),
        "non_positive_price": (F.col("close") > 0) & (F.col("adj_close") > 0) & (F.col("low") > 0),
        "high_below_low": F.col("high") >= F.col("low"),
        "close_outside_range": F.col("close").between(F.col("low") * 0.995, F.col("high") * 1.005),
        "negative_volume": F.col("volume") >= 0,
    }


def transform(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = raw.select(
        F.upper(F.trim("ticker")).alias("ticker"),
        F.to_date("date").alias("trade_date"),
        *[F.col(c).cast("double").alias(c) for c in ["open", "high", "low", "close", "adj_close"]],
        F.col("volume").cast("long").alias("volume"),
        F.col("source"),
        F.to_date(F.regexp_extract("_metadata.file_path", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)).alias("ingest_date"),
        F.col("_metadata.file_path").alias("_source_file"),   # works on Unity Catalog too
    )

    # 3. dedupe: latest ingestion wins (re-downloads can carry vendor corrections)
    w = Window.partitionBy("ticker", "trade_date").orderBy(F.col("ingest_date").desc(), F.col("_source_file").desc())
    deduped = typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # 4. validation: collect every failed rule name into one array column
    failures = F.array_compact(F.array(*[F.when(~cond | cond.isNull(), F.lit(name)) for name, cond in rules().items()]))
    checked = deduped.withColumn("_failed_rules", failures)
    valid = checked.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantine = (checked.filter(F.size("_failed_rules") > 0)
                  .withColumn("quarantine_reason", F.concat_ws(",", "_failed_rules"))
                  .drop("_failed_rules")
                  .withColumn("quarantined_at", F.current_timestamp()))

    # 5. corporate actions: ratio close/adj_close shifts by >25% day over day => split/reverse split
    by_ticker = Window.partitionBy("ticker").orderBy("trade_date")
    ratio = F.col("close") / F.col("adj_close")
    silver = (
        valid.withColumn("_ratio", ratio)
        .withColumn("_ratio_change", F.col("_ratio") / F.lag("_ratio").over(by_ticker))
        .withColumn("is_corporate_action", F.coalesce(F.abs(F.log(F.col("_ratio_change"))) > F.log(F.lit(1.25)), F.lit(False)))
        .withColumn("trade_year", F.year("trade_date"))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_ratio", "_ratio_change")
    )
    return silver, quarantine


def run() -> None:
    s = get_settings()
    spark = get_spark("silver_prices")
    raw = spark.read.parquet(s.path("bronze/market/daily_prices/*/*.parquet"))
    silver, quarantine = transform(raw)
    silver.cache()

    write_table(silver, s.path("silver/daily_prices"), partition_by=["trade_year"], table_name="silver.daily_prices")
    write_table(quarantine, s.path("silver/_quarantine/daily_prices"), table_name="silver.quarantine_daily_prices")

    n_ok, n_bad = silver.count(), quarantine.count()
    n_ca = silver.filter("is_corporate_action").count()
    log.info("silver.daily_prices: %d rows (%d corporate-action days), %d quarantined", n_ok, n_ca, n_bad)


if __name__ == "__main__":
    run()
