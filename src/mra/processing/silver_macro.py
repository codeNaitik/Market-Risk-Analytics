"""Bronze -> Silver: FRED + Bank of Canada series -> one long, typed table.

Both APIs return different shapes; silver unifies them into
    (source, series_id, series_name, unit, frequency, obs_date, value)

  FRED:      observations: [{"date": "2024-01-01", "value": "5.33"}]      ("." = missing)
  BoC Valet: observations: [{"d": "2024-01-02", "FXUSDCAD": {"v": "1.3316"}}]

The BoC series value sits under a key that *is* the series id, so we parse each
observation as MAP<string,string> (nested objects stay as raw JSON text) and pull
`$.v` out with get_json_object.

Run:  python -m mra.processing.silver_macro
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.processing.spark_session import get_spark, write_table

log = get_logger(__name__)

META = T.StructType([T.StructField(n, T.StringType()) for n in ("name", "unit", "frequency")])
ENVELOPE = T.StructType([
    T.StructField("data", T.StructType([
        T.StructField("_series_id", T.StringType()),
        T.StructField("_series_meta", META),
        T.StructField("observations", T.ArrayType(T.MapType(T.StringType(), T.StringType()))),
    ])),
])


def transform(text_df: DataFrame, source: str) -> DataFrame:
    j = text_df.select(
        F.from_json("value", ENVELOPE).alias("j"),
        F.regexp_extract("_metadata.file_path", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1).alias("ingest_date"),
    )
    obs = j.select(
        F.col("j.data._series_id").alias("series_id"),
        F.col("j.data._series_meta.name").alias("series_name"),
        F.col("j.data._series_meta.unit").alias("unit"),
        F.col("j.data._series_meta.frequency").alias("frequency"),
        F.to_date("ingest_date").alias("ingest_date"),
        F.explode("j.data.observations").alias("o"),
    )
    if source == "fred":
        date_col, raw_val = F.col("o")["date"], F.col("o")["value"]
    else:  # boc_valet
        date_col, raw_val = F.col("o")["d"], F.get_json_object(F.col("o")[F.col("series_id")], "$.v")

    out = obs.select(
        F.lit(source).alias("source"), "series_id", "series_name", "unit", "frequency", "ingest_date",
        F.to_date(date_col).alias("obs_date"),
        F.when(raw_val.isin(".", ""), None).otherwise(raw_val).cast("double").alias("value"),
    ).filter(F.col("obs_date").isNotNull())

    w = Window.partitionBy("series_id", "obs_date").orderBy(F.col("ingest_date").desc())
    return out.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def run() -> None:
    s = get_settings()
    spark = get_spark("silver_macro")
    frames = [
        transform(spark.read.text(s.path(f"bronze/{src}/observations/*/*.json"), wholetext=True), src)
        for src in ("fred", "boc_valet")
    ]
    silver = frames[0].unionByName(frames[1]).withColumn("_processed_at", F.current_timestamp()).cache()
    write_table(silver, s.path("silver/macro_observations"), partition_by=["source"], table_name="silver.macro_observations")

    stats = silver.groupBy("source").agg(F.count("*").alias("rows"), F.sum(F.col("value").isNull().cast("int")).alias("missing"))
    for r in stats.collect():
        log.info("silver.macro_observations[%s]: %d rows, %d missing values", r.source, r.rows, r.missing)


if __name__ == "__main__":
    run()
