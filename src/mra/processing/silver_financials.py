"""Bronze -> Silver: SEC EDGAR companyfacts -> one tidy row per (ticker, metric, period).

companyfacts is deeply nested with *dynamic keys* (every XBRL concept is a key), which
schema inference handles badly. So we read each file as text and parse it with an
explicit MAP schema, then explode four levels:

    facts{taxonomy} -> concepts{concept} -> units{unit} -> [filing rows]

Business rules applied
  * Concept mapping: several XBRL tags mean the same thing ("revenue" can be
    `Revenues` or `RevenueFromContractWithCustomerExcludingAssessedTax`). settings.yaml
    lists them in priority order; we keep the highest-priority tag per period.
  * Duration filter: 10-Qs also report year-to-date (6/9-month) values. We keep only
    ~3-month (quarter) and ~12-month (annual) durations for flow metrics.
    Balance-sheet metrics are instants (no start date) and are kept as-is.
  * Restatements: the same period can be re-filed later with a new value. We keep the
    latest filing and flag `is_restated` when values differed across filings.

Run:  python -m mra.processing.silver_financials
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.processing.spark_session import get_spark, write_table

log = get_logger(__name__)

FILING_ROW = T.StructType([
    T.StructField("start", T.StringType()), T.StructField("end", T.StringType()),
    T.StructField("val", T.DoubleType()), T.StructField("accn", T.StringType()),
    T.StructField("fy", T.IntegerType()), T.StructField("fp", T.StringType()),
    T.StructField("form", T.StringType()), T.StructField("filed", T.StringType()),
    T.StructField("frame", T.StringType()),
])
CONCEPT = T.StructType([
    T.StructField("label", T.StringType()),
    T.StructField("units", T.MapType(T.StringType(), T.ArrayType(FILING_ROW))),
])
ENVELOPE = T.StructType([
    T.StructField("_ingested_at", T.StringType()),
    T.StructField("data", T.StructType([
        T.StructField("cik", T.LongType()),
        T.StructField("entityName", T.StringType()),
        T.StructField("_ticker", T.StringType()),
        T.StructField("facts", T.MapType(T.StringType(), T.MapType(T.StringType(), CONCEPT))),
    ])),
])


def concept_map(spark, concepts: dict[str, list[str]]) -> DataFrame:
    rows = [(concept, metric, rank) for metric, tags in concepts.items() for rank, concept in enumerate(tags)]
    return spark.createDataFrame(rows, "concept string, metric string, concept_priority int")


def transform(text_df: DataFrame, mapping: DataFrame) -> DataFrame:
    parsed = text_df.select(F.from_json("value", ENVELOPE).alias("j"))

    exploded = (
        parsed.select(F.col("j.data._ticker").alias("ticker"), F.col("j.data.cik").alias("cik"),
                      F.col("j.data.entityName").alias("entity_name"),
                      F.explode("j.data.facts").alias("taxonomy", "concepts"))
        .filter(F.col("taxonomy") == "us-gaap")
        .select("*", F.explode("concepts").alias("concept", "detail")).drop("concepts")
        .join(F.broadcast(mapping), "concept")                       # prune to concepts we need early
        .select("*", F.explode("detail.units").alias("unit", "rows")).drop("detail")
        .select("*", F.explode("rows").alias("r")).drop("rows")
    )

    tidy = exploded.select(
        "ticker", "cik", "entity_name", "metric", "concept", "concept_priority", "unit",
        F.to_date("r.start").alias("period_start"),
        F.to_date("r.end").alias("period_end"),
        F.col("r.val").alias("value"),
        F.col("r.form").alias("form"),
        F.to_date("r.filed").alias("filed_date"),
        F.col("r.accn").alias("accession_number"),
    ).withColumn("duration_days", F.datediff("period_end", "period_start"))

    tidy = (
        tidy.withColumn("period_type", F.when(F.col("period_start").isNull(), "instant")
                        .when(F.col("duration_days").between(80, 100), "quarter")
                        .when(F.col("duration_days").between(350, 380), "annual"))
        .filter(F.col("period_type").isNotNull())                    # drops 6/9-month YTD values
        .filter(F.col("form").isin("10-Q", "10-K", "10-Q/A", "10-K/A"))
    )

    period_key = ["ticker", "metric", "period_type", "period_end"]

    # restatement flag: >1 distinct value reported for the same period + concept
    restated = (tidy.groupBy(*period_key, "concept")
                .agg(F.countDistinct("value").alias("n_values"))
                .select(*period_key, "concept", (F.col("n_values") > 1).alias("is_restated")))

    # keep best concept (priority) then latest filing
    w = Window.partitionBy(*period_key).orderBy("concept_priority", F.col("filed_date").desc(), F.col("accession_number").desc())
    best = (tidy.join(restated, [*period_key, "concept"])
            .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "concept_priority"))

    return best.withColumns({
        "fiscal_year": F.year("period_end"),
        "fiscal_quarter": F.when(F.col("period_type") == "annual", F.lit(None)).otherwise(F.quarter("period_end")),
        "_processed_at": F.current_timestamp(),
    })


def run() -> None:
    s = get_settings()
    spark = get_spark("silver_financials")
    text = spark.read.text(s.path("bronze/sec_edgar/companyfacts/*/*.json"), wholetext=True)
    mapping = concept_map(spark, s.raw["sec_edgar"]["concepts"])

    silver = transform(text, mapping).cache()
    write_table(silver, s.path("silver/financials"), partition_by=["fiscal_year"], table_name="silver.financials")
    log.info("silver.financials: %d rows, %d tickers, %d restated periods",
             silver.count(), silver.select("ticker").distinct().count(), silver.filter("is_restated").count())


if __name__ == "__main__":
    run()
