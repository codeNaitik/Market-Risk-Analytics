"""Kafka -> Spark Structured Streaming -> bronze trades + silver 1-minute OHLCV bars.

Two streaming queries share one Kafka source:
  1. bronze/crypto_trades   every raw trade, append-only (replayable history)
  2. silver/crypto_bars_1m  event-time tumbling windows with a watermark, so late
                            trades (up to `watermark` late) still land in the right bar
                            and Spark can safely finalise and emit each window once.

Checkpoints make both queries exactly-once w.r.t. the file sink: a restart resumes
from the last committed Kafka offsets instead of re-reading or skipping data.

Run (needs Kafka from docker-compose):
    python -m mra.streaming.stream_consumer
On Databricks, the Kafka connector is built in; on Azure Event Hubs use its Kafka
endpoint (port 9093, SASL_SSL) via KAFKA_* env vars.
"""

from __future__ import annotations

import os

import pyspark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from mra.config import get_settings
from mra.logging_utils import get_logger
from mra.processing.spark_session import get_spark, on_databricks, table_format

log = get_logger(__name__)

TRADE = T.StructType([
    T.StructField("type", T.StringType()), T.StructField("trade_id", T.LongType()),
    T.StructField("product_id", T.StringType()), T.StructField("side", T.StringType()),
    T.StructField("price", T.StringType()), T.StructField("size", T.StringType()),
    T.StructField("time", T.StringType()), T.StructField("sequence", T.LongType()),
    T.StructField("_simulated", T.BooleanType()),
])


def kafka_package() -> list[str]:
    if on_databricks():
        return []
    default_scala = "2.12" if pyspark.__version__.startswith("3.") else "2.13"   # Spark 4.x = Scala 2.13
    scala = os.getenv("SPARK_SCALA_VERSION", default_scala)
    return [f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"]


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), TRADE).alias("t"),
        )
        .select(
            "kafka_partition", "kafka_offset",
            F.col("t.trade_id"), F.col("t.product_id"), F.col("t.side"),
            F.col("t.price").cast("decimal(18,8)").alias("price"),
            F.col("t.size").cast("decimal(24,12)").alias("size"),
            F.to_timestamp("t.time").alias("event_time"),
            F.coalesce(F.col("t._simulated"), F.lit(False)).alias("is_simulated"),
        )
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .withColumn("event_date", F.to_date("event_time"))
    )


def one_minute_bars(trades: DataFrame, interval: str, watermark: str) -> DataFrame:
    return (
        trades.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", interval).alias("w"), "product_id")
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.sum("size").alias("volume"),
            F.sum(F.col("price") * F.col("size")).alias("notional"),
            F.count("*").alias("trade_count"),
        )
        .select(
            "product_id",
            F.col("w.start").alias("bar_start"), F.col("w.end").alias("bar_end"),
            "open", "high", "low", "close", "volume", "trade_count",
            (F.col("notional") / F.col("volume")).cast("decimal(18,8)").alias("vwap"),
            F.to_date("w.start").alias("bar_date"),
        )
    )


def run() -> None:
    s = get_settings()
    cfg = s.raw["streaming"]
    spark = get_spark("crypto_stream", extra_packages=kafka_package())

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", s.kafka_bootstrap)
              .option("subscribe", cfg["kafka_topic"])
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", 50_000)
              .option("failOnDataLoss", "false"))
    if os.getenv("KAFKA_SASL_JAAS"):                   # Azure Event Hubs / Confluent Cloud
        reader = (reader.option("kafka.security.protocol", "SASL_SSL")
                  .option("kafka.sasl.mechanism", "PLAIN")
                  .option("kafka.sasl.jaas.config", os.environ["KAFKA_SASL_JAAS"]))

    trades = parse_trades(reader.load())
    fmt = table_format()

    q_raw = (trades.writeStream.format(fmt).outputMode("append")
             .option("checkpointLocation", s.path("_checkpoints/crypto_trades"))
             .partitionBy("event_date")
             .trigger(processingTime="30 seconds")
             .start(s.path("bronze/crypto_trades")))

    q_bars = (one_minute_bars(trades, cfg["bar_interval"], cfg["watermark"])
              .writeStream.format(fmt).outputMode("append")
              .option("checkpointLocation", s.path("_checkpoints/crypto_bars_1m"))
              .partitionBy("bar_date")
              .trigger(processingTime="30 seconds")
              .start(s.path("silver/crypto_bars_1m")))

    log.info("streaming queries started: %s, %s", q_raw.id, q_bars.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
