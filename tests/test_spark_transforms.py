"""Spark transformation tests on tiny hand-built inputs (run with: pytest -m spark)."""

import json
from datetime import date, datetime

import pytest

pytestmark = pytest.mark.spark


def _prices_df(spark, rows):
    cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume", "source"]
    df = spark.createDataFrame(rows, cols)
    # emulate the file metadata column that real parquet reads provide
    from pyspark.sql import functions as F
    return df.withColumn("_metadata", F.struct(F.lit("/lake/bronze/market/daily_prices/ingest_date=2026-01-05/x.parquet").alias("file_path")))


def test_prices_dedupes_quarantines_and_flags_splits(spark):
    from mra.processing.silver_prices import transform

    d = lambda day: datetime(2026, 1, day)  # noqa: E731
    rows = [
        ("aapl", d(1), 10.0, 11.0, 9.0, 10.0, 10.0, 100, "t"),
        ("aapl", d(1), 10.0, 11.0, 9.0, 10.0, 10.0, 100, "t"),     # duplicate
        ("aapl", d(2), 10.0, 11.0, 9.0, -1.0, -1.0, 100, "t"),     # bad price
        ("nvda", d(1), 400.0, 404.0, 396.0, 400.0, 100.0, 5, "t"),
        ("nvda", d(2), 100.0, 101.0, 99.0, 100.0, 100.0, 5, "t"),  # 4:1 split: close/adj ratio 4 -> 1
    ]
    silver, quarantine = transform(_prices_df(spark, rows))
    assert silver.count() == 3
    assert quarantine.count() == 1
    assert "non_positive_price" in quarantine.first()["quarantine_reason"]
    splits = silver.filter("is_corporate_action").collect()
    assert [(r.ticker, r.trade_date) for r in splits] == [("NVDA", date(2026, 1, 2))]


def test_financials_parsing_filters_ytd_and_handles_restatements(spark):
    from mra.processing.silver_financials import concept_map, transform

    def row(start, end, val, filed, form="10-Q"):
        return {"start": start, "end": end, "val": val, "accn": f"a-{filed}", "fy": 2025, "fp": "Q2",
                "form": form, "filed": filed, "frame": None}

    doc = {"data": {"cik": 1, "entityName": "Test Co", "_ticker": "TST", "facts": {"us-gaap": {
        "Revenues": {"label": "Revenues", "units": {"USD": [
            row("2025-04-01", "2025-06-30", 100.0, "2025-08-01"),
            row("2025-01-01", "2025-06-30", 190.0, "2025-08-01"),            # 6-month YTD: dropped
            row("2025-04-01", "2025-06-30", 105.0, "2026-02-01", "10-Q/A"),  # restated later: wins
        ]}},
        "SalesRevenueNet": {"label": "old tag", "units": {"USD": [row("2025-04-01", "2025-06-30", 999.0, "2025-08-01")]}},
        "Assets": {"label": "Assets", "units": {"USD": [{**row(None, "2025-06-30", 5000.0, "2025-08-01"), "start": None}]}},
        "SomethingIrrelevant": {"label": "x", "units": {"USD": [row("2025-04-01", "2025-06-30", 1.0, "2025-08-01")]}},
    }}}}
    text = spark.createDataFrame([(json.dumps(doc),)], ["value"])
    mapping = concept_map(spark, {"revenue": ["Revenues", "SalesRevenueNet"], "total_assets": ["Assets"]})
    out = {(r.metric, r.period_type): r for r in transform(text, mapping).collect()}

    assert set(out) == {("revenue", "quarter"), ("total_assets", "instant")}
    rev = out[("revenue", "quarter")]
    assert rev.value == 105.0 and rev.is_restated and rev.concept == "Revenues"   # priority tag beats 999
    assert out[("total_assets", "instant")].value == 5000.0


def test_boc_valet_parsing(spark):
    from pyspark.sql import functions as F

    from mra.processing.silver_macro import transform

    doc = {"data": {"_series_id": "FXUSDCAD", "_series_meta": {"name": "USD/CAD", "unit": "rate", "frequency": "daily"},
                    "observations": [{"d": "2026-01-02", "FXUSDCAD": {"v": "1.3716"}},
                                     {"d": "2026-01-05", "FXUSDCAD": {"v": ""}}]}}
    text = (spark.createDataFrame([(json.dumps(doc),)], ["value"])
            .withColumn("_metadata", F.struct(F.lit("/b/boc_valet/observations/ingest_date=2026-01-06/f.json").alias("file_path"))))
    rows = {r.obs_date: r.value for r in transform(text, "boc_valet").collect()}
    assert rows == {date(2026, 1, 2): 1.3716, date(2026, 1, 5): None}


def test_stream_parsing_and_one_minute_bars(spark):
    from mra.streaming.stream_consumer import one_minute_bars, parse_trades

    def msg(price, size, ts):
        return json.dumps({"type": "match", "trade_id": 1, "product_id": "BTC-USD", "side": "buy",
                           "price": str(price), "size": str(size), "time": ts, "sequence": 1})

    kafka_like = spark.createDataFrame([
        (b"BTC-USD", bytearray(msg(100, 1, "2026-01-01T10:00:05Z").encode()), 0, 1),
        (b"BTC-USD", bytearray(msg(110, 1, "2026-01-01T10:00:30Z").encode()), 0, 2),
        (b"BTC-USD", bytearray(msg(90, 2, "2026-01-01T10:00:50Z").encode()), 0, 3),
        (b"BTC-USD", bytearray(msg(95, 1, "2026-01-01T10:01:10Z").encode()), 0, 4),
    ], "key binary, value binary, partition int, offset long")

    bars = {r.bar_start.minute: r for r in one_minute_bars(parse_trades(kafka_like), "1 minute", "2 minutes").collect()}
    b = bars[0]
    assert (float(b.open), float(b.high), float(b.low), float(b.close)) == (100, 110, 90, 90)
    assert float(b.volume) == 4 and b.trade_count == 3
    assert float(b.vwap) == pytest.approx((100 + 110 + 180) / 4)
    assert bars[1].trade_count == 1
