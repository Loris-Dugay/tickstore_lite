from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.tools.spark_builder import spark_builder
from src.tools.config_loader import load_config
from src.tools.change_time_unit import change_time_unit


def _load_source(spark, config: Dict[str, Any]) -> DataFrame | None:
    input_path = (Path.cwd() / config["paths"]["input"]).resolve()
    if not input_path.exists():
        print(f"Input path {input_path} does not exist")
        return None

    df = spark.read.format(config["spark"]["format"]).load(str(input_path))

    filters = config.get("filters", {})
    if filters:
        symbols = filters.get("symbols") or []
        if symbols:
            df = df.filter(F.col("symbol").isin(symbols))

        start_date = filters.get("start_date")
        if start_date:
            df = df.filter(F.col("date") >= F.lit(start_date))

        end_date = filters.get("end_date")
        if end_date:
            df = df.filter(F.col("date") <= F.lit(end_date))

    timestamp_col = config["columns"]["timestamp"]
    required_columns: List[str] = [
        "symbol",
        timestamp_col,
        "price",
        "qty",
        "quote_qty",
        "is_buyer_maker",
        "id",
    ]
    optional_columns = ["date"]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing columns in source data: {', '.join(missing_columns)}"
        )

    select_columns = required_columns + [col for col in optional_columns if col in df.columns]
    return df.select(*select_columns)


def _aggregate_bars(df: DataFrame, timeframe: str, timestamp_col: str) -> DataFrame:
    taker_buy_mask = ~F.col("is_buyer_maker")
    trade_struct = F.struct(F.col(timestamp_col), F.col("id"), F.col("price"))

    aggregated = (
        df.withColumn("bar", F.window(F.col(timestamp_col), timeframe))
        .groupBy("symbol", "bar")
         .agg(
            F.min(trade_struct).alias("first_trade"),
            F.max(trade_struct).alias("last_trade"),
             F.max("price").alias("high"),
             F.min("price").alias("low"),
             F.sum("qty").alias("volume"),
             F.sum("quote_qty").alias("quote_volume"),
            F.sum(F.when(taker_buy_mask, F.col("qty")).otherwise(F.lit(0.0))).alias(
                "taker_buy_volume"
            ),
             F.sum(
                F.when(taker_buy_mask, F.col("quote_qty")).otherwise(F.lit(0.0))
             ).alias("taker_buy_quote_volume"),
            F.count(F.lit(1)).alias("count"),
            F.sum(F.col("price") * F.col("quote_qty")).alias("weighted_price"),
        )
    )

    aggregated = (
        aggregated.withColumn("bar_time", F.col("bar.start"))
        .withColumn("close_time", F.col("bar.end") - F.expr("INTERVAL 1 MICROSECOND"))
        .withColumn("open", F.col("first_trade.price"))
        .withColumn("close", F.col("last_trade.price"))
        .withColumn(
            "vwap",
            F.when(F.col("quote_volume") > 0, F.col("weighted_price") / F.col("quote_volume"))
            .otherwise(F.lit(None))
            .cast("double"),
         )
    )

    return aggregated.select(
        "symbol",
        "bar_time",
        "close_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "quote_volume",
        "taker_buy_volume",
        "taker_buy_quote_volume",
        "count",
    )


def generate_sequence(ohlc_df: DataFrame, timeframe: str) -> DataFrame:
    days = ohlc_df.select("symbol", F.to_date("bar_time").alias("d")).distinct()

    grid = (
        days.withColumn("start", F.expr("CAST(d AS TIMESTAMP)"))
        .withColumn(
            "end", F.expr("CAST(d AS TIMESTAMP) + INTERVAL 1 DAY - INTERVAL 1 MICROSECOND")
    )
    .withColumn(
        "bar_time",
        F.explode(F.sequence("start", "end", F.expr(f"INTERVAL {timeframe}"))),
    )
    .select("symbol", "bar_time")
    )
    return grid


def generate_last_value(new_ohlc_df: DataFrame, timeframe: str) -> DataFrame:
    window_spec = Window.partitionBy("symbol").orderBy("bar_time")


    last_value_df = (
        new_ohlc_df.select(
            F.col("bar_time"),
            F.last(
                F.when(~F.col("close").isNull() & ~F.isnan(F.col("close")), F.col("close")),
                ignorenulls=True,
            ).over(window_spec)
             .alias("last_close"),
            F.last(
                F.when(~F.col("symbol").isNull(), F.col("symbol")), ignorenulls=True
            ).over(window_spec)
            .alias("symbol"),
        )
        .withColumn(
            "last_time",
            F.expr(f"bar_time + interval {timeframe} - interval 1 MICROSECOND"),
        )
     )

    corrected_df = (
        new_ohlc_df.join(last_value_df, ["symbol", "bar_time"])
        .select(
            F.col("symbol"),
            F.col("bar_time").alias("open_time"),
            F.coalesce(F.col("open"), F.col("last_close")).alias("open"),
            F.coalesce(F.col("high"), F.col("last_close")).alias("high"),
            F.coalesce(F.col("low"), F.col("last_close")).alias("low"),
            F.coalesce(F.col("close"), F.col("last_close")).alias("close"),
            F.coalesce(F.col("volume"), F.lit(0.0)).alias("volume"),
            F.coalesce(F.col("close_time"), F.col("last_time")).alias("close_time"),
            F.coalesce(F.col("quote_volume"), F.lit(0.0)).alias("quote_volume"),
            F.coalesce(F.col("count"), F.lit(0)).alias("count"),
            F.coalesce(F.col("taker_buy_volume"), F.lit(0.0)).alias("taker_buy_volume"),
            F.coalesce(F.col("taker_buy_quote_volume"), F.lit(0.0)).alias(
                "taker_buy_quote_volume"
            ),
            F.coalesce(F.col("vwap"), F.lit(0.0)).alias("vwap"),
         )
    )

    return corrected_df


def compute_ohlcv(df: DataFrame, config: Dict[str, Any], timeframe: str) -> DataFrame:
    timestamp_col = config["columns"]["timestamp"]

    ohlc_df = _aggregate_bars(df, timeframe, timestamp_col)

    if config.get("fill_missing", True):
        df_with_sequence = generate_sequence(ohlc_df, timeframe)
        new_ohlc_df = df_with_sequence.join(ohlc_df, ["symbol", "bar_time"], "left")
        corrected_df = generate_last_value(new_ohlc_df, timeframe)
    else:
        corrected_df = ohlc_df.select(
            "symbol",
            F.col("bar_time").alias("open_time"),
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "quote_volume",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "count",
        )

    return corrected_df


def _write_bars(df: DataFrame, config: Dict[str, Any], timeframe: str) -> None:
    partitions = config["spark"].get("partitions")
    partition_cols = config.get("repartition_cols", [])
    sort_cols = config.get("sort_cols", [])
    max_records = config["spark"].get("max_records_per_file")

    writer_df = df
    if partitions:
        if partition_cols:
            writer_df = writer_df.repartition(partitions, *partition_cols)
        else:
            writer_df = writer_df.repartition(partitions)

    if sort_cols:
        writer_df = writer_df.sortWithinPartitions(*sort_cols)

    writer = writer_df.write.format(config["spark"]["format"]).mode(
        config["spark"].get("mode", "append")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    if max_records:
        writer = writer.option("maxRecordsPerFile", max_records)

    output_path = (
        Path.cwd()
        / config["paths"]["output"]
        / timeframe.replace(" ", "_")
    ).resolve()
    writer.save(str(output_path))


def compute_bars(config: Dict[str, Any], time_bars: str) -> None:
    spark = spark_builder()
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        overwrite_mode = config["spark"].get("partition_overwrite_mode")
        if overwrite_mode:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", overwrite_mode)

        source_df = _load_source(spark, config)
        if source_df is None:
            return

        if source_df.rdd.isEmpty():
            print("No data available for the requested filters")
            return

        timeframe = change_time_unit(time_bars)
        bars_df = compute_ohlcv(source_df, config, timeframe)
        result_df = bars_df.withColumn("bucket", F.to_date("open_time"))

        _write_bars(result_df, config, timeframe)
        print(
            f"Bars written to {(Path.cwd() / config['paths']['output'] / timeframe.replace(' ', '_')).resolve()}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    config = load_config("compute_bars")
    compute_bars(config, "1m")
