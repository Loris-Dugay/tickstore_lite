from pyspark.sql import functions as F, Window, DataFrame, Column
from src.tools.spark_builder import spark_builder
from src.tools.config_loader import load_config
from src.tools.change_time_unit import change_time_unit
from pathlib import Path


def group_by_bar_time(df: DataFrame, window: Column) -> DataFrame:
    window_spec = Window.partitionBy("symbol", window).orderBy("ts")
    ohlc_df = df.withColumn("bar", window) \
        .withColumn("row_num", F.row_number().over(window_spec)) \
        .orderBy("symbol", F.col("bar.start"), "ts", "id") \
        .groupby("symbol", F.col("bar").alias("bar")) \
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("qty").alias("volume"),
            F.sum("quote_qty").alias("quote_volume"),
            F.count("*").alias("count"),
            F.sum(
                F.when(~F.col("is_buyer_maker"), F.col("qty")).otherwise(F.lit(0))
            ).alias("taker_buy_volume"),
            F.sum(
                F.when(~F.col("is_buyer_maker"), F.col("quote_qty")).otherwise(F.lit(0))
            ).alias("taker_buy_quote_volume"),
            (F.sum(F.col("price") * F.col("quote_qty")) / F.sum("quote_qty")).alias("vwap")
        ) \
        .select(
            "symbol",
            F.col("bar.start").alias("bar_time"),
            (F.col("bar.end") - F.expr("INTERVAL 1 MICROSECOND")).alias("close_time"),
            "open",
            "close",
            "high", "low", "volume", "vwap", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume", "count"
        )

    return ohlc_df


def generate_sequence(ohlc_df: DataFrame, timeframe: str) -> DataFrame:
    days = ohlc_df.select("symbol", F.to_date("bar_time").alias("d")).distinct()
    grid = (days
        .withColumn("start", F.expr("CAST(d AS TIMESTAMP)"))
        .withColumn("end",   F.expr("CAST(d AS TIMESTAMP) + INTERVAL 1 DAY - INTERVAL 1 MICROSECOND"))
        .withColumn("bar_time",
                    F.explode(F.sequence("start", "end", F.expr(f"INTERVAL {timeframe}"))))
        .select("symbol", "bar_time"))
    return grid


def generate_last_value(new_ohlc_df: DataFrame, timeframe: str) -> DataFrame:
    window_spec = Window.partitionBy("symbol").orderBy("bar_time")

    last_value_df = new_ohlc_df.select(
        F.col("bar_time"),
        F.last(F.when(~F.col("close").isNull() & ~F.isnan(F.col("close")), F.col("close")), ignorenulls=True)
            .over(window_spec)
            .alias("last_close"),
        F.last(F.when(~F.col("symbol").isNull(), F.col("symbol")), ignorenulls=True)
            .over(window_spec)
            .alias("symbol")
    ).withColumn(
        "last_time",
        F.expr(f"bar_time + interval {timeframe} - interval 1 MICROSECOND")
    )

    corrected_df = new_ohlc_df.join(last_value_df, ["symbol", "bar_time"]) \
        .select(F.coalesce(F.col("symbol"), F.col("symbol")).alias("symbol"),
        F.col("bar_time").alias("open_time"),
        F.coalesce(F.col("open"), F.col("last_close")).alias("open"),
        F.coalesce(F.col("high"), F.col("last_close")).alias("high"),
        F.coalesce(F.col("low"), F.col("last_close")).alias("low"),
        F.coalesce(F.col("close"), F.col("last_close")).alias("close"),
        F.coalesce(F.col("volume"), F.lit(0)).alias("volume"),
        F.coalesce(F.col("close_time"), F.col("last_time")).alias("close_time"),
        F.coalesce(F.col("quote_volume"), F.lit(0)).alias("quote_volume"),
        F.coalesce(F.col("count"), F.lit(0)).alias("count"),
        F.coalesce(F.col("taker_buy_volume"), F.lit(0)).alias("taker_buy_volume"),
        F.coalesce(F.col("taker_buy_quote_volume"), F.lit(0)).alias("taker_buy_quote_volume"),
        F.coalesce(F.col("vwap"), F.lit(0)).alias("vwap")
        )

    return corrected_df


def compute_ohlcv(df: DataFrame, config: dict, time: str) -> None:
    try :
        timeframe = change_time_unit(time)
        ts_col = config["columns"]["timestamp"]
        ohlc_window = F.window(ts_col, timeframe)

        ohlc_df = group_by_bar_time(df, ohlc_window)

        df_with_sequence = generate_sequence(ohlc_df, timeframe)

        new_ohlc_df = df_with_sequence.join(ohlc_df, ["symbol", "bar_time"], "left").orderBy("symbol", "bar_time")

        corrected_df = generate_last_value(new_ohlc_df, timeframe)

        corrected_df.withColumn("bucket", F.to_date(F.col("open_time"))) \
            .repartition(config["spark"]["partitions"], *config["repartition_cols"]) \
            .sortWithinPartitions(*config["sort_cols"]) \
            .write.format(config["spark"]["format"]) \
            .mode(config["spark"]["mode"]) \
            .partitionBy(*config["repartition_cols"]) \
            .save(f"{config['paths']['output']}{timeframe.replace(' ', '_')}")

    except ValueError as e:
        print(f"Error : {e}")


def open_delta(config) -> DataFrame:
    spark = spark_builder()

    cwd = Path.cwd()
    input_path = str(cwd / config["paths"]["input"])

    df = spark.read.format(config["spark"]["format"]).load(input_path)
    print(df.select(
        F.countDistinct("symbol").alias("n_symbols"),
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date")
    ).collect())
    return df


def compute_bars(config, time_bars):
    df = open_delta(config)
    compute_ohlcv(df, config, time_bars)


if __name__ == "__main__":
    config = load_config("compute_bars")
    compute_bars(config, "1m")
