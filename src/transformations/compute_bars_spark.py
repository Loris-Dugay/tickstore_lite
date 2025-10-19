from pyspark.sql import functions as F, Window, DataFrame, Column
from src.tools.spark_builder import spark_builder
from src.tools.config_loader import load_config
from pathlib import Path
import re

def change_time_unit(timeframe: str) -> str | ValueError:
        # valid_divisors = [
        #     1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 16, 20, 24, 30, 32, 36, 40, 48, 60,
        #     64, 72, 80, 96, 120, 160, 180, 192, 240, 288, 320, 360, 480, 576, 720,
        #     960, 1440, 1920, 2880, 4320, 5760, 8640, 14400, 17280, 28800, 43200, 86400
        # ]

        if not isinstance(timeframe, str):
            raise ValueError("L'entrée doit être une chaîne de caractères (ex. '60s', '1m30s').")

        timeframe = timeframe.strip().lower()

        pattern = r'^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$'
        match = re.match(pattern, timeframe)
        if not match:
            raise ValueError("Format invalide. Utilisez un format comme '30s', '5m', '2h', ou '1m30s'.")

        # Extraire les valeurs (heures, minutes, secondes)
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)

        # Convertir tout en secondes pour validation
        total_seconds = hours * 3600 + minutes * 60 + seconds

        # Vérifier les limites (1 seconde à 1 jour = 86 400 secondes)
        if total_seconds < 1:
            raise ValueError("La durée doit être d'au moins 1 seconde.")
        if total_seconds > 86400:
            return "1 day"

        # Vérifier si total_seconds est un diviseur de 86 400
        if 86400 % total_seconds != 0:
            valid_intervals = [
                "1s", "2s", "3s", "4s", "5s", "6s", "8s", "10s", "12s", "15s", "16s",
                "20s", "24s", "30s", "32s", "36s", "40s", "48s", "1m", "2m", "3m", "4m",
                "5m", "6m", "8m", "10m", "12m", "15m", "16m", "20m", "24m", "30m", "1h",
                "2h", "3h", "4h", "6h", "8h", "12h", "24h"
            ]
            raise ValueError(
                f"L'intervalle {timeframe} ({total_seconds} secondes) n'est pas un diviseur de 24 heures (86 400 secondes). "
                f"Utilisez l'un des intervalles suivants : {', '.join(valid_intervals)}."
            )

        # Conversion en format lisible
        if total_seconds >= 3600:
            # Convertir en heures, minutes, secondes
            h = total_seconds // 3600
            remainder = total_seconds % 3600
            m = remainder // 60
            s = remainder % 60
            result = []
            if h > 0:
                result.append(f"{h} hour{'s' if h > 1 else ''}")
            if m > 0:
                result.append(f"{m} minute{'s' if m > 1 else ''}")
            if s > 0:
                result.append(f"{s} second{'s' if s > 1 else ''}")
            return " ".join(result) if result else "0 seconds"
        elif total_seconds >= 60:
            m = total_seconds // 60
            s = total_seconds % 60
            result = []
            if m > 0:
                result.append(f"{m} minute{'s' if m > 1 else ''}")
            if s > 0:
                result.append(f"{s} second{'s' if s > 1 else ''}")
            return " ".join(result) if result else "0 seconds"
        else:
            return f"{total_seconds} second{'s' if total_seconds > 1 else ''}"


def group_by_bar_time(df: DataFrame, window: Column) -> DataFrame:
    ohlc_df = df.groupby("symbol", window.alias("bar")) \
        .agg(
            F.min(F.struct("ts", "price")).alias("open_pair"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max(F.struct("ts", "price")).alias("close_pair"),
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
            F.col("open_pair")["price"].alias("open"),
            F.col("close_pair")["price"].alias("close"),
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


def open_delta(config, time_bars) -> None:
    spark = spark_builder()

    cwd = Path.cwd()
    input_path = str(cwd / config["paths"]["input"])

    df = spark.read.format(config["spark"]["format"]).load(input_path)
    print(df.select(
        F.countDistinct("symbol").alias("n_symbols"),
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date")
    ).collect())
    compute_ohlcv(df, config, time_bars)


if __name__ == "__main__":
    config = load_config("compute_bars")
    open_delta(config, "1m")
