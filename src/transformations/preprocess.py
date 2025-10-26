import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.tools.spark_builder import spark_builder
from src.tools.config_loader import load_config

logger = logging.getLogger(__name__)

TRADE_SCHEMA = T.StructType([
    T.StructField("id", T.LongType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("qty", T.DoubleType(), True),
    T.StructField("quote_qty", T.DoubleType(), True),
    T.StructField("time", T.LongType(), True),
    T.StructField("is_buyer_maker", T.BooleanType(), True),
])

def _read_raw_trades(spark, input_path: Path) -> DataFrame:
    logger.info(f"Read csv from {input_path}")
    return (
        spark.read
        .schema(TRADE_SCHEMA)
        .option("header", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.csv")
        .option("mode", "DROPMALFORMED")
        .csv(str(input_path))
    )


def _parse_metadata(df: DataFrame) -> DataFrame:
    file_path = F.input_file_name()
    filename = F.element_at(F.split(file_path, r"[\\\\/]+"), -1)
    symbol = F.substring_index(filename, "-trades-", 1)
    trade_date = F.to_date(
        F.regexp_extract(filename, r"-trades-([0-9]{4}-[0-9]{2}-[0-9]{2})", 1))

    market = F.when(F.upper(symbol).contains("_PERP"), F.lit("cm")).otherwise(F.lit("um"))

    return (
        df.withColumn("symbol", symbol)
        .withColumn("date", trade_date)
        .withColumn("ts", F.to_timestamp(F.col("time") / F.lit(1000)))
        .withColumn("market", market)
    )


def _adjust_quote_qty(df: DataFrame, ratio_binance: int) -> DataFrame:
    safe_divisor = F.when(
        F.col("quote_qty") != 0, F.col("quote_qty")
    ).otherwise(F.lit(None).cast("double"))
    adjusted = F.when(
        (F.col("market") == F.lit("cm")) & safe_divisor.isNotNull(),
        F.col("qty") * F.lit(ratio_binance) / safe_divisor,
    ).otherwise(F.col("quote_qty"))

    return df.withColumn("quote_qty", adjusted)


def _filter_valid_rows(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("price") > 0).dropna()
    )


def _repartition_dataset(df: DataFrame, num_partitions: Optional[int], partition_columns: List[str]) -> DataFrame:
    if not partition_columns:
        return df
    if num_partitions:
        return df.repartition(num_partitions, *partition_columns)

    return df


def _write_delta(
    df: DataFrame,
    output_path: Path,
    max_records: Optional[int],
    partition_columns: List[str],
    write_mode: str,
    sort_within_partitions: bool,
) -> None:
    writer_df = df
    if sort_within_partitions and partition_columns:
        sort_columns = list(dict.fromkeys([*partition_columns, "ts"]))
        writer_df = writer_df.sortWithinPartitions(*sort_columns)

    writer = writer_df.write.format("delta").mode(write_mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    if max_records:
        writer = writer.option("maxRecordsPerFile", max_records)
    writer.save(str(output_path))


def preprocess(config: Dict[str, Any]) -> None:
    spark = spark_builder()
    cwd = Path.cwd()
    input_path = (cwd / config["input"]).resolve()
    output_path = (cwd / config["output"]).resolve()

    ratio_binance = config["ratio_binance"]
    num_partitions = config.get("numPartitions")
    max_records = config.get("maxRecordsPerFile")
    partition_columns = config.get("partition_columns", ["symbol", "date"])
    write_mode = config.get("write_mode", "overwrite")
    overwrite_mode = config.get("partition_overwrite_mode")
    sort_within_partitions = config.get("sort_within_partitions", False)

    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        if overwrite_mode:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", overwrite_mode)

        if not input_path.exists():
            logger.error(f"Input path {input_path} does not exist")
            print(f"Input path {input_path} does not exist")
            return

        dataset = _read_raw_trades(spark, input_path)

        if not dataset._jdf.isEmpty():
            dataset = _parse_metadata(dataset)
            dataset = _adjust_quote_qty(dataset, ratio_binance)
            dataset = _filter_valid_rows(dataset)
            dataset = _repartition_dataset(dataset, num_partitions, partition_columns)

            _write_delta(dataset, output_path, max_records, partition_columns, write_mode, sort_within_partitions)
            logger.info(f"Written Delta table to {output_path}")
            print(f"Written Delta table to {output_path}")
        else:
            logger.error(f"No CSV files found under {input_path}")
            print(f"No CSV files found under {input_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    config = load_config("preprocess")
    preprocess(config)
