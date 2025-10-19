import os
from pathlib import Path
from pyspark.sql.functions import col, to_timestamp, lit, when
from src.tools.spark_builder import spark_builder
from src.tools.config_loader import load_config

RATIO_BINANCE = 100


def process_dataframe(input_path, file, ratio_binance, spark):
    print(f"Processing {file}")
    symbol, _, date = file.rpartition("-trades-")
    date = date.split(".")[0]

    df = spark.read.csv(os.path.join(input_path, file), header=True, inferSchema=True)

    df = df.withColumn("symbol", lit(symbol))                               \
           .withColumn("date", lit(date).cast("date"))                      \
           .withColumn("ts", to_timestamp(col("time") / 1000))              \
           .withColumn("market", lit("cm" if "_PERP" in file else "um"))    \
           .withColumn("quote_qty",
               when(
                   col("market") == lit("cm"),
                   col("qty") * lit(ratio_binance) / col("quote_qty")
               ).otherwise(col("quote_qty")))
    df = df.filter(col("price") > 0).dropna()
    return df


def write_processed_file(dfs, numPartitions, maxRecordsPerFile, output_path):
    union_df = dfs[0]
    for df in dfs[1:]:
        union_df = union_df.unionByName(df, allowMissingColumns=True)
    union_df = union_df.repartition(numPartitions, "symbol", "date")

    union_df.write.format("delta").mode("overwrite")    \
        .partitionBy("symbol", "date")                  \
        .option("maxRecordsPerFile", maxRecordsPerFile) \
        .save(output_path)

    union_df.unpersist()
    print(f"Written unified Delta table to {output_path}")


def preprocess(input: str, output: str, ratio_binance: int, numPartitions: int, maxRecordsPerFile: int) -> None:
    spark = spark_builder()
    cwd = Path.cwd()
    input_path = cwd / input
    output_path = cwd / output
    try:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
        dfs = []
        for file in files:
            # print(f"Processing {file}")
            # symbol, _, date = file.rpartition("-trades-")
            # date = date.split(".")[0]

            # df = spark.read.csv(os.path.join(input_path, file), header=True, inferSchema=True)

            # df = df.withColumn("symbol", lit(symbol))                               \
            #        .withColumn("date", lit(date).cast("date"))                      \
            #        .withColumn("ts", to_timestamp(col("time") / 1000))              \
            #        .withColumn("market", lit("cm" if "_PERP" in file else "um"))    \
            #        .withColumn("quote_qty",
            #            when(
            #                col("market") == lit("cm"),
            #                col("qty") * lit(RATIO_BINANCE) / col("quote_qty")
            #            ).otherwise(col("quote_qty")))
            # df = df.filter(col("price") > 0).dropna()
            df = process_dataframe(input_path, file, ratio_binance, spark)
            dfs.append(df)

        if dfs:
            # union_df = dfs[0]
            # for df in dfs[1:]:
            #     union_df = union_df.unionByName(df, allowMissingColumns=True)
            # union_df = union_df.repartition(200, "symbol", "date")

            # union_df.write.format("delta").mode("overwrite")    \
            #     .partitionBy("symbol", "date")                  \
            #     .option("maxRecordsPerFile", 100000)            \
            #     .save(output_path)

            # union_df.unpersist()
            # print(f"Written unified Delta table to {output_path}")
            write_processed_file(dfs, numPartitions, maxRecordsPerFile, str(output_path))

        print("Running OPTIMIZE on unified lake...")
        spark.sql(f"OPTIMIZE delta.`{output_path}` ZORDER BY (time)")
        spark.sql(f"VACUUM delta.`{output_path}` RETAIN 0 HOURS")
        print("OPTIMIZE done.")
    finally:
        spark.stop()


if __name__ == "__main__":
    config = load_config("preprocess")

    input = config["input"]
    output = config["output"]
    ratio_binance = config["ratio_binance"]
    numPartitions = config["numPartitions"]
    maxRecordsPerFile = config["maxRecordsPerFile"]

    preprocess(input, output, ratio_binance, numPartitions, maxRecordsPerFile)
