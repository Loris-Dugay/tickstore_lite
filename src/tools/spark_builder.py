from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def spark_builder() -> SparkSession:
    builder = SparkSession.builder \
        .appName("TickStoreMetrics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptative.enabled", "true") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "6g")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "GMT")
    return spark
