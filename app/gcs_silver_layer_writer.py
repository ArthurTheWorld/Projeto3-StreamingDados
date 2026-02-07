from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
import pyspark.sql.functions as F


spark = (
    SparkSession.builder
    .appName("Projeto-3-Silver-YFinance")
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
    .getOrCreate()
)

# =====================
# GCS AUTH
# =====================
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
hadoop_conf.set(
    "google.cloud.auth.service.account.json.keyfile",
    "/app/.json"
)
hadoop_conf.set(
    "fs.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
)
hadoop_conf.set(
    "fs.AbstractFileSystem.gs.impl",
    "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
)

# =====================
# PATHS
# =====================
BUCKET_NAME = "bucket-portfolio-projeto-3"

BRONZE_PATH = f"gs://{BUCKET_NAME}/bronze/yfinance/"
SILVER_BASE_PATH = f"gs://{BUCKET_NAME}/silver/finance"
DATA_PATH = f"{SILVER_BASE_PATH}/financial_data"
CHECKPOINT_PATH = f"{SILVER_BASE_PATH}/checkpoints/financial_data"

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("change", DoubleType(), True),
    StructField("change_percent", DoubleType(), True),
    StructField("exchange", StringType(), True),
    StructField("timestamp", StringType(), True)
])


df_bronze = (
    spark.readStream
    .schema(schema)
    .json(BRONZE_PATH)
)

df_silver = (
    df_bronze
    .withColumn("event_time", F.to_timestamp("timestamp"))
    .withColumn("ticker_copy", F.col("ticker"))
    .select(
        "ticker",
        "price",
        "change",
        "change_percent",
        "exchange",
        "event_time",
        "ticker_copy"
    )
)

query = (
    df_silver
    .writeStream
    .format("parquet")
    .partitionBy("ticker_copy")
    .option("path", DATA_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

query.awaitTermination()
