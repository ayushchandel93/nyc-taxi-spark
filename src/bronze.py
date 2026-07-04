
# src/bronze.py
# Reads raw NYC taxi parquet files, repartitions, and saves to bronze

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import time

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("NYC Taxi — Bronze Layer") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def run_bronze(
    raw_path: str = "/home/jovyan/data/raw",
    bronze_path: str = "/home/jovyan/data/bronze"
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n══ Bronze Layer ══\n")
    start = time.time()

    # Read all parquet files in raw folder
    df = spark.read.parquet(raw_path)

    print(f"[bronze] Raw rows:    {df.count():,}")
    print(f"[bronze] Raw columns: {len(df.columns)}")
    print(f"[bronze] Schema:")
    df.printSchema()

    # Add ingestion metadata
    df = df.withColumn("ingested_at", F.current_timestamp()) \
           .withColumn("source_file", F.input_file_name())

    # Repartition by pickup month
    df = df.withColumn(
        "pickup_month",
        F.date_format("tpep_pickup_datetime", "yyyy-MM")
    )

    # Write partitioned parquet to bronze
    df.write \
      .mode("overwrite") \
      .partitionBy("pickup_month") \
      .parquet(bronze_path)

    elapsed = round(time.time() - start, 1)
    print(f"\n[bronze] Done in {elapsed}s → {bronze_path}")
    print(f"[bronze] Partitions written:")

    for p in sorted(Path(bronze_path).glob("pickup_month=*")):
        print(f"  {p.name}")

    spark.stop()

if __name__ == "__main__":
    run_bronze()
