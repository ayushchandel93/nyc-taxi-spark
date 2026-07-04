# src/silver.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path
import time

# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("NYC Taxi — Silver Layer") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

# ── Silver ────────────────────────────────────────────────────────────────────

def run_silver(
    bronze_path: str = "/home/jovyan/data/bronze",
    silver_path: str = "/home/jovyan/data/silver"
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n══ Silver Layer ══\n")
    start = time.time()

    # 1. Read bronze — only valid months
    df = spark.read.parquet(bronze_path) \
               .filter(F.col("pickup_month").isin(["2024-01", "2024-02"]))

    raw_count = df.count()
    print(f"[silver] Bronze rows (valid months): {raw_count:,}")

    # 2. Drop nulls on critical columns
    df = df.dropna(subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "PULocationID",
        "DOLocationID"
    ])

    # 3. Filter invalid values
    df = df.filter(
        (F.col("trip_distance")  >  0)    &   # must have moved
        (F.col("trip_distance")  <  200)  &   # no unrealistic distances
        (F.col("fare_amount")    >  0)    &   # no zero/negative fares
        (F.col("fare_amount")    <  1000) &   # no unrealistic fares
        (F.col("passenger_count") > 0)    &   # at least 1 passenger
        (F.col("passenger_count") <= 6)       # max 6 passengers
    )

    # 4. Filter valid timestamp range (2024 only)
    df = df.filter(
        (F.year("tpep_pickup_datetime")  == 2024) &
        (F.year("tpep_dropoff_datetime") == 2024)
    )

    clean_count = df.count()
    dropped     = raw_count - clean_count
    print(f"[silver] Rows after cleaning: {clean_count:,}")
    print(f"[silver] Rows dropped:        {dropped:,} ({round(dropped/raw_count*100, 1)}%)")

    # 5. Add derived columns
    df = df \
        .withColumn(
            "trip_duration_min",
            F.round(
                (F.unix_timestamp("tpep_dropoff_datetime") -
                 F.unix_timestamp("tpep_pickup_datetime")) / 60, 2
            )
        ) \
        .withColumn(
            "speed_mph",
            F.round(
                F.col("trip_distance") /
                (F.col("trip_duration_min") / 60 + F.lit(0.0001)), 2
            )
        ) \
        .withColumn(
            "pickup_hour",
            F.hour("tpep_pickup_datetime")
        ) \
        .withColumn(
            "pickup_dayofweek",
            F.dayofweek("tpep_pickup_datetime")  # 1=Sunday, 7=Saturday
        ) \
        .withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(6,  11), "Morning")
             .when(F.col("pickup_hour").between(12, 16), "Afternoon")
             .when(F.col("pickup_hour").between(17, 20), "Evening")
             .when(F.col("pickup_hour").between(21, 23), "Night")
             .otherwise("Late Night")
        ) \
        .withColumn(
            "is_weekend",
            F.col("pickup_dayofweek").isin([1, 7])  # Sunday=1, Saturday=7
        ) \
        .withColumn(
            "tip_pct",
            F.round(
                F.col("tip_amount") / (F.col("fare_amount") + F.lit(0.0001)) * 100, 2
            )
        )

    # 6. Filter unrealistic derived values
    df = df.filter(
        (F.col("trip_duration_min") > 1)    &  # at least 1 minute
        (F.col("trip_duration_min") < 300)  &  # max 5 hours
        (F.col("speed_mph")         < 100)     # no unrealistic speeds
    )

    final_count = df.count()
    print(f"[silver] Final rows after derived filters: {final_count:,}")

    # 7. Write silver partitioned by pickup_month
    df.write \
      .mode("overwrite") \
      .partitionBy("pickup_month") \
      .parquet(silver_path)

    elapsed = round(time.time() - start, 1)
    print(f"\n[silver] Done in {elapsed}s → {silver_path}")
    print(f"[silver] Partitions written:")
    for p in sorted(Path(silver_path).glob("pickup_month=*")):
        print(f"  {p.name}")

    # 8. Quick summary stats
    print(f"\n── Sample Stats ──")
    df.select(
        F.round(F.mean("trip_distance"),    2).alias("avg_distance_miles"),
        F.round(F.mean("fare_amount"),      2).alias("avg_fare_usd"),
        F.round(F.mean("trip_duration_min"),2).alias("avg_duration_min"),
        F.round(F.mean("speed_mph"),        2).alias("avg_speed_mph"),
        F.round(F.mean("tip_pct"),          2).alias("avg_tip_pct"),
    ).show()

    spark.stop()


if __name__ == "__main__":
    run_silver()